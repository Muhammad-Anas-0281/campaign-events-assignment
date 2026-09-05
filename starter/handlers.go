package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	db *DB
}

func NewServer(db *DB) *Server {
	return &Server{db: db}
}

// handlePostEvents ingests a JSON array of events from webhook providers.
// It decodes items individually to ensure malformed elements do not fail valid items.
func (s *Server) handlePostEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024)) // 10MB limit
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read request body"})
		return
	}
	defer r.Body.Close()

	if len(strings.TrimSpace(string(body))) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "empty request body"})
		return
	}

	var rawItems []json.RawMessage
	if err := json.Unmarshal(body, &rawItems); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "request body must be a valid JSON array of events",
		})
		return
	}

	var validEvents []StoredEvent
	var ingestErrors []IngestError

	for i, raw := range rawItems {
		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil {
			ingestErrors = append(ingestErrors, IngestError{
				Index:  i,
				Reason: fmt.Sprintf("malformed json object: %v", err),
			})
			continue
		}

		stored, err := validateAndNormalizeEvent(ev)
		if err != nil {
			ingestErrors = append(ingestErrors, IngestError{
				Index:   i,
				EventID: ev.EventID,
				Reason:  err.Error(),
			})
			continue
		}

		validEvents = append(validEvents, *stored)
	}

	accepted, duplicates, err := s.db.IngestBatch(validEvents)
	if err != nil {
		log.Printf("error ingesting batch: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "database insertion failed"})
		return
	}

	resp := BatchIngestResponse{
		Status:     "success",
		Total:      len(rawItems),
		Accepted:   accepted,
		Duplicates: duplicates,
		Rejected:   len(ingestErrors),
		Errors:     ingestErrors,
	}

	httpStatus := http.StatusOK
	if len(ingestErrors) > 0 {
		resp.Status = "partial_success"
		// 207 Multi-Status or 200 OK signals accepted batch with item-level reports
		httpStatus = http.StatusOK
	}

	writeJSON(w, httpStatus, resp)
}

// handleGetStats returns aggregated statistics for the specified campaign.
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	campaignID := strings.TrimSpace(r.PathValue("campaignID"))
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing campaignID in path"})
		return
	}

	stats, err := s.db.GetCampaignStats(campaignID)
	if err != nil {
		log.Printf("error fetching stats for %s: %v", campaignID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to query statistics"})
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// handleGetEvents provides the optional paginated event listing for a campaign.
func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	campaignID := strings.TrimSpace(r.PathValue("campaignID"))
	if campaignID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "missing campaignID in path"})
		return
	}

	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	offset := 0
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	eventsResp, err := s.db.GetCampaignEvents(campaignID, limit, offset)
	if err != nil {
		log.Printf("error fetching events for %s: %v", campaignID, err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to query events"})
		return
	}

	writeJSON(w, http.StatusOK, eventsResp)
}

func validateAndNormalizeEvent(ev Event) (*StoredEvent, error) {
	evID := strings.TrimSpace(ev.EventID)
	if evID == "" {
		return nil, fmt.Errorf("missing or empty 'event_id'")
	}

	campID := strings.TrimSpace(ev.CampaignID)
	if campID == "" {
		return nil, fmt.Errorf("missing or empty 'campaign_id'")
	}

	ctID := strings.TrimSpace(ev.ContactID)
	if ctID == "" {
		return nil, fmt.Errorf("missing or empty 'contact_id'")
	}

	normType := strings.ToLower(strings.TrimSpace(ev.Type))
	switch normType {
	case TypeSent, TypeDelivered, TypeOpened, TypeClicked:
		// valid
	default:
		return nil, fmt.Errorf("unsupported or unknown event type %q (allowed: sent, delivered, opened, clicked)", ev.Type)
	}

	tsStr := strings.TrimSpace(ev.Timestamp)
	if tsStr == "" {
		return nil, fmt.Errorf("missing or empty 'timestamp'")
	}

	parsedTime, err := time.Parse(time.RFC3339, tsStr)
	if err != nil {
		// Also try RFC3339Nano in case provider includes fractional seconds
		parsedTime, err = time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp %q, expected RFC3339 format", ev.Timestamp)
		}
	}

	return &StoredEvent{
		EventID:    evID,
		CampaignID: campID,
		ContactID:  ctID,
		Type:       normType,
		Timestamp:  parsedTime.UTC(),
		Metadata:   ev.Metadata,
	}, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("writeJSON: %v", err)
	}
}

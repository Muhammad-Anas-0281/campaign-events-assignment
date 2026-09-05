package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func setupTestServer(t *testing.T) (*Server, func()) {
	t.Helper()
	// Use temporary SQLite file for real SQLite behavior (WAL mode, file locking)
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	db, err := NewDB(dbPath)
	if err != nil {
		t.Fatalf("failed to init test db: %v", err)
	}

	server := NewServer(db)
	cleanup := func() {
		db.Close()
	}

	return server, cleanup
}

// TestSingleAndDuplicateIngest verifies that duplicate events are safely ignored and do not increase stats.
func TestSingleAndDuplicateIngest(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	payload := `[
		{"event_id":"evt_1","campaign_id":"cmp_test","contact_id":"ct_01","type":"sent","timestamp":"2026-08-10T10:00:00Z"},
		{"event_id":"evt_2","campaign_id":"cmp_test","contact_id":"ct_01","type":"delivered","timestamp":"2026-08-10T10:01:00Z"}
	]`

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()
	server.handlePostEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp BatchIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if resp.Accepted != 2 || resp.Duplicates != 0 || resp.Rejected != 0 {
		t.Errorf("expected 2 accepted, 0 dupes; got accepted=%d, dupes=%d", resp.Accepted, resp.Duplicates)
	}

	// Post the exact same payload again (simulate provider retry)
	req2 := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload))
	rec2 := httptest.NewRecorder()
	server.handlePostEvents(rec2, req2)

	var resp2 BatchIngestResponse
	json.Unmarshal(rec2.Body.Bytes(), &resp2)
	if resp2.Accepted != 0 || resp2.Duplicates != 2 {
		t.Errorf("expected 0 accepted, 2 dupes on retry; got accepted=%d, dupes=%d", resp2.Accepted, resp2.Duplicates)
	}

	// Verify stats show counts of 1 each, not 2
	statsReq := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_test/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_test")
	statsRec := httptest.NewRecorder()
	server.handleGetStats(statsRec, statsReq)

	var stats CampaignStats
	json.Unmarshal(statsRec.Body.Bytes(), &stats)
	if stats.Sent != 1 || stats.Delivered != 1 {
		t.Errorf("expected sent=1 delivered=1; got sent=%d delivered=%d", stats.Sent, stats.Delivered)
	}
}

// TestPartialBatchAcceptance verifies that 1 bad event does not fail valid events in the batch.
func TestPartialBatchAcceptance(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	payload := `[
		{"event_id":"evt_good1","campaign_id":"cmp_partial","contact_id":"ct_01","type":"sent","timestamp":"2026-08-10T10:00:00Z"},
		{"event_id":"evt_bad_time","campaign_id":"cmp_partial","contact_id":"ct_02","type":"delivered","timestamp":"not-a-time"},
		{"event_id":"","campaign_id":"cmp_partial","contact_id":"ct_03","type":"opened","timestamp":"2026-08-10T10:02:00Z"},
		{"event_id":"evt_good2","campaign_id":"cmp_partial","contact_id":"ct_04","type":"OPENED","timestamp":"2026-08-10T10:03:00Z"},
		{"event_id":"evt_unknown_type","campaign_id":"cmp_partial","contact_id":"ct_05","type":"spam_report","timestamp":"2026-08-10T10:04:00Z"}
	]`

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload))
	rec := httptest.NewRecorder()
	server.handlePostEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp BatchIngestResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)

	if resp.Accepted != 2 {
		t.Errorf("expected 2 accepted (evt_good1, evt_good2), got %d", resp.Accepted)
	}
	if resp.Rejected != 3 {
		t.Errorf("expected 3 rejected (bad time, empty id, unknown type), got %d", resp.Rejected)
	}
}

// TestOutOfOrderDelivery verifies that opened arriving before delivered is correctly stored and reported.
func TestOutOfOrderDelivery(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// Opened arrives first
	payload1 := `[
		{"event_id":"evt_open","campaign_id":"cmp_ooo","contact_id":"ct_01","type":"opened","timestamp":"2026-08-10T11:00:00Z"}
	]`
	req1 := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload1))
	rec1 := httptest.NewRecorder()
	server.handlePostEvents(rec1, req1)

	// Delivered arrives later
	payload2 := `[
		{"event_id":"evt_deliv","campaign_id":"cmp_ooo","contact_id":"ct_01","type":"delivered","timestamp":"2026-08-10T10:55:00Z"}
	]`
	req2 := httptest.NewRequest(http.MethodPost, "/events", bytes.NewBufferString(payload2))
	rec2 := httptest.NewRecorder()
	server.handlePostEvents(rec2, req2)

	statsReq := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_ooo/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_ooo")
	statsRec := httptest.NewRecorder()
	server.handleGetStats(statsRec, statsReq)

	var stats CampaignStats
	json.Unmarshal(statsRec.Body.Bytes(), &stats)
	if stats.Opened != 1 || stats.Delivered != 1 {
		t.Errorf("expected opened=1 delivered=1; got opened=%d delivered=%d", stats.Opened, stats.Delivered)
	}
}

// TestConcurrentIngestion verifies that concurrent requests do not corrupt SQLite or trigger locking crashes.
func TestConcurrentIngestion(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	var wg sync.WaitGroup
	goroutines := 10
	eventsPerRoutine := 20

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()
			var events []Event
			for i := 0; i < eventsPerRoutine; i++ {
				events = append(events, Event{
					EventID:    jsonNumber(routineID*1000 + i),
					CampaignID: "cmp_concurrent",
					ContactID:  "ct_concurrent",
					Type:       "delivered",
					Timestamp:  "2026-08-10T12:00:00Z",
				})
			}
			data, _ := json.Marshal(events)
			req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(data))
			rec := httptest.NewRecorder()
			server.handlePostEvents(rec, req)
			if rec.Code != http.StatusOK {
				t.Errorf("routine %d failed: code %d, body %s", routineID, rec.Code, rec.Body.String())
			}
		}(g)
	}

	wg.Wait()

	statsReq := httptest.NewRequest(http.MethodGet, "/campaigns/cmp_concurrent/stats", nil)
	statsReq.SetPathValue("campaignID", "cmp_concurrent")
	statsRec := httptest.NewRecorder()
	server.handleGetStats(statsRec, statsReq)

	var stats CampaignStats
	json.Unmarshal(statsRec.Body.Bytes(), &stats)
	expectedTotal := goroutines * eventsPerRoutine
	if stats.Delivered != expectedTotal {
		t.Errorf("expected %d delivered, got %d", expectedTotal, stats.Delivered)
	}
}

// TestSeedEventsSurvives verifies that the service survives the real-world seed/events.json dataset.
func TestSeedEventsSurvives(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	seedPath := filepath.Join("seed", "events.json")
	data, err := os.ReadFile(seedPath)
	if err != nil {
		t.Fatalf("failed to read seed events.json: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(data))
	rec := httptest.NewRecorder()
	server.handlePostEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp BatchIngestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	t.Logf("Seed Ingestion Summary: Total=%d, Accepted=%d, Duplicates=%d, Rejected=%d",
		resp.Total, resp.Accepted, resp.Duplicates, resp.Rejected)

	if resp.Total != 235 {
		t.Errorf("expected 235 total events in seed, got %d", resp.Total)
	}
	if resp.Accepted <= 0 {
		t.Errorf("expected positive accepted count, got %d", resp.Accepted)
	}
	if resp.Rejected <= 0 {
		t.Errorf("expected seed to have rejected invalid records (e.g. not-a-time), got %d", resp.Rejected)
	}
}

func jsonNumber(i int) string {
	b, _ := json.Marshal(i)
	return "evt_" + string(b)
}

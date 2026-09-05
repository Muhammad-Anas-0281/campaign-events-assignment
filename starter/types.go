package main

import "time"

// Supported event types
const (
	TypeSent      = "sent"
	TypeDelivered = "delivered"
	TypeOpened    = "opened"
	TypeClicked   = "clicked"
)

// Event is one webhook event from a message provider.
type Event struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"` // sent | delivered | opened | clicked
	Timestamp  string            `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// StoredEvent represents an event persisted in SQLite with parsed time.
type StoredEvent struct {
	EventID    string            `json:"event_id"`
	CampaignID string            `json:"campaign_id"`
	ContactID  string            `json:"contact_id"`
	Type       string            `json:"type"`
	Timestamp  time.Time         `json:"timestamp"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// CampaignStats is the response payload for GET /campaigns/{campaign_id}/stats
type CampaignStats struct {
	CampaignID       string  `json:"campaign_id"`
	Sent             int     `json:"sent"`
	Delivered        int     `json:"delivered"`
	Opened           int     `json:"opened"`
	Clicked          int     `json:"clicked"`
	DeliveryRate     float64 `json:"delivery_rate"`      // delivered / sent
	OpenRate         float64 `json:"open_rate"`          // opened / delivered
	ClickThroughRate float64 `json:"click_through_rate"` // clicked / opened
}

// IngestError represents validation or decoding failure for an individual event in a batch.
type IngestError struct {
	Index   int    `json:"index"`
	EventID string `json:"event_id,omitempty"`
	Reason  string `json:"reason"`
}

// BatchIngestResponse is the structured report returned by POST /events.
type BatchIngestResponse struct {
	Status     string        `json:"status"` // "success" or "partial_success"
	Total      int           `json:"total"`
	Accepted   int           `json:"accepted"`
	Duplicates int           `json:"duplicates"`
	Rejected   int           `json:"rejected"`
	Errors     []IngestError `json:"errors,omitempty"`
}

// PaginatedEventsResponse represents a paginated event listing for a campaign.
type PaginatedEventsResponse struct {
	CampaignID string  `json:"campaign_id"`
	Total      int     `json:"total"`
	Limit      int     `json:"limit"`
	Offset     int     `json:"offset"`
	Events     []Event `json:"events"`
}

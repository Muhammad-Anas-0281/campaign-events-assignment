package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// DB wraps the SQLite database handle and provides high-level queries.
type DB struct {
	db      *sql.DB
	writeMu sync.Mutex
}

// NewDB initializes the SQLite database with WAL mode and creates required tables and indexes.
func NewDB(dbPath string) (*DB, error) {
	conn, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	// Optimize SQLite for concurrent webhooks and crash safety
	pragmas := []string{
		"PRAGMA journal_mode = WAL;",
		"PRAGMA busy_timeout = 5000;",
		"PRAGMA synchronous = NORMAL;",
	}
	for _, p := range pragmas {
		if _, err := conn.Exec(p); err != nil {
			conn.Close()
			return nil, fmt.Errorf("exec %q: %w", p, err)
		}
	}

	schema := `
	CREATE TABLE IF NOT EXISTS events (
		event_id TEXT PRIMARY KEY,
		campaign_id TEXT NOT NULL,
		contact_id TEXT NOT NULL,
		type TEXT NOT NULL,
		timestamp TEXT NOT NULL,
		metadata TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_events_campaign_type ON events(campaign_id, type);
	CREATE INDEX IF NOT EXISTS idx_events_campaign_time ON events(campaign_id, timestamp);
	`
	if _, err := conn.Exec(schema); err != nil {
		conn.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}

	return &DB{db: conn}, nil
}

// IngestBatch inserts a slice of events atomically using INSERT OR IGNORE.
// Returns the number of newly accepted events and the count of duplicates ignored.
func (d *DB) IngestBatch(events []StoredEvent) (int, int, error) {
	if len(events) == 0 {
		return 0, 0, nil
	}

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	tx, err := d.db.Begin()
	if err != nil {
		return 0, 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(`
		INSERT OR IGNORE INTO events (event_id, campaign_id, contact_id, type, timestamp, metadata)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return 0, 0, fmt.Errorf("prepare insert statement: %w", err)
	}
	defer stmt.Close()

	var accepted, duplicates int
	for _, ev := range events {
		var metaJSON sql.NullString
		if len(ev.Metadata) > 0 {
			raw, err := json.Marshal(ev.Metadata)
			if err == nil {
				metaJSON = sql.NullString{String: string(raw), Valid: true}
			}
		}

		// Store ISO 8601 UTC timestamp
		tsStr := ev.Timestamp.UTC().Format(time.RFC3339)

		res, err := stmt.Exec(ev.EventID, ev.CampaignID, ev.ContactID, ev.Type, tsStr, metaJSON)
		if err != nil {
			return 0, 0, fmt.Errorf("exec insert for event_id %q: %w", ev.EventID, err)
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return 0, 0, fmt.Errorf("rows affected check: %w", err)
		}

		if rows == 1 {
			accepted++
		} else {
			duplicates++
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit transaction: %w", err)
	}

	return accepted, duplicates, nil
}

// GetCampaignStats computes aggregated statistics for a specific campaign.
// If the campaign has no events recorded, it returns zeroed statistics.
func (d *DB) GetCampaignStats(campaignID string) (*CampaignStats, error) {
	query := `
		SELECT type, COUNT(*) 
		FROM events 
		WHERE campaign_id = ? 
		GROUP BY type
	`
	rows, err := d.db.Query(query, campaignID)
	if err != nil {
		return nil, fmt.Errorf("query campaign stats: %w", err)
	}
	defer rows.Close()

	stats := &CampaignStats{CampaignID: campaignID}

	for rows.Next() {
		var evType string
		var count int
		if err := rows.Scan(&evType, &count); err != nil {
			return nil, fmt.Errorf("scan stat row: %w", err)
		}

		switch evType {
		case TypeSent:
			stats.Sent = count
		case TypeDelivered:
			stats.Delivered = count
		case TypeOpened:
			stats.Opened = count
		case TypeClicked:
			stats.Clicked = count
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	// Calculate funnel rates (rounded to 4 decimal places)
	if stats.Sent > 0 {
		stats.DeliveryRate = round4(float64(stats.Delivered) / float64(stats.Sent))
	}
	if stats.Delivered > 0 {
		stats.OpenRate = round4(float64(stats.Opened) / float64(stats.Delivered))
	}
	if stats.Opened > 0 {
		stats.ClickThroughRate = round4(float64(stats.Clicked) / float64(stats.Opened))
	}

	return stats, nil
}

// GetCampaignEvents returns a paginated list of events for the campaign.
func (d *DB) GetCampaignEvents(campaignID string, limit, offset int) (*PaginatedEventsResponse, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	var total int
	err := d.db.QueryRow(`SELECT COUNT(*) FROM events WHERE campaign_id = ?`, campaignID).Scan(&total)
	if err != nil {
		return nil, fmt.Errorf("count campaign events: %w", err)
	}

	rows, err := d.db.Query(`
		SELECT event_id, campaign_id, contact_id, type, timestamp, metadata
		FROM events
		WHERE campaign_id = ?
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?
	`, campaignID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query campaign events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)
	for rows.Next() {
		var ev Event
		var metaStr sql.NullString
		if err := rows.Scan(&ev.EventID, &ev.CampaignID, &ev.ContactID, &ev.Type, &ev.Timestamp, &metaStr); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if metaStr.Valid && metaStr.String != "" {
			var m map[string]string
			if json.Unmarshal([]byte(metaStr.String), &m) == nil {
				ev.Metadata = m
			}
		}
		events = append(events, ev)
	}

	return &PaginatedEventsResponse{
		CampaignID: campaignID,
		Total:      total,
		Limit:      limit,
		Offset:     offset,
		Events:     events,
	}, nil
}

func round4(val float64) float64 {
	return math.Round(val*10000) / 10000
}

func (d *DB) Close() error {
	return d.db.Close()
}

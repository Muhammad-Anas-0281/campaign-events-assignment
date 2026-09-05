# Relay Campaign Events Service (Part 2)

A resilient, concurrent Go backend service for ingesting webhook event notifications from marketing message delivery providers (sent, delivered, opened, clicked) and serving real-time campaign performance analytics.

---

## 1. Quick Start

### Prerequisites
- Go 1.22+ (tested on Go 1.27.1).
- No external CGO or GCC compiler required (`modernc.org/sqlite` is 100% pure Go).

### Run the Service
```bash
cd starter
go run .
```

By default, the server starts on `http://localhost:8080` with database stored at `campaigns.db` (or configure via environment variables `PORT=8080` and `DB_PATH=campaigns.db`).

### Run Automated Tests
```bash
cd starter
go test -v ./...
```
All unit, concurrency, partial acceptance, and seed data tests run and pass in ~1 second.

---

## 2. API Endpoints

### 1. Ingest Events
- **Endpoint**: `POST /events`
- **Content-Type**: `application/json`
- **Description**: Accepts a JSON array of webhook events. Uses individual element inspection (`[]json.RawMessage`) to ensure valid events in a batch are accepted even if other elements are malformed.
- **Deduplication**: Idempotent first-write wins (`INSERT OR IGNORE`). Duplicate provider retries do not increment stats.

#### Example Request:
```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  -d '[
    {
      "event_id": "evt_001",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_001",
      "type": "delivered",
      "timestamp": "2026-08-10T06:15:00Z"
    },
    {
      "event_id": "evt_002",
      "campaign_id": "cmp_summer_sale",
      "contact_id": "ct_001",
      "type": "opened",
      "timestamp": "2026-08-10T09:30:00Z"
    }
  ]'
```

#### Example Response (200 OK):
```json
{
  "status": "success",
  "total": 2,
  "accepted": 2,
  "duplicates": 0,
  "rejected": 0
}
```

If a batch contains malformed records (e.g. invalid timestamps or missing IDs), valid events are safely committed and errors are reported:
```json
{
  "status": "partial_success",
  "total": 2,
  "accepted": 1,
  "duplicates": 0,
  "rejected": 1,
  "errors": [
    {
      "index": 1,
      "event_id": "evt_bad",
      "reason": "invalid timestamp \"not-a-time\", expected RFC3339 format"
    }
  ]
}
```

---

### 2. Campaign Statistics
- **Endpoint**: `GET /campaigns/{campaign_id}/stats`
- **Description**: Returns real-time aggregate counts for a campaign along with calculated conversion rates (delivery rate, open rate, click-through rate).
- If a campaign has no events recorded, returns HTTP 200 with zeroed counts to prevent dashboard frontend crashes.

#### Example Request:
```bash
curl http://localhost:8080/campaigns/cmp_summer_sale/stats
```

#### Example Response (200 OK):
```json
{
  "campaign_id": "cmp_summer_sale",
  "sent": 100,
  "delivered": 95,
  "opened": 45,
  "clicked": 12,
  "delivery_rate": 0.95,
  "open_rate": 0.4737,
  "click_through_rate": 0.2667
}
```

---

### 3. Campaign Events (Optional Feature Implemented)
- **Endpoint**: `GET /campaigns/{campaign_id}/events?limit=50&offset=0`
- **Description**: Returns paginated recent activity for a campaign, ordered by timestamp descending.

#### Example Request:
```bash
curl "http://localhost:8080/campaigns/cmp_summer_sale/events?limit=2&offset=0"
```

---

## 3. Seed Traffic Verification

To load the real-world provider traffic sample containing 235 events:
```bash
curl -X POST http://localhost:8080/events \
  -H "Content-Type: application/json" \
  --data-binary @seed/events.json
```

**Seed Ingestion Result**:
- `Total`: 235 events
- `Accepted`: 211 valid unique events
- `Duplicates`: 18 provider retries safely deduplicated
- `Rejected`: 6 malformed records safely quarantined (non-RFC3339 dates, empty IDs, unknown event types)

Verify stats after seed load:
```bash
curl http://localhost:8080/campaigns/cmp_summer_sale/stats
curl http://localhost:8080/campaigns/cmp_welcome/stats
curl http://localhost:8080/campaigns/cmp_winback/stats
```

---

## 4. Key Architectural Decisions

1. **Partial Ingestion vs Atomic Rejection**:
   Instead of parsing into `[]Event` (where one bad timestamp fails 200 events), we parse into `[]json.RawMessage`. Valid events are accepted while bad events are reported with line indices and reasons. This prevents provider retry storms.
2. **First-Write Wins Deduplication**:
   Enforced at the storage layer via `INSERT OR IGNORE` on `PRIMARY KEY (event_id)`. Subsequent duplicate deliveries are recorded as duplicates without double-counting.
3. **Out-of-Order Delivery**:
   Events are stored independently with their provider timestamp. `opened` can precede `delivered` without error.
4. **Concurrency Safety**:
   SQLite is configured with WAL mode (`PRAGMA journal_mode=WAL;`), busy timeouts, and an internal mutex guard (`writeMu`) on batch inserts, guaranteeing zero `database is locked` errors under concurrent webhook spikes.

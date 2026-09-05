# Engineering Assignment Notes

## Part 1 — Read and Think

### 1. My interpretation of the problem

Relay receives event notifications from third-party delivery providers (e.g., SendGrid, Twilio, Mailgun).

When Relay sends a marketing message, the provider can later notify Relay about what happened to that message. The supported event types are:
- `sent`
- `delivered`
- `opened`
- `clicked`

I need to build a Go backend service that:
1. Accepts batches of events through `POST /events`.
2. Validates and stores the events.
3. Handles provider retries so the same event is not counted multiple times (idempotency).
4. Handles events arriving late or out of order.
5. Provides campaign-level statistics through `GET /campaigns/{campaign_id}/stats`.
6. Keeps the statistics correct and consistent even when multiple requests are processed concurrently.

The dashboard primarily needs the count of sent, delivered, opened, and clicked events for a campaign, plus derived performance metrics (delivery rate, open rate, click-through rate).

Current scale is modest (~a few thousand events per day across ~50 active campaigns), so I will prioritize correctness, data integrity, and simplicity over prematurely designing a complex distributed system.

I will use SQLite for persistence because it gives the service durable storage, ACID transactions, and database-enforced uniqueness constraints (`event_id PRIMARY KEY`).

---

### 2. Assumptions I am making

1. **Event Identity & Deduplication**:
   - `event_id` is assigned by the provider and globally identifies one unique event.
   - Any event with an existing `event_id` is treated as a duplicate.
   - Duplicate events must never increase campaign statistics.
   - **First-write wins**: If a retry arrives with identical or contradictory payload for an already recorded `event_id`, the original stored record is retained, and the duplicate is safely ignored (idempotent no-op).

2. **Event Types & Normalization**:
   - The system recognizes four standard event types: `sent`, `delivered`, `opened`, `clicked`.
   - Event types are normalized to lowercase (`strings.ToLower`) to tolerate minor provider inconsistencies (e.g., `"OPENED"`).
   - Unknown event types (such as `"spam_report"` seen in real seed data) are rejected or recorded without polluting core campaign counters.

3. **Required Fields & Validation**:
   - `event_id`, `campaign_id`, `contact_id`, `type`, and `timestamp` are mandatory non-empty strings.
   - `metadata` is optional JSON.
   - `timestamp` must parse according to ISO 8601 / RFC 3339 (`2006-01-02T15:04:05Z07:00`). Invalid formats like `"not-a-time"` or `"10-08-2026 09:00"` must be rejected.

4. **Event Timing & Ordering**:
   - The `timestamp` field represents when the event occurred at the provider, not when our webhook endpoint received it.
   - Out-of-order arrival is expected and acceptable: an `opened` event can arrive before `delivered`. The service stores each event independently without enforcing strict lifecycle sequencing.

5. **Statistics Definition**:
   - Campaign statistics represent total valid unique events stored for that campaign.
   - Repeated events from different interactions by the same contact (e.g., two distinct `opened` events with different `event_id`s for contact `ct_001`) both count toward total opens.

6. **Batch Ingestion Behavior**:
   - `POST /events` receives an array of events.
   - **Partial Ingestion Strategy**: If a batch of 200 events contains 1 malformed item, the service ingests the 199 valid events and returns an ingestion report detailing accepted, duplicate, and rejected items with error reasons. (Rejecting all 199 valid events due to 1 bad item causes cascading retry storms from webhook providers).

7. **Storage & Concurrency**:
   - SQLite is configured in WAL mode (`PRAGMA journal_mode=WAL;`) with a busy timeout (`PRAGMA busy_timeout=5000;`).
   - Inserts use `INSERT OR IGNORE INTO events` to guarantee race-free deduplication under concurrent webhook requests.

---

### 3. Ambiguities I noticed in the brief

1. **Batch Failure Semantics**:
   - The brief does not define whether batch ingestion should be atomic (all-or-nothing rollback) or partial (accept valid, report errors for invalid). In real webhook integrations, all-or-nothing creates severe duplicate retry loops.
2. **Conflicting Retries**:
   - The seed data contains `evt_00164` sent first as `sent` and later as `opened`. The brief is silent on whether to overwrite, reject with conflict (409), or ignore.
3. **Event Semantics (Total vs Unique Contacts)**:
   - Does "how many opened" mean raw open events or distinct contacts who opened? (The debugging exercise in Part 3 explicitly introduces `unique_opens`, highlighting that product teams often conflate the two).
4. **Campaign Lifecycle / Non-existent Campaign**:
   - What should `GET /campaigns/{campaign_id}/stats` return if a campaign has zero events? (Returning 404 breaks dashboards for newly launched campaigns; returning 200 with zero counts is much more dashboard-friendly).
5. **Timestamp Range Boundaries**:
   - The brief does not state whether timestamps from years in the future or distant past should be rejected.

---

### 4. Questions I would ask the PM

1. When a provider batch contains malformed records alongside valid events, should we accept the valid events and return a 207 Multi-Status / 200 with error summary, or reject the entire batch with 400?
2. If a provider retries an `event_id` but modifies fields (e.g. changes `type` or `metadata`), should we ignore the update, overwrite the record, or raise an alert?
3. On the dashboard, do marketers expect **Total Opens / Clicks** or **Unique Contacts Opened / Clicked**? Should we provide both?
4. What should `GET /campaigns/{campaign_id}/stats` return when a campaign exists in Relay's system but has recorded zero events so far? (HTTP 200 with 0 counts vs HTTP 404).
5. Are unknown event types (like `bounced`, `spam_report`, `unsubscribed`) planned for near-term support, and should our schema persist them even if they are omitted from standard stats?
6. Is `event_id` guaranteed unique across different providers, or is uniqueness scoped per `(provider_id, event_id)`?

---

### 5. What I will prioritize

Given the 3.5–4 hour constraint, I will prioritize correctness, data integrity, and resilient ingestion over optional features:

1. **Storage & Concurrency Setup**:
   - Set up SQLite with proper WAL mode, busy timeout, and primary key on `event_id`.
2. **Resilient `POST /events` Ingestion**:
   - Stream-decode or parse items via `json.RawMessage` so single bad lines don't fail entire batches.
   - Comprehensive validation (RFC3339 timestamps, required strings, type normalization).
   - Atomic `INSERT OR IGNORE` for idempotent deduplication.
3. **`GET /campaigns/{campaign_id}/stats`**:
   - Fast aggregate query grouping by `type`.
   - Compute counts and conversion rates (delivery rate, open rate, CTR).
   - Return clean, intuitive JSON structure.
4. **Verification with Real Traffic**:
   - Run the full `starter/seed/events.json` through the service and ensure zero crashes, correct counts, and graceful rejection of bad records.
5. **Concurrency & Edge Case Tests**:
   - Automated tests for simultaneous duplicate requests, out-of-order events, and malformed inputs.
6. **Optional Endpoint (`GET /campaigns/{campaign_id}/events`)**:
   - Implement only if time permits after required core functionality is verified.

---

## Part 4 — Scale Memo (100 Million Events / Day)

When traffic grows from a few thousand events to **100,000,000 events/day** (~1,160 events/second average, with peak spikes of 5,000–10,000 events/sec), the architecture must evolve.

### 1. What breaks first in the current implementation?
- **SQLite Write Serialization**: SQLite allows only one writer at a time. Under hundreds of concurrent webhook connections, write transactions will queue up, hit `busy_timeout`, and fail with `database is locked`.
- **Synchronous Ingestion Bottleneck**: Parsing JSON, checking constraints, and inserting synchronously in HTTP request threads will saturate CPU and connection limits, leading to provider timeouts and aggressive retry storms.
- **Dynamic Aggregation Queries**: Running `SELECT type, COUNT(*) ... GROUP BY type` across 100M rows on every dashboard load will cause table scans, disk I/O thrashing, and high query latency.

### 2. What would you change, and in what order?
1. **Decouple Ingestion from Storage (Introduce a Buffer/Queue)**:
   - Accept the HTTP payload, quickly validate basic JSON structure, push raw batches onto a high-throughput distributed log (e.g., Apache Kafka or AWS Kinesis), and immediately return `202 Accepted`.
2. **Switch Persistence to a Distributed Database**:
   - Replace single-node SQLite with a horizontally scalable distributed datastore (e.g., PostgreSQL with partitioning, ClickHouse for analytics, or Cassandra/DynamoDB for high write throughput).
3. **Pre-aggregated Campaign Counters (Redis / In-memory aggregates)**:
   - Instead of querying historical event tables on every GET request, maintain atomic real-time counters in Redis (`HINCRBY campaign:{id}:stats opened 1`).

### 3. Would the API contract change? Would storage design change?
- **API Contract**:
  - `POST /events` remains largely compatible, but returns `202 Accepted` with a batch tracking identifier instead of synchronous DB commit details. A max batch size limit (e.g., 500 events per request) would be enforced.
  - `GET /campaigns/{campaign_id}/stats` remains identical for callers, but reads from cache/pre-aggregated views.
- **Storage Design**:
  - Partition the events storage by `(campaign_id, date)` so data is evenly distributed across cluster nodes and old campaigns can be archived.
  - Separate the **Write Model** (raw immutable append-only event log) from the **Read Model** (pre-aggregated campaign statistics rollups).

### 4. Would you introduce a queue? What new problems does that create?
- **Yes**, a distributed queue/log (Kafka) is required to absorb provider traffic spikes.
- **New Problems Created**:
  - **Eventual Consistency**: There will be a slight delay (sub-second to seconds) between when a provider posts an event and when the dashboard counter reflects it.
  - **Duplicate Delivery**: At-least-once message queues deliver messages multiple times during worker rebalances or network blips.
  - **Lag Monitoring & Backpressure**: If worker consumers slow down, queue lag grows, requiring consumer auto-scaling and dead-letter queues (DLQs).

### 5. Where can duplicates now sneak in that couldn't before?
- At the queue consumer level: if a worker processes a batch of events but crashes before committing its Kafka offset, the next worker will re-process the entire batch.
- At the ingress edge: multi-region deployments or multiple API gateways receiving concurrent retries from providers simultaneously.
- **Deduplication Strategy**: Maintain a short-term distributed deduplication cache (e.g., Redis Bloom filter or Redis key `SET event:{id} 1 EX 86400 NX`) combined with unique constraints in the primary storage layer.

### 6. How would you keep counters trustworthy?
- **Idempotent Consumers**: Each worker verifies whether an `event_id` has already been recorded before incrementing stats.
- **Periodic Reconciliation (Lambda Architecture / Batch Sync)**:
  - Run an asynchronous hourly reconciliation job (using ClickHouse/Spark) that computes exact ground-truth counts from raw immutable event logs and corrects any drifting Redis counters.

### 7. What would you monitor?
- **Ingress Metrics**: Webhook HTTP status codes (2xx, 4xx, 5xx), request rates, p95/p99 latency.
- **Queue Metrics**: Consumer lag (messages pending per campaign/partition), consumer processing time.
- **Data Quality Metrics**: Duplicate event rate, malformed event rejection rate, invalid timestamp rate.
- **Database & Cache Health**: Connection pool utilization, query response time, Redis memory and cache hit ratio.

### 8. What would you deliberately NOT solve yet?
- Global cross-region multi-master active-active database replication.
- Complex real-time user-defined ad-hoc querying across arbitrary metadata attributes.
- Complex machine-learning fraud detection on webhook events.

---

## Part 5 — The Angry Marketer

### Scenario
- **10:00**: Sent: 1,000,000 | Delivered: 970,000 | Opened: 250,000 | Clicked: 20,000
- **10:30**: Sent: 1,000,000 | Delivered: 975,000 | Opened: 248,000 | Clicked: 20,000
- Delivered increased by +5,000. Opened decreased by -2,000. Marketer filed a ticket: *"Your dashboard is broken."*

### 1. Plausible explanations (before assuming code is broken)
1. **Metric Definition Shift (Unique Contacts vs Total Opens)**:
   - The dashboard might be displaying **Unique Opens** (distinct contacts), not raw open counts. If contact identity merges occurred (e.g., identity resolution deduplicated multiple alias IDs into a single contact), the unique open count naturally decreases.
2. **Bot / Spam Filtering Reversal (Anti-Fraud Cleansing)**:
   - Delivery providers (like Gmail, Apple Mail Privacy Protection, or Microsoft Defender) frequently run automated security scanners that pre-fetch/open links upon delivery. Providers often send post-hoc spam/bot retraction notices or invalidate automated opens retroactively.
3. **Data Correction / Reprocessing by Provider**:
   - Webhook providers can issue adjustments or cancellations when false positives are detected.
4. **Timezone or Time-Window Filter on the Dashboard**:
   - If the dashboard defaults to a sliding time window (e.g. "Last 24 Hours" or "Today UTC"), as the clock ticks from 10:00 to 10:30, events that occurred between 09:30 and 10:00 yesterday roll out of the time window, causing the displayed count to drop.
5. **Database Node Inconsistency / Read Replica Lag**:
   - At 10:00, the request hit a fully synchronized database node. At 10:30, the request hit a replica experiencing replication lag or an inconsistent cache node.

### 2. What would you check first?
1. **Query Ground Truth in the Raw Database**:
   - Execute an exact query directly on the raw events table:
     ```sql
     SELECT COUNT(*), COUNT(DISTINCT contact_id)
     FROM events
     WHERE campaign_id = '...' AND type = 'opened';
     ```
   - Check if any delete/update operations occurred on the events table between 10:00 and 10:30 (audit logs/WAL).
2. **Inspect the Dashboard Query Parameters**:
   - Inspect the network tab or API access logs for the 10:00 and 10:30 requests. Did the dashboard pass a `start_time`, `end_time`, or timezone offset that shifted between 10:00 and 10:30?
3. **Check Cache / Read-Replica State**:
   - Compare results across all replica nodes to check for split-brain or stale cache reads.

### 3. How to decide whether this is a bug or expected behavior?
- If the dashboard claims to show **cumulative lifetime opens** for the campaign and the raw stored events were never deleted, a decreasing counter is a **bug** (likely an in-memory counter race condition, a faulty cache invalidation, or an unpinned replica).
- If the dashboard shows **unique contacts** or **active window metrics**, and audit logs confirm contact deduplication or sliding-window expiration, it is **expected behavior** that needs clearer UI explanation for marketers.

### 4. Is the Delivered number rising actually suspicious?
- **No, it is completely normal and expected.**
- Providers deliver messages asynchronously over hours due to throttling, grey-listing, and network retries.
- Furthermore, an `opened` event arriving before a `delivered` event is an explicit reality in the email world (Apple Mail Privacy Protection pre-opens messages before the final delivery confirmation webhook is dispatched, or delivery webhooks are queued and arrive late). Therefore, delivered count rising at 10:30 is completely consistent with real-world email delivery lifecycles.

---

## What I completed / what I intentionally skipped

- **Completed**:
  - Resilient `POST /events` handling batch payloads with partial acceptance and detailed rejection logging.
  - Idempotent event deduplication and out-of-order event resilience.
  - `GET /campaigns/{campaign_id}/stats` returning clean counts and computed conversion rates.
  - Full test suite verifying deduplication, out-of-order ingestion, concurrent safety, and seed dataset survival.
  - Fixed all 4 bugs in Part 3 (`debugging/`).
  - Completed Scale Memo (Part 4) and Angry Marketer analysis (Part 5).

- **Intentionally Skipped**:
  - Cloud infrastructure, Docker, Kubernetes, microservice orchestration (per instructions).
  - Frontend UI / dashboard views (backend API focus per instructions).
  - User authentication and API key permissions (not requested in the brief).

---

## If I had another day

1. **Dead Letter Queue (DLQ) & Provider Error Categorization**:
   - Build a structured DLQ endpoint allowing operations teams to inspect rejected webhook payloads and replay them once schema fixes or provider updates are deployed.
2. **Prometheus Metrics & Structured Tracing**:
   - Add OpenTelemetry tracing and Prometheus metrics (`events_ingested_total`, `events_rejected_total`, `ingest_latency_seconds`) for real-time visibility into provider webhook health.
3. **Enhanced Stats Filtering**:
   - Expand `GET /campaigns/{campaign_id}/stats` with optional query parameters (`?from=...&to=...&interval=hour`) to support time-series graphs on marketer dashboards.

# AI Usage Disclosure (AI_USAGE.md)

This document provides a transparent accounting of the AI chat tools used during the completion of this engineering assignment, per the submission guidelines.

---

## 1. Which tools you used

- **ChatGPT** (web chat interface)
- **Gemini** (web chat interface)

These tools were used solely through conversational chat interfaces for brainstorming, rubber-ducking, discussing implementation trade-offs, and clarifying Go standard library concepts. No autonomous AI agents or automated code execution tools were used.

---

## 2. Roughly what you used them for

- **Requirement Clarification**: Bouncing questions in chat about real-world delivery provider behaviors, out-of-order webhook delivery, and retry semantics.
- **Data Inspection**: Reviewing patterns and quirks in messy seed and debugging datasets (such as non-standard timestamp strings, missing fields, and case sensitivity).
- **Architecture Discussion**: Discussing SQLite configuration options (WAL mode, busy timeout, and connection pool behavior) versus in-memory storage.
- **Batch Processing Design**: Discussing the trade-offs between atomic batch rejection and partial acceptance with itemized error reporting.
- **Debugging Discussion**: Consulting chat when analyzing the bugs in Part 3 (`debugging/main.go`), specifically around time zone handling (`Local()` vs `UTC()`) and data races during concurrent map/struct updates.
- **Testing Ideas**: Formulating test scenarios for concurrency and deduplication verification.

---

## 3. One suggestion from AI that you rejected or changed, and why

When asking ChatGPT about parsing JSON batches in Go HTTP handlers, an initial suggestion was decoding the request body directly into a typed slice:

```go
var events []Event
if err := json.NewDecoder(r.Body).Decode(&events); err != nil { ... }
```

**Why it was rejected and changed:**  
In Go, direct decoding into `[]Event` causes `json.Unmarshal` / `Decode` to fail the entire array if even a single item contains an invalid field (such as an unparseable timestamp like `"not-a-time"`). For delivery provider webhooks, rejecting an entire batch of 200 events because 1 item is malformed causes providers to retry the entire payload repeatedly, exacerbating traffic and retry loops.

Instead, the approach was changed to decode the batch into `[]json.RawMessage`. Each raw element is then unmarshaled and validated individually, allowing valid events to be committed safely while invalid ones are isolated and reported with line indices and error messages.

---

## 4. One thing AI helped you understand

**SQLite Concurrency in Go:**  
Prompting Gemini about Go's `database/sql` connection pooling with SQLite helped clarify that SQLite only permits one writer at a time. While Write-Ahead Logging (`WAL` mode) enables concurrent readers alongside a writer, executing `PRAGMA busy_timeout = 5000` via a single `conn.Exec()` call only configures the specific connection checked out from the pool at that moment; other connections spawned by `*sql.DB` under concurrent load may not share the same behavior.

This prompted the decision to add a Go-level mutex (`writeMu sync.Mutex`) around write transactions to cleanly serialize write attempts in memory without file lock contention, while allowing concurrent read queries (`GET /stats`) to proceed without interference.

---

## 5. Anything AI-generated that you then had to debug

When running automated concurrency tests (`TestConcurrentIngestion`) where 10 goroutines concurrently posted batches of events, an initial implementation based on chat suggestions failed with:

```text
database is locked (5) (SQLITE_BUSY)
```

Under 10 concurrent goroutines, multiple write transactions attempted to acquire locks simultaneously across different pool connections. I investigated the error, added a `sync.Mutex` (`writeMu`) in the database wrapper to coordinate batch write transactions, and verified across multiple test runs that the issue was completely eliminated.

---

## 6. Transparency statement

AI chat tools were used as conversational reference and brainstorming aids. All architectural decisions, code implementation, debugging, testing, and verification were performed and verified by the candidate.

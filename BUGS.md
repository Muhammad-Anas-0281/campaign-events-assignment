# Debugging Exercise Findings (BUGS.md)

This document details the diagnosis, root causes, minimal fixes, and verification methods for the four bugs identified in `debugging/main.go`.

---

## Bug 1: Batch-Local Deduplication Map (`seen`)

### 1. What the bug is
The deduplication map `seen` was declared locally inside the `processBatch` function instead of persisting across the entire file stream. Consequently, duplicate events that appeared across different batches were not deduplicated, artificially inflating sent, delivered, opened, and clicked counts.

### 2. Why it happens
In `processBatch(events []Event)`:
```go
seen := make(map[string]bool) // re-instantiated for every 200 events!
```
Every batch of 200 events allocated a fresh `seen` map. When providers retried an event across different batches, the subsequent batch had no memory of previously ingested event IDs.

### 3. Your fix
Elevated `seen` to package-level scope (`var seen = map[string]bool{}`) and removed the local re-allocation from `processBatch`:
```go
// At package level:
var seen = map[string]bool{}

func processBatch(events []Event) {
    unique := make([]Event, 0, len(events))
    for _, ev := range events {
        if seen[ev.EventID] {
            continue
        }
        seen[ev.EventID] = true
        // ...
    }
}
```

### 4. How you verified the fix
Comparing actual output against `expected_output.txt` showed that total counts for sent, delivered, opened, and clicked dropped to their true values, eliminating the inflated numbers caused by cross-batch duplicates.

---

## Bug 2: Global `openedBy` Map Instead of Per-Campaign Scoping

### 1. What the bug is
`UniqueOpens` counts were undercounted for campaigns when a contact was engaged across multiple different campaigns.

### 2. Why it happens
The specification requires:
> "`unique_opens` = the number of distinct contacts that opened, per campaign. One contact opening five times in a campaign counts once. The same contact opening in two campaigns counts once in each."

However, `openedBy` was keyed solely on `ev.ContactID`:
```go
if !openedBy[ev.ContactID] {
    openedBy[ev.ContactID] = true
    cs.UniqueOpens++
}
```
If contact `ct_123` opened an email in `cmp_A`, `openedBy["ct_123"]` was set to `true`. When the same contact later opened an email in `cmp_B`, the check evaluated to true, failing to increment `UniqueOpens` for `cmp_B`.

### 3. Your fix
Keyed `openedBy` by the combination of campaign ID and contact ID (`ev.CampaignID + ":" + ev.ContactID`):
```go
case "opened":
    key := ev.CampaignID + ":" + ev.ContactID
    if !openedBy[key] {
        openedBy[key] = true
        cs.UniqueOpens++
    }
```

### 4. How you verified the fix
Compared output with `expected_output.txt`. The `unique_opens` count for each campaign matched expected numbers (e.g., `cmp_A unique_opens=549`, `cmp_B unique_opens=441`, `cmp_C unique_opens=351`, `cmp_D unique_opens=250`, `cmp_E unique_opens=164`).

---

## Bug 3: Local Timezone Used Instead of UTC for Daily Bucketing

### 1. What the bug is
Events were bucketed into the machine's local calendar day instead of the UTC date specified in the requirements. On machines running in non-UTC time zones (such as IST UTC+5:30), late-night UTC timestamps rolled over into the next local calendar day (e.g., producing spurious `2026-08-08` daily buckets).

### 2. Why it happens
In `track(ev Event)`:
```go
case "delivered":
    day := ev.Timestamp.Local().Format("2006-01-02")
    cs.DailyDelivered[day]++
```
Calling `.Local()` converted the UTC timestamp into local server time before extracting the date string.

### 3. Your fix
Replaced `.Local()` with `.UTC()`:
```go
case "delivered":
    day := ev.Timestamp.UTC().Format("2006-01-02")
    cs.DailyDelivered[day]++
```

### 4. How you verified the fix
Verified that daily delivered dates spanned strictly from `2026-08-01` through `2026-08-07`, completely matching the daily delivery counts in `expected_output.txt` and removing the spurious `2026-08-08` bucket.

---

## Bug 4: Data Race on Shared Counters in Concurrent Worker Pool

### 1. What the bug is
A concurrent data race occurred in `apply(ev)`. Eight worker goroutines simultaneously read and modified shared integer fields (`cs.Sent++`, `cs.Delivered++`, etc.) on the same `*CampaignStats` pointer without synchronization.

### 2. Why it happens
In Go, `++` is not atomic; it performs a memory read, register increment, and memory write. When 8 worker goroutines process events from the `jobs` channel concurrently:
```go
func apply(ev Event) {
    cs := stats[ev.CampaignID]
    switch ev.Type {
    case "sent":
        cs.Sent++ // DATA RACE: multiple goroutines read/write cs.Sent concurrently
    // ...
```
This causes race conditions, corrupted counts, lost increments, and non-deterministic behavior under varying CPU loads.

### 3. Your fix
Added a `sync.Mutex` (`mu`) to `CampaignStats` and locked it inside `apply()`:
```go
type CampaignStats struct {
    mu             sync.Mutex
    Sent           int
    Delivered      int
    Opened         int
    Clicked        int
    UniqueOpens    int
    DailyDelivered map[string]int
}

func apply(ev Event) {
    cs := stats[ev.CampaignID]
    cs.mu.Lock()
    defer cs.mu.Unlock()
    switch ev.Type {
    case "sent":
        cs.Sent++
    case "delivered":
        cs.Delivered++
    case "opened":
        cs.Opened++
    case "clicked":
        cs.Clicked++
    }
}
```

### 4. How you verified the fix
Ran the CLI 5 consecutive times across all 20,000 events. The stdout matched `expected_output.txt` with zero discrepancies on every single run, confirming deterministic and race-free execution.

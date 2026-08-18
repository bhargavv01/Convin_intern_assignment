# SOLUTION

## 1. Background recording goroutine uses the HTTP request context("Calls are landing but their recordings never get marked processed and there’s nothing in the logs about it")

**Symptom**: `recording_processed` stays `false` for every call that has a `recording_url`, even though the endpoint returns 200.

**Cause**: In `service.go`, the `Ingest` method spawns a goroutine to process the recording, but passes it `ctx` — the HTTP request context (`r.Context()`). Go's `http.Server` cancels that context as soon as the handler returns its response. The goroutine sleeps 50ms then calls `MarkRecordingProcessed`, but by that point the context is already dead and the Postgres query fails with `context canceled`.

**Fix**: Use `context.Background()` for the goroutine so it is not tied to the request lifecycle. Also log errors from the goroutine instead of silently swallowing them.

## 2. Duplicate events cause double-counting (TOCTOU race) ("account call-counts are drifting higher than the actual number of calls")

**Symptom**: Under concurrent redeliveries, the same event is stored multiple times and `account_stats` is inflated (e.g. `call_count=6` for 1 call).

**Cause**: `Ingest` does `EventExists` (check) then `InsertEvent` (act) with no atomicity between them. Two concurrent requests both read "not exists" before either inserts, so both proceed. The `events` table had only a non-unique index on `event_id`, so both inserts succeed, and both call `IncrementAccountStats`.

**Fix**: Two-layer dedup:
1. **Redis `SETNX`** — atomic, sub-millisecond. The first request sets the key and proceeds; all others see the key already exists and return immediately. This eliminates the race.
2. **Postgres `UNIQUE` constraint on `event_id`** + `ON CONFLICT DO NOTHING` — durable safety net. If Redis is briefly unavailable, Postgres still rejects the duplicate. `InsertEvent` now returns `(bool, error)` so the caller knows whether the row was actually new.

Testcase: TestConcurrentDuplicateDoesNotDoubleCount() in service_test.go

## 3. Failed/no-answer calls inflate stats ("account stats include calls that never connected")

**Symptom**: A `failed` call and a `no_answer` call are counted in `account_stats` alongside `completed` calls, inflating `call_count` and `total_duration_sec`. For example, one completed call (143 s), one failed call (0 s), and one no-answer call (12 s) produce `call_count=3` and `total_duration_sec=155` instead of the correct `1` / `143`.

**Cause**: In `service.go`, `Ingest` unconditionally calls `IncrementAccountStats` and `cache.Record` for every event regardless of `Status`. The provider sends webhooks for all call outcomes (`completed`, `failed`, `no_answer`), but only completed calls represent actual billable / successful calls that should appear in account totals.

**Fix**: Gate the stats-increment and cache-record calls behind `evt.Status == "completed"`. Failed and no-answer events are still stored in the `events` and `calls` tables (for auditing and debugging), but they no longer inflate the per-account aggregates.

Testcase: TestFailedCallDoesNotInflateStats() in service_test.go

## 4. Data race crash in stats cache ("duplicate call records are showing up in the dashboard")

**Symptom**: Under concurrent webhook traffic the service panics with `fatal error: concurrent map read and map write` or silently loses updates — dashboard stats become inconsistent / duplicated.

**Cause**: In `cache.go`, `Cache.Get` correctly holds `c.mu.RLock()`, but `Cache.Record` acquires **no lock at all**. When multiple HTTP requests call `Record` and `Get` concurrently on the same account, Go's runtime detects a simultaneous map read and map write and crashes the process. Even when it doesn't crash, the unsynchronised increments (`CallCount++`, `TotalDurationSec +=`) cause lost updates.

**Fix**: Add `c.mu.Lock()` / `defer c.mu.Unlock()` at the top of `Cache.Record`, making it the exclusive writer while `Get` remains a shared reader via `RLock`.

Testcase: TestCacheConcurrentRecordAndGet() in cache_test.go

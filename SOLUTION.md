# SOLUTION

## What was broken, and why

### 1. Background recording goroutine uses the HTTP request context

**Symptom**: `recording_processed` stays `false` for every call that has a `recording_url`, even though the endpoint returns 200.

**Cause**: In `service.go`, the `Ingest` method spawns a goroutine to process the recording, but passes it `ctx` — the HTTP request context (`r.Context()`). Go's `http.Server` cancels that context as soon as the handler returns its response. The goroutine sleeps 50ms then calls `MarkRecordingProcessed`, but by that point the context is already dead and the Postgres query fails with `context canceled`.

**Fix**: Use `context.Background()` for the goroutine so it is not tied to the request lifecycle. Also log errors from the goroutine instead of silently swallowing them.

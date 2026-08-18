-- Replace the non-unique index with a unique constraint so the database
-- itself rejects duplicate event_id values (safety net for the Redis dedup).
DROP INDEX IF EXISTS idx_events_event_id;
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);

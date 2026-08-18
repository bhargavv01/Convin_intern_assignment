// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// dedupTTL is how long Redis remembers a seen event_id.
const dedupTTL = 24 * time.Hour

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
//
// Deduplication is two-layered:
//  1. Redis SETNX — atomic, sub-millisecond, eliminates the TOCTOU race.
//  2. Postgres UNIQUE + ON CONFLICT DO NOTHING — durable safety net in case
//     Redis is momentarily unavailable or the process crashes between layers.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	// ── Layer 1: Redis fast-path dedup ──
	dedupKey := "dedup:" + evt.EventID
	set, err := s.rdb.SetNX(ctx, dedupKey, 1, dedupTTL).Result()
	if err != nil {
		// Redis is down — fall through to Postgres safety net.
		s.log.Warn("redis dedup unavailable, falling back to postgres", "err", err)
	} else if !set {
		s.log.Info("duplicate delivery ignored (redis)", "event_id", evt.EventID)
		return nil
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// ── Layer 2: Postgres safety-net dedup ──
	inserted, err := s.store.InsertEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored (postgres)", "event_id", evt.EventID)
		return nil
	}

	if err := s.store.UpsertCall(ctx, rec); err != nil {
		return err
	}

	// Only completed calls count toward account stats. Failed and no-answer
	// calls are stored (events + calls tables) but must not inflate totals.
	if evt.Status == "completed" {
		if err := s.store.IncrementAccountStats(ctx, rec.AccountID, rec.DurationSec); err != nil {
			return err
		}
		s.cache.Record(rec.AccountID, rec.DurationSec)
	}

	// Recordings are slow to fetch, so that part does not block the provider.
	// Use a detached context: the goroutine outlives the HTTP request, so the
	// request context would be cancelled before the work finishes.
	if rec.RecordingURL != "" {
		go func() {
			bgCtx := context.Background()
			if err := s.processRecording(bgCtx, rec); err != nil {
				s.log.Error("processRecording failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

package stats_test

import (
	"sync"
	"testing"

	"github.com/convin/webhook-ingest/internal/stats"
)

func TestCacheRecordAccumulates(t *testing.T) {
	c := stats.NewCache()

	c.Record("acc_1", 30)
	c.Record("acc_1", 12)
	c.Record("acc_2", 5)

	got := c.Get("acc_1")
	if got.CallCount != 2 || got.TotalDurationSec != 42 {
		t.Fatalf("acc_1: got %+v, want CallCount=2 TotalDurationSec=42", got)
	}

	other := c.Get("acc_2")
	if other.CallCount != 1 || other.TotalDurationSec != 5 {
		t.Fatalf("acc_2: got %+v, want CallCount=1 TotalDurationSec=5", other)
	}
}

func TestCacheGetUnknownAccountIsZero(t *testing.T) {
	c := stats.NewCache()
	if got := c.Get("nobody"); got.CallCount != 0 || got.TotalDurationSec != 0 {
		t.Fatalf("got %+v, want zero value", got)
	}
}

// TestCacheConcurrentRecordAndGet fires concurrent Record and Get calls on the
// same account. Before the fix, Record acquires no lock while Get holds RLock,
// so the Go race detector (go test -race) reports a data race on the map and
// on the AccountStats fields — and under heavy load the runtime may panic with
// "concurrent map read and map write".
func TestCacheConcurrentRecordAndGet(t *testing.T) {
	c := stats.NewCache()

	const workers = 10
	const iterations = 100
	var wg sync.WaitGroup
	wg.Add(workers * 2) // writers + readers

	// Writers: Record calls.
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				c.Record("acc_race", 10)
			}
		}()
	}

	// Readers: Get calls running simultaneously.
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = c.Get("acc_race")
			}
		}()
	}

	wg.Wait()

	// After all writes complete, totals must be exact.
	got := c.Get("acc_race")
	wantCount := int64(workers * iterations)
	wantDur := int64(workers * iterations * 10)
	if got.CallCount != wantCount || got.TotalDurationSec != wantDur {
		t.Fatalf("got CallCount=%d Duration=%d, want %d/%d (lost updates from data race)",
			got.CallCount, got.TotalDurationSec, wantCount, wantDur)
	}
}

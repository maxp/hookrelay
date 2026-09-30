package ingestion

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

type manualClock struct{ now time.Time }

func (c *manualClock) Now() time.Time { return c.now }

// TestTokenBucket pins burst, refill at the configured rate, Retry-After
// rounding up to whole seconds (at least one), and that a refusal takes
// no token from either bucket.
func TestTokenBucket(t *testing.T) {
	start := time.UnixMilli(1740000000000)
	l := newRateLimiter(RateLimits{GlobalRate: 0.5, GlobalBurst: 2}, start)
	for i := 0; i < 2; i++ {
		if ok, _ := l.allow("e", start); !ok {
			t.Fatalf("burst request %d refused", i)
		}
	}
	if ok, retry := l.allow("e", start); ok || retry != 2 {
		t.Errorf("empty bucket = %v retry %d, want refused with 2 s at 0.5/s", ok, retry)
	}
	if ok, retry := l.allow("e", start.Add(1500*time.Millisecond)); ok || retry != 1 {
		t.Errorf("0.75 tokens = %v retry %d, want refused with 1 s", ok, retry)
	}
	if ok, _ := l.allow("e", start.Add(2*time.Second)); !ok {
		t.Error("refilled token refused")
	}

	// Endpoint buckets are independent; a request refused by its endpoint
	// bucket does not spend a global token.
	l = newRateLimiter(RateLimits{GlobalRate: 100, GlobalBurst: 3, EndpointRate: 1, EndpointBurst: 1}, start)
	if ok, _ := l.allow("a", start); !ok {
		t.Fatal("first a refused")
	}
	if ok, retry := l.allow("a", start); ok || retry != 1 {
		t.Errorf("second a = %v retry %d", ok, retry)
	}
	for _, e := range []string{"b", "c"} {
		if ok, _ := l.allow(e, start); !ok {
			t.Errorf("%s refused although the global bucket still had tokens", e)
		}
	}
	if ok, _ := l.allow("d", start); ok {
		t.Error("global burst exceeded")
	}

	// Zero rates disable both limits; a zero burst still holds one token.
	l = newRateLimiter(RateLimits{}, start)
	for i := 0; i < 1000; i++ {
		if ok, _ := l.allow("e", start); !ok {
			t.Fatal("disabled limiter refused")
		}
	}
	if l.size() != 0 {
		t.Error("disabled endpoint limit tracked buckets")
	}
	l = newRateLimiter(RateLimits{EndpointRate: 1}, start)
	if ok, _ := l.allow("e", start); !ok {
		t.Error("zero burst refused its single token")
	}
}

// TestEndpointBucketsAreBounded pins the LRU bound: the least recently used
// endpoint's bucket is dropped and restarts full.
func TestEndpointBucketsAreBounded(t *testing.T) {
	start := time.UnixMilli(1740000000000)
	l := newRateLimiter(RateLimits{EndpointRate: 0.001, EndpointBurst: 1}, start)
	l.allow("first", start)
	for i := 0; i < maxEndpointBuckets; i++ {
		l.allow(fmt.Sprintf("e%d", i), start)
	}
	if l.size() != maxEndpointBuckets {
		t.Errorf("size = %d, want %d", l.size(), maxEndpointBuckets)
	}
	if ok, _ := l.allow("first", start); !ok {
		t.Error("evicted endpoint did not restart with a full bucket")
	}
	if ok, _ := l.allow("e9999", start); ok {
		t.Error("a recently used endpoint lost its bucket")
	}
}

// TestRateLimitedWebhook pins the handler contract: 429 with Retry-After
// in whole seconds, counted as rate_limited, checked after route
// resolution (unknown endpoints stay 404) and before the body is read and
// anything is accepted.
func TestRateLimitedWebhook(t *testing.T) {
	hs := newHarness(t)
	clock := &manualClock{now: time.UnixMilli(1740000000000)}
	hs.h.d.Clock = clock
	hs.h.limiter = newRateLimiter(RateLimits{EndpointRate: 0.25, EndpointBurst: 1}, clock.now)

	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusOK {
		t.Fatalf("first = %d", w.Code)
	}
	w := hs.post("/webhook/telegram/wh_on", `{not read}`)
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "4" || w.Body.Len() != 0 {
		t.Fatalf("limited = %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
	}
	if hs.count("rate_limited", "telegram") != 1 || len(hs.acceptor.requests) != 1 {
		t.Errorf("rate_limited count %v, acceptances %d", hs.count("rate_limited", "telegram"), len(hs.acceptor.requests))
	}
	if hs.count("invalid_json", "telegram") != 0 {
		t.Error("the limited request's body was read")
	}
	if w := hs.post("/webhook/telegram/wh_missing", chatUpdate); w.Code != http.StatusNotFound {
		t.Errorf("unknown endpoint = %d, want 404 before rate limiting", w.Code)
	}
	clock.now = clock.now.Add(4 * time.Second)
	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusOK {
		t.Errorf("after refill = %d", w.Code)
	}
}

// TestMemoryAcceptanceStop pins the memory stop: at the configured share of
// maxmemory the probe stops acceptance and the handler refuses new
// messages without writing, as a retryable capacity rejection; below it
// acceptance resumes; maxmemory 0 or a zero percent never stops.
func TestMemoryAcceptanceStop(t *testing.T) {
	hs := newHarness(t)
	hs.h.d.MemoryStopPercent = 90
	capacity := &fakeCapacity{c: Capacity{MaxQueuedMessages: 10, MaxDedupRecords: 100, UsedMemoryBytes: 900, MaxMemoryBytes: 1000}}
	hs.h.d.Capacity = capacity
	if hs.h.AcceptingWebhooks(context.Background()) {
		t.Fatal("accepting at 90% of maxmemory")
	}
	w := hs.post("/webhook/telegram/wh_on", chatUpdate)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" || len(hs.acceptor.requests) != 0 {
		t.Errorf("memory stop = %d, acceptances %d", w.Code, len(hs.acceptor.requests))
	}
	if hs.count("capacity_rejection", "telegram") != 1 || !strings.Contains(hs.logs.String(), `"reason_code":"memory_stop"`) ||
		!strings.Contains(hs.logs.String(), `"event":"memory_acceptance_stop"`) {
		t.Errorf("memory stop not reported: %s", hs.logs.String())
	}

	capacity.c.UsedMemoryBytes = 899
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("still stopped below the share")
	}
	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusOK {
		t.Errorf("after resume = %d", w.Code)
	}
	for name, c := range map[string]Capacity{
		"no maxmemory": {MaxQueuedMessages: 10, MaxDedupRecords: 100, UsedMemoryBytes: 1 << 40},
	} {
		capacity.c = c
		if !hs.h.AcceptingWebhooks(context.Background()) {
			t.Errorf("%s: stopped", name)
		}
	}
	hs.h.d.MemoryStopPercent = 0
	capacity.c = Capacity{MaxQueuedMessages: 10, MaxDedupRecords: 100, UsedMemoryBytes: 1000, MaxMemoryBytes: 1000}
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("a zero percent stopped acceptance")
	}
}

// TestDedupStopWithEarlyEviction pins that a full deduplication index stops
// acceptance only while its oldest live record is younger than the minimum
// retention, and the oldest-age and effective-retention gauges.
func TestDedupStopWithEarlyEviction(t *testing.T) {
	hs := newHarness(t)
	hs.h.d.DedupRetention, hs.h.d.DedupMinRetention = 7*24*time.Hour, 24*time.Hour
	now := int64(1740000000000)
	capacity := &fakeCapacity{c: Capacity{NowMs: now, MaxQueuedMessages: 10, DedupRecords: 100, MaxDedupRecords: 100,
		OldestDedupAcceptedMs: now - (48 * time.Hour).Milliseconds()}}
	hs.h.d.Capacity = capacity
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("full index with an evictable oldest record stopped acceptance")
	}
	if got := gaugeValue(t, hs.reg, "hookrelay_dedup_oldest_record_age_seconds"); got != 48*3600 {
		t.Errorf("oldest age = %v", got)
	}
	if got := gaugeValue(t, hs.reg, "hookrelay_dedup_effective_retention_seconds"); got != 48*3600 {
		t.Errorf("effective retention at the cap = %v, want the oldest age", got)
	}
	capacity.c.OldestDedupAcceptedMs = now - (23 * time.Hour).Milliseconds()
	if hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("full index whose oldest record is too young kept accepting")
	}
	capacity.c.DedupRecords = 10
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("below the cap stopped acceptance")
	}
	if got := gaugeValue(t, hs.reg, "hookrelay_dedup_effective_retention_seconds"); got != 7*24*3600 {
		t.Errorf("effective retention below the cap = %v, want the configured retention", got)
	}
}

type fakeEvictor struct {
	n     int
	err   error
	calls int
}

func (f *fakeEvictor) EvictDedup(context.Context) (int, error) {
	f.calls++
	return f.n, f.err
}

// TestMaintainCapacity pins the maintenance hook: it re-evaluates the stop
// conditions and counts proactive evictions, including those made before a
// failure; acceptance-time evictions are counted too.
func TestMaintainCapacity(t *testing.T) {
	hs := newHarness(t)
	hs.h.d.MemoryStopPercent = 90
	hs.h.d.Capacity = &fakeCapacity{c: Capacity{MaxQueuedMessages: 10, MaxDedupRecords: 100, UsedMemoryBytes: 95, MaxMemoryBytes: 100}}
	ev := &fakeEvictor{n: 3}
	hs.h.d.DedupEvictor = ev
	hs.h.MaintainCapacity(context.Background())
	if !hs.h.memoryStopped.Load() || ev.calls != 1 {
		t.Errorf("memory stop %v, evictor calls %d", hs.h.memoryStopped.Load(), ev.calls)
	}
	ev.n, ev.err = 2, errors.New("candidate invalid")
	hs.h.MaintainCapacity(context.Background())
	if !strings.Contains(hs.logs.String(), `"event":"dedup_eviction_failed"`) {
		t.Error("eviction failure not logged")
	}

	hs.h.memoryStopped.Store(false)
	hs.acceptor.result = AcceptResult{Outcome: AcceptAccepted, MessageID: "m", EarlyEvicted: 1}
	hs.post("/webhook/telegram/wh_on", chatUpdate)
	families, _ := hs.reg.Gather()
	for _, f := range families {
		if f.GetName() == "hookrelay_dedup_early_evictions_total" {
			if got := f.GetMetric()[0].GetCounter().GetValue(); got != 6 {
				t.Errorf("early evictions = %v, want 3 + 2 + 1", got)
			}
			return
		}
	}
	t.Error("early evictions metric missing")
}

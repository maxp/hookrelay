package delivery

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

// scriptedClaimer answers each check through next and records the calls.
type scriptedClaimer struct {
	mu    sync.Mutex
	calls []ClaimRequest
	next  func(call int) ClaimResult
}

func (s *scriptedClaimer) Claim(_ context.Context, req ClaimRequest) ClaimResult {
	s.mu.Lock()
	s.calls = append(s.calls, req)
	n := len(s.calls)
	s.mu.Unlock()
	return s.next(n)
}

func (s *scriptedClaimer) snapshot() []ClaimRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ClaimRequest(nil), s.calls...)
}

func alwaysEmpty(int) ClaimResult { return ClaimResult{Outcome: ClaimEmpty} }

type pollHarness struct {
	h       *Handler
	claimer *scriptedClaimer
	logs    *syncBuffer
	reg     *prometheus.Registry
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func newPollHarness(t *testing.T, deps HandlerDeps, next func(int) ClaimResult) *pollHarness {
	t.Helper()
	ph := &pollHarness{claimer: &scriptedClaimer{next: next}, logs: &syncBuffer{}, reg: prometheus.NewRegistry()}
	deps.Claimer, deps.Acknowledger, deps.NegativeAcknowledger, deps.RetryPolicy = ph.claimer, &fakeAcker{}, &fakeNacker{}, defaultPolicy()
	deps.Extender = &fakeExtender{}
	deps.ConsumerSecret, deps.Gen, deps.Clock = secret, fixedGen{}, fixedClock{}
	deps.Logger, deps.Registerer = observability.NewTestLogger("debug", ph.logs), ph.reg
	var err error
	if deps.Attempts, err = NewAttemptMetrics(ph.reg); err != nil {
		t.Fatal(err)
	}
	if ph.h, err = NewHandler(deps); err != nil {
		t.Fatal(err)
	}
	return ph
}

func (ph *pollHarness) claim(ctx context.Context, waitMs string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/claim", strings.NewReader(`{"operation_id":"`+opID+`","wait_ms":`+waitMs+`}`)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+secret)
	w := httptest.NewRecorder()
	ph.h.ServeHTTP(w, r)
	return w
}

func (ph *pollHarness) waitingGauge(t *testing.T) float64 {
	t.Helper()
	families, _ := ph.reg.Gather()
	for _, f := range families {
		if f.GetName() == "hookrelay_waiting_claims" {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatal("hookrelay_waiting_claims missing")
	return 0
}

// waitUntil polls cond with a generous bound.
func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestLongPollWorkAppearsMidWait pins that rechecks run on the interval and
// return as soon as work appears, without recording an empty outcome.
func TestLongPollWorkAppearsMidWait(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 20 * time.Millisecond, RecheckJitter: 5 * time.Millisecond}, func(n int) ClaimResult {
		if n < 4 {
			return ClaimResult{Outcome: ClaimEmpty}
		}
		return claimedResult(ClaimClaimed)
	})
	start := time.Now()
	w := ph.claim(context.Background(), "30000")
	elapsed := time.Since(start)
	if w.Code != http.StatusOK {
		t.Fatalf("claim = %d", w.Code)
	}
	calls := ph.claimer.snapshot()
	if len(calls) != 4 {
		t.Fatalf("checks = %d, want 4", len(calls))
	}
	for i, c := range calls {
		if c.RecordEmpty {
			t.Errorf("check %d recorded an empty outcome before the deadline", i)
		}
		if c.Token != calls[0].Token || c.OperationID != opID {
			t.Errorf("check %d changed the operation or token", i)
		}
	}
	// Three pauses of 20–25 ms: tolerant window.
	if elapsed < 55*time.Millisecond || elapsed > time.Second {
		t.Errorf("returned after %v", elapsed)
	}
}

// TestLongPollDeadline pins the empty completion at the deadline: 204, only
// the final check records the empty outcome, no per-request log.
func TestLongPollDeadline(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 40 * time.Millisecond, RecheckJitter: 10 * time.Millisecond}, alwaysEmpty)
	start := time.Now()
	w := ph.claim(context.Background(), "200")
	elapsed := time.Since(start)
	if w.Code != http.StatusNoContent || w.Body.Len() != 0 {
		t.Fatalf("deadline = %d %q", w.Code, w.Body.String())
	}
	if elapsed < 190*time.Millisecond || elapsed > 800*time.Millisecond {
		t.Errorf("completed after %v, want about 200 ms", elapsed)
	}
	calls := ph.claimer.snapshot()
	if len(calls) < 3 || len(calls) > 7 {
		t.Errorf("checks = %d, want about 200/(40..50)+1", len(calls))
	}
	for i, c := range calls {
		if c.RecordEmpty != (i == len(calls)-1) {
			t.Errorf("check %d RecordEmpty = %v", i, c.RecordEmpty)
		}
	}
	if ph.logs.String() != "" {
		t.Errorf("empty long poll logged: %s", ph.logs.String())
	}
}

// TestLongPollDefaultCadence pins the default 250 ms + 0–50 ms recheck.
func TestLongPollDefaultCadence(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	ph := newPollHarness(t, HandlerDeps{}, alwaysEmpty)
	w := ph.claim(context.Background(), "1000")
	if w.Code != http.StatusNoContent {
		t.Fatalf("= %d", w.Code)
	}
	// 1000 ms / 250–300 ms pauses: 4–5 checks including the final one.
	if n := len(ph.claimer.snapshot()); n < 4 || n > 6 {
		t.Errorf("checks = %d, want 4–6 for the default cadence", n)
	}
}

// TestLongPollCancellation pins that a client leaving abandons the wait
// without a final (recording) check.
func TestLongPollCancellation(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 20 * time.Millisecond, RecheckJitter: 1}, alwaysEmpty)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ph.claim(ctx, "30000")
		close(done)
	}()
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) >= 2 })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait not abandoned after cancellation")
	}
	for _, c := range ph.claimer.snapshot() {
		if c.RecordEmpty {
			t.Error("cancelled wait recorded an empty outcome")
		}
	}
	if got := ph.waitingGauge(t); got != 0 {
		t.Errorf("waiting gauge after cancellation = %v", got)
	}
}

// TestWaitingClaimLimit pins the process-local limit, the gauge, and that
// immediate claims are not limited.
func TestWaitingClaimLimit(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{MaxWaitingClaims: 1, RecheckInterval: 20 * time.Millisecond, RecheckJitter: 1}, alwaysEmpty)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		ph.claim(ctx, "30000")
		close(done)
	}()
	waitUntil(t, func() bool { return ph.waitingGauge(t) == 1 })

	w := ph.claim(context.Background(), "5000")
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "1" || !strings.Contains(w.Body.String(), "consumer_limit_exceeded") {
		t.Errorf("over the waiting limit = %d %s", w.Code, w.Body.String())
	}
	if w := ph.claim(context.Background(), "0"); w.Code != http.StatusNoContent {
		t.Errorf("immediate claim under the waiting limit = %d", w.Code)
	}
	cancel()
	<-done
}

// TestLongPollShutdown pins that shutdown ends waiting claims with 503 +
// Retry-After and refuses new ones.
func TestLongPollShutdown(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 20 * time.Millisecond, RecheckJitter: 1}, alwaysEmpty)
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- ph.claim(context.Background(), "30000") }()
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) >= 1 })
	ph.h.Shutdown()
	select {
	case w := <-result:
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
			t.Errorf("waiting claim at shutdown = %d", w.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting claim not ended by shutdown")
	}
	if w := ph.claim(context.Background(), "0"); w.Code != http.StatusServiceUnavailable {
		t.Errorf("claim after shutdown = %d", w.Code)
	}
	ph.h.Shutdown() // idempotent
}

// ctxAwareClaimer blocks inside the check until its context ends and then
// fails like a cancelled Valkey call.
type ctxAwareClaimer struct{ entered chan struct{} }

func (c *ctxAwareClaimer) Claim(ctx context.Context, _ ClaimRequest) ClaimResult {
	close(c.entered)
	<-ctx.Done()
	return ClaimResult{Outcome: ClaimDependencyUnavailable}
}

// TestClientCancelDuringCheck pins that a client leaving while a check is
// in flight is a cancellation, not a dependency failure: no response body,
// no error log, and the cancelled outcome is counted.
func TestClientCancelDuringCheck(t *testing.T) {
	ph := newPollHarness(t, HandlerDeps{}, alwaysEmpty)
	blocker := &ctxAwareClaimer{entered: make(chan struct{})}
	ph.h.d.Claimer = blocker
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- ph.claim(ctx, "0") }()
	<-blocker.entered
	cancel()
	w := <-result
	if w.Body.Len() != 0 {
		t.Errorf("cancelled check wrote a response: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(ph.logs.String(), "delivery_claim_failed") {
		t.Errorf("cancellation logged as a failure: %s", ph.logs.String())
	}
	families, _ := ph.reg.Gather()
	counts := map[string]float64{}
	for _, f := range families {
		if f.GetName() == "hookrelay_delivery_claims_total" {
			for _, m := range f.GetMetric() {
				counts[m.GetLabel()[0].GetValue()] = m.GetCounter().GetValue()
			}
		}
	}
	if counts["cancelled"] != 1 || counts["dependency_unavailable"] != 0 {
		t.Errorf("claim outcomes = %v", counts)
	}
}

// fakeInline records inline passes and the number of claim checks run
// before each.
type fakeInline struct {
	mu      sync.Mutex
	claimer *scriptedClaimer
	passes  []int
}

func (f *fakeInline) InlinePass(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passes = append(f.passes, len(f.claimer.snapshot()))
}

func (f *fakeInline) snapshot() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.passes...)
}

// TestLongPollInlinePassBeforeWaiting pins the bounded self-healing path:
// a waiting claim whose first check is empty runs one inline maintenance
// pass and rechecks at once (without recording empty), so a due retry is
// claimed by the very claim that found the index empty.
func TestLongPollInlinePassBeforeWaiting(t *testing.T) {
	inline := &fakeInline{}
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 500 * time.Millisecond, RecheckJitter: 1, InlineMaintenance: inline}, func(n int) ClaimResult {
		if n == 1 {
			return ClaimResult{Outcome: ClaimEmpty}
		}
		return claimedResult(ClaimClaimed)
	})
	inline.claimer = ph.claimer
	start := time.Now()
	w := ph.claim(context.Background(), "30000")
	if w.Code != http.StatusOK {
		t.Fatalf("claim = %d", w.Code)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Errorf("returned after %v; the recheck after the inline pass must not wait for the interval", elapsed)
	}
	if passes := inline.snapshot(); len(passes) != 1 || passes[0] != 1 {
		t.Errorf("inline passes = %v, want one after the first check", passes)
	}
	calls := ph.claimer.snapshot()
	if len(calls) != 2 || calls[1].RecordEmpty {
		t.Errorf("checks = %+v, want an immediate non-recording recheck", calls)
	}
}

// TestLongPollInlinePassOnlyOnce pins that the pass runs once per claim and
// the claim then waits normally; a wait_ms=0 claim never runs it.
func TestLongPollInlinePassOnlyOnce(t *testing.T) {
	inline := &fakeInline{}
	ph := newPollHarness(t, HandlerDeps{RecheckInterval: 20 * time.Millisecond, RecheckJitter: 1, InlineMaintenance: inline}, alwaysEmpty)
	inline.claimer = ph.claimer
	if w := ph.claim(context.Background(), "150"); w.Code != http.StatusNoContent {
		t.Fatalf("claim = %d", w.Code)
	}
	if passes := inline.snapshot(); len(passes) != 1 {
		t.Errorf("inline passes = %v, want exactly one", passes)
	}
	if calls := ph.claimer.snapshot(); len(calls) < 4 || !calls[len(calls)-1].RecordEmpty {
		t.Errorf("checks = %d, want the normal waiting loop ending in a recorded empty", len(calls))
	}

	inline = &fakeInline{}
	ph = newPollHarness(t, HandlerDeps{InlineMaintenance: inline}, alwaysEmpty)
	inline.claimer = ph.claimer
	if w := ph.claim(context.Background(), "0"); w.Code != http.StatusNoContent {
		t.Fatalf("claim = %d", w.Code)
	}
	if passes := inline.snapshot(); len(passes) != 0 {
		t.Errorf("wait_ms=0 ran inline passes: %v", passes)
	}
}

// blockingInline runs its hook inside the pass.
type blockingInline struct{ during func() }

func (b blockingInline) InlinePass(context.Context) { b.during() }

// TestLongPollInlinePassThenStopOrLeave pins that a claim does not recheck
// after the pass when shutdown began (503) or the client left (no
// response, cancelled) meanwhile.
func TestLongPollInlinePassThenStopOrLeave(t *testing.T) {
	var ph *pollHarness
	ph = newPollHarness(t, HandlerDeps{InlineMaintenance: blockingInline{during: func() { ph.h.Shutdown() }}}, alwaysEmpty)
	if w := ph.claim(context.Background(), "30000"); w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Errorf("claim during shutdown = %d", w.Code)
	}
	if n := len(ph.claimer.snapshot()); n != 1 {
		t.Errorf("checks = %d, want no recheck after shutdown began", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ph = newPollHarness(t, HandlerDeps{InlineMaintenance: blockingInline{during: cancel}}, alwaysEmpty)
	w := ph.claim(ctx, "30000")
	if n := len(ph.claimer.snapshot()); n != 1 || w.Body.Len() != 0 {
		t.Errorf("checks = %d, body %q; want no recheck and no response after the client left", n, w.Body.String())
	}
}

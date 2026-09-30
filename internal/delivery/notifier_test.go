package delivery

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// counterValue reads one counter series.
func counterValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// seriesCount counts the series a collector currently exposes.
func seriesCount(c prometheus.Collector) int {
	ch := make(chan prometheus.Metric, 64)
	go func() { c.Collect(ch); close(ch) }()
	n := 0
	for range ch {
		n++
	}
	return n
}

func newTestNotifier(t *testing.T, enabled bool) (*ReadyNotifier, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	n, err := NewReadyNotifier(enabled, reg)
	if err != nil {
		t.Fatal(err)
	}
	return n, reg
}

func woken(w *readyWaiter) bool {
	select {
	case <-w.wake():
		return true
	default:
		return false
	}
}

// TestNotifierWakesOldestOnce pins FIFO single wake-up: each signal wakes
// exactly the oldest registered waiter, once.
func TestNotifierWakesOldestOnce(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	a, b := n.register(), n.register()
	n.Signal(SignalAccept)
	if !woken(a) || woken(b) {
		t.Fatal("first signal must wake only the oldest waiter")
	}
	n.Signal(SignalAccept)
	if woken(a) || !woken(b) {
		t.Fatal("second signal must wake the next waiter, never the spent one")
	}
	n.Signal(SignalAccept) // nobody waits
	if got := counterValue(t, n.signals, SignalAccept, "delivered"); got != 2 {
		t.Errorf("delivered = %v, want 2", got)
	}
	if got := counterValue(t, n.signals, SignalAccept, "no_waiter"); got != 1 {
		t.Errorf("no_waiter = %v, want 1", got)
	}
}

// TestNotifierDropsWithoutWaiter pins that no credit is stored: a waiter
// registered after an unheard signal is not woken by it.
func TestNotifierDropsWithoutWaiter(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	n.Signal(SignalAck)
	w := n.register()
	if woken(w) {
		t.Fatal("a dropped signal woke a later waiter")
	}
}

// TestNotifierDeregisterRemovesWaiter pins that a deregistered waiter no
// longer absorbs signals.
func TestNotifierDeregisterRemovesWaiter(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	a, b := n.register(), n.register()
	n.deregister(a)
	n.Signal(SignalAccept)
	if !woken(b) {
		t.Fatal("the signal went to a deregistered waiter")
	}
}

// TestNotifierHandsOnUnconsumedWake pins that a waiter leaving with an
// unconsumed wake passes it to the next waiter instead of losing it.
func TestNotifierHandsOnUnconsumedWake(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	a, b := n.register(), n.register()
	n.Signal(SignalAccept)
	n.deregister(a) // a hit its deadline before reading the wake
	if !woken(b) {
		t.Fatal("the unconsumed wake was lost")
	}
	n.deregister(b) // consumed: nothing to hand on
	c := n.register()
	if woken(c) {
		t.Fatal("a consumed wake was handed on")
	}
}

// TestNotifierDisabledAndNil pins the rollback modes: no registration, no
// wake, no metric.
func TestNotifierDisabledAndNil(t *testing.T) {
	n, _ := newTestNotifier(t, false)
	if w := n.register(); w != nil {
		t.Fatal("disabled notifier registered a waiter")
	}
	n.Signal(SignalAccept)
	if got := seriesCount(n.signals); got != 0 {
		t.Errorf("disabled notifier counted %d series", got)
	}
	var nilN *ReadyNotifier
	nilN.Signal(SignalAccept)
	nilN.deregister(nilN.register())
	var w *readyWaiter
	if w.wake() != nil {
		t.Fatal("nil waiter channel must be nil")
	}
}

// TestNotifierConcurrent exercises registration, signals, and departures
// under the race detector; every signal is either delivered or counted as
// dropped.
func TestNotifierConcurrent(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	var wg sync.WaitGroup
	var got atomic.Int64
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				w := n.register()
				select {
				case <-w.wake():
					got.Add(1)
				case <-time.After(time.Microsecond):
				}
				n.deregister(w)
			}
		}()
	}
	for range 1000 {
		n.Signal(SignalAccept)
	}
	wg.Wait()
	delivered := counterValue(t, n.signals, SignalAccept, "delivered")
	dropped := counterValue(t, n.signals, SignalAccept, "no_waiter")
	if delivered+dropped != 1000 {
		t.Errorf("delivered %v + dropped %v != 1000", delivered, dropped)
	}
}

// TestClaimWakesOnSignal pins that a signal ends the wait long before the
// periodic recheck, and that the woken recheck is counted as a
// notification wake-up.
func TestClaimWakesOnSignal(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	var ready atomic.Bool
	ph := newPollHarness(t, HandlerDeps{Notifier: n, RecheckInterval: time.Hour}, func(int) ClaimResult {
		if ready.Load() {
			return claimedResult(ClaimClaimed)
		}
		return ClaimResult{Outcome: ClaimEmpty}
	})
	done := make(chan int, 1)
	start := time.Now()
	go func() { done <- ph.claim(context.Background(), "30000").Code }()
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) == 1 })
	ready.Store(true)
	n.Signal(SignalAccept)
	select {
	case code := <-done:
		if code != http.StatusOK {
			t.Fatalf("claim = %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the signal did not wake the claim")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("woken claim took %s", elapsed)
	}
	if got := counterValue(t, ph.h.metrics.wakeups, "notification", "claimed"); got != 1 {
		t.Errorf("notification wake-ups = %v, want 1", got)
	}
	if got := counterValue(t, n.signals, SignalAccept, "delivered"); got != 1 {
		t.Errorf("delivered signals = %v, want 1", got)
	}
}

// TestClaimFalsePositiveKeepsWaiting pins that an empty wake re-registers:
// a second signal wakes the same claim again, and the deadline still ends
// it with 204.
func TestClaimFalsePositiveKeepsWaiting(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	ph := newPollHarness(t, HandlerDeps{Notifier: n, RecheckInterval: time.Hour}, alwaysEmpty)
	done := make(chan int, 1)
	go func() { done <- ph.claim(context.Background(), "1000").Code }()
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) == 1 })
	n.Signal(SignalAccept)
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) == 2 })
	n.Signal(SignalAccept)
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) == 3 })
	select {
	case code := <-done:
		if code != http.StatusNoContent {
			t.Fatalf("claim = %d, want 204", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the deadline did not end the claim")
	}
	calls := ph.claimer.snapshot()
	if len(calls) != 4 || !calls[3].RecordEmpty {
		t.Fatalf("checks = %d; the final deadline check must record empty", len(calls))
	}
	if got := counterValue(t, ph.h.metrics.wakeups, "notification", "empty"); got != 2 {
		t.Errorf("empty notification wake-ups = %v, want 2", got)
	}
	if got := counterValue(t, n.signals, SignalAccept, "delivered"); got != 2 {
		t.Errorf("delivered = %v, want 2", got)
	}
}

// TestClaimLeavesNoRegistration pins that a finished claim deregisters, so
// a later signal finds nobody waiting.
func TestClaimLeavesNoRegistration(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	ph := newPollHarness(t, HandlerDeps{Notifier: n, RecheckInterval: 10 * time.Millisecond}, alwaysEmpty)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { ph.claim(ctx, "30000"); close(done) }()
	waitUntil(t, func() bool { return len(ph.claimer.snapshot()) >= 1 })
	cancel()
	<-done
	if code := ph.claim(context.Background(), "20").Code; code != http.StatusNoContent {
		t.Fatalf("short claim = %d", code)
	}
	n.Signal(SignalAck)
	if got := counterValue(t, n.signals, SignalAck, "no_waiter"); got != 1 {
		t.Errorf("a finished claim still absorbed a signal")
	}
	// One cancelled and one empty waiting claim were timed.
	if got := seriesCount(ph.h.metrics.waitDuration); got != 2 {
		t.Errorf("wait-duration series = %d, want 2 (cancelled, empty)", got)
	}
}

// TestClaimPeriodicWithoutNotifier pins the rollback path: with
// notifications off the periodic recheck still finds work.
func TestClaimPeriodicWithoutNotifier(t *testing.T) {
	n, _ := newTestNotifier(t, false)
	ph := newPollHarness(t, HandlerDeps{Notifier: n, RecheckInterval: 10 * time.Millisecond}, func(c int) ClaimResult {
		if c < 3 {
			return ClaimResult{Outcome: ClaimEmpty}
		}
		return claimedResult(ClaimClaimed)
	})
	if code := ph.claim(context.Background(), "30000").Code; code != http.StatusOK {
		t.Fatalf("claim = %d", code)
	}
	if got := counterValue(t, ph.h.metrics.wakeups, "periodic", "claimed"); got != 1 {
		t.Errorf("periodic claimed wake-ups = %v, want 1", got)
	}
	if got := counterValue(t, ph.h.metrics.wakeups, "periodic", "empty"); got != 1 {
		t.Errorf("periodic empty wake-ups = %v, want 1", got)
	}
}

// signalCount is the number of signals from source, delivered or dropped.
func signalCount(t *testing.T, n *ReadyNotifier, source string) float64 {
	t.Helper()
	return counterValue(t, n.signals, source, "delivered") + counterValue(t, n.signals, source, "no_waiter")
}

// TestConsumerAPISignals pins the Consumer API sources: a first ack and a
// dead-lettering nack signal; a repeated ack and a retry-scheduling nack
// do not (the head is not ready).
func TestConsumerAPISignals(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	hs := newHarness(t)
	hs.h.d.Notifier = n
	body := `{"delivery_token":"` + validToken + `"}`

	hs.acker.result = AckResult{Outcome: AckAcknowledged, MessageID: "m1", AcknowledgedMs: 500, RecipientIdentity: "telegram:42:chat:-1", DeliveryCycle: 1, Attempt: 1}
	hs.ack(body)
	hs.acker.result = AckResult{Outcome: AckAlreadyAcknowledged, MessageID: "m1", AcknowledgedMs: 500}
	hs.ack(body)
	if got := signalCount(t, n, SignalAck); got != 1 {
		t.Errorf("ack signals = %v, want 1", got)
	}

	hs.nacker.result = scheduled()
	hs.nack(body)
	if got := signalCount(t, n, SignalDeadLetter); got != 0 {
		t.Errorf("a retry-scheduling nack signalled")
	}
	hs.nacker.result = NackResult{
		Outcome: NackDeadLettered, Result: "dead_lettered", MessageID: "m1", DeliveryCycle: 1, DeadLetteredMs: 9000,
		RecipientIdentity: "telegram:42:chat:-1", Attempt: 4, ClaimedMs: 1000, CompletedMs: 9000,
	}
	hs.nack(body)
	if got := signalCount(t, n, SignalDeadLetter); got != 1 {
		t.Errorf("dead-letter signals = %v, want 1", got)
	}
}

// TestMaintenanceSignals pins the maintenance sources: each activated retry
// and each dead-lettering expiry signals; a retry-scheduling expiry and
// stale, blocked, or failed transitions do not.
func TestMaintenanceSignals(t *testing.T) {
	n, _ := newTestNotifier(t, true)
	outcomes := map[string]ExpiryOutcome{
		"telegram:42:chat:0": ExpiryRetryScheduled,
		"telegram:42:chat:1": ExpiryDeadLettered,
		"telegram:42:chat:2": ExpiryNotDue,
		"telegram:42:chat:3": ExpiryRecipientBlocked,
	}
	expirer := &fakeExpirer{nowMs: 10_000, pending: dueEntries(4, 9_000), outcome: func(rid string) ExpiryOutcome { return outcomes[rid] }}
	activations := map[string]ActivationOutcome{
		"telegram:42:chat:0": ActivationActivated,
		"telegram:42:chat:1": ActivationActivated,
		"telegram:42:chat:2": ActivationNotDue,
		"telegram:42:chat:3": ActivationRecipientBlocked,
	}
	activator := &fakeActivator{nowMs: 10_000, pending: dueEntries(4, 9_000), outcome: func(rid string) ActivationOutcome { return activations[rid] }}
	mh := newExpiryHarness(t, activator, expirer, nil)
	mh.m.d.Notifier = n
	mh.m.RunRound(context.Background())
	if got := signalCount(t, n, SignalRetryActivation); got != 2 {
		t.Errorf("retry-activation signals = %v, want 2", got)
	}
	if got := signalCount(t, n, SignalDeadLetter); got != 1 {
		t.Errorf("dead-letter signals = %v, want 1", got)
	}
}

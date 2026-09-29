package delivery

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

// fakeActivator serves due retries from a pending list, oldest first, and
// removes each one it activates.
type fakeActivator struct {
	nowMs     int64
	pending   []DueEntry
	reads     []int
	activated []string
	outcome   func(rid string) ActivationOutcome
	readErr   error
	onFirst   func()
}

func (f *fakeActivator) DueRetries(_ context.Context, limit int) (DueBatch, error) {
	f.reads = append(f.reads, limit)
	if f.readErr != nil {
		return DueBatch{}, f.readErr
	}
	n := min(limit, len(f.pending))
	return DueBatch{NowMs: f.nowMs, Entries: append([]DueEntry(nil), f.pending[:n]...)}, nil
}

func (f *fakeActivator) ActivateRetry(_ context.Context, rid string) ActivationResult {
	if f.onFirst != nil && len(f.activated) == 0 {
		f.onFirst()
	}
	f.activated = append(f.activated, rid)
	for i, e := range f.pending {
		if e.RecipientIdentity == rid {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			break
		}
	}
	if f.outcome != nil {
		return ActivationResult{Outcome: f.outcome(rid)}
	}
	return ActivationResult{Outcome: ActivationActivated, MessageID: "m-" + rid, Attempt: 2}
}

func dueEntries(n int, firstDueMs int64) []DueEntry {
	out := make([]DueEntry, n)
	for i := range out {
		out[i] = DueEntry{RecipientIdentity: fmt.Sprintf("telegram:42:chat:%d", i), DueMs: firstDueMs + int64(i)}
	}
	return out
}

type maintenanceHarness struct {
	m          *Maintenance
	fake       *fakeActivator
	reg        *prometheus.Registry
	logs       *bytes.Buffer
	sleeps     []time.Duration
	clockCalls int
}

type countingClock struct{ calls *int }

func (c countingClock) Now() time.Time {
	*c.calls++
	return time.UnixMilli(1740000000000)
}

func newMaintenanceHarness(t *testing.T, fake *fakeActivator, sleep func(ctx context.Context, d time.Duration) error) *maintenanceHarness {
	t.Helper()
	mh := &maintenanceHarness{fake: fake, reg: prometheus.NewRegistry(), logs: &bytes.Buffer{}}
	if sleep == nil {
		sleep = func(ctx context.Context, d time.Duration) error { return ctx.Err() }
	}
	var err error
	mh.m, err = NewMaintenance(MaintenanceDeps{
		Retries: fake,
		Config:  MaintenanceConfig{Interval: time.Second, IntervalJitter: 250 * time.Millisecond, BatchSize: 100, MaxContinuousBatches: 5},
		Uniform: func() float64 { return 0.5 },
		Clock:   countingClock{calls: &mh.clockCalls},
		Sleep: func(ctx context.Context, d time.Duration) error {
			mh.sleeps = append(mh.sleeps, d)
			return sleep(ctx, d)
		},
		Logger:     observability.NewTestLogger("debug", mh.logs),
		Registerer: mh.reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return mh
}

func (mh *maintenanceHarness) value(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := mh.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want != lp.GetValue() {
					continue next
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return -1
}

var retryKind = map[string]string{"kind": "retry_activation"}

// TestMaintenanceRoundActivatesInBatches pins one round: bounded batches
// oldest first, another batch only after a full one, the due lag from the
// oldest due entry, and the per-batch size and duration metrics.
func TestMaintenanceRoundActivatesInBatches(t *testing.T) {
	fake := &fakeActivator{nowMs: 10_000, pending: dueEntries(250, 7_500)}
	mh := newMaintenanceHarness(t, fake, nil)
	mh.m.RunRound(context.Background())

	if len(fake.activated) != 250 || fake.activated[0] != "telegram:42:chat:0" {
		t.Fatalf("activated %d, first %v", len(fake.activated), fake.activated[:1])
	}
	if len(fake.reads) != 3 || fake.reads[0] != 100 {
		t.Errorf("reads = %v, want three batches of limit 100", fake.reads)
	}
	if got := mh.value(t, "hookrelay_maintenance_processed_total", map[string]string{"kind": "retry_activation", "result": "applied"}); got != 250 {
		t.Errorf("processed{applied} = %v", got)
	}
	if got := mh.value(t, "hookrelay_maintenance_due_lag_seconds", retryKind); got != 2.5 {
		t.Errorf("due lag = %v, want 2.5 s from the oldest due entry", got)
	}
	if got := mh.value(t, "hookrelay_maintenance_batch_size", retryKind); got != 3 {
		t.Errorf("batch size samples = %v, want 3", got)
	}
	if got := mh.value(t, "hookrelay_maintenance_duration_seconds", retryKind); got != 3 {
		t.Errorf("duration samples = %v, want 3", got)
	}
	if mh.clockCalls == 0 {
		t.Error("batch duration not measured with the injected Clock")
	}

	// Nothing due: the lag gauge returns to zero.
	mh.m.RunRound(context.Background())
	if got := mh.value(t, "hookrelay_maintenance_due_lag_seconds", retryKind); got != 0 {
		t.Errorf("due lag with nothing due = %v", got)
	}
}

// TestMaintenanceRoundYieldsAfterMaxContinuousBatches pins the yield: a
// round takes at most five full batches, the next round continues.
func TestMaintenanceRoundYieldsAfterMaxContinuousBatches(t *testing.T) {
	fake := &fakeActivator{nowMs: 10_000, pending: dueEntries(700, 9_000)}
	mh := newMaintenanceHarness(t, fake, nil)
	mh.m.RunRound(context.Background())
	if len(fake.activated) != 500 || len(fake.reads) != 5 {
		t.Fatalf("first round activated %d in %d batches, want 500 in 5", len(fake.activated), len(fake.reads))
	}
	mh.m.RunRound(context.Background())
	if len(fake.activated) != 700 {
		t.Errorf("second round total = %d, want 700", len(fake.activated))
	}
}

// TestMaintenanceRoundOutcomes pins the bounded result labels and that a
// failed read ends the round without activation.
func TestMaintenanceRoundOutcomes(t *testing.T) {
	outcomes := map[string]ActivationOutcome{
		"telegram:42:chat:0": ActivationActivated,
		"telegram:42:chat:1": ActivationNotDue,
		"telegram:42:chat:2": ActivationRecipientBlocked,
		"telegram:42:chat:3": ActivationInternalFailure,
		"telegram:42:chat:4": ActivationDependencyUnavailable,
	}
	fake := &fakeActivator{nowMs: 10_000, pending: dueEntries(5, 9_000), outcome: func(rid string) ActivationOutcome { return outcomes[rid] }}
	mh := newMaintenanceHarness(t, fake, nil)
	mh.m.RunRound(context.Background())
	for result, want := range map[string]float64{"applied": 1, "stale": 1, "blocked": 1, "failed": 2} {
		if got := mh.value(t, "hookrelay_maintenance_processed_total", map[string]string{"kind": "retry_activation", "result": result}); got != want {
			t.Errorf("processed{%s} = %v, want %v", result, got, want)
		}
	}
	if !bytes.Contains(mh.logs.Bytes(), []byte(`"event":"maintenance_transition_failed"`)) {
		t.Errorf("failed transitions not logged:\n%s", mh.logs.String())
	}

	failing := &fakeActivator{readErr: errors.New("connection refused")}
	mh = newMaintenanceHarness(t, failing, nil)
	mh.m.RunRound(context.Background())
	if len(failing.reads) != 1 || len(failing.activated) != 0 {
		t.Errorf("failed read: reads %d, activated %d", len(failing.reads), len(failing.activated))
	}
	if got := mh.value(t, "hookrelay_maintenance_processed_total", map[string]string{"kind": "retry_activation", "result": "failed"}); got != 1 {
		t.Errorf("failed read counted %v times, want 1", got)
	}
	if !bytes.Contains(mh.logs.Bytes(), []byte(`"event":"maintenance_read_failed"`)) || bytes.Contains(mh.logs.Bytes(), []byte("connection refused")) {
		t.Errorf("read failure log = %s", mh.logs.String())
	}
}

// TestMaintenanceRunPacesRoundsAndStops pins the interval plus jitter
// between rounds and that Run returns when its context ends.
func TestMaintenanceRunPacesRoundsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeActivator{nowMs: 10_000}
	calls := 0
	mh := newMaintenanceHarness(t, fake, func(ctx context.Context, d time.Duration) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return ctx.Err()
	})
	done := make(chan struct{})
	go func() { mh.m.Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
	if len(fake.reads) != 2 {
		t.Errorf("rounds = %d, want 2", len(fake.reads))
	}
	for _, d := range mh.sleeps {
		if d != 1125*time.Millisecond {
			t.Errorf("pause = %s, want interval + 0.5 × jitter = 1.125 s", d)
		}
	}
}

// TestMaintenanceStopsTakingBatchesWhenCancelled pins shutdown: the batch in
// progress completes (its transitions are atomic), no new batch starts.
func TestMaintenanceStopsTakingBatchesWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &fakeActivator{nowMs: 10_000, pending: dueEntries(250, 9_000), onFirst: cancel}
	mh := newMaintenanceHarness(t, fake, nil)
	mh.m.RunRound(ctx)
	if len(fake.activated) != 100 || len(fake.reads) != 1 {
		t.Errorf("activated %d in %d batches after cancellation, want the one batch in progress", len(fake.activated), len(fake.reads))
	}
}

package delivery

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

type dlqExpiry struct {
	messageID string
	retention time.Duration
	eventID   string
}

// fakeDLQ serves dead letters past their retention, oldest first, and
// removes each one it expires.
type fakeDLQ struct {
	nowMs      int64
	pending    []DueEntry
	reads      []int
	retentions []time.Duration
	expired    []dlqExpiry
	outcome    func(id string) DLQExpiryOutcome
}

func (f *fakeDLQ) DueDeadLetters(_ context.Context, limit int, retention time.Duration) (DueBatch, error) {
	f.reads = append(f.reads, limit)
	f.retentions = append(f.retentions, retention)
	n := min(limit, len(f.pending))
	return DueBatch{NowMs: f.nowMs, Entries: append([]DueEntry(nil), f.pending[:n]...)}, nil
}

func (f *fakeDLQ) ExpireDeadLetter(_ context.Context, id string, retention time.Duration, eventID string) DLQExpiryResult {
	f.expired = append(f.expired, dlqExpiry{id, retention, eventID})
	for i, e := range f.pending {
		if e.MessageID == id {
			f.pending = append(f.pending[:i], f.pending[i+1:]...)
			break
		}
	}
	if f.outcome != nil {
		if o := f.outcome(id); o != DLQExpiryExpired {
			return DLQExpiryResult{Outcome: o}
		}
	}
	return DLQExpiryResult{Outcome: DLQExpiryExpired, RecipientIdentity: "telegram:42:chat:-100", Reason: "nack_exhausted",
		DeadLetteredMs: 1_000, ExpiredMs: 9_000}
}

type fixedIDs struct{}

func (fixedIDs) UUIDv7() string                { return "0195-event" }
func (fixedIDs) Base64URL(int) (string, error) { return "x", nil }

func newDLQHarness(t *testing.T, dlq DeadLetterExpirer) (*maintenanceHarness, error) {
	t.Helper()
	mh := &maintenanceHarness{fake: &fakeActivator{}, expirer: &fakeExpirer{}, reg: prometheus.NewRegistry(), logs: &bytes.Buffer{}}
	attempts, err := NewAttemptMetrics(mh.reg)
	if err != nil {
		t.Fatal(err)
	}
	mh.m, err = NewMaintenance(MaintenanceDeps{
		Retries: mh.fake, Leases: mh.expirer, RetryPolicy: defaultPolicy(), Attempts: attempts,
		DeadLetters: dlq, DLQRetention: 720 * time.Hour, Gen: fixedIDs{},
		Config:     MaintenanceConfig{Interval: time.Second, BatchSize: 100, MaxContinuousBatches: 5},
		Clock:      countingClock{calls: &mh.clockCalls},
		Logger:     observability.NewTestLogger("debug", mh.logs),
		Registerer: mh.reg,
	})
	return mh, err
}

// TestMaintenanceDLQRetention pins the dlq_retention kind: bounded batches
// after leases and retries, the configured retention and a fresh audit
// event id per deletion, the due lag from the oldest retention deadline,
// the bounded results, and the dead_letter_expired event.
func TestMaintenanceDLQRetention(t *testing.T) {
	pending := make([]DueEntry, 150)
	for i := range pending {
		pending[i] = DueEntry{MessageID: fmt.Sprintf("m%03d", i), DueMs: 6_000 + int64(i)}
	}
	dlq := &fakeDLQ{nowMs: 10_000, pending: pending, outcome: func(id string) DLQExpiryOutcome {
		switch id {
		case "m001":
			return DLQExpiryStale
		case "m002":
			return DLQExpiryNotDue
		case "m003":
			return DLQExpiryInternalFailure
		}
		return DLQExpiryExpired
	}}
	mh, err := newDLQHarness(t, dlq)
	if err != nil {
		t.Fatal(err)
	}
	mh.m.RunRound(context.Background())

	if len(dlq.expired) != 150 || dlq.expired[0].messageID != "m000" || dlq.expired[0].retention != 720*time.Hour || dlq.expired[0].eventID != "0195-event" {
		t.Fatalf("expired %d, first %+v", len(dlq.expired), dlq.expired[0])
	}
	if len(dlq.reads) != 2 || dlq.reads[0] != 100 || dlq.retentions[0] != 720*time.Hour {
		t.Errorf("reads = %v retentions = %v", dlq.reads, dlq.retentions)
	}
	kind := map[string]string{"kind": "dlq_retention"}
	for result, want := range map[string]float64{"applied": 147, "stale": 2, "failed": 1} {
		if got := mh.value(t, "hookrelay_maintenance_processed_total", map[string]string{"kind": "dlq_retention", "result": result}); got != want {
			t.Errorf("processed{%s} = %v, want %v", result, got, want)
		}
	}
	if got := mh.value(t, "hookrelay_maintenance_due_lag_seconds", kind); got != 4 {
		t.Errorf("due lag = %v, want 4 s past the oldest retention deadline", got)
	}
	if got := mh.value(t, "hookrelay_maintenance_batch_size", kind); got != 2 {
		t.Errorf("batch samples = %v", got)
	}
	if !strings.Contains(mh.logs.String(), `"event":"dead_letter_expired"`) || !strings.Contains(mh.logs.String(), `"chat_id":"-100"`) ||
		!strings.Contains(mh.logs.String(), `"dead_letter_reason":"nack_exhausted"`) {
		t.Errorf("logs = %s", mh.logs.String())
	}
	if !strings.Contains(mh.logs.String(), `"event":"maintenance_transition_failed"`) {
		t.Error("failed deletion not logged")
	}
}

// TestMaintenanceDLQRetentionOptional pins that DLQ retention is skipped
// without an expirer and that a non-positive retention is rejected.
func TestMaintenanceDLQRetentionOptional(t *testing.T) {
	mh, err := newDLQHarness(t, nil)
	if err != nil {
		t.Fatal(err)
	}
	mh.m.RunRound(context.Background())
	if got := mh.value(t, "hookrelay_maintenance_batch_size", map[string]string{"kind": "dlq_retention"}); got != -1 {
		t.Errorf("dlq_retention ran without an expirer: %v", got)
	}

	mh = &maintenanceHarness{reg: prometheus.NewRegistry()}
	attempts, _ := NewAttemptMetrics(mh.reg)
	_, err = NewMaintenance(MaintenanceDeps{
		Retries: &fakeActivator{}, Leases: &fakeExpirer{}, RetryPolicy: defaultPolicy(), Attempts: attempts,
		DeadLetters: &fakeDLQ{}, Config: MaintenanceConfig{Interval: time.Second, BatchSize: 1, MaxContinuousBatches: 1},
	})
	if err == nil || !strings.Contains(err.Error(), "DLQ retention") {
		t.Errorf("zero retention accepted: %v", err)
	}
}

// TestMaintenanceRoundHooks pins that every hook runs once per round after
// the kinds, with a deadline, and that a failing hook is logged without
// stopping the others.
func TestMaintenanceRoundHooks(t *testing.T) {
	mh := &maintenanceHarness{reg: prometheus.NewRegistry(), logs: &bytes.Buffer{}}
	attempts, _ := NewAttemptMetrics(mh.reg)
	var calls []string
	m, err := NewMaintenance(MaintenanceDeps{
		Retries: &fakeActivator{}, Leases: &fakeExpirer{}, RetryPolicy: defaultPolicy(), Attempts: attempts,
		Config: MaintenanceConfig{Interval: time.Second, BatchSize: 1, MaxContinuousBatches: 1},
		RoundHooks: []func(context.Context) error{
			func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); !ok {
					t.Error("hook without a deadline")
				}
				calls = append(calls, "sample")
				return fmt.Errorf("info failed")
			},
			func(context.Context) error { calls = append(calls, "capacity"); return nil },
		},
		Logger: observability.NewTestLogger("debug", mh.logs),
	})
	if err != nil {
		t.Fatal(err)
	}
	m.RunRound(context.Background())
	if strings.Join(calls, ",") != "sample,capacity" || !strings.Contains(mh.logs.String(), `"event":"maintenance_hook_failed"`) {
		t.Errorf("calls %v, logs %s", calls, mh.logs.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	m.RunRound(ctx)
	if len(calls) != 2 {
		t.Errorf("hooks ran after cancellation: %v", calls)
	}
}

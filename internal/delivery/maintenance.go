package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/observability"
)

// maintenanceOpTimeout bounds one maintenance read or transition. Batch
// work is detached from the loop context so a started batch completes
// during shutdown; only new batches stop.
const maintenanceOpTimeout = 5 * time.Second

// Maintenance kinds (the bounded kind label).
const (
	kindLeaseExpiry           = "lease_expiry"
	kindRetryActivation       = "retry_activation"
	kindInlineLeaseExpiry     = "inline_lease_expiry"
	kindInlineRetryActivation = "inline_retry_activation"
)

// inlinePassLimit bounds the due entries one claim processes before it
// waits; inlinePassBudget bounds the time the pass may take, well inside
// the claim's route deadline slack.
const (
	inlinePassLimit  = 10
	inlinePassBudget = 2 * time.Second
)

// maintenanceResult is the bounded result label of one maintenance entry.
type maintenanceResult string

const (
	resultApplied maintenanceResult = "applied"
	resultStale   maintenanceResult = "stale"
	resultBlocked maintenanceResult = "blocked"
	resultFailed  maintenanceResult = "failed"
)

// MaintenanceConfig paces cooperative background maintenance.
type MaintenanceConfig struct {
	Interval             time.Duration
	IntervalJitter       time.Duration
	BatchSize            int
	MaxContinuousBatches int
}

// MaintenanceDeps carries the maintenance collaborators.
type MaintenanceDeps struct {
	Retries RetryActivator
	Leases  LeaseExpirer
	// RetryPolicy chooses the retry delay after an expired attempt.
	RetryPolicy RetryPolicy
	// Attempts counts expired attempts; share the Consumer API's instance.
	Attempts *AttemptMetrics
	Config   MaintenanceConfig
	// Uniform draws the interval and retry-delay jitter in [0, 1)
	// (rand.Float64 when nil).
	Uniform func() float64
	// Clock times batches (gen.SystemClock when nil); due checks use
	// Valkey time.
	Clock gen.Clock
	// Sleep waits between rounds and returns ctx.Err() when cancelled
	// (a timer wait when nil).
	Sleep func(ctx context.Context, d time.Duration) error
	// Logger receives failure events; nil discards them.
	Logger *slog.Logger
	// Registerer receives the maintenance metrics; nil keeps them private.
	Registerer prometheus.Registerer
}

// Maintenance runs cooperative background maintenance: any process may run
// it, no leader is elected, and every transition re-validates authoritative
// state, so a stale or concurrently processed index entry changes nothing.
type Maintenance struct {
	d         MaintenanceDeps
	log       *slog.Logger
	processed *prometheus.CounterVec
	dueLag    *prometheus.GaugeVec
	batchSize *prometheus.HistogramVec
	duration  *prometheus.HistogramVec
}

// NewMaintenance composes the maintenance loop.
func NewMaintenance(d MaintenanceDeps) (*Maintenance, error) {
	if d.Retries == nil || d.Leases == nil || d.Attempts == nil {
		return nil, errors.New("delivery: maintenance needs a RetryActivator, a LeaseExpirer, and AttemptMetrics")
	}
	if err := d.RetryPolicy.Validate(); err != nil {
		return nil, err
	}
	if c := d.Config; c.Interval <= 0 || c.IntervalJitter < 0 || c.BatchSize <= 0 || c.MaxContinuousBatches <= 0 {
		return nil, fmt.Errorf("delivery: invalid maintenance configuration %+v", c)
	}
	if d.Uniform == nil {
		d.Uniform = rand.Float64
	}
	if d.Sleep == nil {
		d.Sleep = sleep
	}
	if d.Clock == nil {
		d.Clock = gen.SystemClock{}
	}
	reg := d.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Maintenance{
		d:   d,
		log: d.Logger,
		processed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_maintenance_processed_total",
			Help: "Maintenance transitions by kind and bounded result (applied, stale, blocked, failed).",
		}, []string{"kind", "result"}),
		dueLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "hookrelay_maintenance_due_lag_seconds",
			Help: "Lag between Valkey time and the oldest due deadline at the start of the last round, by kind.",
		}, []string{"kind"}),
		batchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_maintenance_batch_size",
			Help:    "Due entries read per maintenance batch, by kind.",
			Buckets: []float64{0, 1, 5, 10, 25, 50, 100, 250, 500, 1000},
		}, []string{"kind"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "hookrelay_maintenance_duration_seconds",
			Help:    "Maintenance batch duration, by kind.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind"}),
	}
	if m.log == nil {
		m.log = slog.New(slog.DiscardHandler)
	}
	for _, c := range []prometheus.Collector{m.processed, m.dueLag, m.batchSize, m.duration} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("delivery: register maintenance metrics: %w", err)
		}
	}
	return m, nil
}

// Run executes rounds until ctx ends, pausing the interval plus uniform
// jitter between rounds.
func (m *Maintenance) Run(ctx context.Context) {
	for ctx.Err() == nil {
		m.RunRound(ctx)
		pause := m.d.Config.Interval + time.Duration(float64(m.d.Config.IntervalJitter)*m.d.Uniform())
		if m.d.Sleep(ctx, pause) != nil {
			return
		}
	}
}

// RunRound processes due leases, then due retries, each kind in its own
// batches of at most BatchSize; a full batch triggers another, up to
// MaxContinuousBatches, then the kind yields. A cancelled ctx stops new
// batches; the batch in progress completes (its transitions are atomic).
func (m *Maintenance) RunRound(ctx context.Context) {
	m.runKind(ctx, kindLeaseExpiry, m.d.Leases.DueLeases, m.expireLease)
	m.runKind(ctx, kindRetryActivation, m.d.Retries.DueRetries, m.activateRetry)
}

// transition applies one maintenance transition and returns its bounded
// result; kind is the label its failures are logged under.
type transition func(ctx context.Context, kind string, e DueEntry) maintenanceResult

// dueReader reads at most limit due entries of one kind.
type dueReader func(ctx context.Context, limit int) (DueBatch, error)

// InlinePass is the bounded self-healing path of a claim that found no
// ready work before it waits: at most inlinePassLimit due entries in total,
// due leases first, through the same transitions, events, and metrics as a
// round, attributed to the inline kinds. The pass is detached from the
// request (a started transition completes even if the client leaves) and
// bounded by inlinePassBudget. Background maintenance remains the primary
// path.
func (m *Maintenance) InlinePass(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), inlinePassBudget)
	defer cancel()
	remaining := inlinePassLimit
	for _, k := range []struct {
		kind    string
		read    dueReader
		process transition
	}{{kindInlineLeaseExpiry, m.d.Leases.DueLeases, m.expireLease}, {kindInlineRetryActivation, m.d.Retries.DueRetries, m.activateRetry}} {
		if remaining == 0 || ctx.Err() != nil {
			return
		}
		batch, ok := m.read(ctx, k.kind, k.read, remaining)
		if !ok {
			continue // the other kind keeps its budget
		}
		for _, e := range batch.Entries {
			m.processOne(ctx, k.kind, k.process, e)
		}
		remaining -= len(batch.Entries)
	}
}

// ProcessDue expires the named due leases and activates the named due
// retries through the same transitions, events, and metrics as a round.
// Startup reconciliation reports these Recipients (without mutating them)
// and hands them over before readiness. Each transition re-validates
// state, so a Recipient that is no longer due is counted as stale.
func (m *Maintenance) ProcessDue(ctx context.Context, leases, retries []string) {
	for _, k := range []struct {
		kind    string
		rids    []string
		process transition
	}{{kindLeaseExpiry, leases, m.expireLease}, {kindRetryActivation, retries, m.activateRetry}} {
		for _, rid := range k.rids {
			m.processOne(ctx, k.kind, k.process, DueEntry{RecipientIdentity: rid})
		}
	}
}

// read reads one batch of due entries with its own deadline; a failure is
// logged and counted, and ok is false.
func (m *Maintenance) read(ctx context.Context, kind string, read dueReader, limit int) (DueBatch, bool) {
	readCtx, cancel := context.WithTimeout(ctx, maintenanceOpTimeout)
	defer cancel()
	batch, err := read(readCtx, limit)
	if err != nil {
		observability.LogEvent(m.log, slog.LevelWarn, "maintenance_read_failed", "maintenance could not read due entries",
			"kind", kind, "error_code", "dependency_unavailable")
		m.processed.WithLabelValues(kind, string(resultFailed)).Inc()
		return DueBatch{}, false
	}
	return batch, true
}

// runKind runs the continuous batches of one maintenance kind.
func (m *Maintenance) runKind(ctx context.Context, kind string, read dueReader, process transition) {
	for i := 0; i < m.d.Config.MaxContinuousBatches && ctx.Err() == nil; i++ {
		n, ok := m.batch(context.WithoutCancel(ctx), kind, i == 0, read, process)
		if !ok || n < m.d.Config.BatchSize {
			return
		}
	}
}

// batch processes one batch of due entries and reports how many were read;
// ok is false when the read failed.
func (m *Maintenance) batch(ctx context.Context, kind string, first bool, read dueReader, process transition) (int, bool) {
	start := m.d.Clock.Now()
	batch, ok := m.read(ctx, kind, read, m.d.Config.BatchSize)
	if !ok {
		return 0, false
	}
	if first {
		lag := 0.0
		if len(batch.Entries) > 0 && batch.NowMs > batch.Entries[0].DueMs {
			lag = float64(batch.NowMs-batch.Entries[0].DueMs) / 1000
		}
		m.dueLag.WithLabelValues(kind).Set(lag)
	}
	for _, e := range batch.Entries {
		m.processOne(ctx, kind, process, e)
	}
	m.batchSize.WithLabelValues(kind).Observe(float64(len(batch.Entries)))
	m.duration.WithLabelValues(kind).Observe(m.d.Clock.Now().Sub(start).Seconds())
	return len(batch.Entries), true
}

// processOne applies one transition with its own deadline and counts its
// result.
func (m *Maintenance) processOne(ctx context.Context, kind string, process transition, e DueEntry) {
	opCtx, cancel := context.WithTimeout(ctx, maintenanceOpTimeout)
	defer cancel()
	m.processed.WithLabelValues(kind, string(process(opCtx, kind, e))).Inc()
}

// activateRetry applies one retry activation.
func (m *Maintenance) activateRetry(ctx context.Context, kind string, e DueEntry) maintenanceResult {
	res := m.d.Retries.ActivateRetry(ctx, e.RecipientIdentity)
	switch res.Outcome {
	case ActivationActivated:
		return resultApplied
	case ActivationNotDue:
		return resultStale
	case ActivationRecipientBlocked:
		return resultBlocked
	default:
		m.transitionFailed(kind, string(res.Outcome))
		return resultFailed
	}
}

// expireLease applies one lease expiry with freshly drawn retry delays and
// records the expired attempt.
func (m *Maintenance) expireLease(ctx context.Context, kind string, e DueEntry) maintenanceResult {
	res := m.d.Leases.ExpireLease(ctx, e.RecipientIdentity, m.d.RetryPolicy.DrawDelaysMs(m.d.Uniform), m.d.RetryPolicy.MaxAttempts)
	switch res.Outcome {
	case ExpiryRetryScheduled:
		fields := []any{
			"message_id", res.MessageID, "delivery_cycle", res.DeliveryCycle, "attempt", res.Attempt,
			"retry_at_ms", res.RetryAtMs, "expired_ms", res.ExpiredMs, "duration_ms", res.ExpiredMs - res.ClaimedMs,
		}
		fields, scope := recipientEventFields(e.RecipientIdentity, fields)
		if res.ConsumerInstanceID != "" {
			fields = append(fields, "consumer_instance_id", res.ConsumerInstanceID)
		}
		m.d.Attempts.Observe(scope, "expired", res.ClaimedMs, res.ExpiredMs)
		observability.LogEvent(m.log, slog.LevelInfo, "delivery_lease_expired", "lease expired; retry scheduled", fields...)
		return resultApplied
	case ExpiryDeadLettered:
		var extra []any
		if res.ConsumerInstanceID != "" {
			extra = append(extra, "consumer_instance_id", res.ConsumerInstanceID)
		}
		recordDeadLetter(m.log, m.d.Attempts, deadLetter{
			RecipientIdentity: e.RecipientIdentity, MessageID: res.MessageID, Reason: reasonExpiryExhausted,
			DeliveryCycle: res.DeliveryCycle, Attempt: res.Attempt, ClaimedMs: res.ClaimedMs, DeadLetteredMs: res.DeadLetteredMs,
		}, extra...)
		return resultApplied
	case ExpiryNotDue:
		return resultStale
	case ExpiryRecipientBlocked:
		return resultBlocked
	default:
		m.transitionFailed(kind, string(res.Outcome))
		return resultFailed
	}
}

func (m *Maintenance) transitionFailed(kind, outcome string) {
	observability.LogEvent(m.log, slog.LevelError, "maintenance_transition_failed", "maintenance transition failed",
		"kind", kind, "outcome", outcome)
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

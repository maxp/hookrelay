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
const kindRetryActivation = "retry_activation"

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
	Config  MaintenanceConfig
	// Uniform draws the interval jitter in [0, 1) (rand.Float64 when nil).
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
	if d.Retries == nil {
		return nil, errors.New("delivery: maintenance needs a RetryActivator")
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

// RunRound processes due retries in batches of at most BatchSize; a full
// batch triggers another, up to MaxContinuousBatches, then the round
// yields. A cancelled ctx stops new batches; the batch in progress
// completes.
func (m *Maintenance) RunRound(ctx context.Context) {
	for i := 0; i < m.d.Config.MaxContinuousBatches && ctx.Err() == nil; i++ {
		n, ok := m.retryBatch(context.WithoutCancel(ctx), i == 0)
		if !ok || n < m.d.Config.BatchSize {
			return
		}
	}
}

// retryBatch activates one batch of due retries and reports how many were
// read; ok is false when the read failed.
func (m *Maintenance) retryBatch(ctx context.Context, first bool) (int, bool) {
	start := m.d.Clock.Now()
	readCtx, cancel := context.WithTimeout(ctx, maintenanceOpTimeout)
	batch, err := m.d.Retries.DueRetries(readCtx, m.d.Config.BatchSize)
	cancel()
	if err != nil {
		observability.LogEvent(m.log, slog.LevelWarn, "maintenance_read_failed", "maintenance could not read due entries",
			"kind", kindRetryActivation, "error_code", "dependency_unavailable")
		m.processed.WithLabelValues(kindRetryActivation, "failed").Inc()
		return 0, false
	}
	if first {
		lag := 0.0
		if len(batch.Entries) > 0 && batch.NowMs > batch.Entries[0].DueMs {
			lag = float64(batch.NowMs-batch.Entries[0].DueMs) / 1000
		}
		m.dueLag.WithLabelValues(kindRetryActivation).Set(lag)
	}
	for _, e := range batch.Entries {
		opCtx, cancel := context.WithTimeout(ctx, maintenanceOpTimeout)
		res := m.d.Retries.ActivateRetry(opCtx, e.RecipientIdentity)
		cancel()
		result := "failed"
		switch res.Outcome {
		case ActivationActivated:
			result = "applied"
		case ActivationNotDue:
			result = "stale"
		case ActivationRecipientBlocked:
			result = "blocked"
		default:
			observability.LogEvent(m.log, slog.LevelError, "maintenance_transition_failed", "maintenance transition failed",
				"kind", kindRetryActivation, "outcome", string(res.Outcome))
		}
		m.processed.WithLabelValues(kindRetryActivation, result).Inc()
	}
	m.batchSize.WithLabelValues(kindRetryActivation).Observe(float64(len(batch.Entries)))
	m.duration.WithLabelValues(kindRetryActivation).Observe(m.d.Clock.Now().Sub(start).Seconds())
	return len(batch.Entries), true
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

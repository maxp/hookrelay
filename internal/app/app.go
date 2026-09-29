package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/maxp/hookrelay/internal/config"
	"github.com/maxp/hookrelay/internal/ingestion"
)

// shutdownDeadline is the controlled shutdown deadline.
const shutdownDeadline = 30 * time.Second

// readinessProbeInterval is the Valkey-loss monitor cadence. Loss flips
// readiness false; recovery re-runs the full gate before readiness returns.
const readinessProbeInterval = time.Second

// gateTimeout bounds one gate run so a hung dependency cannot stall startup
// or the loss monitor indefinitely.
const gateTimeout = 5 * time.Second

// Readiness is the shared readiness/acceptance state. The startup
// reconciliation gate flips it to ready; it never reports ready by default,
// so a partially built process is never falsely ready.
type Readiness struct {
	ready     atomic.Bool
	accepting atomic.Bool
	// reconciliation is the bounded startup/recovery reconciliation state
	// shown in the readiness body: pending, in_progress, held, failed,
	// complete.
	reconciliation atomic.Value
}

// ReconciliationState reports the bounded reconciliation state.
func (r *Readiness) ReconciliationState() string {
	if v, ok := r.reconciliation.Load().(string); ok {
		return v
	}
	return "pending"
}

func (r *Readiness) setReconciliation(state string) { r.reconciliation.Store(state) }

// MarkReady reports that startup validation and reconciliation succeeded.
func (r *Readiness) MarkReady() { r.ready.Store(true) }

// MarkNotReady withdraws readiness (dependency loss, diagnostic hold).
func (r *Readiness) MarkNotReady() { r.ready.Store(false) }

// SetAcceptingWebhooks toggles the ingestion-acceptance signal.
func (r *Readiness) SetAcceptingWebhooks(v bool) { r.accepting.Store(v) }

// Ready reports current readiness.
func (r *Readiness) Ready() bool { return r.ready.Load() }

// AcceptingWebhooks reports current ingestion acceptance.
func (r *Readiness) AcceptingWebhooks() bool { return r.accepting.Load() }

// Deps carries the collaborators the application needs. Feature modules wire
// into it as they arrive.
type Deps struct {
	Config    *config.Config
	Logger    logger
	Registry  *prometheus.Registry
	Readiness *Readiness

	// AdminAPI is mounted on the administrative listener (feature routes
	// such as /admin/v1/...). Health and metrics stay unauthenticated:
	// they are protected by the listener placement.
	AdminAPI http.Handler

	// Webhooks serves the /webhook/ routes on the public listener. It
	// receives those paths before the standard mux so path cleaning and its
	// redirects never alter webhook route validation.
	Webhooks http.Handler

	// ConsumerAPI serves the /v1/ Consumer API routes on the public listener.
	ConsumerAPI http.Handler

	// Probes refresh state-derived gauges on every gate run while ready.
	Probes []func(context.Context)

	// BeforeDrain runs after readiness and acceptance are withdrawn and
	// before the listeners drain, e.g. to end outstanding long polls.
	BeforeDrain []func()

	// Reconcile validates and safely repairs persisted state before
	// readiness. full is true until the public listener has served once
	// (the startup pass); recovery after Valkey loss runs lightweight
	// passes. A non-empty Hold keeps readiness false.
	Reconcile func(ctx context.Context, full bool) (ReconcileResult, error)

	// Acceptance evaluates the global acceptance stop conditions on every
	// gate run while ready; nil means acceptance follows readiness.
	Acceptance func(context.Context) bool

	// Gate is the startup/recovery readiness gate: connectivity, production
	// persistence checks, script loads, and structure validation. The
	// administrative listener starts before the gate; the public listener
	// opens only after the first successful gate.
	Gate func(context.Context) error
}

// ReconcileResult is the outcome of one reconciliation pass: bounded
// finding counts and, when readiness must stay false, the hold reason.
type ReconcileResult struct {
	Findings map[string]int
	Hold     string
}

// reconcileTimeout bounds one reconciliation pass.
const reconcileTimeout = 5 * time.Minute

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// App owns the listeners and the shutdown sequence.
type App struct {
	deps Deps

	reconciling atomic.Bool
	findings    *prometheus.CounterVec

	adminServer  *http.Server
	publicServer *http.Server
}

// New composes the application. It builds the HTTP handlers immediately so
// tests can exercise them without binding ports; Run starts the listeners.
func New(deps Deps) *App {
	a := &App{deps: deps}
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /health/live", a.handleLive)
	adminMux.HandleFunc("GET /health/ready", a.handleReady)
	adminMux.HandleFunc("GET /health/accepting-webhooks", a.handleAcceptingWebhooks)
	adminMux.Handle("GET /metrics", promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{}))
	if deps.Registry != nil {
		a.findings = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "hookrelay_reconciliation_findings_total",
			Help: "Reconciliation findings and repairs by bounded kind.",
		}, []string{"kind"})
		deps.Registry.MustRegister(a.findings, prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hookrelay_reconciliation_in_progress",
			Help: "1 while a reconciliation pass runs.",
		}, func() float64 {
			if a.reconciling.Load() {
				return 1
			}
			return 0
		}))
	}
	if deps.Registry != nil && deps.Readiness != nil {
		readiness := deps.Readiness
		deps.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "hookrelay_accepting_webhooks",
			Help: "1 while new webhook messages may be accepted, 0 under a stop condition.",
		}, func() float64 {
			if readiness.Ready() && readiness.AcceptingWebhooks() {
				return 1
			}
			return 0
		}))
	}
	if deps.AdminAPI != nil {
		adminMux.Handle("/", deps.AdminAPI)
	}

	// The public listener carries the webhook routes and the Consumer API;
	// it starts only after the readiness gate succeeds.
	publicMux := http.NewServeMux()
	if deps.ConsumerAPI != nil {
		publicMux.Handle("/v1/", deps.ConsumerAPI)
	}
	var publicHandler http.Handler = publicMux
	if deps.Webhooks != nil {
		publicHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, ingestion.RoutePrefix) {
				deps.Webhooks.ServeHTTP(w, r)
				return
			}
			publicMux.ServeHTTP(w, r)
		})
	}

	a.adminServer = &http.Server{
		Handler:           adminMux,
		MaxHeaderBytes:    32 << 10,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	a.publicServer = &http.Server{
		Handler:           publicHandler,
		MaxHeaderBytes:    32 << 10,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return a
}

// AdminHandler exposes the administrative HTTP handler for tests.
func (a *App) AdminHandler() http.Handler { return a.adminServer.Handler }

// PublicHandler exposes the public HTTP handler for tests.
func (a *App) PublicHandler() http.Handler { return a.publicServer.Handler }

// Run starts the administrative listener, runs the readiness gate, opens the
// public listener only after the gate succeeds, monitors Valkey loss, and
// blocks until ctx is cancelled, then performs the controlled shutdown.
// openPublic must bind and return the public listener at call time.
func (a *App) Run(ctx context.Context, adminLn net.Listener, openPublic func(context.Context) (net.Listener, error)) error {
	log := a.deps.Logger
	errAdmin := make(chan error, 1)

	log.Info("starting administrative listener", "event", "listener_started", "listener", "admin", "address", a.deps.Config.AdminAddress)
	go func() { errAdmin <- a.adminServer.Serve(adminLn) }()

	// everServed records that readiness was acquired once; later passes are
	// recovery (lightweight) reconciliation.
	var everServed atomic.Bool

	// Public listener lifecycle: opened after the first successful gate. A
	// public server that stops unexpectedly clears publicOpen and withdraws
	// readiness, so the next gate run reopens it.
	var publicLn atomic.Value // net.Listener
	publicOpen := &atomic.Bool{}
	openPublicListener := func() bool {
		if publicOpen.Load() || openPublic == nil {
			return true
		}
		ln, err := openPublic(ctx)
		if err != nil {
			log.Error("public listener failed", "event", "listener_failed", "listener", "public", "error_code", "internal_error")
			return false
		}
		publicLn.Store(ln)
		publicOpen.Store(true)
		log.Info("starting public listener", "event", "listener_started", "listener", "public", "address", a.deps.Config.PublicAddress)
		go func() {
			if err := a.publicServer.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("public listener stopped", "event", "listener_failed", "listener", "public", "error_code", "internal_error")
				a.deps.Readiness.MarkNotReady()
				a.deps.Readiness.SetAcceptingWebhooks(false)
				publicOpen.Store(false)
			}
		}()
		return true
	}

	// Readiness gate + monitor: initial gate gates the public listener;
	// subsequent ticks recover or withdraw readiness. Readiness is reported
	// only once the public listener is actually open. Gate is cheap enough
	// for a fixed one-second cadence in the first version.
	runGate := func() {
		if a.deps.Gate == nil {
			return
		}
		gateCtx, cancel := context.WithTimeout(ctx, gateTimeout)
		defer cancel()
		err := a.deps.Gate(gateCtx)
		if err != nil {
			if a.deps.Readiness.Ready() {
				log.Warn("readiness withdrawn", "event", "readiness_withdrawn", "reason", "gate_failed")
			}
			a.deps.Readiness.MarkNotReady()
			a.deps.Readiness.SetAcceptingWebhooks(false)
			return
		}
		if ctx.Err() != nil {
			return
		}
		for _, probe := range a.deps.Probes {
			probe(gateCtx)
		}
		if a.deps.Readiness.Ready() {
			a.deps.Readiness.SetAcceptingWebhooks(a.accepting(gateCtx))
			return
		}
		if !a.reconcile(ctx, !everServed.Load()) {
			return
		}
		if !openPublicListener() {
			return
		}
		everServed.Store(true)
		log.Info("readiness acquired", "event", "readiness_acquired")
		// Acceptance is set first so readiness is never observed without it.
		a.deps.Readiness.SetAcceptingWebhooks(a.accepting(gateCtx))
		a.deps.Readiness.MarkReady()
	}
	runGate()
	monitor := time.NewTicker(readinessProbeInterval)
	defer monitor.Stop()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-monitor.C:
				runGate()
			}
		}
	}()

	<-ctx.Done()
	// The monitor must not reopen the public listener during shutdown.
	<-monitorDone
	log.Info("shutdown initiated", "event", "shutdown_initiated", "deadline_ms", shutdownDeadline.Milliseconds())
	a.deps.Readiness.MarkNotReady()
	a.deps.Readiness.SetAcceptingWebhooks(false)
	for _, stop := range a.deps.BeforeDrain {
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownDeadline)
	defer cancel()
	var firstErr error
	if err := a.adminServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := a.publicServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	if ln, ok := publicLn.Load().(net.Listener); ok {
		ln.Close()
	}
	<-errAdmin
	log.Info("shutdown complete", "event", "shutdown_complete")
	return firstErr
}

func (a *App) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeBoundedJSON(w, http.StatusOK, map[string]any{"status": "live"})
}

// handleReady reports whether hookrelay can safely serve its required APIs.
// Queue, memory, or dedup pressure never fails readiness; the storage and
// reconciliation slices extend the checks behind this endpoint.
func (a *App) handleReady(w http.ResponseWriter, _ *http.Request) {
	if a.deps.Readiness.Ready() {
		writeBoundedJSON(w, http.StatusOK, map[string]any{
			"status":             "ready",
			"accepting_webhooks": a.deps.Readiness.AcceptingWebhooks(),
		})
		return
	}
	writeBoundedJSON(w, http.StatusServiceUnavailable, map[string]any{
		"status":             "not_ready",
		"accepting_webhooks": false,
		"checks":             map[string]string{"startup_reconciliation": a.deps.Readiness.ReconciliationState()},
	})
}

// reconcile runs one reconciliation pass and reports whether readiness may
// be acquired.
func (a *App) reconcile(ctx context.Context, full bool) bool {
	if a.deps.Reconcile == nil {
		a.deps.Readiness.setReconciliation("complete")
		return true
	}
	log := a.deps.Logger
	a.deps.Readiness.setReconciliation("in_progress")
	a.reconciling.Store(true)
	defer a.reconciling.Store(false)
	log.Info("reconciliation started", "event", "reconciliation_started", "full", full)

	rctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()
	res, err := a.deps.Reconcile(rctx, full)
	for kind, n := range res.Findings {
		if n > 0 && a.findings != nil {
			a.findings.WithLabelValues(kind).Add(float64(n))
		}
	}
	switch {
	case err != nil:
		a.deps.Readiness.setReconciliation("failed")
		log.Error("reconciliation failed", "event", "reconciliation_failed", "error_code", "dependency_unavailable")
		return false
	case res.Hold != "":
		a.deps.Readiness.setReconciliation("held")
		log.Error("readiness held by reconciliation", "event", "reconciliation_hold", "reason_code", res.Hold, "error_code", "internal_error")
		return false
	}
	a.deps.Readiness.setReconciliation("complete")
	args := []any{"event", "reconciliation_completed", "full", full}
	for kind, n := range res.Findings {
		args = append(args, kind, n)
	}
	log.Info("reconciliation completed", args...)
	return true
}

// accepting reports whether new webhook messages may be accepted: ingestion
// is wired and no global stop condition applies.
func (a *App) accepting(ctx context.Context) bool {
	if a.deps.Webhooks == nil {
		return false
	}
	return a.deps.Acceptance == nil || a.deps.Acceptance(ctx)
}

// handleAcceptingWebhooks is the ingestion-acceptance signal: accepting only
// while ready, with ingestion wired, and below the global queue and
// deduplication stop conditions.
func (a *App) handleAcceptingWebhooks(w http.ResponseWriter, _ *http.Request) {
	if a.deps.Readiness.Ready() && a.deps.Readiness.AcceptingWebhooks() {
		writeBoundedJSON(w, http.StatusOK, map[string]any{"status": "accepting", "accepting_webhooks": true})
		return
	}
	writeBoundedJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "not_accepting", "accepting_webhooks": false})
}

func writeBoundedJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	data, err := json.Marshal(body)
	if err != nil {
		// A marshaling failure on a bounded body is an internal bug; the
		// header is already written, so fall back to a minimal body.
		fmt.Fprintln(w, "{}")
		return
	}
	w.Write(data)
	w.Write([]byte("\n"))
}

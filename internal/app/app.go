// Package app is the composition root and lifecycle owner. It wires immutable
// configuration, logging, metrics, feature modules, and the public and
// administrative HTTP listeners, and owns readiness and graceful shutdown.
package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/maxp/hookrelay/internal/config"
)

// shutdownDeadline is the controlled shutdown deadline.
const shutdownDeadline = 30 * time.Second

// Readiness is the shared readiness/acceptance state. The startup
// reconciliation gate (storage slices) flips it to ready; it never reports
// ready by default, so a partially built process is never falsely ready.
type Readiness struct {
	ready     atomic.Bool
	accepting atomic.Bool
}

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
// into it as they arrive; the scaffold knows only logging, metrics, and
// configuration.
type Deps struct {
	Config    *config.Config
	Logger    logger
	Registry  *prometheus.Registry
	Readiness *Readiness
}

type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// App owns the listeners and the shutdown sequence.
type App struct {
	deps Deps

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

	// The public listener carries webhook and Consumer API routes in later
	// slices; it serves nothing yet and starts only after configuration
	// validation (the reconciliation gate joins here with the storage work).
	publicMux := http.NewServeMux()

	a.adminServer = &http.Server{
		Handler:           adminMux,
		MaxHeaderBytes:    32 << 10,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	a.publicServer = &http.Server{
		Handler:           publicMux,
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

// Run starts both listeners and blocks until ctx is cancelled, then performs
// the controlled shutdown: readiness false first, then connection draining.
// The administrative listener starts before the public listener so that
// liveness, readiness, and metrics expose startup progress.
func (a *App) Run(ctx context.Context, lnAdmin, lnPublic net.Listener) error {
	log := a.deps.Logger
	errAdmin := make(chan error, 1)
	errPublic := make(chan error, 1)

	log.Info("starting administrative listener", "event", "listener_started", "listener", "admin", "address", a.deps.Config.AdminAddress)
	go func() { errAdmin <- a.adminServer.Serve(lnAdmin) }()

	log.Info("starting public listener", "event", "listener_started", "listener", "public", "address", a.deps.Config.PublicAddress)
	go func() { errPublic <- a.publicServer.Serve(lnPublic) }()

	<-ctx.Done()
	log.Info("shutdown initiated", "event", "shutdown_initiated", "deadline_ms", shutdownDeadline.Milliseconds())
	a.deps.Readiness.MarkNotReady()
	a.deps.Readiness.SetAcceptingWebhooks(false)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownDeadline)
	defer cancel()
	var firstErr error
	if err := a.adminServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := a.publicServer.Shutdown(shutdownCtx); err != nil && firstErr == nil {
		firstErr = err
	}
	// Serve on a closed listener returns http.ErrServerClosed; drain both.
	<-errAdmin
	<-errPublic
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
		"checks":             map[string]string{"startup_reconciliation": "pending"},
	})
}

// handleAcceptingWebhooks is the ingestion-acceptance signal. Before the
// ingestion slice exists nothing may be accepted, so the endpoint reports
// not-accepting rather than making a false claim.
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

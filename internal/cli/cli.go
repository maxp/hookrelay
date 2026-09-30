// Package cli implements the hookrelay command families: serve, admin,
// version, generate, and healthcheck. The entry point only delegates here; HTTP,
// domain, Valkey, and observability logic live in the feature modules.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/app"
	"github.com/maxp/hookrelay/internal/config"
	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/valkey"
)

// types is the process-wide Webhook Type registry, wired into administration
// through its narrow TypeCatalog view.
var types *ingestion.Registry

func init() {
	r, err := ingestion.Builtin(ingestion.BuiltinOptions{})
	if err != nil {
		panic(err)
	}
	types = r
}

// typeCatalog adapts the ingestion registry to the administration catalog.
type typeCatalog struct{ registry *ingestion.Registry }

func (c typeCatalog) Lookup(webhookType string) (string, []string, bool) {
	d, ok := c.registry.Lookup(ingestion.WebhookType(webhookType))
	if !ok {
		return "", nil, false
	}
	return string(d.Platform), d.CredentialKinds, true
}

func (c typeCatalog) KnownPlatform(p string) bool {
	return c.registry.HasPlatform(ingestion.BotPlatform(p))
}

// Exit codes per the lifecycle contract: controlled clean shutdown 0,
// runtime/internal failure 1, configuration or CLI usage failure 2.
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
)

// version is the embedded build identity; real values are injected via
// linker flags at build time.
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
	dirty     = "unknown"
)

// Main is the process entry point; it returns the process exit code.
func Main(args []string) int {
	if len(args) == 0 {
		usage(os.Stderr)
		return ExitUsage
	}
	switch args[0] {
	case "serve":
		return Serve(args[1:])
	case "admin":
		return Admin(args[1:])
	case "version":
		return Version(args[1:], os.Stdout)
	case "generate":
		return Generate(args[1:])
	case "healthcheck":
		return Healthcheck(args[1:])
	default:
		fmt.Fprintf(os.Stderr, "hookrelay: unknown command %q\n", args[0])
		usage(os.Stderr)
		return ExitUsage
	}
}

func usage(w io.Writer) {
	fmt.Fprint(w, `hookrelay — webhook relay that distributes incoming events to recipients through ordered delivery channels

Usage:
  hookrelay serve        run the hookrelay server
  hookrelay admin        Admin API client: webhook create | get
  hookrelay version      print build information (human or --json)
  hookrelay generate     generate consumer-secret | admin-secret | webhook-id
  hookrelay healthcheck  probe a health endpoint (container healthcheck)
`)
}

// Serve runs the server until a termination signal. Configuration failures
// exit 2; runtime failures exit 1.
func Serve(args []string) int {
	cfg, err := config.Load(args, os.Getenv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay serve: %v\n", err)
		return ExitUsage
	}

	consumerSecret, err := config.LoadSecrets(cfg.ConsumerSecret, cfg.ConsumerSecretFile, config.OSReadFile, "consumer secret")
	if err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay serve: %v\n", err)
		return ExitUsage
	}
	adminSecret, err := config.LoadSecrets(cfg.AdminSecret, cfg.AdminSecretFile, config.OSReadFile, "admin secret")
	if err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay serve: %v\n", err)
		return ExitUsage
	}

	observability.SetBuildVersion(version)
	log := observability.NewLogger(cfg.LogLevel)
	registry := observability.NewMetricsRegistry()
	if cfg.PprofEnabled {
		log.Warn("profiling is not available in this build; HOOKRELAY_PPROF_ENABLED has no effect yet",
			"event", "configuration_not_enforced", "setting", "HOOKRELAY_PPROF_ENABLED")
	}

	// Valkey adapter: eager dial is the first dependency check (ADR 0006).
	// HOOKRELAY_VALKEY_MAX_CONNECTIONS bounds the pipelined ring plus the
	// blocking pool; long polls are periodic atomic rechecks on the ring.
	opt, err := valkey.ParseURL(cfg.ValkeyURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay serve: %v\n", err)
		return ExitUsage
	}
	valkey.ApplyConnectionLimits(&opt, cfg.ValkeyMaxConnections, cfg.ValkeyMinIdle)
	adapter, err := valkey.NewAdapter(opt)
	if err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay serve: %v\n", err)
		return ExitError
	}
	defer adapter.Close()
	if err := adapter.Instrument(registry); err != nil {
		log.Error("valkey metrics wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	// The serving registry carries the operator's adapter configuration.
	webhookTypes, err := ingestion.Builtin(ingestion.BuiltinOptions{TelegramSourceCIDRs: cfg.TelegramSourceCIDRs})
	if err != nil {
		log.Error("webhook type registry failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	notifier, err := delivery.NewReadyNotifier(cfg.ClaimNotifications, registry)
	if err != nil {
		log.Error("delivery wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}
	readiness := &app.Readiness{}
	svc, err := administration.NewService(administration.ServiceDeps{
		Ready:        notifier,
		Repo:         valkey.NewEndpointStore(adapter),
		Recipients:   valkey.NewRecipientStore(adapter, cfg.MaxQueuedMessagesPerRecipient),
		DeadLetters:  valkey.NewDeadLetterStore(adapter),
		Messages:     valkey.NewMessageStateStore(adapter),
		Catalog:      typeCatalog{registry: webhookTypes},
		Audit:        valkey.NewAuditSink(adapter),
		AuditLog:     valkey.NewAuditLog(adapter),
		Operations:   valkey.NewOperationsStore(adapter),
		Readiness:    readiness,
		GrafanaURL:   cfg.UIGrafanaURL,
		Sessions:     valkey.NewSessionStore(adapter),
		AdminOrigin:  adminOrigin(cfg.AdminOrigin),
		CookieSecure: cfg.AdminCookieSecure,
		AdminSecret:  adminSecret,
		Gen:          gen.Crypto{},
		Logger:       log,
		Registerer:   registry,
	})
	if err != nil {
		log.Error("administration wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	acceptor := valkey.NewMessageAcceptor(adapter, valkey.AcceptLimits{
		MaxQueuedMessages:             cfg.MaxQueuedMessages,
		MaxQueuedMessagesPerRecipient: cfg.MaxQueuedMessagesPerRecipient,
		MaxDedupRecords:               cfg.MaxDedupRecords,
		DedupRetention:                cfg.DedupRetention,
		DedupMinRetention:             cfg.DedupMinRetention,
		EvictionBatch:                 cfg.MaintenanceBatchSize,
	})
	webhooks, err := ingestion.NewHandler(ingestion.HandlerDeps{
		Ready:            notifier,
		Registry:         webhookTypes,
		Endpoints:        valkey.NewEndpointLookup(adapter),
		Acceptor:         acceptor,
		Capacity:         acceptor,
		OnAcceptanceStop: func() { readiness.SetAcceptingWebhooks(false) },
		RateLimits: ingestion.RateLimits{
			GlobalRate: cfg.WebhookGlobalRate, GlobalBurst: cfg.WebhookGlobalBurst,
			EndpointRate: cfg.WebhookEndpointRate, EndpointBurst: cfg.WebhookEndpointBurst,
		},
		MemoryStopPercent: cfg.MemoryAcceptanceStopPercent,
		DedupRetention:    cfg.DedupRetention,
		DedupMinRetention: cfg.DedupMinRetention,
		DedupEvictor:      acceptor,
		MaxInflight:       cfg.MaxInflightWebhooks,
		Gen:               gen.Crypto{},
		Clock:             gen.SystemClock{},
		TrustedProxies:    cfg.TrustedProxyCIDRs,
		Logger:            log,
		Registerer:        registry,
	})
	if err != nil {
		log.Error("ingestion wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	deliveryStore := valkey.NewDeliveryStore(adapter, valkey.ClaimLimits{
		MaxActiveLeases:      cfg.MaxActiveLeases,
		InitialLeaseDuration: cfg.InitialLeaseDuration,
		LeaseExtension:       cfg.LeaseExtensionDuration,
		MaxLeaseLifetime:     cfg.MaxLeaseLifetime,
	})
	attempts, err := delivery.NewAttemptMetrics(registry)
	if err != nil {
		log.Error("delivery wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}
	retryPolicy := delivery.RetryPolicy{
		MaxAttempts: cfg.MaxDeliveryAttempts,
		Delays:      cfg.RetryDelays,
		JitterMin:   cfg.RetryJitterMin,
		JitterMax:   cfg.RetryJitterMax,
	}
	maintenance, err := delivery.NewMaintenance(delivery.MaintenanceDeps{
		RunRoundGuard: readiness.RunMaintenanceRound,
		Notifier:      notifier,
		Retries:       deliveryStore,
		Leases:        deliveryStore,
		RetryPolicy:   retryPolicy,
		Attempts:      attempts,
		DeadLetters:   deliveryStore,
		DLQRetention:  cfg.DLQRetention,
		RoundHooks: []func(context.Context) error{
			adapter.SampleServer,
			func(ctx context.Context) error { webhooks.MaintainCapacity(ctx); return nil },
			svc.MaintainSessions,
		},
		Gen:   gen.Crypto{},
		Clock: gen.SystemClock{},
		Config: delivery.MaintenanceConfig{
			Interval:             cfg.MaintenanceInterval,
			IntervalJitter:       cfg.MaintenanceIntervalJitter,
			BatchSize:            cfg.MaintenanceBatchSize,
			MaxContinuousBatches: cfg.MaintenanceMaxContinuousBatches,
		},
		Logger:     log,
		Registerer: registry,
	})
	if err != nil {
		log.Error("maintenance wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	consumerAPI, err := delivery.NewHandler(delivery.HandlerDeps{
		Claimer:              deliveryStore,
		Acknowledger:         deliveryStore,
		NegativeAcknowledger: deliveryStore,
		Extender:             deliveryStore,
		InlineMaintenance:    maintenance,
		Notifier:             notifier,
		Stats:                deliveryStore,
		RetryPolicy:          retryPolicy,
		Attempts:             attempts,
		ConsumerSecret:       consumerSecret,
		MaxWaitingClaims:     cfg.MaxWaitingClaims,
		Gen:                  gen.Crypto{},
		Clock:                gen.SystemClock{},
		Logger:               log,
		Registerer:           registry,
	})
	if err != nil {
		log.Error("delivery wiring failed", "event", "startup_failed", "error_code", "internal_error")
		return ExitError
	}

	gate := func(ctx context.Context) error {
		_, err := adapter.ValidateReadiness(ctx, cfg.Production())
		return err
	}

	reconcile := func(ctx context.Context, full bool) (app.ReconcileResult, error) {
		report, err := adapter.ReconcileAndProcessDue(ctx, valkey.ReconcileOptions{
			Full:              full,
			MessageCheckBound: cfg.MaxQueuedMessagesPerRecipient,
			DedupRetention:    cfg.DedupRetention,
			Logger:            log,
			AdminSecret:       adminSecret,
		}, maintenance.ProcessDue)
		findings := report.Findings
		for reason, n := range report.BlockReasons {
			findings["blocked_"+reason] = n
		}
		var issues []app.ConsistencyIssue
		for _, i := range report.ConsistencyIssues() {
			issues = append(issues, app.ConsistencyIssue{Kind: i.Kind, Resolution: i.Resolution, Count: i.Count})
		}
		return app.ReconcileResult{Findings: findings, Issues: issues, Hold: report.Hold()}, err
	}

	application := app.New(app.Deps{
		Config:      cfg,
		Logger:      log,
		Registry:    registry,
		Readiness:   readiness,
		AdminAPI:    administration.Handler(svc),
		Webhooks:    webhooks,
		ConsumerAPI: consumerAPI,
		Probes:      []func(context.Context){consumerAPI.RefreshGauges},
		Maintenance: []func(context.Context){maintenance.Run},
		BeforeDrain: []func(){consumerAPI.Shutdown},
		Acceptance:  webhooks.AcceptingWebhooks,
		Gate:        gate,
		Reconcile:   reconcile,
	})

	log.Info("hookrelay starting",
		"event", "startup",
		"environment", string(cfg.Environment),
		"public_address", cfg.PublicAddress,
		"admin_address", cfg.AdminAddress,
		"valkey_url", cfg.RedactedValkeyURL(),
	)

	serveCtx, stopNotify := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopNotify()

	// The force channel stays registered while the graceful path drains: a
	// second signal forces termination instead of the default disposition.
	forceCh := make(chan os.Signal, 4)
	signal.Notify(forceCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(forceCh)
	appDone := make(chan struct{})
	secondSignal := make(chan bool, 1)
	go func() { secondSignal <- watchSecondSignal(serveCtx, forceCh, appDone) }()

	adminLn, err := net.Listen("tcp", cfg.AdminAddress)
	if err != nil {
		log.Error("admin listener failed", "event", "listener_failed", "error_code", "internal_error", "listener", "admin")
		return ExitError
	}
	defer adminLn.Close()

	// The public listener opens only after the readiness gate succeeds; the
	// app owns the deferred open so recovery re-gates before it returns.
	runErr := application.Run(serveCtx, adminLn, func(ctx context.Context) (net.Listener, error) {
		return net.Listen("tcp", cfg.PublicAddress)
	})
	close(appDone)
	forced := <-secondSignal
	if forced {
		fmt.Fprintln(os.Stderr, "hookrelay: forced termination on second signal")
		return ExitError
	}
	if runErr != nil {
		log.Error("server failure", "event", "server_failed", "error_code", "internal_error")
		return ExitError
	}
	return ExitOK
}

// watchSecondSignal reports whether a second signal arrives while the
// controlled shutdown drains. The first signal also lands in forceCh, so its
// duplicate is drained first; a genuinely second signal then fires the force
// path. Without a second signal the watchdog completes through appDone.
func watchSecondSignal(serveCtx context.Context, forceCh <-chan os.Signal, appDone <-chan struct{}) bool {
	<-serveCtx.Done()
	select {
	case <-forceCh:
	default:
	}
	select {
	case <-forceCh:
		fmt.Fprintln(os.Stderr, "hookrelay: forced termination on second signal")
		return true
	case <-appDone:
		return false
	}
}

// Version prints build information in human-readable or JSON form.
func Version(args []string, stdout io.Writer) int {
	fs := flag.NewFlagSet("hookrelay version", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(map[string]string{
			"version": version, "commit": commit, "build_time": buildTime, "dirty": dirty,
		}); err != nil {
			return ExitError
		}
		return ExitOK
	}
	fmt.Fprintf(stdout, "hookrelay %s\ncommit: %s\nbuild time: %s\ndirty: %s\n", version, commit, buildTime, dirty)
	return ExitOK
}

// Generate produces the operator secrets and identifiers. Generated values go
// only to standard output or --output-file; existing files are never
// overwritten without --force.
func Generate(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: hookrelay generate consumer-secret|admin-secret|webhook-id [--output-file path] [--force]")
		return ExitUsage
	}
	kind := args[0]
	fs := flag.NewFlagSet("hookrelay generate", flag.ContinueOnError)
	output := fs.String("output-file", "", "write the generated value to this file instead of standard output")
	force := fs.Bool("force", false, "allow overwriting an existing output file")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitUsage
	}

	var value string
	switch kind {
	case "consumer-secret", "admin-secret":
		v, err := randomSecret()
		if err != nil {
			fmt.Fprintf(os.Stderr, "hookrelay generate: %v\n", err)
			return ExitError
		}
		value = v
	case "webhook-id":
		v, err := randomWebhookID()
		if err != nil {
			fmt.Fprintf(os.Stderr, "hookrelay generate: %v\n", err)
			return ExitError
		}
		value = v
	default:
		fmt.Fprintf(os.Stderr, "hookrelay generate: unknown kind %q (want consumer-secret, admin-secret, or webhook-id)\n", kind)
		return ExitUsage
	}

	if *output == "" {
		fmt.Fprintln(os.Stdout, value)
		return ExitOK
	}
	if _, err := os.Stat(*output); err == nil && !*force {
		fmt.Fprintf(os.Stderr, "hookrelay generate: %s already exists (use --force to overwrite)\n", *output)
		return ExitUsage
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay generate: %v\n", err)
		return ExitError
	}
	if err := os.WriteFile(*output, []byte(value+"\n"), 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "hookrelay generate: %v\n", err)
		return ExitError
	}
	return ExitOK
}

// Healthcheck is the container healthcheck: it probes a health endpoint with
// a short timeout, exits successfully only for a 2xx response, prints nothing
// on success, and writes a safe diagnostic to standard error on failure.
func Healthcheck(args []string) int {
	fs := flag.NewFlagSet("hookrelay healthcheck", flag.ContinueOnError)
	urlFlag := fs.String("url", "http://127.0.0.1:8081/health/live", "health endpoint to probe")
	timeout := fs.Duration("timeout", 3*time.Second, "probe timeout")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}

	client := &http.Client{Timeout: *timeout}
	resp, err := client.Get(*urlFlag)
	if err != nil {
		// Keep the diagnostic minimal; the probe URL is operator-supplied
		// and must never mask the failure class.
		fmt.Fprintf(os.Stderr, "hookrelay healthcheck: probe failed: %v\n", err)
		return ExitError
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Fprintf(os.Stderr, "hookrelay healthcheck: endpoint returned %d\n", resp.StatusCode)
		return ExitError
	}
	return ExitOK
}

// adminOrigin serializes the configured administrative origin the way a
// browser sends it: lowercase scheme://host[:port], no path.
func adminOrigin(u *url.URL) string {
	if u == nil {
		return ""
	}
	return strings.ToLower(u.Scheme + "://" + u.Host)
}

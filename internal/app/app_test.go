package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/config"
	"github.com/maxp/hookrelay/internal/observability"
)

func testDeps(t *testing.T) Deps {
	t.Helper()
	cfg, err := config.Load(nil, func(string) string { return "" })
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return Deps{
		Config:    cfg,
		Logger:    observability.NewTestLogger("error", &bytes.Buffer{}),
		Registry:  observability.NewMetricsRegistry(),
		Readiness: &Readiness{},
	}
}

// TestScaffoldNeverFalselyReady pins the health contract before the storage
// and reconciliation slices exist: live answers, readiness and acceptance do
// not falsely claim readiness, and bounded bodies hide internals.
func TestScaffoldNeverFalselyReady(t *testing.T) {
	app := New(testDeps(t))

	live := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if live.Code != http.StatusOK {
		t.Errorf("live = %d, want 200", live.Code)
	}
	var liveBody map[string]any
	if err := json.Unmarshal(live.Body.Bytes(), &liveBody); err != nil || liveBody["status"] != "live" {
		t.Errorf("live body = %q (%v)", live.Body.String(), err)
	}

	ready := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Errorf("ready = %d, want 503 before reconciliation exists", ready.Code)
	}
	var readyBody struct {
		Status            string            `json:"status"`
		AcceptingWebhooks bool              `json:"accepting_webhooks"`
		Checks            map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(ready.Body.Bytes(), &readyBody); err != nil {
		t.Fatalf("ready body not JSON: %v", err)
	}
	if readyBody.Status != "not_ready" || readyBody.AcceptingWebhooks {
		t.Errorf("ready body = %+v", readyBody)
	}
	if _, ok := readyBody.Checks["startup_reconciliation"]; !ok {
		t.Errorf("ready body should name the pending check: %+v", readyBody)
	}

	accepting := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(accepting, httptest.NewRequest(http.MethodGet, "/health/accepting-webhooks", nil))
	if accepting.Code != http.StatusServiceUnavailable {
		t.Errorf("accepting-webhooks = %d, want 503 before ingestion exists", accepting.Code)
	}

	// Readiness flips only through the gate, never implicitly.
	app.deps.Readiness.MarkReady()
	ready2 := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(ready2, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if ready2.Code != http.StatusOK {
		t.Errorf("ready after MarkReady = %d, want 200", ready2.Code)
	}
	if !strings.Contains(ready2.Body.String(), `"accepting_webhooks":false`) {
		t.Errorf("ready body should still report not-accepting before the ingestion slice: %s", ready2.Body.String())
	}
}

// TestMetricsEndpointServesRegistry pins the private-registry exposition.
func TestMetricsEndpointServesRegistry(t *testing.T) {
	app := New(testDeps(t))
	rec := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("metrics content-type = %q, want Prometheus text exposition", ct)
	}
}

// TestPublicListenerServesNothingYet pins that the public surface stays empty
// until the webhook and Consumer slices land.
func TestPublicListenerServesNothingYet(t *testing.T) {
	app := New(testDeps(t))
	rec := httptest.NewRecorder()
	app.PublicHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("public root = %d, want 404", rec.Code)
	}
}

// TestRunGatesPublicListener exercises the lifecycle: the administrative
// listener starts before the gate, the public listener opens only after the
// gate succeeds, a failing gate holds readiness down, and context
// cancellation drains without error within the deadline.
func TestRunGatesPublicListener(t *testing.T) {
	deps := testDeps(t)
	gateOK := &atomic.Bool{}
	deps.Gate = func(context.Context) error {
		if gateOK.Load() {
			return nil
		}
		return errors.New("gate failed")
	}
	deps.Readiness = &Readiness{}
	app := New(deps)

	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("admin listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	opened := &atomic.Bool{}
	go func() {
		errCh <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) {
			opened.Store(true)
			return net.Listen("tcp", "127.0.0.1:0")
		})
	}()

	// Admin answers while the gate fails; readiness stays down; the public
	// listener is NOT opened.
	time.Sleep(1500 * time.Millisecond) // let initial gate + one probe tick run
	resp, err := http.Get("http://" + adminLn.Addr().String() + "/health/ready")
	if err != nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("ready during failed gate: %v %v", resp, err)
	}
	resp.Body.Close()
	if opened.Load() {
		t.Fatal("public listener opened before the gate succeeded")
	}

	// The gate succeeds: readiness flips and the public listener opens.
	gateOK.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + adminLn.Addr().String() + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && opened.Load() {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !opened.Load() {
		t.Fatal("public listener did not open after the gate succeeded")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error on clean shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// After shutdown the admin listener is closed.
	if _, err := http.Get("http://" + adminLn.Addr().String() + "/health/live"); err == nil {
		t.Error("admin listener still accepting after shutdown")
	}
}

// TestRunHoldsReadinessUntilPublicListenerOpens pins that a successful gate
// alone never reports ready: while the public bind fails, readiness stays
// down and the bind is retried on the next gate run.
func TestRunHoldsReadinessUntilPublicListenerOpens(t *testing.T) {
	deps := testDeps(t)
	deps.Gate = func(context.Context) error { return nil }
	deps.Readiness = &Readiness{}
	app := New(deps)

	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("admin listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	bindOK := &atomic.Bool{}
	attempts := &atomic.Int32{}
	go func() {
		errCh <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) {
			attempts.Add(1)
			if !bindOK.Load() {
				return nil, errors.New("address already in use")
			}
			return net.Listen("tcp", "127.0.0.1:0")
		})
	}()

	readyStatus := func() int {
		resp, err := http.Get("http://" + adminLn.Addr().String() + "/health/ready")
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	time.Sleep(1500 * time.Millisecond) // initial gate + at least one tick
	if got := readyStatus(); got != http.StatusServiceUnavailable {
		t.Fatalf("ready while the public bind fails = %d, want 503", got)
	}
	if attempts.Load() < 2 {
		t.Fatalf("public bind attempts = %d, want a retry on the next gate run", attempts.Load())
	}

	bindOK.Store(true)
	deadline := time.Now().Add(3 * time.Second)
	for readyStatus() != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("readiness not acquired after the public bind succeeded")
		}
		time.Sleep(100 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error on clean shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestWebhookRoutesBypassMuxCleaning pins that /webhook/ paths reach the
// ingestion handler verbatim (no path-cleaning redirect) and that other
// public paths stay on the mux.
func TestWebhookRoutesBypassMuxCleaning(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness.MarkReady()
	var seen []string
	deps.Webhooks = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.EscapedPath())
		w.WriteHeader(http.StatusTeapot)
	})
	app := New(deps)
	for _, path := range []string{"/webhook/telegram/../x", "/webhook/telegram/wh_a"} {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.URL.Path = path
		rec := httptest.NewRecorder()
		app.PublicHandler().ServeHTTP(rec, req)
		if rec.Code != http.StatusTeapot {
			t.Errorf("%s = %d, want the webhook handler", path, rec.Code)
		}
	}
	if len(seen) != 2 || seen[0] != "/webhook/telegram/../x" {
		t.Errorf("webhook handler saw %v", seen)
	}
	rec := httptest.NewRecorder()
	app.PublicHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/other", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("non-webhook path = %d, want the mux 404", rec.Code)
	}
}

// TestAcceptingFollowsReadinessWithIngestion pins that acceptance is
// reported only once ready and only when ingestion is wired.
func TestAcceptingFollowsReadinessWithIngestion(t *testing.T) {
	for _, wired := range []bool{false, true} {
		deps := testDeps(t)
		deps.Readiness = &Readiness{}
		deps.Gate = func(context.Context) error { return nil }
		if wired {
			deps.Webhooks = http.NotFoundHandler()
		}
		app := New(deps)
		adminLn, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") })
		}()
		deadline := time.Now().Add(3 * time.Second)
		for !deps.Readiness.Ready() && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if got := deps.Readiness.AcceptingWebhooks(); got != wired {
			t.Errorf("ingestion wired %v: accepting = %v", wired, got)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if deps.Readiness.AcceptingWebhooks() {
			t.Error("still accepting after shutdown")
		}
	}
}

// TestAcceptanceStopKeepsReadiness pins that a global stop condition makes
// /health/accepting-webhooks 503 and the gauge 0 while /health/ready stays
// 200 with accepting_webhooks false, and that recovery restores both.
func TestAcceptanceStopKeepsReadiness(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness = &Readiness{}
	deps.Gate = func(context.Context) error { return nil }
	deps.Webhooks = http.NotFoundHandler()
	accepting := &atomic.Bool{}
	deps.Acceptance = func(context.Context) bool { return accepting.Load() }
	app := New(deps)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") })
	}()
	defer func() {
		cancel()
		<-done
	}()

	get := func(path string) (int, string) {
		rec := httptest.NewRecorder()
		app.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code, rec.Body.String()
	}
	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(3 * time.Second)
		for !cond() && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor(deps.Readiness.Ready)
	if code, body := get("/health/ready"); code != 200 || !strings.Contains(body, `"accepting_webhooks":false`) {
		t.Errorf("ready under a stop condition = %d %s", code, body)
	}
	if code, _ := get("/health/accepting-webhooks"); code != 503 {
		t.Errorf("accepting-webhooks under a stop condition = %d", code)
	}
	if _, body := get("/metrics"); !strings.Contains(body, "hookrelay_accepting_webhooks 0") {
		t.Error("gauge not 0 under a stop condition")
	}

	accepting.Store(true)
	waitFor(deps.Readiness.AcceptingWebhooks)
	if code, _ := get("/health/accepting-webhooks"); code != 200 {
		t.Errorf("accepting-webhooks after recovery = %d", code)
	}
	if _, body := get("/metrics"); !strings.Contains(body, "hookrelay_accepting_webhooks 1") {
		t.Error("gauge not 1 after recovery")
	}
}

// TestBeforeDrainRunsAfterReadinessWithdrawn pins the shutdown order:
// readiness and acceptance are withdrawn before BeforeDrain hooks run, and
// the hooks run before Run returns.
func TestBeforeDrainRunsAfterReadinessWithdrawn(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness = &Readiness{}
	deps.Gate = func(context.Context) error { return nil }
	var readyAtDrain, called atomic.Bool
	readyAtDrain.Store(true)
	deps.BeforeDrain = []func(){func() {
		called.Store(true)
		readyAtDrain.Store(deps.Readiness.Ready())
	}}
	app := New(deps)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") })
	}()
	deadline := time.Now().Add(3 * time.Second)
	for !deps.Readiness.Ready() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !called.Load() || readyAtDrain.Load() {
		t.Errorf("BeforeDrain called=%v, ready at drain=%v", called.Load(), readyAtDrain.Load())
	}
}

// runApp starts Run with the given deps and returns a stop function.
func runApp(t *testing.T, deps Deps) (*App, func()) {
	t.Helper()
	app := New(deps)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") })
	}()
	return app, func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}
}

func readyBody(app *App) (int, string) {
	rec := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	return rec.Code, rec.Body.String()
}

// TestReconciliationGatesReadiness pins that readiness waits for a
// reconciliation pass, the first pass is full and later ones lightweight,
// a hold keeps readiness false with a bounded body, and findings are
// metered.
func TestReconciliationGatesReadiness(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness = &Readiness{}
	gateOK := &atomic.Bool{}
	gateOK.Store(true)
	deps.Gate = func(context.Context) error {
		if gateOK.Load() {
			return nil
		}
		return errors.New("valkey lost")
	}
	var mu sync.Mutex
	var passes []bool
	hold := &atomic.Bool{}
	hold.Store(true)
	deps.Reconcile = func(_ context.Context, full bool) (ReconcileResult, error) {
		mu.Lock()
		passes = append(passes, full)
		mu.Unlock()
		if hold.Load() {
			return ReconcileResult{Findings: map[string]int{"due_lease": 1}, Issues: []ConsistencyIssue{{"due_lease", "held", 1}}, Hold: "due_lease"}, nil
		}
		return ReconcileResult{Findings: map[string]int{"repaired": 2}, Issues: []ConsistencyIssue{{"derived_index_drift", "repaired", 2}}}, nil
	}
	app, stop := runApp(t, deps)
	defer stop()

	waitFor := func(cond func() bool) {
		deadline := time.Now().Add(4 * time.Second)
		for !cond() && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
	}
	waitFor(func() bool { return deps.Readiness.ReconciliationState() == "held" })
	if code, body := readyBody(app); code != 503 || !strings.Contains(body, `"startup_reconciliation":"held"`) {
		t.Errorf("held readiness = %d %s", code, body)
	}

	hold.Store(false)
	waitFor(deps.Readiness.Ready)
	if !deps.Readiness.Ready() || deps.Readiness.ReconciliationState() != "complete" {
		t.Fatal("readiness not acquired after the hold cleared")
	}
	rec := httptest.NewRecorder()
	app.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	for _, want := range []string{`hookrelay_consistency_issues_total{kind="derived_index_drift",resolution="repaired"} 2`, `hookrelay_consistency_issues_total{kind="due_lease",resolution="held"}`, "hookrelay_reconciliation_in_progress 0"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("metrics missing %s", want)
		}
	}

	// Valkey loss and recovery: a lightweight pass runs before readiness
	// returns.
	gateOK.Store(false)
	waitFor(func() bool { return !deps.Readiness.Ready() })
	mu.Lock()
	before := len(passes)
	mu.Unlock()
	gateOK.Store(true)
	waitFor(deps.Readiness.Ready)
	mu.Lock()
	defer mu.Unlock()
	if len(passes) <= before || passes[len(passes)-1] {
		t.Errorf("recovery passes = %v, want a new lightweight (false) pass", passes)
	}
	for i, full := range passes[:before] {
		if !full {
			t.Errorf("startup pass %d was not full", i)
		}
	}
}

// TestReconciliationFailureKeepsNotReady pins that a failing pass keeps
// readiness false and reports failed.
func TestReconciliationFailureKeepsNotReady(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness = &Readiness{}
	deps.Gate = func(context.Context) error { return nil }
	deps.Reconcile = func(context.Context, bool) (ReconcileResult, error) {
		return ReconcileResult{Findings: map[string]int{}}, errors.New("scan failed")
	}
	app, stop := runApp(t, deps)
	defer stop()
	time.Sleep(300 * time.Millisecond)
	if code, body := readyBody(app); code != 503 || !strings.Contains(body, `"startup_reconciliation":"failed"`) || deps.Readiness.Ready() {
		t.Errorf("failed reconciliation readiness = %d %s", code, body)
	}
}

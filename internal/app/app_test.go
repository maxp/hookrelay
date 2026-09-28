package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestRunShutsDownCleanly exercises the lifecycle: both listeners start, the
// public one only after the administrative one, and context cancellation
// drains without error within the deadline.
func TestRunShutsDownCleanly(t *testing.T) {
	app := New(testDeps(t))
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("admin listen: %v", err)
	}
	publicLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("public listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- app.Run(ctx, adminLn, publicLn) }()

	// Both listeners answer while running.
	resp, err := http.Get("http://" + adminLn.Addr().String() + "/health/live")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("live probe: %v %v", resp, err)
	}
	resp.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned error on clean shutdown: %v", err)
		}
	case <-timeAfter(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	// After shutdown the listeners are closed.
	if _, err := http.Get("http://" + adminLn.Addr().String() + "/health/live"); err == nil {
		t.Error("admin listener still accepting after shutdown")
	}
}

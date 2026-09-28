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
	gateOK := false
	deps.Gate = func(context.Context) error {
		if gateOK {
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
	opened := false
	go func() {
		errCh <- app.Run(ctx, adminLn, func(context.Context) (net.Listener, error) {
			opened = true
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
	if opened {
		t.Fatal("public listener opened before the gate succeeded")
	}

	// The gate succeeds: readiness flips and the public listener opens.
	gateOK = true
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://" + adminLn.Addr().String() + "/health/ready")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && opened {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !opened {
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

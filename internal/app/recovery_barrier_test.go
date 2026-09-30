package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecoveryDrainsAdmittedWorkAndRefusesNewStorageRequests(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness.MarkReady()
	entered, release, scanned, finish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	deps.ConsumerAPI = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.WriteHeader(204)
	})
	deps.AdminAPI = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	deps.Reconcile = func(context.Context, bool) (ReconcileResult, error) {
		close(scanned)
		<-finish
		return ReconcileResult{}, nil
	}
	a := New(deps)
	requestDone := make(chan struct{})
	go func() {
		a.PublicHandler().ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/v1/deliveries/ack", nil))
		close(requestDone)
	}()
	<-entered
	deps.Readiness.MarkNotReady()
	reconcileDone := make(chan bool, 1)
	go func() { reconcileDone <- a.reconcile(context.Background(), false) }()
	waitFor(t, "reconciliation admission closed", func() bool { return deps.Readiness.workPaused() })
	select {
	case <-scanned:
		t.Fatal("scan started before admitted request drained")
	default:
	}
	for _, target := range []struct {
		handler http.Handler
		path    string
		want    int
	}{
		{a.PublicHandler(), "/v1/deliveries/claim", 503},
		{a.AdminHandler(), "/admin/v1/session", 503},
		{a.AdminHandler(), "/admin/v1/operations/summary", 503},
		{a.AdminHandler(), "/health/live", 200},
		{a.AdminHandler(), "/metrics", 200},
		{a.AdminHandler(), "/ui/", 204},
	} {
		rec := httptest.NewRecorder()
		target.handler.ServeHTTP(rec, httptest.NewRequest("GET", target.path, nil))
		if rec.Code != target.want {
			t.Errorf("%s = %d, want %d", target.path, rec.Code, target.want)
		}
		if target.want == 503 && (rec.Header().Get("Retry-After") != "1" || rec.Header().Get("X-Request-Id") == "") {
			t.Errorf("refusal missing retry/correlation headers: %v", rec.Header())
		}
	}
	close(release)
	<-requestDone
	<-scanned
	close(finish)
	if !<-reconcileDone {
		t.Fatal("reconciliation failed")
	}
	rec := httptest.NewRecorder()
	a.AdminHandler().ServeHTTP(rec, httptest.NewRequest("GET", "/admin/v1/operations/summary", nil))
	if rec.Code != 204 {
		t.Fatal("Admin diagnosis did not reopen while unready")
	}
}

func TestRecoveryBarrierDrainsMaintenanceAndCancelledDrainDoesNotScan(t *testing.T) {
	deps := testDeps(t)
	deps.Readiness.MarkReady()
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		deps.Readiness.RunMaintenanceRound(context.Background(), func(context.Context) {
			close(entered)
			<-release
		})
		close(done)
	}()
	<-entered
	deps.Readiness.MarkNotReady()
	var scanned atomic.Bool
	deps.Reconcile = func(context.Context, bool) (ReconcileResult, error) {
		scanned.Store(true)
		return ReconcileResult{}, nil
	}
	a := New(deps)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if a.reconcile(ctx, false) || scanned.Load() {
		t.Fatal("cancelled drain started a scan")
	}
	close(release)
	<-done
	deps.Readiness.RunMaintenanceRound(context.Background(), func(context.Context) { t.Error("round ran while unready") })
	if !a.reconcile(context.Background(), false) || !scanned.Load() {
		t.Fatal("barrier could not be reacquired after cancellation")
	}
}

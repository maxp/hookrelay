package app

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/observability"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMaintenanceStartsAfterReadinessAndStopsOnShutdown pins the loop
// lifecycle: it starts only once readiness is acquired, and shutdown
// cancels it after readiness is withdrawn and waits for it to return.
func TestMaintenanceStartsAfterReadinessAndStopsOnShutdown(t *testing.T) {
	deps := testDeps(t)
	var gateOpen atomic.Bool
	deps.Gate = func(context.Context) error {
		if !gateOpen.Load() {
			return errors.New("not yet")
		}
		return nil
	}
	var started, returned, readyAtStart, readyAtStop atomic.Bool
	deps.Maintenance = []func(context.Context){func(ctx context.Context) {
		readyAtStart.Store(deps.Readiness.Ready())
		started.Store(true)
		<-ctx.Done()
		readyAtStop.Store(deps.Readiness.Ready())
		returned.Store(true)
	}}
	_, stop := runApp(t, deps)
	time.Sleep(50 * time.Millisecond)
	if started.Load() {
		t.Fatal("maintenance started before readiness")
	}
	gateOpen.Store(true)
	waitFor(t, "maintenance start", started.Load)
	if !readyAtStart.Load() {
		t.Error("maintenance started while not ready")
	}
	stop()
	if !returned.Load() {
		t.Error("Run returned before the maintenance loop stopped")
	}
	if readyAtStop.Load() {
		t.Error("maintenance stopped while still ready")
	}
}

// TestMaintenancePanicWithdrawsReadinessAndShutsDown pins the panic seam: a
// redacted stack is logged, readiness goes false, and Run ends with an
// error without the caller cancelling it.
func TestMaintenancePanicWithdrawsReadinessAndShutsDown(t *testing.T) {
	deps := testDeps(t)
	logs := &syncLog{}
	deps.Logger = observability.NewTestLogger("debug", logs)
	deps.Gate = func(context.Context) error { return nil }
	deps.Maintenance = []func(context.Context){func(ctx context.Context) {
		panic("boom")
	}}
	app := New(deps)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- app.Run(context.Background(), adminLn, func(context.Context) (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") })
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrMaintenancePanic) {
			t.Errorf("Run = %v, want ErrMaintenancePanic", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not shut down after a maintenance panic")
	}
	if deps.Readiness.Ready() || deps.Readiness.AcceptingWebhooks() {
		t.Error("readiness or acceptance kept after a maintenance panic")
	}
	out := logs.String()
	if !strings.Contains(out, `"event":"maintenance_panic"`) || !strings.Contains(out, `"stack"`) {
		t.Errorf("panic not logged with a stack:\n%s", out)
	}
	if strings.Contains(out, "boom") || strings.Contains(out, "(0x") {
		t.Errorf("panic value or stack arguments not redacted:\n%s", out)
	}
}

type syncLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncLog) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLog) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

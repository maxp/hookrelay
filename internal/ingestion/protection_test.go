package ingestion

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

// blockingAcceptor holds every acceptance until released.
type blockingAcceptor struct {
	entered chan struct{}
	release chan struct{}
}

func (b *blockingAcceptor) Accept(_ context.Context, req AcceptRequest) AcceptResult {
	b.entered <- struct{}{}
	<-b.release
	return AcceptResult{Outcome: AcceptAccepted, MessageID: req.MessageID}
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() == name {
			return f.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
}

// TestInflightLimit pins the process-local semaphore: excess requests past
// route resolution get 503 + Retry-After before their body is read, and
// hookrelay_webhook_inflight reports the occupancy.
func TestInflightLimit(t *testing.T) {
	hs := newHarness(t)
	blocker := &blockingAcceptor{entered: make(chan struct{}, 1), release: make(chan struct{})}
	hs.h.d.Acceptor = blocker
	hs.h.inflight = make(chan struct{}, 1)

	done := make(chan int, 1)
	go func() { done <- hs.post("/webhook/telegram/wh_on", chatUpdate).Code }()
	<-blocker.entered
	if got := gaugeValue(t, hs.reg, "hookrelay_webhook_inflight"); got != 1 {
		t.Errorf("inflight gauge = %v, want 1", got)
	}

	w := hs.post("/webhook/telegram/wh_on", chatUpdate)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" || w.Body.Len() != 0 {
		t.Errorf("excess request = %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
	}
	if hs.count("overloaded", "telegram") != 1 {
		t.Error("overloaded outcome not counted")
	}
	// Unknown endpoints are resolved before the semaphore and stay 404.
	if w := hs.post("/webhook/telegram/wh_missing", chatUpdate); w.Code != http.StatusNotFound {
		t.Errorf("unknown endpoint under load = %d", w.Code)
	}

	close(blocker.release)
	if code := <-done; code != http.StatusOK {
		t.Errorf("held request = %d", code)
	}
	if got := gaugeValue(t, hs.reg, "hookrelay_webhook_inflight"); got != 0 {
		t.Errorf("inflight gauge after release = %v", got)
	}
}

// TestRequestDeadlineCoversBodyRead pins that a sender stalling mid-body is
// cut off by the request deadline with 408 and nothing accepted.
func TestRequestDeadlineCoversBodyRead(t *testing.T) {
	hs := newHarness(t)
	hs.h.d.RequestTimeout = 200 * time.Millisecond
	srv := httptest.NewServer(hs.h)
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "POST /webhook/telegram/wh_on HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\n"+
		"X-Telegram-Bot-Api-Secret-Token: %s\r\nContent-Length: 100\r\n\r\n{\"update_id\":", testSecret)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	start := time.Now()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response to a stalled body: %v", err)
	}
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Errorf("stalled body = %d, want 408", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("deadline took %v", elapsed)
	}
	if len(hs.acceptor.requests) != 0 {
		t.Error("stalled request reached acceptance")
	}
	if hs.count("request_timeout", "telegram") != 1 {
		t.Error("request_timeout not counted")
	}
}

type fakeCapacity struct {
	c   Capacity
	err error
}

func (f *fakeCapacity) Capacity(context.Context) (Capacity, error) { return f.c, f.err }

// TestAcceptingWebhooks pins the global stop conditions and the dedup
// capacity gauges.
func TestAcceptingWebhooks(t *testing.T) {
	hs := newHarness(t)
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("no capacity reader must mean accepting")
	}
	capacity := &fakeCapacity{c: Capacity{QueuedMessages: 5, MaxQueuedMessages: 10, DedupRecords: 7, MaxDedupRecords: 100}}
	hs.h.d.Capacity = capacity
	if !hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("below caps must be accepting")
	}
	if gaugeValue(t, hs.reg, "hookrelay_dedup_records") != 7 || gaugeValue(t, hs.reg, "hookrelay_dedup_record_capacity") != 100 {
		t.Error("dedup gauges not refreshed")
	}
	for name, c := range map[string]Capacity{
		"global queue full": {QueuedMessages: 10, MaxQueuedMessages: 10, DedupRecords: 0, MaxDedupRecords: 100},
		"dedup full":        {QueuedMessages: 0, MaxQueuedMessages: 10, DedupRecords: 100, MaxDedupRecords: 100},
	} {
		capacity.c = c
		if hs.h.AcceptingWebhooks(context.Background()) {
			t.Errorf("%s: still accepting", name)
		}
	}
	capacity.err = errors.New("valkey down")
	if hs.h.AcceptingWebhooks(context.Background()) {
		t.Error("unreadable capacity must stop acceptance")
	}
}

// TestAcceptanceStopSignal pins that global and dedup capacity rejections
// signal the stop immediately while a full Recipient does not.
func TestAcceptanceStopSignal(t *testing.T) {
	for outcome, want := range map[AcceptOutcome]bool{
		AcceptGlobalCapacity:    true,
		AcceptDedupCapacity:     true,
		AcceptRecipientCapacity: false,
		AcceptRecipientBlocked:  false,
	} {
		hs := newHarness(t)
		stopped := false
		hs.h.d.OnAcceptanceStop = func() { stopped = true }
		hs.acceptor.result = AcceptResult{Outcome: outcome}
		if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
			t.Errorf("%s: %d", outcome, w.Code)
		}
		if stopped != want {
			t.Errorf("%s: stop signalled = %v, want %v", outcome, stopped, want)
		}
	}
}

// TestNewHandlerDefaults pins the documented protection defaults.
func TestNewHandlerDefaults(t *testing.T) {
	types, _ := Builtin(BuiltinOptions{})
	h, err := NewHandler(HandlerDeps{Registry: types, Gen: &seqGen{}, Clock: fixedClock{}, Logger: observability.NewTestLogger("error", &strings.Builder{})})
	if err != nil {
		t.Fatal(err)
	}
	if cap(h.inflight) != 100 || h.d.RequestTimeout != 10*time.Second {
		t.Errorf("defaults: inflight %d, timeout %v", cap(h.inflight), h.d.RequestTimeout)
	}
	if _, err := NewHandler(HandlerDeps{Registry: types, Gen: &seqGen{}}); err == nil {
		t.Error("missing Clock accepted")
	}
}

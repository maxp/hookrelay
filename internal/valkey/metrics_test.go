package valkey

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/ingestion"
)

// TestValkeyMetrics pins the required Valkey metrics over real Valkey:
// operation outcomes and durations per bounded operation, script errors
// for server-side script failures only, and the connected gauge.
func TestValkeyMetrics(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	reg := prometheus.NewRegistry()
	if err := a.Instrument(reg); err != nil {
		t.Fatal(err)
	}
	gate(t, a, false)
	if got := value(t, reg, "hookrelay_valkey_connected"); got != 1 {
		t.Errorf("connected after a successful gate = %v", got)
	}

	if r := NewMessageAcceptor(a, testLimits()).Accept(ctx, acceptReq("m1", "d1", "b1")); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("accept = %+v", r)
	}
	if _, err := a.GetEndpoint(ctx, "telegram", "wh_none"); err != nil {
		t.Fatal(err)
	}
	// A Lua argument error is a server-side script error.
	if _, err := a.RunScript(ctx, "accept_v1", []string{"x"}, []string{"y"}); err == nil {
		t.Fatal("malformed call succeeded")
	}
	for _, tc := range []struct {
		operation, outcome string
		want               float64
	}{
		{"accept", "success", 1},
		{"accept", "error", 1},
		{"endpoint_read", "success", 1},
	} {
		if got := value(t, reg, "hookrelay_valkey_operations_total", "operation", tc.operation, "outcome", tc.outcome); got != tc.want {
			t.Errorf("operations{%s,%s} = %v, want %v", tc.operation, tc.outcome, got, tc.want)
		}
	}
	if got := value(t, reg, "hookrelay_valkey_script_errors_total", "script", "accept_v1"); got != 1 {
		t.Errorf("script errors = %v, want 1", got)
	}
	if n := value(t, reg, "hookrelay_valkey_operation_duration_seconds", "operation", "accept"); n != 2 {
		t.Errorf("accept duration samples = %v, want 2", n)
	}

	// A transport failure is an operation error but not a script error, and
	// a failed gate PING clears the connected gauge.
	a.Close()
	NewMessageAcceptor(a, testLimits()).Accept(ctx, acceptReq("m2", "d2", "b2"))
	if got := value(t, reg, "hookrelay_valkey_operations_total", "operation", "accept", "outcome", "error"); got != 2 {
		t.Errorf("accept errors after close = %v, want 2", got)
	}
	if got := value(t, reg, "hookrelay_valkey_script_errors_total", "script", "accept_v1"); got != 1 {
		t.Errorf("transport failure counted as a script error: %v", got)
	}
	if _, err := a.ValidateReadiness(ctx, false); err == nil {
		t.Fatal("gate passed on a closed client")
	}
	if got := value(t, reg, "hookrelay_valkey_connected"); got != 0 {
		t.Errorf("connected after a failed gate = %v", got)
	}
}

// TestUninstrumentedAdapterRecordsNothing pins that metrics are optional.
func TestUninstrumentedAdapterRecordsNothing(t *testing.T) {
	var m *adapterMetrics
	m.observe("accept", timeZero, nil)
	m.observeScript("accept_v1", timeZero, errScriptResult)
	m.setConnected(true)
}

var timeZero = time.Time{}

// value reads one series (by metric name and label values) from reg.
func value(t *testing.T, reg *prometheus.Registry, name string, labels ...string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	series:
		for _, m := range f.GetMetric() {
			for i := 0; i+1 < len(labels); i += 2 {
				found := false
				for _, l := range m.GetLabel() {
					found = found || (l.GetName() == labels[i] && l.GetValue() == labels[i+1])
				}
				if !found {
					continue series
				}
			}
			switch {
			case m.GetCounter() != nil:
				return m.GetCounter().GetValue()
			case m.GetGauge() != nil:
				return m.GetGauge().GetValue()
			case m.GetHistogram() != nil:
				return float64(m.GetHistogram().GetSampleCount())
			}
		}
	}
	return 0
}

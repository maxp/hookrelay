package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type fakeNacker struct {
	requests  []NackRequest
	deadlines []time.Duration
	result    NackResult
}

func (f *fakeNacker) Nack(ctx context.Context, req NackRequest) NackResult {
	f.requests = append(f.requests, req)
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, time.Until(d))
	}
	return f.result
}

type fakeStats struct{ stats Stats }

func (f fakeStats) Stats(context.Context) (Stats, error) { return f.stats, nil }

func (hs *harness) nack(body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/nack", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+secret)
	r.Header.Set("Consumer-Instance-Id", "worker-7")
	w := httptest.NewRecorder()
	hs.h.ServeHTTP(w, r)
	return w
}

// metric returns the family's metric whose labels match, or nil.
func metric(t *testing.T, hs *harness, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	families, err := hs.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	next:
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want != lp.GetValue() {
					continue next
				}
			}
			return m
		}
	}
	return nil
}

func scheduled() NackResult {
	return NackResult{
		Outcome: NackRetryScheduled, Result: "retry_scheduled", MessageID: "m1", Attempt: 1, RetryAtMs: 5000,
		RecipientIdentity: "telegram:42:chat:-1", DeliveryCycle: 1, ClaimedMs: 1000, CompletedMs: 3500,
	}
}

// TestNackContract pins the retry_scheduled body (identical for a repeat),
// the request passed to storage (token digest, reason code, the drawn delay
// list, the attempt limit, the 5 s deadline), the feature event, and the
// attempt counter and duration observed once.
func TestNackContract(t *testing.T) {
	hs := newHarness(t)
	hs.nacker.result = scheduled()
	w := hs.nack(`{"delivery_token":"` + validToken + `","reason_code":"temporary_dependency_failure"}`)
	want := `{"status":"retry_scheduled","message_id":"m1","attempt":1,"retry_at_ms":5000}` + "\n"
	if w.Code != 200 || w.Body.String() != want {
		t.Fatalf("nack = %d %s", w.Code, w.Body.String())
	}
	sum := sha256.Sum256([]byte(validToken))
	req := hs.nacker.requests[0]
	if req.Token != validToken || req.TokenDigest != hex.EncodeToString(sum[:]) || req.ReasonCode != "temporary_dependency_failure" || req.MaxAttempts != 4 {
		t.Errorf("nack request = %+v", req)
	}
	// The harness draws the lowest jitter: 0.5 × nominal.
	if got := req.RetryDelaysMs; len(got) != 3 || got[0] != 500 || got[1] != 2500 || got[2] != 15000 {
		t.Errorf("retry delays = %v, want [500 2500 15000]", got)
	}
	if d := hs.nacker.deadlines[0]; d <= 0 || d > NackTimeout {
		t.Errorf("storage deadline = %s, want within %s", d, NackTimeout)
	}

	hs.nacker.result = NackResult{Outcome: NackAlreadyNacked, Result: "retry_scheduled", MessageID: "m1", Attempt: 1, RetryAtMs: 5000}
	if w := hs.nack(`{"delivery_token":"` + validToken + `"}`); w.Code != 200 || w.Body.String() != want {
		t.Errorf("repeat = %d %s", w.Code, w.Body.String())
	}
	if hs.nacker.requests[1].ReasonCode != "" {
		t.Errorf("absent reason_code passed as %q", hs.nacker.requests[1].ReasonCode)
	}

	var event map[string]any
	if err := json.Unmarshal(hs.logs.Bytes(), &event); err != nil {
		t.Fatalf("expected exactly one event: %v\n%s", err, hs.logs.String())
	}
	for k, v := range map[string]any{
		"event": "delivery_nacked", "message_id": "m1", "recipient_scope": "chat", "chat_id": "-1", "delivery_cycle": 1.0,
		"attempt": 1.0, "retry_at_ms": 5000.0, "reason_code": "temporary_dependency_failure", "consumer_instance_id": "worker-7",
	} {
		if event[k] != v {
			t.Errorf("event %s = %v, want %v", k, event[k], v)
		}
	}
	if strings.Contains(hs.logs.String(), "dlv_") {
		t.Error("token logged")
	}
	labels := map[string]string{"recipient_scope": "chat", "outcome": "nack"}
	if m := metric(t, hs, "hookrelay_delivery_attempts_total", labels); m == nil || m.GetCounter().GetValue() != 1 {
		t.Errorf("attempts{nack} = %v", m)
	}
	if m := metric(t, hs, "hookrelay_delivery_attempt_duration_seconds", labels); m == nil ||
		m.GetHistogram().GetSampleCount() != 1 || m.GetHistogram().GetSampleSum() != 2.5 {
		t.Errorf("attempt duration{nack} = %v, want one 2.5 s sample", m)
	}
}

// TestNackOutcomesAndValidation pins the nack error mapping and body checks.
func TestNackOutcomesAndValidation(t *testing.T) {
	for outcome, want := range map[NackOutcome]struct {
		status int
		code   string
	}{
		NackAlreadyAcknowledged:   {409, "delivery_already_acknowledged"},
		NackNotFound:              {404, "delivery_token_not_found"},
		NackStale:                 {409, "stale_delivery_token"},
		NackRecipientBlocked:      {409, "recipient_blocked"},
		NackAttemptsExhausted:     {500, "internal_error"},
		NackInternalFailure:       {500, "internal_error"},
		NackDependencyUnavailable: {503, "dependency_unavailable"},
	} {
		hs := newHarness(t)
		hs.nacker.result = NackResult{Outcome: outcome}
		w := hs.nack(`{"delivery_token":"` + validToken + `"}`)
		if w.Code != want.status || errorCode(t, w) != want.code {
			t.Errorf("%s: %d %s", outcome, w.Code, w.Body.String())
		}
		if outcome == NackDependencyUnavailable && w.Header().Get("Retry-After") != "1" {
			t.Errorf("%s: Retry-After = %q", outcome, w.Header().Get("Retry-After"))
		}
		if strings.Contains(w.Body.String(), validToken) {
			t.Errorf("%s: token echoed", outcome)
		}
		if m := metric(t, hs, "hookrelay_delivery_attempts_total", map[string]string{"outcome": "nack"}); m != nil {
			t.Errorf("%s: attempt counted", outcome)
		}
	}
	for name, body := range map[string]string{
		"missing token":      `{}`,
		"malformed token":    `{"delivery_token":"abc"}`,
		"number token":       `{"delivery_token":5}`,
		"unknown field":      `{"delivery_token":"` + validToken + `","x":1}`,
		"empty reason":       `{"delivery_token":"` + validToken + `","reason_code":""}`,
		"reason with space":  `{"delivery_token":"` + validToken + `","reason_code":"bad reason"}`,
		"reason too long":    `{"delivery_token":"` + validToken + `","reason_code":"` + strings.Repeat("r", 65) + `"}`,
		"non-ASCII reason":   `{"delivery_token":"` + validToken + `","reason_code":"é"}`,
		"number reason_code": `{"delivery_token":"` + validToken + `","reason_code":5}`,
	} {
		hs := newHarness(t)
		if w := hs.nack(body); w.Code != 400 || errorCode(t, w) != "invalid_request" || len(hs.nacker.requests) != 0 {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
	hs := newHarness(t)
	hs.nacker.result = scheduled()
	if w := hs.nack(`{"delivery_token":"` + validToken + `","reason_code":"` + strings.Repeat("r", 64) + `"}`); w.Code != 200 {
		t.Errorf("64-character reason_code = %d %s", w.Code, w.Body.String())
	}
}

// TestAckAfterNackAndAttemptDuration pins 409 delivery_already_nacked and
// the acknowledged attempt duration from claim to acknowledgement.
func TestAckAfterNackAndAttemptDuration(t *testing.T) {
	hs := newHarness(t)
	hs.acker.result = AckResult{Outcome: AckAlreadyNacked}
	if w := hs.ack(`{"delivery_token":"` + validToken + `"}`); w.Code != 409 || errorCode(t, w) != "delivery_already_nacked" {
		t.Errorf("ack after nack = %d %s", w.Code, w.Body.String())
	}

	hs.acker.result = AckResult{Outcome: AckAcknowledged, MessageID: "m1", AcknowledgedMs: 1750, RecipientIdentity: "telegram:42:chat:-1", DeliveryCycle: 1, Attempt: 1, ClaimedMs: 1000}
	if w := hs.ack(`{"delivery_token":"` + validToken + `"}`); w.Code != 200 {
		t.Fatalf("ack = %d %s", w.Code, w.Body.String())
	}
	m := metric(t, hs, "hookrelay_delivery_attempt_duration_seconds", map[string]string{"recipient_scope": "chat", "outcome": "acknowledged"})
	if m == nil || m.GetHistogram().GetSampleCount() != 1 || m.GetHistogram().GetSampleSum() != 0.75 {
		t.Errorf("attempt duration{acknowledged} = %v, want one 0.75 s sample", m)
	}
}

// TestRetriesWaitingGauge pins the retries_waiting gauge refresh.
func TestRetriesWaitingGauge(t *testing.T) {
	hs := newHarness(t)
	hs.h.d.Stats = fakeStats{Stats{RetriesWaiting: 3}}
	hs.h.RefreshGauges(context.Background())
	if m := metric(t, hs, "hookrelay_retries_waiting", nil); m == nil || m.GetGauge().GetValue() != 3 {
		t.Errorf("retries_waiting = %v", m)
	}
}

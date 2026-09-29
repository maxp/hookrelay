package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

const (
	secret = "consumer-secret-value-01"
	opID   = "0195c4d8-0000-7000-8000-000000000001"
)

type fakeClaimer struct {
	requests []ClaimRequest
	result   ClaimResult
}

func (f *fakeClaimer) Claim(_ context.Context, req ClaimRequest) ClaimResult {
	f.requests = append(f.requests, req)
	return f.result
}

type fixedGen struct{}

func (fixedGen) UUIDv7() string                { return "0195-request" }
func (fixedGen) Base64URL(int) (string, error) { return "AAAAAAAAAAAAAAAAAAAAAA", nil }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.UnixMilli(1740000000000) }

type fakeAcker struct {
	requests []AckRequest
	result   AckResult
}

func (f *fakeAcker) Ack(_ context.Context, req AckRequest) AckResult {
	f.requests = append(f.requests, req)
	return f.result
}

type harness struct {
	h       *Handler
	acker   *fakeAcker
	nacker  *fakeNacker
	claimer *fakeClaimer
	logs    *bytes.Buffer
	reg     *prometheus.Registry
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	hs := &harness{claimer: &fakeClaimer{}, acker: &fakeAcker{}, nacker: &fakeNacker{}, logs: &bytes.Buffer{}, reg: prometheus.NewRegistry()}
	attempts, err := NewAttemptMetrics(hs.reg)
	if err != nil {
		t.Fatal(err)
	}
	hs.h, err = NewHandler(HandlerDeps{
		Attempts: attempts,
		Claimer:  hs.claimer, Acknowledger: hs.acker, NegativeAcknowledger: hs.nacker, RetryPolicy: defaultPolicy(),
		Uniform: func() float64 { return 0 }, ConsumerSecret: secret, Gen: fixedGen{}, Clock: fixedClock{},
		Logger: observability.NewTestLogger("debug", hs.logs), Registerer: hs.reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hs
}

func (hs *harness) claim(body string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/claim", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+secret)
	for _, m := range mutate {
		m(r)
	}
	w := httptest.NewRecorder()
	hs.h.ServeHTTP(w, r)
	return w
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var b errorBody
	if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
		t.Fatalf("error body %q: %v", w.Body.String(), err)
	}
	if b.Error.RequestID != "0195-request" {
		t.Errorf("request_id = %q", b.Error.RequestID)
	}
	return b.Error.Code
}

const storedMessage = `{"message_id":"m1","received_ms":1,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-1"},"platform_event_type":"message","payload":{"x":1}}`

func claimedResult(outcome ClaimOutcome) ClaimResult {
	return ClaimResult{
		Outcome:     outcome,
		Delivery:    Delivery{Token: "dlv_secret_token", MessageID: "m1", DeliveryCycle: 1, Attempt: 1, ClaimedMs: 100, LeaseExpiresMs: 60100},
		MessageJSON: []byte(storedMessage),
	}
}

// TestClaimSuccessContract pins the 200 body, the request passed to storage
// (token, digests, defaults, instance id), and the feature event without
// the token.
func TestClaimSuccessContract(t *testing.T) {
	hs := newHarness(t)
	hs.claimer.result = claimedResult(ClaimClaimed)
	w := hs.claim(`{"operation_id":"`+opID+`","wait_ms":0}`, func(r *http.Request) { r.Header.Set("Consumer-Instance-Id", "worker-7") })
	if w.Code != http.StatusOK || w.Header().Get("X-Request-Id") != "0195-request" {
		t.Fatalf("claim = %d %s", w.Code, w.Body.String())
	}
	want := `{"delivery":{"delivery_token":"dlv_secret_token","delivery_cycle":1,"attempt":1,"claimed_ms":100,"lease_expires_ms":60100},"message":` + storedMessage + "}\n"
	if w.Body.String() != want {
		t.Errorf("body:\n%s\nwant:\n%s", w.Body.String(), want)
	}
	req := hs.claimer.requests[0]
	tokenSum := sha256.Sum256([]byte("dlv_AAAAAAAAAAAAAAAAAAAAAA"))
	argsSum := sha256.Sum256([]byte(opID + "\n0"))
	if req.Token != "dlv_AAAAAAAAAAAAAAAAAAAAAA" || req.TokenDigest != hex.EncodeToString(tokenSum[:]) ||
		req.ArgsDigest != hex.EncodeToString(argsSum[:]) || req.ConsumerInstanceID != "worker-7" || !req.RecordEmpty {
		t.Errorf("claim request = %+v", req)
	}

	var event map[string]any
	if err := json.Unmarshal(hs.logs.Bytes(), &event); err != nil {
		t.Fatalf("log: %v\n%s", err, hs.logs.String())
	}
	for k, v := range map[string]any{"event": "delivery_claimed", "message_id": "m1", "recipient_scope": "chat", "chat_id": "-1",
		"bot_id": "42", "delivery_cycle": float64(1), "attempt": float64(1), "consumer_instance_id": "worker-7", "operation_id": opID} {
		if event[k] != v {
			t.Errorf("event %s = %v, want %v", k, event[k], v)
		}
	}
	if strings.Contains(hs.logs.String(), "dlv_") || strings.Contains(hs.logs.String(), `"x":1`) {
		t.Error("token or payload logged")
	}

	// wait_ms defaults to 30000 in the arguments digest; an unusable
	// instance id is ignored.
	hs.claim(`{"operation_id":"`+opID+`"}`, func(r *http.Request) { r.Header.Set("Consumer-Instance-Id", "bad id!") })
	req = hs.claimer.requests[1]
	argsSum = sha256.Sum256([]byte(opID + "\n30000"))
	if req.ArgsDigest != hex.EncodeToString(argsSum[:]) || req.ConsumerInstanceID != "" {
		t.Errorf("defaults = %+v", req)
	}
}

// TestClaimOutcomeMapping pins status, error code, and Retry-After per
// outcome.
func TestClaimOutcomeMapping(t *testing.T) {
	cases := []struct {
		result     ClaimResult
		status     int
		code       string
		retryAfter bool
	}{
		{claimedResult(ClaimReplayActive), 200, "", false},
		{ClaimResult{Outcome: ClaimEmpty}, 204, "", false},
		{ClaimResult{Outcome: ClaimReplayEmpty}, 204, "", false},
		{ClaimResult{Outcome: ClaimNoLongerActive}, 409, "claim_no_longer_active", false},
		{ClaimResult{Outcome: ClaimOperationConflict}, 409, "operation_conflict", false},
		{ClaimResult{Outcome: ClaimLimitExceeded}, 429, "consumer_limit_exceeded", true},
		{ClaimResult{Outcome: ClaimDependencyUnavailable}, 503, "dependency_unavailable", true},
		{ClaimResult{Outcome: ClaimInternalFailure}, 500, "internal_error", false},
		{ClaimResult{Outcome: ClaimClaimed, MessageJSON: []byte("{broken")}, 500, "internal_error", false},
	}
	for _, tc := range cases {
		hs := newHarness(t)
		hs.claimer.result = tc.result
		w := hs.claim(`{"operation_id":"` + opID + `","wait_ms":0}`)
		if w.Code != tc.status || (w.Header().Get("Retry-After") == "1") != tc.retryAfter {
			t.Errorf("%s: %d Retry-After %q", tc.result.Outcome, w.Code, w.Header().Get("Retry-After"))
			continue
		}
		switch {
		case tc.status == 204 && w.Body.Len() != 0:
			t.Errorf("%s: 204 with a body", tc.result.Outcome)
		case tc.code != "" && errorCode(t, w) != tc.code:
			t.Errorf("%s: code = %s", tc.result.Outcome, errorCode(t, w))
		}
		if strings.Contains(w.Body.String()+hs.logs.String(), "dlv_") && tc.status != 200 {
			t.Errorf("%s: token leaked", tc.result.Outcome)
		}
	}
}

// TestClaimRequestValidation pins auth and the strict body discipline; no
// invalid request reaches storage.
func TestClaimRequestValidation(t *testing.T) {
	valid := `{"operation_id":"` + opID + `","wait_ms":0}`
	cases := []struct {
		name   string
		body   string
		mutate func(*http.Request)
		status int
		code   string
	}{
		{"missing secret", valid, func(r *http.Request) { r.Header.Del("Authorization") }, 401, "unauthenticated"},
		{"wrong secret", valid, func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") }, 401, "unauthenticated"},
		{"basic scheme", valid, func(r *http.Request) { r.Header.Set("Authorization", "Basic "+secret) }, 401, "unauthenticated"},
		{"text/plain", valid, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, 415, "unsupported_media_type"},
		{"too large", `{"operation_id":"` + strings.Repeat("a", 17000) + `"}`, nil, 413, "request_too_large"},
		{"unknown field", `{"operation_id":"` + opID + `","extra":1}`, nil, 400, "invalid_request"},
		{"string wait", `{"operation_id":"` + opID + `","wait_ms":"5"}`, nil, 400, "invalid_request"},
		{"fractional wait", `{"operation_id":"` + opID + `","wait_ms":1.5}`, nil, 400, "invalid_request"},
		{"negative wait", `{"operation_id":"` + opID + `","wait_ms":-1}`, nil, 400, "invalid_request"},
		{"wait too long", `{"operation_id":"` + opID + `","wait_ms":30001}`, nil, 400, "invalid_request"},
		{"missing operation", `{"wait_ms":0}`, nil, 400, "invalid_request"},
		{"uuid v4", `{"operation_id":"0195c4d8-0000-4000-8000-000000000001"}`, nil, 400, "invalid_request"},
		{"trailing data", valid + `{}`, nil, 400, "invalid_request"},
		{"array body", `[1]`, nil, 400, "invalid_request"},
	}
	for _, tc := range cases {
		hs := newHarness(t)
		w := hs.claim(tc.body, func(r *http.Request) {
			if tc.mutate != nil {
				tc.mutate(r)
			}
		})
		if w.Code != tc.status || errorCode(t, w) != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body.String())
		}
		if w.Header().Get("X-Request-Id") == "" {
			t.Errorf("%s: missing X-Request-Id", tc.name)
		}
		if len(hs.claimer.requests) != 0 {
			t.Errorf("%s: reached storage", tc.name)
		}
		if strings.Contains(w.Body.String(), secret) {
			t.Errorf("%s: secret echoed", tc.name)
		}
	}
}

func (hs *harness) ack(body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/ack", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+secret)
	r.Header.Set("Consumer-Instance-Id", "worker-7")
	w := httptest.NewRecorder()
	hs.h.ServeHTTP(w, r)
	return w
}

const validToken = "dlv_AAAAAAAAAAAAAAAAAAAAAA"

// TestAckContract pins the ack success body (identical for a repeat), the
// token digest passed to storage, the feature event, and the attempts
// metric counted once.
func TestAckContract(t *testing.T) {
	hs := newHarness(t)
	hs.acker.result = AckResult{Outcome: AckAcknowledged, MessageID: "m1", AcknowledgedMs: 500, RecipientIdentity: "telegram:42:chat:-1", DeliveryCycle: 1, Attempt: 1}
	w := hs.ack(`{"delivery_token":"` + validToken + `"}`)
	want := `{"status":"acknowledged","message_id":"m1","acknowledged_ms":500}` + "\n"
	if w.Code != 200 || w.Body.String() != want {
		t.Fatalf("ack = %d %s", w.Code, w.Body.String())
	}
	sum := sha256.Sum256([]byte(validToken))
	if req := hs.acker.requests[0]; req.Token != validToken || req.TokenDigest != hex.EncodeToString(sum[:]) {
		t.Errorf("ack request = %+v", req)
	}
	hs.acker.result = AckResult{Outcome: AckAlreadyAcknowledged, MessageID: "m1", AcknowledgedMs: 500}
	if w := hs.ack(`{"delivery_token":"` + validToken + `"}`); w.Code != 200 || w.Body.String() != want {
		t.Errorf("repeat = %d %s", w.Code, w.Body.String())
	}

	var event map[string]any
	if err := json.Unmarshal(hs.logs.Bytes(), &event); err != nil {
		t.Fatalf("expected exactly one event: %v\n%s", err, hs.logs.String())
	}
	for k, v := range map[string]any{"event": "delivery_acknowledged", "message_id": "m1", "recipient_scope": "chat", "chat_id": "-1", "consumer_instance_id": "worker-7"} {
		if event[k] != v {
			t.Errorf("event %s = %v, want %v", k, event[k], v)
		}
	}
	if strings.Contains(hs.logs.String(), "dlv_") {
		t.Error("token logged")
	}
	families, _ := hs.reg.Gather()
	for _, f := range families {
		if f.GetName() == "hookrelay_delivery_attempts_total" {
			m := f.GetMetric()[0]
			if m.GetCounter().GetValue() != 1 || len(f.GetMetric()) != 1 {
				t.Errorf("attempts = %v", f.GetMetric())
			}
			return
		}
	}
	t.Error("hookrelay_delivery_attempts_total not exported")
}

// TestAckOutcomesAndValidation pins the ack error mapping and body checks.
func TestAckOutcomesAndValidation(t *testing.T) {
	for outcome, want := range map[AckOutcome]struct {
		status int
		code   string
	}{
		AckNotFound:              {404, "delivery_token_not_found"},
		AckStale:                 {409, "stale_delivery_token"},
		AckRecipientBlocked:      {409, "recipient_blocked"},
		AckDependencyUnavailable: {503, "dependency_unavailable"},
		AckInternalFailure:       {500, "internal_error"},
	} {
		hs := newHarness(t)
		hs.acker.result = AckResult{Outcome: outcome}
		w := hs.ack(`{"delivery_token":"` + validToken + `"}`)
		if w.Code != want.status || errorCode(t, w) != want.code {
			t.Errorf("%s: %d %s", outcome, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), validToken) {
			t.Errorf("%s: token echoed", outcome)
		}
	}
	for name, body := range map[string]string{
		"missing token": `{}`,
		"malformed":     `{"delivery_token":"abc"}`,
		"number token":  `{"delivery_token":5}`,
		"unknown field": `{"delivery_token":"` + validToken + `","x":1}`,
		"operation_id":  `{"delivery_token":"` + validToken + `","operation_id":"x"}`,
	} {
		hs := newHarness(t)
		if w := hs.ack(body); w.Code != 400 || errorCode(t, w) != "invalid_request" || len(hs.acker.requests) != 0 {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
}

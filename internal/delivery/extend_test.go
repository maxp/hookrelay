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
)

type fakeExtender struct {
	requests  []ExtendRequest
	deadlines []time.Duration
	result    ExtendResult
}

func (f *fakeExtender) Extend(ctx context.Context, req ExtendRequest) ExtendResult {
	f.requests = append(f.requests, req)
	if d, ok := ctx.Deadline(); ok {
		f.deadlines = append(f.deadlines, time.Until(d))
	}
	return f.result
}

func (hs *harness) extend(body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/extend", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+secret)
	r.Header.Set("Consumer-Instance-Id", "worker-7")
	w := httptest.NewRecorder()
	hs.h.ServeHTTP(w, r)
	return w
}

// TestExtendContract pins the extended body (identical for a replay), the
// request passed to storage (token digest, operation id, arguments digest
// over the operation and token digest, the 5 s deadline), and the
// delivery_lease_extended event emitted once.
func TestExtendContract(t *testing.T) {
	hs := newHarness(t)
	hs.extender.result = ExtendResult{Outcome: ExtendExtended, MessageID: "m1", LeaseExpiresMs: 90_000, MaxLeaseExpiresMs: 300_000,
		RecipientIdentity: "telegram:42:chat:-1", DeliveryCycle: 1, Attempt: 2}
	body := `{"delivery_token":"` + validToken + `","operation_id":"` + opID + `"}`
	w := hs.extend(body)
	want := `{"status":"extended","message_id":"m1","lease_expires_ms":90000,"max_lease_expires_ms":300000}` + "\n"
	if w.Code != 200 || w.Body.String() != want {
		t.Fatalf("extend = %d %s", w.Code, w.Body.String())
	}
	tok := sha256.Sum256([]byte(validToken))
	args := sha256.Sum256([]byte(opID + "\n" + hex.EncodeToString(tok[:])))
	req := hs.extender.requests[0]
	if req.Token != validToken || req.TokenDigest != hex.EncodeToString(tok[:]) || req.OperationID != opID || req.ArgsDigest != hex.EncodeToString(args[:]) {
		t.Errorf("extend request = %+v", req)
	}
	if d := hs.extender.deadlines[0]; d <= 0 || d > ExtendTimeout {
		t.Errorf("storage deadline = %s", d)
	}
	hs.extender.result = ExtendResult{Outcome: ExtendReplay, MessageID: "m1", LeaseExpiresMs: 90_000, MaxLeaseExpiresMs: 300_000}
	if w := hs.extend(body); w.Code != 200 || w.Body.String() != want {
		t.Errorf("replay = %d %s", w.Code, w.Body.String())
	}

	var event map[string]any
	if err := json.Unmarshal(hs.logs.Bytes(), &event); err != nil {
		t.Fatalf("expected exactly one event: %v\n%s", err, hs.logs.String())
	}
	for k, v := range map[string]any{
		"event": "delivery_lease_extended", "message_id": "m1", "recipient_scope": "chat", "chat_id": "-1",
		"lease_expires_ms": 90000.0, "max_lease_expires_ms": 300000.0, "consumer_instance_id": "worker-7",
		"delivery_cycle": 1.0, "attempt": 2.0, "operation_id": opID,
	} {
		if event[k] != v {
			t.Errorf("event %s = %v, want %v", k, event[k], v)
		}
	}
	if strings.Contains(hs.logs.String(), "dlv_") {
		t.Error("token logged")
	}
}

// TestExtendOutcomesAndValidation pins the extend error mapping and the
// body checks.
func TestExtendOutcomesAndValidation(t *testing.T) {
	body := `{"delivery_token":"` + validToken + `","operation_id":"` + opID + `"}`
	for outcome, want := range map[ExtendOutcome]struct {
		status int
		code   string
	}{
		ExtendOperationConflict:      {409, "operation_conflict"},
		ExtendNotFound:               {404, "delivery_token_not_found"},
		ExtendStale:                  {409, "stale_delivery_token"},
		ExtendRecipientBlocked:       {409, "recipient_blocked"},
		ExtendMaximumLifetimeReached: {409, "maximum_lease_lifetime_reached"},
		ExtendInternalFailure:        {500, "internal_error"},
		ExtendDependencyUnavailable:  {503, "dependency_unavailable"},
	} {
		hs := newHarness(t)
		hs.extender.result = ExtendResult{Outcome: outcome}
		w := hs.extend(body)
		if w.Code != want.status || errorCode(t, w) != want.code {
			t.Errorf("%s: %d %s", outcome, w.Code, w.Body.String())
		}
		if outcome == ExtendDependencyUnavailable && w.Header().Get("Retry-After") != "1" {
			t.Errorf("%s: Retry-After = %q", outcome, w.Header().Get("Retry-After"))
		}
		if strings.Contains(w.Body.String(), validToken) {
			t.Errorf("%s: token echoed", outcome)
		}
	}
	for name, body := range map[string]string{
		"missing token":        `{"operation_id":"` + opID + `"}`,
		"malformed token":      `{"delivery_token":"abc","operation_id":"` + opID + `"}`,
		"missing operation":    `{"delivery_token":"` + validToken + `"}`,
		"non-UUIDv7 operation": `{"delivery_token":"` + validToken + `","operation_id":"0195c4d8-0000-4000-8000-000000000001"}`,
		"unknown field":        `{"delivery_token":"` + validToken + `","operation_id":"` + opID + `","x":1}`,
		"client deadline":      `{"delivery_token":"` + validToken + `","operation_id":"` + opID + `","lease_expires_ms":1}`,
	} {
		hs := newHarness(t)
		if w := hs.extend(body); w.Code != 400 || errorCode(t, w) != "invalid_request" || len(hs.extender.requests) != 0 {
			t.Errorf("%s: %d %s", name, w.Code, w.Body.String())
		}
	}
}

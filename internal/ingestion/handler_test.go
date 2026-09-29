package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
)

const testSecret = "telegram-secret-001"

type fakeEndpoints struct {
	endpoints map[string]*Endpoint
	err       error
}

func (f *fakeEndpoints) LookupEndpoint(_ context.Context, webhookType, identifier string) (*Endpoint, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.endpoints[webhookType+":"+identifier], nil
}

type fakeAcceptor struct {
	requests []AcceptRequest
	result   AcceptResult
}

func (f *fakeAcceptor) Accept(_ context.Context, req AcceptRequest) AcceptResult {
	f.requests = append(f.requests, req)
	if f.result.Outcome == "" {
		return AcceptResult{Outcome: AcceptAccepted, MessageID: req.MessageID, AcceptedMs: 1}
	}
	return f.result
}

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.UnixMilli(1740000000123) }

type seqGen struct{ n int }

func (g *seqGen) UUIDv7() string {
	g.n++
	return "0195-uuid-" + string(rune('a'+g.n))
}
func (g *seqGen) Base64URL(int) (string, error) { return "x", nil }

type harness struct {
	h         *Handler
	endpoints *fakeEndpoints
	acceptor  *fakeAcceptor
	logs      *bytes.Buffer
	reg       *prometheus.Registry
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	types, err := Builtin(BuiltinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hs := &harness{
		endpoints: &fakeEndpoints{endpoints: map[string]*Endpoint{
			"telegram:wh_on":  {BotID: "42", Enabled: true, CredentialKind: "secret_token", CredentialValue: testSecret},
			"telegram:wh_off": {BotID: "42", Enabled: false, CredentialKind: "secret_token", CredentialValue: testSecret},
		}},
		acceptor: &fakeAcceptor{},
		logs:     &bytes.Buffer{},
		reg:      prometheus.NewRegistry(),
	}
	hs.h, err = NewHandler(HandlerDeps{
		Registry:   types,
		Endpoints:  hs.endpoints,
		Acceptor:   hs.acceptor,
		Gen:        &seqGen{},
		Clock:      fixedClock{},
		Logger:     observability.NewTestLogger("debug", hs.logs),
		Registerer: hs.reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return hs
}

const chatUpdate = `{"update_id":100,"message":{"message_id":1,"date":1700000000,"chat":{"id":-100},"text":"hi"}}`

func (hs *harness) post(path, body string, mutate ...func(*http.Request)) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(telegramSecretHeader, testSecret)
	for _, m := range mutate {
		m(r)
	}
	w := httptest.NewRecorder()
	hs.h.ServeHTTP(w, r)
	return w
}

func (hs *harness) count(outcome, webhookType string) float64 {
	families, _ := hs.reg.Gather()
	for _, f := range families {
		if f.GetName() != "hookrelay_webhook_requests_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["outcome"] == outcome && labels["webhook_type"] == webhookType {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

// TestWebhookAccepted pins the happy path: empty 200 with X-Request-Id and
// an acceptance request carrying the Canonical Message and identities.
func TestWebhookAccepted(t *testing.T) {
	hs := newHarness(t)
	w := hs.post("/webhook/telegram/wh_on", chatUpdate)
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("X-Request-Id") == "" {
		t.Fatalf("response = %d %q request_id %q", w.Code, w.Body.String(), w.Header().Get("X-Request-Id"))
	}
	if len(hs.acceptor.requests) != 1 {
		t.Fatalf("accept calls = %d", len(hs.acceptor.requests))
	}
	req := hs.acceptor.requests[0]
	if req.RecipientIdentity != "telegram:42:chat:-100" || req.ReceivedMs != 1740000000123 || *req.OccurredMs != 1700000000000 {
		t.Errorf("accept request = %+v", req)
	}
	if req.DedupIdentityDigest != dedupIdentityDigest("telegram", "telegram", "42", "100") || len(req.BodyDigest) != 64 {
		t.Errorf("digests = %s / %s", req.DedupIdentityDigest, req.BodyDigest)
	}
	var msg model.CanonicalMessage
	if err := json.Unmarshal(req.MessageJSON, &msg); err != nil {
		t.Fatalf("stored message: %v", err)
	}
	if msg.MessageID != req.MessageID || msg.SourceEventID != "100" || msg.PlatformEventType != "message" || msg.Recipient.ChatID != "-100" {
		t.Errorf("message = %+v", msg)
	}
	if msg.MessageID == w.Header().Get("X-Request-Id") {
		t.Error("message_id reused the request id")
	}
	if hs.count("accepted", "telegram") != 1 {
		t.Error("accepted outcome not counted")
	}

	var event map[string]any
	if err := json.Unmarshal(hs.logs.Bytes(), &event); err != nil {
		t.Fatalf("log is not one JSON event: %v\n%s", err, hs.logs.String())
	}
	for k, want := range map[string]any{
		"event": "webhook_accepted", "webhook_type": "telegram", "webhook_identifier": "wh_on", "bot_id": "42",
		"recipient_scope": "chat", "chat_id": "-100", "platform_event_type": "message", "status": float64(200),
		"message_id": msg.MessageID, "body_size": float64(len(chatUpdate)),
	} {
		if event[k] != want {
			t.Errorf("event %s = %v, want %v", k, event[k], want)
		}
	}
	for _, k := range []string{"duration_ms", "canonical_payload_size", "request_id", "source_ip"} {
		if _, ok := event[k]; !ok {
			t.Errorf("event missing %s", k)
		}
	}
	if strings.Contains(hs.logs.String(), testSecret) || strings.Contains(hs.logs.String(), `"hi"`) {
		t.Error("secret or payload leaked into logs")
	}
}

// TestWebhookDuplicateOutcomes pins the identical empty 200 for duplicate
// and conflicting duplicate, and the conflict metric and warning.
func TestWebhookDuplicateOutcomes(t *testing.T) {
	hs := newHarness(t)
	hs.acceptor.result = AcceptResult{Outcome: AcceptDuplicate, MessageID: "orig"}
	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("duplicate = %d", w.Code)
	}
	hs.acceptor.result = AcceptResult{Outcome: AcceptDuplicateConflict, MessageID: "orig"}
	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Fatalf("conflict = %d", w.Code)
	}
	if hs.count("duplicate", "telegram") != 2 {
		t.Error("duplicates not counted")
	}
	if !strings.Contains(hs.logs.String(), `"dedup_conflict":true`) || !strings.Contains(hs.logs.String(), `"message_id":"orig"`) {
		t.Errorf("conflict log = %s", hs.logs.String())
	}
	families, _ := hs.reg.Gather()
	found := false
	for _, f := range families {
		if f.GetName() == "hookrelay_dedup_conflicts_total" && f.GetMetric()[0].GetCounter().GetValue() == 1 {
			found = true
		}
	}
	if !found {
		t.Error("dedup_conflicts_total not incremented once")
	}
}

// TestWebhookRouteAndRejections pins route validation, 405, uniform 404,
// verification, media type, size, JSON, and retryable storage failures.
func TestWebhookRouteAndRejections(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		path       string
		body       string
		mutate     func(*http.Request)
		setup      func(*harness)
		status     int
		retryAfter bool
		outcome    string
		typeLabel  string
	}{
		{"unknown identifier", "POST", "/webhook/telegram/wh_missing", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "telegram"},
		{"disabled endpoint", "POST", "/webhook/telegram/wh_off", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "telegram"},
		{"unknown type", "POST", "/webhook/maxbot/wh_on", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"encoded slash", "POST", "/webhook/telegram/wh%2Fon", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"colon", "POST", "/webhook/telegram/wh:on", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"encoded space", "POST", "/webhook/telegram/wh%20on", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"unicode", "POST", "/webhook/telegram/wh_%C3%A9", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"dot segment", "POST", "/webhook/telegram/..", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"empty identifier", "POST", "/webhook/telegram/", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"extra segment", "POST", "/webhook/telegram/wh_on/x", chatUpdate, nil, nil, 404, false, "unknown_endpoint", "unknown"},
		{"GET", "GET", "/webhook/telegram/wh_on", "", nil, nil, 405, false, "method_not_allowed", "unknown"},
		{"wrong secret", "POST", "/webhook/telegram/wh_on", chatUpdate, func(r *http.Request) { r.Header.Set(telegramSecretHeader, "nope") }, nil, 403, false, "verification_failure", "telegram"},
		{"text/plain", "POST", "/webhook/telegram/wh_on", chatUpdate, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }, nil, 415, false, "unsupported_media_type", "telegram"},
		{"gzip", "POST", "/webhook/telegram/wh_on", chatUpdate, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }, nil, 415, false, "unsupported_media_type", "telegram"},
		{"declared too large", "POST", "/webhook/telegram/wh_on", chatUpdate, func(r *http.Request) { r.ContentLength = MaxBodyBytes + 1 }, nil, 413, false, "oversized_body", "telegram"},
		{"streamed too large", "POST", "/webhook/telegram/wh_on", `{"update_id":1,"x":"` + strings.Repeat("a", MaxBodyBytes) + `"}`, func(r *http.Request) { r.ContentLength = -1 }, nil, 413, false, "oversized_body", "telegram"},
		{"invalid JSON", "POST", "/webhook/telegram/wh_on", `{"update_id":`, nil, nil, 400, false, "invalid_json", "telegram"},
		{"non-object JSON", "POST", "/webhook/telegram/wh_on", `[1,2]`, nil, nil, 400, false, "invalid_json", "telegram"},
		{"lookup failure", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) { h.endpoints.err = errors.New("down") }, 503, true, "dependency_unavailable", "telegram"},
		{"accept unavailable", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) {
			h.acceptor.result = AcceptResult{Outcome: AcceptDependencyUnavailable}
		}, 503, true, "dependency_unavailable", "telegram"},
		{"recipient blocked", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) {
			h.acceptor.result = AcceptResult{Outcome: AcceptRecipientBlocked}
		}, 503, true, "recipient_blocked", "telegram"},
		{"capacity", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) {
			h.acceptor.result = AcceptResult{Outcome: AcceptGlobalCapacity}
		}, 503, true, "capacity_rejection", "telegram"},
		{"accept internal failure", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) {
			h.acceptor.result = AcceptResult{Outcome: AcceptInternalFailure}
		}, 500, false, "internal_error", "telegram"},
		{"corrupt endpoint", "POST", "/webhook/telegram/wh_on", chatUpdate, nil, func(h *harness) { h.endpoints.err = ErrStoredWrongType }, 500, false, "internal_error", "telegram"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hs := newHarness(t)
			if tc.setup != nil {
				tc.setup(hs)
			}
			r := httptest.NewRequest(tc.method, "/", strings.NewReader(tc.body))
			r.URL.Path = ""
			r.URL.RawPath = ""
			r.URL, _ = r.URL.Parse(tc.path)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set(telegramSecretHeader, testSecret)
			if tc.mutate != nil {
				tc.mutate(r)
			}
			w := httptest.NewRecorder()
			hs.h.ServeHTTP(w, r)
			if w.Code != tc.status || w.Body.Len() != 0 {
				t.Errorf("response = %d %q, want %d with an empty body", w.Code, w.Body.String(), tc.status)
			}
			if (w.Header().Get("Retry-After") == "1") != tc.retryAfter {
				t.Errorf("Retry-After = %q", w.Header().Get("Retry-After"))
			}
			if tc.status == 405 && w.Header().Get("Allow") != "POST" {
				t.Errorf("Allow = %q", w.Header().Get("Allow"))
			}
			if w.Header().Get("X-Request-Id") == "" {
				t.Error("missing X-Request-Id")
			}
			if hs.count(tc.outcome, tc.typeLabel) != 1 {
				t.Errorf("outcome %s{%s} not counted once", tc.outcome, tc.typeLabel)
			}
			if tc.status < 500 && len(hs.acceptor.requests) != 0 {
				t.Error("a rejected request reached acceptance")
			}
			if strings.Contains(hs.logs.String(), testSecret) {
				t.Error("secret leaked into logs")
			}
		})
	}
}

// TestWebhookLogsBoundUnknownEventType pins that a sender-chosen event name
// is truncated in logs while the Canonical Message keeps it in full.
func TestWebhookLogsBoundUnknownEventType(t *testing.T) {
	hs := newHarness(t)
	name := strings.Repeat("x", 500)
	if w := hs.post("/webhook/telegram/wh_on", `{"update_id":1,"`+name+`":{}}`); w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if strings.Contains(hs.logs.String(), name) {
		t.Error("unbounded event type logged")
	}
	var msg model.CanonicalMessage
	if err := json.Unmarshal(hs.acceptor.requests[0].MessageJSON, &msg); err != nil || msg.PlatformEventType != name {
		t.Errorf("stored event type truncated: %v", err)
	}
}

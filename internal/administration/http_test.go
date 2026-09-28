package administration

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const validCreate = `{
	"webhook_type": "telegram",
	"bot_id": "123456789",
	"credential": {"kind": "secret_token", "value": "telegram-secret-001"},
	"enabled": true
}`

// TestCreateSuccessContract pins 201 + Location + ETag + safe body.
func TestCreateSuccessContract(t *testing.T) {
	repo := newFakeRepo()
	svc, _ := testService(t, repo)
	h := Handler(svc)

	rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	if loc := rec.Header().Get("Location"); loc != "/admin/v1/webhooks/telegram/wh_AAAAAAAAAAAAAAAAAAAAAA" {
		t.Errorf("Location = %q", loc)
	}
	if etag := rec.Header().Get("ETag"); etag != `"0195-uuid:1"` {
		t.Errorf("ETag = %q", etag)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body: %v", err)
	}
	for key, want := range map[string]any{
		"webhook_type":   "telegram",
		"bot_platform":   "telegram",
		"bot_id":         "123456789",
		"enabled":        true,
		"generation_id":  "0195-uuid",
		"config_version": float64(1),
		"webhook_path":   "/webhook/telegram/wh_AAAAAAAAAAAAAAAAAAAAAA",
	} {
		if body[key] != want {
			t.Errorf("%s = %v, want %v", key, body[key], want)
		}
	}
	cred, _ := body["credential"].(map[string]any)
	if cred["kind"] != "secret_token" || cred["configured"] != true {
		t.Errorf("credential = %v", cred)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), "telegram-secret-001") {
		t.Error("credential value leaked into the response")
	}
}

// TestAuthContract pins uniform 401, constant-time compare semantics at the
// API level, and best-effort audit of rejected authentication.
func TestAuthContract(t *testing.T) {
	repo := newFakeRepo()
	svc, audit := testService(t, repo)
	h := Handler(svc)

	for _, tc := range []struct {
		name   string
		secret string
	}{
		{"missing", ""},
		{"wrong", "wrong-secret-00000000001"},
		{"prefix", "admin-secret-value"},
		{"extension", "admin-secret-value-016x"},
	} {
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", tc.secret, validCreate)
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: status = %d, want 401", tc.name, rec.Code)
		}
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body.Error.Code != "unauthenticated" {
			t.Errorf("%s: code = %q", tc.name, body.Error.Code)
		}
	}
	if audit.rejected != 4 {
		t.Errorf("rejected auth audits = %d, want 4 (best effort)", audit.rejected)
	}
}

// TestRejectedAuthAuditMetrics pins the audit metrics for best-effort
// rejected-authentication appends: a failed append is counted and the
// response stays the uniform 401.
func TestRejectedAuthAuditMetrics(t *testing.T) {
	svc, audit, _, reg := observedService(t, newFakeRepo())
	h := Handler(svc)

	doJSON(t, h, http.MethodGet, "/admin/v1/webhooks/telegram/wh_x", "wrong-secret-00000000001", "")
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "admin_auth_rejected", "outcome": "failure"}); got != 1 {
		t.Errorf("audit events = %v, want 1", got)
	}

	audit.fail = true
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/webhooks/telegram/wh_x", "wrong-secret-00000000001", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status with failing audit = %d, want 401", rec.Code)
	}
	if got := counterValue(t, reg, "hookrelay_audit_write_failures_total", map[string]string{"operation": "admin_auth_rejected"}); got != 1 {
		t.Errorf("audit write failures = %v, want 1", got)
	}
}

// TestCreateFeatureEventAndAuditMetric pins the webhook_endpoint_created
// feature event (credential kind, never the value) and the audit metric.
func TestCreateFeatureEventAndAuditMetric(t *testing.T) {
	svc, _, logs, reg := observedService(t, newFakeRepo())
	h := Handler(svc)

	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate); rec.Code != http.StatusCreated {
		t.Fatalf("create = %d", rec.Code)
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "webhook_endpoint_created", "outcome": "success"}); got != 1 {
		t.Errorf("audit events = %v, want 1", got)
	}
	var event map[string]any
	if err := json.Unmarshal(logs.Bytes(), &event); err != nil {
		t.Fatalf("feature event is not one JSON record: %v (%s)", err, logs.String())
	}
	for field, want := range map[string]any{
		"event":              "webhook_endpoint_created",
		"webhook_type":       "telegram",
		"webhook_identifier": "wh_AAAAAAAAAAAAAAAAAAAAAA",
		"bot_platform":       "telegram",
		"credential_kind":    "secret_token",
		"request_id":         "0195-uuid",
	} {
		if event[field] != want {
			t.Errorf("event %s = %v, want %v", field, event[field], want)
		}
	}
	if strings.Contains(logs.String(), "telegram-secret-001") {
		t.Error("credential value leaked into the feature event")
	}
}

// TestMediaTypeAndBodySize pins 415 for non-JSON media types and 413 for
// bodies above 16 KiB.
func TestMediaTypeAndBodySize(t *testing.T) {
	svc, _ := testService(t, newFakeRepo())
	h := Handler(svc)

	post := func(contentType, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/admin/v1/webhooks", strings.NewReader(body))
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}
		req.Header.Set("Authorization", "Bearer admin-secret-value-016")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	for _, ct := range []string{"", "text/plain", "application/json; charset=latin1", "application/json; foo=bar", "application/jsonx"} {
		rec := post(ct, validCreate)
		if rec.Code != http.StatusUnsupportedMediaType || errorCode(t, rec) != "unsupported_media_type" {
			t.Errorf("Content-Type %q: %d %s, want 415 unsupported_media_type", ct, rec.Code, rec.Body.String())
		}
	}
	if rec := post("application/json; charset=UTF-8", validCreate); rec.Code != http.StatusCreated {
		t.Errorf("utf-8 charset: %d, want 201", rec.Code)
	}

	oversized := `{"webhook_type":"telegram","bot_id":"1","credential":{"kind":"secret_token","value":"` + strings.Repeat("x", 16<<10) + `"}}`
	rec := post("application/json", oversized)
	if rec.Code != http.StatusRequestEntityTooLarge || errorCode(t, rec) != "request_too_large" {
		t.Errorf("oversized body: %d %s, want 413 request_too_large", rec.Code, rec.Body.String())
	}
}

// TestCreateUncertainOutcome pins that a possibly executed transition is
// reported as uncertain, never as a definite failure or success.
func TestCreateUncertainOutcome(t *testing.T) {
	repo := newFakeRepo()
	repo.createResult = CreateUncertain
	svc, _, logs, reg := observedService(t, repo)
	h := Handler(svc)

	rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusServiceUnavailable || errorCode(t, rec) != "dependency_unavailable" {
		t.Fatalf("uncertain create = %d %s, want 503 dependency_unavailable", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "uncertain") {
		t.Errorf("uncertain outcome not reported as uncertain: %s", rec.Body.String())
	}
	if !strings.Contains(logs.String(), `"reason_code":"outcome_uncertain"`) {
		t.Errorf("uncertain outcome not logged: %s", logs.String())
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "webhook_endpoint_created", "outcome": "success"}); got != 0 {
		t.Errorf("uncertain create counted as audited success: %v", got)
	}
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return body.Error.Code
}

// TestValidationErrorCodes pins the bounded validation failures.
func TestValidationErrorCodes(t *testing.T) {
	repo := newFakeRepo()
	svc, _ := testService(t, repo)
	h := Handler(svc)

	cases := []struct {
		name     string
		body     string
		status   int
		wantCode string
	}{
		{"unknown webhook type", `{"webhook_type":"maxbot","bot_id":"1","credential":{"kind":"secret_token","value":"x"}}`, 400, "unsupported_webhook_type"},
		{"unknown credential kind", `{"webhook_type":"telegram","bot_id":"1","credential":{"kind":"password","value":"x"}}`, 400, "unsupported_credential_kind"},
		{"bad bot id (leading zero)", `{"webhook_type":"telegram","bot_id":"0123","credential":{"kind":"secret_token","value":"x"}}`, 400, "invalid_request"},
		{"bad bot id (not decimal)", `{"webhook_type":"telegram","bot_id":"abc","credential":{"kind":"secret_token","value":"x"}}`, 400, "invalid_request"},
		{"bad identifier", `{"webhook_type":"telegram","webhook_identifier":"bad/identifier","bot_id":"1","credential":{"kind":"secret_token","value":"x"}}`, 400, "invalid_request"},
		{"unknown field", `{"webhook_type":"telegram","bot_id":"1","credential":{"kind":"secret_token","value":"x"},"extra":true}`, 400, "invalid_request"},
		{"trailing data", validCreate + ` {"more":1}`, 400, "invalid_request"},
		{"depth exceeded", `{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":{"a":1}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}`, 400, "invalid_request"},
		{"telegram secret too long", `{"webhook_type":"telegram","bot_id":"1","credential":{"kind":"secret_token","value":"` + strings.Repeat("x", 257) + `"}}`, 400, "invalid_request"},
		{"telegram secret bad char", `{"webhook_type":"telegram","bot_id":"1","credential":{"kind":"secret_token","value":"bad char!"}}`, 400, "invalid_request"},
	}
	for _, tc := range cases {
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", tc.body)
		if rec.Code != tc.status {
			t.Errorf("%s: status = %d, want %d (%s)", tc.name, rec.Code, tc.status, rec.Body.String())
			continue
		}
		var body struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body.Error.Code != tc.wantCode {
			t.Errorf("%s: code = %q, want %q", tc.name, body.Error.Code, tc.wantCode)
		}
	}
}

// TestGetContract pins the read model and 404 uniformity.
func TestGetContract(t *testing.T) {
	repo := newFakeRepo()
	svc, _ := testService(t, repo)
	h := Handler(svc)

	rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusCreated {
		t.Fatalf("setup create failed: %d", rec.Code)
	}

	rec = doJSON(t, h, http.MethodGet, "/admin/v1/webhooks/telegram/wh_AAAAAAAAAAAAAAAAAAAAAA", "admin-secret-value-016", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get = %d", rec.Code)
	}
	if etag := rec.Header().Get("ETag"); etag != `"0195-uuid:1"` {
		t.Errorf("ETag = %q", etag)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if _, ok := body["credential_value"]; ok {
		t.Error("credential_value exposed in read response")
	}
	cred, _ := body["credential"].(map[string]any)
	if cred["configured"] != true {
		t.Errorf("credential.configured = %v", cred["configured"])
	}
	if body["bot_platform"] != "telegram" {
		t.Errorf("bot_platform = %v, want telegram (derived from the Webhook Type)", body["bot_platform"])
	}

	for _, path := range []string{
		"/admin/v1/webhooks/telegram/wh_missing",
		"/admin/v1/webhooks/maxbot/wh_x",
		"/admin/v1/webhooks/bad%2Ftype/wh_x",
	} {
		rec := doJSON(t, h, http.MethodGet, path, "admin-secret-value-016", "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("get %s = %d, want uniform 404", path, rec.Code)
		}
	}
}

// TestConflictAndLimitAndDependency pins the 409/503 mappings.
func TestConflictAndLimitAndDependency(t *testing.T) {
	repo := newFakeRepo()
	svc, _ := testService(t, repo)
	h := Handler(svc)

	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate); rec.Code != 201 {
		t.Fatalf("setup: %d", rec.Code)
	}
	dup := strings.Replace(validCreate, `"enabled": true`, `"enabled": true, "webhook_identifier": "wh_AAAAAAAAAAAAAAAAAAAAAA"`, 1)
	rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", dup)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate = %d", rec.Code)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error.Code != "webhook_identifier_conflict" {
		t.Errorf("conflict code = %q", body.Error.Code)
	}

	// Dependency: repo wrong-type refusal maps to 503 dependency_unavailable.
	repo2 := newFakeRepo()
	repo2.failWrongType = true
	svc2, _ := testService(t, repo2)
	h2 := Handler(svc2)
	rec = doJSON(t, h2, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("wrong type = %d, want 503", rec.Code)
	}
}

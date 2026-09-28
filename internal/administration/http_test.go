package administration

import (
	"encoding/json"
	"net/http"
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
	h := Handler(svc, fixedGen{})

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
	h := Handler(svc, fixedGen{})

	for _, tc := range []struct {
		name   string
		secret string
	}{
		{"missing", ""},
		{"wrong", "wrong-secret-00000000001"},
		{"prefix", "admin-secret-value"},
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
	if audit.rejected != 3 {
		t.Errorf("rejected auth audits = %d, want 3 (best effort)", audit.rejected)
	}
}

// TestValidationErrorCodes pins the bounded validation failures.
func TestValidationErrorCodes(t *testing.T) {
	repo := newFakeRepo()
	svc, _ := testService(t, repo)
	h := Handler(svc, fixedGen{})

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
	h := Handler(svc, fixedGen{})

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
	h := Handler(svc, fixedGen{})

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
	h2 := Handler(svc2, fixedGen{})
	rec = doJSON(t, h2, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("wrong type = %d, want 503", rec.Code)
	}
}

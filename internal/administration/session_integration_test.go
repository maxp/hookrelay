package administration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/valkey"
)

// TestBrowserSessionOverValkey composes the session routes with the real
// stores: login sets the cookie, the session read returns the CSRF token,
// the cookie reads the audit (where the login appears as admin_session),
// a state-changing request needs that token, logout revokes the session,
// and a changed Admin Secret revokes sessions at the next reconciliation.
func TestBrowserSessionOverValkey(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)
	ctx := context.Background()
	const secret = "admin-secret-value-016"
	if _, err := a.Reconcile(ctx, valkey.ReconcileOptions{AdminSecret: secret}); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(nil)
	defer srv.Close()
	svc, err := administration.NewService(administration.ServiceDeps{
		Repo: valkey.NewEndpointStore(a), DeadLetters: valkey.NewDeadLetterStore(a), AuditLog: valkey.NewAuditLog(a),
		Sessions: valkey.NewSessionStore(a), AdminOrigin: srv.URL, Catalog: builtinCatalog{}, Audit: valkey.NewAuditSink(a),
		AdminSecret: secret, Gen: gen.Crypto{},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv.Config.Handler = administration.Handler(svc)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}
	do := func(method, path, body, csrf string) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		req.Header.Set("Origin", srv.URL)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}

	if resp := do(http.MethodPost, "/admin/v1/session", `{"admin_secret":"`+secret+`"}`, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("login = %d", resp.StatusCode)
	}
	u, _ := url.Parse(srv.URL)
	if cookies := jar.Cookies(u); len(cookies) != 1 || cookies[0].Name != "hookrelay_admin" {
		t.Fatalf("cookies = %v", cookies)
	}
	resp := do(http.MethodGet, "/admin/v1/session", "", "")
	var sess struct {
		CSRFToken string `json:"csrf_token"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&sess) != nil || len(sess.CSRFToken) != 22 {
		t.Fatalf("session = %d %+v", resp.StatusCode, sess)
	}
	resp = do(http.MethodGet, "/admin/v1/audit?limit=1", "", "")
	var page struct {
		Items []map[string]any `json:"items"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&page) != nil || len(page.Items) != 1 ||
		page.Items[0]["operation"] != "admin_login" || page.Items[0]["actor"] != "admin_session" {
		t.Fatalf("audit via cookie = %d %v", resp.StatusCode, page.Items)
	}
	// State-changing: the CSRF token is required; the absent dead letter
	// then answers 204 without audit.
	if resp := do(http.MethodDelete, "/admin/v1/dead-letters/01950000-0000-7000-8000-000000000001", "", ""); resp.StatusCode != http.StatusForbidden {
		t.Errorf("delete without CSRF = %d", resp.StatusCode)
	}
	if resp := do(http.MethodDelete, "/admin/v1/dead-letters/01950000-0000-7000-8000-000000000001", "", sess.CSRFToken); resp.StatusCode != http.StatusNoContent {
		t.Errorf("delete with CSRF = %d", resp.StatusCode)
	}

	if resp := do(http.MethodDelete, "/admin/v1/session", "", sess.CSRFToken); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("logout = %d", resp.StatusCode)
	}
	if resp := do(http.MethodGet, "/admin/v1/session", "", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("after logout = %d", resp.StatusCode)
	}
	if entries := auditTail(t, a, 100); entries[len(entries)-1]["operation"] != "admin_logout" {
		t.Errorf("last audit = %v", entries[len(entries)-1])
	}

	// A new session is revoked when reconciliation sees another secret.
	if resp := do(http.MethodPost, "/admin/v1/session", `{"admin_secret":"`+secret+`"}`, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("second login = %d", resp.StatusCode)
	}
	if _, err := a.Reconcile(ctx, valkey.ReconcileOptions{AdminSecret: "admin-secret-value-017"}); err != nil {
		t.Fatal(err)
	}
	if resp := do(http.MethodGet, "/admin/v1/session", "", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("after rotation = %d", resp.StatusCode)
	}
}

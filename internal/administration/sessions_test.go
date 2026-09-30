package administration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/ratelimit"
)

const (
	testOrigin = "https://admin.example"
	// testToken is what fixedGen issues as a session token (43 characters).
	testToken = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	testCSRF  = "AAAAAAAAAAAAAAAAAAAAAA"
)

type fakeSessions struct {
	create   SessionCreate
	auth     SessionAuth
	del      SessionDeleteResult
	creates  []string // digests
	auths    []string
	deletes  []string
	csrfSeen []string
}

func (f *fakeSessions) CreateSession(_ context.Context, digest, csrf, _, _ string) SessionCreate {
	f.creates = append(f.creates, digest)
	f.csrfSeen = append(f.csrfSeen, csrf)
	return f.create
}

func (f *fakeSessions) AuthenticateSession(_ context.Context, digest string) SessionAuth {
	f.auths = append(f.auths, digest)
	return f.auth
}

func (f *fakeSessions) DeleteSession(_ context.Context, digest, _, _ string) SessionDeleteResult {
	f.deletes = append(f.deletes, digest)
	return f.del
}

type sessionHarness struct {
	h     http.Handler
	store *fakeSessions
	audit *fakeAudit
	reg   *prometheus.Registry
	logs  *bytes.Buffer
}

func newSessionHarness(t *testing.T, secure bool, limits ratelimit.Limits) *sessionHarness {
	t.Helper()
	hs := &sessionHarness{store: &fakeSessions{
		create: SessionCreate{Result: SessionCreated, CreatedMs: 1, IdleExpiresMs: 3600001, AbsoluteExpiresMs: 43200001},
		auth:   SessionAuth{Result: SessionValid, CSRFToken: testCSRF, IdleExpiresMs: 3600001, AbsoluteExpiresMs: 43200001},
		del:    SessionDeleted,
	}, audit: &fakeAudit{}, reg: prometheus.NewRegistry(), logs: &bytes.Buffer{}}
	svc, err := NewService(ServiceDeps{Repo: newFakeRepo(), Sessions: hs.store, AdminOrigin: testOrigin, CookieSecure: secure,
		LoginLimits: limits, Now: func() time.Time { return time.UnixMilli(1740000000000) },
		Catalog: fakeCatalog{}, Audit: hs.audit, AdminSecret: "admin-secret-value-016", Gen: fixedGen{},
		Logger: observability.NewTestLogger("info", hs.logs), Registerer: hs.reg})
	if err != nil {
		t.Fatal(err)
	}
	hs.h = Handler(svc)
	return hs
}

// request sends one session request; cookie "" sends none.
func (hs *sessionHarness) request(method, body, origin, cookie, csrf string) *httptest.ResponseRecorder {
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, "/admin/v1/session", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, "/admin/v1/session", nil)
	}
	req.RemoteAddr = "192.0.2.10:5555"
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	if cookie != "" {
		req.AddCookie(&http.Cookie{Name: SessionCookieName, Value: cookie})
	}
	if csrf != "" {
		req.Header.Set("X-CSRF-Token", csrf)
	}
	rec := httptest.NewRecorder()
	hs.h.ServeHTTP(rec, req)
	return rec
}

func (hs *sessionHarness) logins(t *testing.T, outcome string) float64 {
	t.Helper()
	return counterValue(t, hs.reg, "hookrelay_admin_login_attempts_total", map[string]string{"outcome": outcome})
}

func sessionCookieOf(rec *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range rec.Result().Cookies() {
		if c.Name == SessionCookieName {
			return c
		}
	}
	return nil
}

const loginBody = `{"admin_secret":"admin-secret-value-016"}`

// TestLoginContract pins login: the cookie attributes, the token stored
// only as its digest, the metric, and no body or secret in logs.
func TestLoginContract(t *testing.T) {
	for _, secure := range []bool{true, false} {
		hs := newSessionHarness(t, secure, ratelimit.Limits{})
		rec := hs.request(http.MethodPost, loginBody, testOrigin, "", "")
		if rec.Code != http.StatusNoContent || rec.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("login = %d %s", rec.Code, rec.Body.String())
		}
		c := sessionCookieOf(rec)
		if c == nil || c.Value != testToken || c.Path != "/" || !c.HttpOnly || c.SameSite != http.SameSiteStrictMode ||
			c.MaxAge != 43200 || c.Secure != secure || c.Domain != "" {
			t.Errorf("secure=%v cookie = %+v", secure, c)
		}
		if len(hs.store.creates) != 1 || hs.store.creates[0] != sessionDigest(testToken) || hs.store.csrfSeen[0] != testCSRF {
			t.Errorf("create calls = %v %v", hs.store.creates, hs.store.csrfSeen)
		}
		if hs.logins(t, "success") != 1 {
			t.Error("success not counted")
		}
		if strings.Contains(hs.logs.String(), "admin-secret-value-016") || strings.Contains(hs.logs.String(), testToken) {
			t.Errorf("secret or token logged: %s", hs.logs.String())
		}
	}
}

// TestLoginRefusals pins every refusal: Origin, rate limit, body, secret,
// capacity, and an unconfirmed session, none of which issues a cookie.
func TestLoginRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		origin, body string
		create       SessionCreateResult
		status       int
		code         string
		outcome      string
	}{
		"no origin":        {"", loginBody, SessionCreated, 403, "forbidden", ""},
		"other origin":     {"https://evil.example", loginBody, SessionCreated, 403, "forbidden", ""},
		"origin with path": {testOrigin + "/", loginBody, SessionCreated, 403, "forbidden", ""},
		"wrong secret":     {testOrigin, `{"admin_secret":"wrong-secret-value-0001"}`, SessionCreated, 401, "unauthenticated", "failure"},
		"no secret":        {testOrigin, `{}`, SessionCreated, 400, "invalid_request", ""},
		"unknown field":    {testOrigin, `{"admin_secret":"x","remember":true}`, SessionCreated, 400, "invalid_request", ""},
		"too large":        {testOrigin, `{"admin_secret":"` + strings.Repeat("a", 17000) + `"}`, SessionCreated, 413, "request_too_large", ""},
		"capacity":         {testOrigin, loginBody, SessionCapacityExceeded, 429, "session_capacity_exceeded", "capacity_exceeded"},
		"unavailable":      {testOrigin, loginBody, SessionCreateUnavailable, 503, "dependency_unavailable", "unavailable"},
		"uninitialized":    {testOrigin, loginBody, SessionAuthUninitialized, 503, "dependency_unavailable", "unavailable"},
		"wrong type":       {testOrigin, loginBody, SessionCreateWrongType, 503, "dependency_unavailable", "unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			hs := newSessionHarness(t, true, ratelimit.Limits{})
			hs.store.create = SessionCreate{Result: tc.create}
			rec := hs.request(http.MethodPost, tc.body, tc.origin, "", "")
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), `"code":"`+tc.code+`"`) {
				t.Errorf("= %d %s", rec.Code, rec.Body.String())
			}
			if c := sessionCookieOf(rec); c != nil {
				t.Errorf("cookie issued: %+v", c)
			}
			if tc.outcome != "" && hs.logins(t, tc.outcome) != 1 {
				t.Errorf("outcome %s not counted", tc.outcome)
			}
			if tc.status == 403 && counterValue(t, hs.reg, "hookrelay_admin_csrf_rejections_total", map[string]string{"reason": "origin"}) != 1 {
				t.Error("origin rejection not counted")
			}
			if tc.status == 401 && (len(hs.audit.events) != 1 || hs.audit.events[0].Operation != "admin_login" || hs.audit.events[0].Outcome != "failure") {
				t.Errorf("failure audit = %+v", hs.audit.events)
			}
			if strings.Contains(rec.Body.String(), "admin-secret-value") || strings.Contains(hs.logs.String(), "wrong-secret-value") {
				t.Error("secret echoed")
			}
		})
	}
}

// TestLoginRateLimits pins the per-address and global buckets with
// Retry-After, checked before the body and the secret.
func TestLoginRateLimits(t *testing.T) {
	hs := newSessionHarness(t, true, ratelimit.Limits{KeyRate: 5.0 / 60, KeyBurst: 2, GlobalRate: 1, GlobalBurst: 20})
	for range 2 {
		if rec := hs.request(http.MethodPost, `{"admin_secret":"wrong-secret-value-0001"}`, testOrigin, "", ""); rec.Code != http.StatusUnauthorized {
			t.Fatalf("within burst = %d", rec.Code)
		}
	}
	rec := hs.request(http.MethodPost, loginBody, testOrigin, "", "")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "12" || !strings.Contains(rec.Body.String(), "rate_limit_exceeded") ||
		sessionCookieOf(rec) != nil || len(hs.store.creates) != 0 {
		t.Errorf("limited = %d %q %s", rec.Code, rec.Header().Get("Retry-After"), rec.Body.String())
	}
	if hs.logins(t, "rate_limited") != 1 || hs.logins(t, "failure") != 2 {
		t.Error("outcomes miscounted")
	}
	if DefaultLoginLimits != (ratelimit.Limits{GlobalRate: 1, GlobalBurst: 20, KeyRate: 5.0 / 60, KeyBurst: 5}) {
		t.Errorf("default limits = %+v", DefaultLoginLimits)
	}
}

// TestGetSessionContract pins the session read: expiries and the CSRF
// token for a valid cookie, 401 with a clearing cookie otherwise, and a
// malformed cookie never reaching storage.
func TestGetSessionContract(t *testing.T) {
	hs := newSessionHarness(t, true, ratelimit.Limits{})
	rec := hs.request(http.MethodGet, "", "", testToken, "")
	var v map[string]any
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &v) != nil || v["authenticated"] != true ||
		v["csrf_token"] != testCSRF || v["idle_expires_ms"] != 3600001.0 || v["absolute_expires_ms"] != 43200001.0 ||
		rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("get = %d %s", rec.Code, rec.Body.String())
	}
	if hs.store.auths[0] != sessionDigest(testToken) {
		t.Errorf("auth digest = %s", hs.store.auths[0])
	}
	if rec := hs.request(http.MethodGet, "", "", "", ""); rec.Code != http.StatusUnauthorized || sessionCookieOf(rec) != nil {
		t.Errorf("no cookie = %d", rec.Code)
	}
	if rec := hs.request(http.MethodGet, "", "", "short", ""); rec.Code != http.StatusUnauthorized || sessionCookieOf(rec).MaxAge >= 0 || len(hs.store.auths) != 1 {
		t.Errorf("malformed cookie = %d, auths %d", rec.Code, len(hs.store.auths))
	}
	hs.store.auth = SessionAuth{Result: SessionInvalid, Reason: SessionReasonExpired}
	rec = hs.request(http.MethodGet, "", "", testToken, "")
	if rec.Code != http.StatusUnauthorized || sessionCookieOf(rec) == nil || sessionCookieOf(rec).MaxAge >= 0 {
		t.Errorf("expired = %d %+v", rec.Code, sessionCookieOf(rec))
	}
	if len(hs.audit.events) != 1 || hs.audit.events[0].Operation != "admin_session_expired" || hs.audit.events[0].Actor != "maintenance" {
		t.Errorf("expiry audit = %+v", hs.audit.events)
	}
	hs.store.auth = SessionAuth{Result: SessionAuthUnavailable}
	if rec := hs.request(http.MethodGet, "", "", testToken, ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unavailable = %d", rec.Code)
	}
}

// TestLogoutContract pins logout: 204 and a clearing cookie without a
// valid session, Origin and CSRF required for a valid one, revocation
// claimed only after confirmation.
func TestLogoutContract(t *testing.T) {
	hs := newSessionHarness(t, true, ratelimit.Limits{})
	for name, tc := range map[string]struct{ origin, csrf, reason string }{
		"no origin":    {"", testCSRF, "origin"},
		"wrong origin": {"https://evil.example", testCSRF, "origin"},
		"no csrf":      {testOrigin, "", "token"},
		"wrong csrf":   {testOrigin, "BBBBBBBBBBBBBBBBBBBBBB", "token"},
	} {
		rec := hs.request(http.MethodDelete, "", tc.origin, testToken, tc.csrf)
		if rec.Code != http.StatusForbidden || sessionCookieOf(rec) != nil {
			t.Errorf("%s = %d", name, rec.Code)
		}
	}
	if len(hs.store.deletes) != 0 {
		t.Error("a refused logout reached storage")
	}
	rec := hs.request(http.MethodDelete, "", testOrigin, testToken, testCSRF)
	if rec.Code != http.StatusNoContent || sessionCookieOf(rec) == nil || sessionCookieOf(rec).MaxAge >= 0 ||
		len(hs.store.deletes) != 1 || hs.store.deletes[0] != sessionDigest(testToken) {
		t.Fatalf("logout = %d, deletes %v", rec.Code, hs.store.deletes)
	}
	hs.store.del = SessionDeleteAbsent
	if rec := hs.request(http.MethodDelete, "", testOrigin, testToken, testCSRF); rec.Code != http.StatusNoContent {
		t.Errorf("absent = %d", rec.Code)
	}
	for _, r := range []SessionDeleteResult{SessionDeleteUnavailable, SessionDeleteWrongType} {
		hs.store.del = r
		if rec := hs.request(http.MethodDelete, "", testOrigin, testToken, testCSRF); rec.Code != http.StatusServiceUnavailable || sessionCookieOf(rec) != nil {
			t.Errorf("%s = %d", r, rec.Code)
		}
	}
	for name, auth := range map[string]SessionAuth{"invalid": {Result: SessionInvalid, Reason: SessionReasonAbsent}} {
		hs.store.auth = auth
		if rec := hs.request(http.MethodDelete, "", "", testToken, ""); rec.Code != http.StatusNoContent || sessionCookieOf(rec) == nil {
			t.Errorf("%s session = %d", name, rec.Code)
		}
	}
	if rec := hs.request(http.MethodDelete, "", "", "", ""); rec.Code != http.StatusNoContent || sessionCookieOf(rec) == nil {
		t.Errorf("no cookie = %d", rec.Code)
	}
	hs.store.auth = SessionAuth{Result: SessionAuthUnavailable}
	if rec := hs.request(http.MethodDelete, "", testOrigin, testToken, testCSRF); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unavailable authentication = %d", rec.Code)
	}
}

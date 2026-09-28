package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const (
	testAdminSecret = "admin-secret-value-016"
	testCredential  = "telegram-secret-001"
	testWebhookID   = "wh_AAAAAAAAAAAAAAAAAAAAAA"
)

// fakeAdminAPI records every request and answers with scripted handlers.
type fakeAdminAPI struct {
	mu       sync.Mutex
	requests []recordedRequest
	create   http.HandlerFunc
	get      http.HandlerFunc
}

type recordedRequest struct {
	Method, Path, Authorization, ContentType string
	Body                                     map[string]any
}

func (f *fakeAdminAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	rec := recordedRequest{Method: r.Method, Path: r.URL.Path, Authorization: r.Header.Get("Authorization"), ContentType: r.Header.Get("Content-Type")}
	_ = json.Unmarshal(data, &rec.Body)
	f.mu.Lock()
	f.requests = append(f.requests, rec)
	f.mu.Unlock()
	switch {
	case r.Method == http.MethodPost && f.create != nil:
		f.create(w, r)
	case r.Method == http.MethodGet && f.get != nil:
		f.get(w, r)
	default:
		w.WriteHeader(http.StatusTeapot)
	}
}

func (f *fakeAdminAPI) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, r := range f.requests {
		if r.Method == method {
			n++
		}
	}
	return n
}

func (f *fakeAdminAPI) first(method string) recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.requests {
		if r.Method == method {
			return r
		}
	}
	return recordedRequest{}
}

func endpointJSON(enabled bool, botID string) string {
	e := map[string]any{
		"webhook_type":       "telegram",
		"webhook_identifier": testWebhookID,
		"bot_platform":       "telegram",
		"bot_id":             botID,
		"enabled":            enabled,
		"credential":         map[string]any{"kind": "secret_token", "configured": true},
		"generation_id":      "0195-uuid",
		"config_version":     1,
		"created_ms":         1740000000000,
		"updated_ms":         1740000000000,
		"webhook_path":       "/webhook/telegram/" + testWebhookID,
	}
	data, _ := json.Marshal(e)
	return string(data)
}

func respond(status int, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		io.WriteString(w, body)
	}
}

func apiErrorBody(code, message string) string {
	return `{"error":{"code":"` + code + `","message":"` + message + `","request_id":"0195-req"}}`
}

// dropConnection simulates a lost response: the request is received, then
// the connection closes without any response.
func dropConnection(w http.ResponseWriter, _ *http.Request) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err == nil {
		conn.Close()
	}
}

type testRun struct {
	env    map[string]string
	files  map[string]string
	stdout bytes.Buffer
	stderr bytes.Buffer
	tty    bool
	prompt func() (string, error)
}

func newTestRun(apiURL string) *testRun {
	return &testRun{
		env: map[string]string{
			"HOOKRELAY_ADMIN_URL":          apiURL,
			"HOOKRELAY_ADMIN_SECRET":       testAdminSecret,
			"HOOKRELAY_WEBHOOK_CREDENTIAL": testCredential,
		},
		files: map[string]string{},
	}
}

func (r *testRun) io() adminIO {
	return adminIO{
		Getenv: func(k string) string { return r.env[k] },
		ReadFile: func(p string) ([]byte, error) {
			if v, ok := r.files[p]; ok {
				return []byte(v), nil
			}
			return nil, os.ErrNotExist
		},
		Stdout:       &r.stdout,
		Stderr:       &r.stderr,
		StdinIsTTY:   r.tty,
		StdoutIsTTY:  r.tty,
		ReadSecret:   r.prompt,
		NewWebhookID: func() (string, error) { return testWebhookID, nil },
		HTTPClient:   &http.Client{},
	}
}

func (r *testRun) run(args ...string) int { return runAdmin(args, r.io()) }

// assertNoSecrets pins that neither the Admin Secret nor the credential is
// ever printed.
func (r *testRun) assertNoSecrets(t *testing.T) {
	t.Helper()
	for _, secret := range []string{testAdminSecret, testCredential} {
		if strings.Contains(r.stdout.String()+r.stderr.String(), secret) {
			t.Errorf("secret %q printed", secret)
		}
	}
}

var createArgs = []string{"webhook", "create", "--type", "telegram", "--bot-id", "123456789"}

// TestAdminCreateSuccess pins the create request (client-side identifier,
// default credential kind, bearer auth, JSON) and the JSON result.
func TestAdminCreateSuccess(t *testing.T) {
	api := &fakeAdminAPI{create: respond(http.StatusCreated, endpointJSON(true, "123456789"))}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run(createArgs...); code != ExitOK {
		t.Fatalf("exit = %d, stderr: %s", code, r.stderr.String())
	}
	req := api.first(http.MethodPost)
	if req.Path != "/admin/v1/webhooks" || req.Authorization != "Bearer "+testAdminSecret || req.ContentType != "application/json" {
		t.Errorf("request = %+v", req)
	}
	if req.Body["webhook_identifier"] != testWebhookID || req.Body["bot_id"] != "123456789" || req.Body["enabled"] != true {
		t.Errorf("body = %v", req.Body)
	}
	cred, _ := req.Body["credential"].(map[string]any)
	if cred["kind"] != "secret_token" || cred["value"] != testCredential {
		t.Errorf("credential = %v", cred)
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, r.stdout.String())
	}
	if out["webhook_identifier"] != testWebhookID {
		t.Errorf("stdout = %v", out)
	}
	if r.stderr.Len() != 0 {
		t.Errorf("stderr on success: %s", r.stderr.String())
	}
	r.assertNoSecrets(t)
}

// TestAdminCreateSuppliedIdentifierAndDisabled pins --identifier and
// --disabled.
func TestAdminCreateSuppliedIdentifierAndDisabled(t *testing.T) {
	api := &fakeAdminAPI{create: respond(http.StatusCreated, endpointJSON(false, "123456789"))}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run(append(createArgs, "--identifier", "wh_mine", "--disabled")...); code != ExitOK {
		t.Fatalf("exit = %d, stderr: %s", code, r.stderr.String())
	}
	body := api.first(http.MethodPost).Body
	if body["webhook_identifier"] != "wh_mine" || body["enabled"] != false {
		t.Errorf("body = %v", body)
	}
}

// TestAdminCreateDefiniteRejection pins that a 4xx is reported with its
// bounded code and is neither retried nor reconciled.
func TestAdminCreateDefiniteRejection(t *testing.T) {
	api := &fakeAdminAPI{create: respond(http.StatusConflict, apiErrorBody("webhook_identifier_conflict", "webhook identifier already exists"))}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run(createArgs...); code != ExitError {
		t.Fatalf("exit = %d, want 1", code)
	}
	if !strings.Contains(r.stderr.String(), "webhook_identifier_conflict") || !strings.Contains(r.stderr.String(), "0195-req") {
		t.Errorf("stderr = %s", r.stderr.String())
	}
	if api.count(http.MethodPost) != 1 || api.count(http.MethodGet) != 0 {
		t.Errorf("posts = %d, gets = %d; want 1 and 0", api.count(http.MethodPost), api.count(http.MethodGet))
	}
	if r.stdout.Len() != 0 {
		t.Errorf("stdout on failure: %s", r.stdout.String())
	}
}

// TestAdminCreateUncertainOutcomes pins the reconciliation path: exactly one
// POST, one read by the known identity, an honest outcome on stdout, a
// warning on stderr, and a failing exit code even when the desired state is
// observed.
func TestAdminCreateUncertainOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		create      http.HandlerFunc
		get         http.HandlerFunc
		wantOutcome string
		wantWarning string
	}{
		{"lost response, desired state observed", dropConnection, respond(http.StatusOK, endpointJSON(true, "123456789")), outcomeDesiredStateObserved, "does NOT confirm the create"},
		{"503, endpoint absent", respond(http.StatusServiceUnavailable, apiErrorBody("dependency_unavailable", "create outcome is uncertain")), respond(http.StatusNotFound, apiErrorBody("webhook_endpoint_not_found", "webhook endpoint not found")), outcomeUncertain, "is not present"},
		{"500, endpoint differs", respond(http.StatusInternalServerError, apiErrorBody("internal_error", "unexpected internal error")), respond(http.StatusOK, endpointJSON(true, "999")), outcomeUncertain, "differs from the request"},
		{"lost response, read fails", dropConnection, dropConnection, outcomeUncertain, "could not be read"},
		{"unreadable 201", respond(http.StatusCreated, "{not json"), respond(http.StatusOK, endpointJSON(true, "123456789")), outcomeDesiredStateObserved, "does NOT confirm the create"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeAdminAPI{create: tc.create, get: tc.get}
			srv := httptest.NewServer(api)
			defer srv.Close()

			r := newTestRun(srv.URL)
			if code := r.run(createArgs...); code != ExitError {
				t.Fatalf("exit = %d, want 1 (an unconfirmed create never succeeds)", code)
			}
			if api.count(http.MethodPost) != 1 {
				t.Errorf("POST count = %d, want exactly 1 (no blind retry)", api.count(http.MethodPost))
			}
			if get := api.first(http.MethodGet); get.Path != "/admin/v1/webhooks/telegram/"+testWebhookID {
				t.Errorf("reconciliation read = %q, want the known identity", get.Path)
			}
			var out uncertainResult
			if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil {
				t.Fatalf("stdout is not JSON: %v\n%s", err, r.stdout.String())
			}
			if out.Outcome != tc.wantOutcome || out.WebhookIdentifier != testWebhookID {
				t.Errorf("result = %+v, want outcome %s", out, tc.wantOutcome)
			}
			if !strings.Contains(r.stderr.String(), tc.wantWarning) || !strings.Contains(r.stderr.String(), testWebhookID) {
				t.Errorf("stderr = %s", r.stderr.String())
			}
			r.assertNoSecrets(t)
		})
	}
}

// TestAdminGet pins get output in both formats and the 404 error path.
func TestAdminGet(t *testing.T) {
	api := &fakeAdminAPI{get: func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/"+testWebhookID) {
			respond(http.StatusOK, endpointJSON(true, "123456789"))(w, r)
			return
		}
		respond(http.StatusNotFound, apiErrorBody("webhook_endpoint_not_found", "webhook endpoint not found"))(w, r)
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run("webhook", "get", "--type", "telegram", "--identifier", testWebhookID, "--output", "table"); code != ExitOK {
		t.Fatalf("exit = %d, stderr: %s", code, r.stderr.String())
	}
	for _, want := range []string{"webhook_identifier  " + testWebhookID, "bot_platform        telegram", "credential          secret_token (configured: true)"} {
		if !strings.Contains(r.stdout.String(), want) {
			t.Errorf("table output missing %q:\n%s", want, r.stdout.String())
		}
	}

	r = newTestRun(srv.URL)
	if code := r.run("webhook", "get", "--type", "telegram", "--identifier", "wh_missing"); code != ExitError {
		t.Fatalf("missing endpoint exit = %d, want 1", code)
	}
	if !strings.Contains(r.stderr.String(), "webhook_endpoint_not_found") || r.stdout.Len() != 0 {
		t.Errorf("missing endpoint: stdout %q stderr %q", r.stdout.String(), r.stderr.String())
	}
}

// TestAdminOutputDefaults pins table on a terminal and json otherwise.
func TestAdminOutputDefaults(t *testing.T) {
	api := &fakeAdminAPI{get: respond(http.StatusOK, endpointJSON(true, "123456789"))}
	srv := httptest.NewServer(api)
	defer srv.Close()
	args := []string{"webhook", "get", "--type", "telegram", "--identifier", testWebhookID}

	r := newTestRun(srv.URL)
	r.run(args...)
	if !json.Valid(r.stdout.Bytes()) {
		t.Errorf("non-terminal default is not json: %s", r.stdout.String())
	}
	r = newTestRun(srv.URL)
	r.tty = true
	r.run(args...)
	if json.Valid(r.stdout.Bytes()) || !strings.Contains(r.stdout.String(), "webhook_type") {
		t.Errorf("terminal default is not table: %s", r.stdout.String())
	}
	r = newTestRun(srv.URL)
	if code := r.run(append(args, "--output", "yaml")...); code != ExitUsage {
		t.Errorf("unknown format exit = %d, want 2", code)
	}
}

// TestAdminSecretResolution pins the precedence: --admin-secret-file →
// HOOKRELAY_ADMIN_SECRET_FILE → HOOKRELAY_ADMIN_SECRET → terminal prompt.
func TestAdminSecretResolution(t *testing.T) {
	api := &fakeAdminAPI{get: respond(http.StatusOK, endpointJSON(true, "123456789"))}
	srv := httptest.NewServer(api)
	defer srv.Close()
	args := []string{"webhook", "get", "--type", "telegram", "--identifier", testWebhookID}

	cases := []struct {
		name   string
		setup  func(r *testRun) []string
		want   string
		wantEx int
	}{
		{"flag file wins", func(r *testRun) []string {
			r.files["/flag"] = "from-flag-file\n"
			r.files["/env"] = "from-env-file\n"
			r.env["HOOKRELAY_ADMIN_SECRET_FILE"] = "/env"
			return []string{"--admin-secret-file", "/flag"}
		}, "from-flag-file", ExitOK},
		{"env file before env value", func(r *testRun) []string {
			r.files["/env"] = "from-env-file\n"
			r.env["HOOKRELAY_ADMIN_SECRET_FILE"] = "/env"
			return nil
		}, "from-env-file", ExitOK},
		{"env value", func(*testRun) []string { return nil }, testAdminSecret, ExitOK},
		{"terminal prompt", func(r *testRun) []string {
			delete(r.env, "HOOKRELAY_ADMIN_SECRET")
			r.tty = true
			r.prompt = func() (string, error) { return "from-prompt", nil }
			return nil
		}, "from-prompt", ExitOK},
		{"no source without terminal", func(r *testRun) []string {
			delete(r.env, "HOOKRELAY_ADMIN_SECRET")
			r.prompt = func() (string, error) { return "", errors.New("must not prompt") }
			return nil
		}, "", ExitUsage},
		{"unreadable file", func(r *testRun) []string {
			return []string{"--admin-secret-file", "/missing"}
		}, "", ExitUsage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := api.count(http.MethodGet)
			r := newTestRun(srv.URL)
			extra := tc.setup(r)
			if code := r.run(append(append([]string{}, args...), extra...)...); code != tc.wantEx {
				t.Fatalf("exit = %d, want %d (stderr %s)", code, tc.wantEx, r.stderr.String())
			}
			if tc.want == "" {
				if api.count(http.MethodGet) != before {
					t.Error("request sent without a resolved secret")
				}
				return
			}
			api.mu.Lock()
			last := api.requests[len(api.requests)-1]
			api.mu.Unlock()
			if last.Authorization != "Bearer "+tc.want {
				t.Errorf("Authorization = %q, want Bearer %s", last.Authorization, tc.want)
			}
		})
	}
}

// TestAdminURLResolution pins flag → env → loopback default.
func TestAdminURLResolution(t *testing.T) {
	var flagCalled atomic.Bool
	flagSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flagCalled.Store(true)
		respond(http.StatusOK, endpointJSON(true, "123456789"))(w, r)
	}))
	defer flagSrv.Close()

	r := newTestRun("http://127.0.0.1:1") // env points at a dead port
	if code := r.run("webhook", "get", "--type", "telegram", "--identifier", testWebhookID, "--admin-url", flagSrv.URL); code != ExitOK || !flagCalled.Load() {
		t.Fatalf("--admin-url not preferred over HOOKRELAY_ADMIN_URL: exit %d", code)
	}

	var client *adminClient
	r = newTestRun("")
	delete(r.env, "HOOKRELAY_ADMIN_URL")
	client, err := (&commonFlags{}).resolve(r.io())
	if err != nil || client.baseURL.String() != defaultAdminURL {
		t.Fatalf("default URL = %v (%v), want %s", client, err, defaultAdminURL)
	}

	r = newTestRun("ftp://example")
	if code := r.run("webhook", "get", "--type", "telegram", "--identifier", testWebhookID); code != ExitUsage {
		t.Errorf("non-http URL exit = %d, want 2", code)
	}
}

// TestAdminCreateCredentialInput pins --credential-file vs
// HOOKRELAY_WEBHOOK_CREDENTIAL: exactly one source, trailing newline removed.
func TestAdminCreateCredentialInput(t *testing.T) {
	api := &fakeAdminAPI{create: respond(http.StatusCreated, endpointJSON(true, "123456789"))}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	delete(r.env, "HOOKRELAY_WEBHOOK_CREDENTIAL")
	r.files["/cred"] = "from-file-001\n"
	if code := r.run(append(createArgs, "--credential-file", "/cred")...); code != ExitOK {
		t.Fatalf("file credential exit = %d: %s", code, r.stderr.String())
	}
	cred, _ := api.first(http.MethodPost).Body["credential"].(map[string]any)
	if cred["value"] != "from-file-001" {
		t.Errorf("credential value = %v", cred["value"])
	}

	for name, setup := range map[string]func(r *testRun) []string{
		"both sources": func(r *testRun) []string {
			r.files["/cred"] = "x"
			return []string{"--credential-file", "/cred"}
		},
		"no source": func(r *testRun) []string {
			delete(r.env, "HOOKRELAY_WEBHOOK_CREDENTIAL")
			return nil
		},
		"missing bot id": func(r *testRun) []string { return []string{"--bot-id", ""} },
		"unknown type needs a kind": func(r *testRun) []string {
			return []string{"--type", "maxbot"}
		},
	} {
		posts := api.count(http.MethodPost)
		r := newTestRun(srv.URL)
		extra := setup(r)
		if code := r.run(append(append([]string{}, createArgs...), extra...)...); code != ExitUsage {
			t.Errorf("%s: exit = %d, want 2", name, code)
		}
		if api.count(http.MethodPost) != posts {
			t.Errorf("%s: request sent despite a usage error", name)
		}
	}
}

// TestAdminUsage pins usage failures for unknown commands.
func TestAdminUsage(t *testing.T) {
	for _, args := range [][]string{{}, {"webhook"}, {"webhook", "delete"}, {"bot", "list"}} {
		r := newTestRun("http://127.0.0.1:1")
		if code := r.run(args...); code != ExitUsage {
			t.Errorf("%v: exit = %d, want 2", args, code)
		}
	}
}

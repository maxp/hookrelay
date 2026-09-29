package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// routedAdminAPI answers by "METHOD path" and records every request.
type routedAdminAPI struct {
	mu       sync.Mutex
	routes   map[string]http.HandlerFunc
	requests []recordedRequest
	queries  []string
}

func (f *routedAdminAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	data, _ := io.ReadAll(r.Body)
	rec := recordedRequest{Method: r.Method, Path: r.URL.Path, Authorization: r.Header.Get("Authorization"), ContentType: r.Header.Get("Content-Type")}
	_ = json.Unmarshal(data, &rec.Body)
	f.mu.Lock()
	f.requests = append(f.requests, rec)
	f.queries = append(f.queries, r.URL.RawQuery)
	h := f.routes[r.Method+" "+r.URL.Path]
	f.mu.Unlock()
	if h == nil {
		w.WriteHeader(http.StatusTeapot)
		return
	}
	h(w, r)
}

func (f *routedAdminAPI) calls(method, path string) []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recordedRequest
	for _, r := range f.requests {
		if r.Method == method && r.Path == path {
			out = append(out, r)
		}
	}
	return out
}

const (
	inspectPath = "/admin/v1/recipient-blocks/inspect"
	clearPath   = "/admin/v1/recipient-blocks/clear"
	listPath    = "/admin/v1/recipient-states"
)

var chatFlags = []string{"--bot-platform", "telegram", "--bot-id", "42", "--scope", "chat", "--chat-id", "-100"}

func recipientArgs(cmd string, extra ...string) []string {
	return append(append([]string{"recipients", cmd}, chatFlags...), extra...)
}

const inspectionJSON = `{"marker":%s,"queue_length":1,"head":{"message_id":"m1","status":"ready","delivery_cycle":1,"attempt":1},` +
	`"head_message_present":true,"memberships":{"blocked":%t,"leases":false,"ready":%t,"retries":false},"violated_invariants":[]}`

func inspectionBody(blocked bool) string {
	marker := "null"
	if blocked {
		marker = `{"detected_ms":55,"reason_code":"queue_head_mismatch"}`
	}
	return fmt.Sprintf(inspectionJSON, marker, blocked, !blocked)
}

// TestAdminRecipientsList pins the list request (status, limit, cursor)
// and the passthrough JSON result.
func TestAdminRecipientsList(t *testing.T) {
	page := `{"items":[{"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-100"},"status":"blocked","detected_ms":55,"reason_code":"queue_head_mismatch"}],"next_cursor":"abc"}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET " + listPath: respond(http.StatusOK, page)}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run("recipients", "list", "--status", "blocked", "--limit", "10", "--cursor", "xyz"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	if q := api.queries[0]; !strings.Contains(q, "status=blocked") || !strings.Contains(q, "limit=10") || !strings.Contains(q, "cursor=xyz") {
		t.Errorf("query = %q", q)
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["next_cursor"] != "abc" || len(out["items"].([]any)) != 1 {
		t.Errorf("stdout = %s (%v)", r.stdout.String(), err)
	}

	r = newTestRun(srv.URL)
	r.tty = true
	r.run("recipients", "list", "--status", "blocked")
	if !strings.Contains(r.stdout.String(), "chat") || !strings.Contains(r.stdout.String(), "queue_head_mismatch") || !strings.Contains(r.stdout.String(), "next_cursor") {
		t.Errorf("table = %s", r.stdout.String())
	}

	if code := newTestRun(srv.URL).run("recipients", "list"); code != ExitUsage {
		t.Errorf("list without --status = %d, want usage", code)
	}
	r.assertNoSecrets(t)
}

// TestAdminRecipientsInspect pins the structured Recipient in the request
// and the flag validation.
func TestAdminRecipientsInspect(t *testing.T) {
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"POST " + inspectPath: respond(http.StatusOK, inspectionBody(true))}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run(recipientArgs("inspect-block")...); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	got, _ := json.Marshal(api.calls(http.MethodPost, inspectPath)[0].Body)
	if string(got) != `{"recipient":{"bot_id":"42","bot_platform":"telegram","chat_id":"-100","scope":"chat"}}` {
		t.Errorf("request = %s", got)
	}
	if !strings.Contains(r.stdout.String(), `"reason_code": "queue_head_mismatch"`) {
		t.Errorf("stdout = %s", r.stdout.String())
	}
	for name, args := range map[string][]string{
		"no scope":        {"recipients", "inspect-block", "--bot-platform", "telegram", "--bot-id", "42"},
		"chat without id": {"recipients", "inspect-block", "--bot-platform", "telegram", "--bot-id", "42", "--scope", "chat"},
		"both ids":        append(recipientArgs("inspect-block"), "--user-id", "7"),
	} {
		if code := newTestRun(srv.URL).run(args...); code != ExitUsage {
			t.Errorf("%s: exit = %d, want usage", name, code)
		}
	}
}

// TestAdminRecipientsClear pins the confirmed clear: --yes is required, the
// exact preconditions are sent, and definite refusals fail without retry.
func TestAdminRecipientsClear(t *testing.T) {
	clearArgs := recipientArgs("clear-block", "--expected-detected-ms", "55", "--expected-reason-code", "queue_head_mismatch", "--yes")
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"POST " + clearPath: func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run(clearArgs...); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	body := api.calls(http.MethodPost, clearPath)[0].Body
	if body["expected_detected_ms"] != 55.0 || body["expected_reason_code"] != "queue_head_mismatch" || body["recipient"] == nil {
		t.Errorf("request = %v", body)
	}
	if !strings.Contains(r.stdout.String(), `"outcome": "cleared"`) {
		t.Errorf("stdout = %s", r.stdout.String())
	}

	noYes := clearArgs[:len(clearArgs)-1]
	if code := newTestRun(srv.URL).run(noYes...); code != ExitUsage || len(api.calls(http.MethodPost, clearPath)) != 1 {
		t.Errorf("clear without --yes: exit %d, requests %d", code, len(api.calls(http.MethodPost, clearPath)))
	}

	for status, code := range map[int]string{
		http.StatusConflict:           "recipient_state_ambiguous",
		http.StatusPreconditionFailed: "precondition_failed",
		http.StatusNotFound:           "recipient_block_not_found",
	} {
		api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"POST " + clearPath: respond(status, apiErrorBody(code, "refused"))}}
		srv := httptest.NewServer(api)
		r := newTestRun(srv.URL)
		if exit := r.run(clearArgs...); exit != ExitError || !strings.Contains(r.stderr.String(), code) ||
			len(api.calls(http.MethodPost, clearPath)) != 1 || len(api.calls(http.MethodPost, inspectPath)) != 0 {
			t.Errorf("%d: exit %d, stderr %s", status, exit, r.stderr.String())
		}
		srv.Close()
	}
}

// TestAdminRecipientsClearUncertain pins the uncertain-outcome discipline:
// no retry, one inspection, the observed state reported with the audit
// caveat, and a failing exit either way.
func TestAdminRecipientsClearUncertain(t *testing.T) {
	clearArgs := recipientArgs("clear-block", "--expected-detected-ms", "55", "--expected-reason-code", "queue_head_mismatch", "--yes")
	for name, tc := range map[string]struct {
		clear   http.HandlerFunc
		inspect http.HandlerFunc
		outcome string
		warning string
	}{
		"lost response, marker gone":   {dropConnection, respond(http.StatusOK, inspectionBody(false)), outcomeDesiredStateObserved, "does NOT confirm the clear"},
		"503, marker still present":    {respond(http.StatusServiceUnavailable, apiErrorBody("dependency_unavailable", "clear outcome is uncertain")), respond(http.StatusOK, inspectionBody(true)), outcomeUncertain, "still blocked"},
		"lost response, inspect fails": {dropConnection, dropConnection, outcomeUncertain, "could not be inspected"},
	} {
		t.Run(name, func(t *testing.T) {
			api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"POST " + clearPath: tc.clear, "POST " + inspectPath: tc.inspect}}
			srv := httptest.NewServer(api)
			defer srv.Close()
			r := newTestRun(srv.URL)
			if code := r.run(clearArgs...); code != ExitError {
				t.Fatalf("exit = %d, want 1", code)
			}
			if n := len(api.calls(http.MethodPost, clearPath)); n != 1 {
				t.Errorf("clear requests = %d, want exactly 1 (no blind retry)", n)
			}
			if n := len(api.calls(http.MethodPost, inspectPath)); n != 1 {
				t.Errorf("inspect requests = %d, want 1", n)
			}
			var out map[string]any
			if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["outcome"] != tc.outcome {
				t.Errorf("stdout = %s", r.stdout.String())
			}
			if !strings.Contains(r.stderr.String(), tc.warning) {
				t.Errorf("stderr = %s", r.stderr.String())
			}
			r.assertNoSecrets(t)
		})
	}
}

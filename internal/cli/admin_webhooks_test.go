package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const webhookListPath = "/admin/v1/webhooks"

func endpointVersionJSON(id string, enabled bool, version int) string {
	b, _ := json.Marshal(map[string]any{
		"webhook_type": "telegram", "webhook_identifier": id, "bot_platform": "telegram", "bot_id": "42", "enabled": enabled,
		"credential": map[string]any{"kind": "secret_token", "configured": true}, "generation_id": "0195c4d8-0000-7000-8000-000000000001",
		"config_version": version, "created_ms": 1740000000000, "updated_ms": 1740000000001, "webhook_path": "/webhook/telegram/" + id,
	})
	return string(b)
}

// TestAdminWebhookList pins the list query, the JSON passthrough, and the
// table form with its continuation cursor.
func TestAdminWebhookList(t *testing.T) {
	page := `{"items":[` + endpointVersionJSON("wh_a", true, 1) + `,` + endpointVersionJSON("wh_b", false, 3) + `],"next_cursor":"abc"}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET " + webhookListPath: respond(http.StatusOK, page)}}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run("webhook", "list", "--limit", "10", "--cursor", "xyz"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	if q := api.queries[0]; !strings.Contains(q, "limit=10") || !strings.Contains(q, "cursor=xyz") {
		t.Errorf("query = %q", q)
	}
	var out webhookPage
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.NextCursor != "abc" || len(out.Items) != 2 || out.Items[1].ConfigVersion != 3 {
		t.Errorf("stdout = %s (%v)", r.stdout.String(), err)
	}

	r = newTestRun(srv.URL)
	r.tty = true
	if code := r.run("webhook", "list"); code != ExitOK {
		t.Fatalf("table exit = %d", code)
	}
	for _, want := range []string{"WEBHOOK_IDENTIFIER", "wh_a", "wh_b", "false", "next_cursor", "abc"} {
		if !strings.Contains(r.stdout.String(), want) {
			t.Errorf("table lacks %q: %s", want, r.stdout.String())
		}
	}
	if code := newTestRun(srv.URL).run("webhook", "list", "extra"); code != ExitUsage {
		t.Errorf("stray argument = %d, want usage", code)
	}
	r.assertNoSecrets(t)
}

const (
	webhookAPath = "/admin/v1/webhooks/telegram/wh_a"
	etagV1       = `"0195c4d8-0000-7000-8000-000000000001:1"`
)

// tagged answers with body and an ETag.
func tagged(status int, etag, body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		w.Write([]byte(body))
	}
}

// sequence answers each call with the next handler (the last repeats).
func sequence(hs ...http.HandlerFunc) http.HandlerFunc {
	n := 0
	return func(w http.ResponseWriter, r *http.Request) {
		h := hs[min(n, len(hs)-1)]
		n++
		h(w, r)
	}
}

// TestAdminWebhookSetEnabled pins disable/enable: --yes is required, the
// ETag read first is sent as If-Match with the requested flag, and a stale
// ETag or another refusal fails without a retry.
func TestAdminWebhookSetEnabled(t *testing.T) {
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
		"GET " + webhookAPath:   tagged(http.StatusOK, etagV1, endpointVersionJSON("wh_a", true, 1)),
		"PATCH " + webhookAPath: tagged(http.StatusOK, `"0195c4d8-0000-7000-8000-000000000001:2"`, endpointVersionJSON("wh_a", false, 2)),
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()

	if code := newTestRun(srv.URL).run("webhook", "disable", "--type", "telegram", "--identifier", "wh_a"); code != ExitUsage {
		t.Errorf("without --yes = %d, want usage", code)
	}
	if len(api.requests) != 0 {
		t.Fatal("a request was sent without --yes")
	}
	r := newTestRun(srv.URL)
	if code := r.run("webhook", "disable", "--type", "telegram", "--identifier", "wh_a", "--yes"); code != ExitOK {
		t.Fatalf("disable = %d: %s", code, r.stderr.String())
	}
	patch := api.calls(http.MethodPatch, webhookAPath)
	if len(patch) != 1 || patch[0].IfMatch != etagV1 || patch[0].Body["enabled"] != false || patch[0].ContentType != "application/json" {
		t.Fatalf("patch = %+v", patch)
	}
	var out webhookResponse
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.Enabled || out.ConfigVersion != 2 {
		t.Errorf("stdout = %s", r.stdout.String())
	}

	api.routes["PATCH "+webhookAPath] = respond(http.StatusPreconditionFailed, `{"error":{"code":"precondition_failed","message":"changed","request_id":"r"}}`)
	r = newTestRun(srv.URL)
	if code := r.run("webhook", "enable", "--type", "telegram", "--identifier", "wh_a", "--yes"); code != ExitError ||
		!strings.Contains(r.stderr.String(), "not retrying") || len(api.calls(http.MethodPatch, webhookAPath)) != 2 {
		t.Errorf("stale = %d %s", code, r.stderr.String())
	}
	api.routes["GET "+webhookAPath] = respond(http.StatusNotFound, `{"error":{"code":"webhook_endpoint_not_found","message":"no","request_id":"r"}}`)
	r = newTestRun(srv.URL)
	if code := r.run("webhook", "enable", "--type", "telegram", "--identifier", "wh_a", "--yes"); code != ExitError ||
		len(api.calls(http.MethodPatch, webhookAPath)) != 2 || !strings.Contains(r.stderr.String(), "webhook_endpoint_not_found") {
		t.Errorf("missing endpoint = %d %s", code, r.stderr.String())
	}
	r.assertNoSecrets(t)
}

// TestAdminWebhookSetEnabledUncertain pins the lost-response discipline: a
// 5xx is never retried; the re-read reports desired_state_observed only
// for the requested flag in the same generation, else uncertain.
func TestAdminWebhookSetEnabledUncertain(t *testing.T) {
	for name, tc := range map[string]struct {
		reread  string
		outcome string
	}{
		"observed":         {endpointVersionJSON("wh_a", false, 2), outcomeDesiredStateObserved},
		"not applied":      {endpointVersionJSON("wh_a", true, 1), outcomeUncertain},
		"other generation": {strings.Replace(endpointVersionJSON("wh_a", false, 1), "000000000001", "000000000002", 1), outcomeUncertain},
	} {
		api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
			"GET " + webhookAPath: sequence(tagged(http.StatusOK, etagV1, endpointVersionJSON("wh_a", true, 1)), tagged(http.StatusOK, "x", tc.reread)),
			"PATCH " + webhookAPath: respond(http.StatusServiceUnavailable,
				`{"error":{"code":"dependency_unavailable","message":"uncertain","request_id":"r"}}`),
		}}
		srv := httptest.NewServer(api)
		r := newTestRun(srv.URL)
		code := r.run("webhook", "disable", "--type", "telegram", "--identifier", "wh_a", "--yes")
		srv.Close()
		var out uncertainResult
		if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.Outcome != tc.outcome || code != ExitError {
			t.Errorf("%s: exit %d stdout %s", name, code, r.stdout.String())
		}
		if len(api.calls(http.MethodPatch, webhookAPath)) != 1 {
			t.Errorf("%s: the mutation was retried", name)
		}
		if !strings.Contains(r.stderr.String(), "not retrying") {
			t.Errorf("%s: stderr = %s", name, r.stderr.String())
		}
	}
}

// TestAdminWebhookDelete pins delete: --yes required, the read ETag sent as
// If-Match, a deleted result, the disable hint for an enabled endpoint, and
// the lost-response reconciliation that never retries.
func TestAdminWebhookDelete(t *testing.T) {
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
		"GET " + webhookAPath:    tagged(http.StatusOK, etagV1, endpointVersionJSON("wh_a", false, 1)),
		"DELETE " + webhookAPath: respond(http.StatusNoContent, ""),
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	if code := newTestRun(srv.URL).run("webhook", "delete", "--type", "telegram", "--identifier", "wh_a"); code != ExitUsage || len(api.requests) != 0 {
		t.Fatalf("without --yes = %d", code)
	}
	r := newTestRun(srv.URL)
	if code := r.run("webhook", "delete", "--type", "telegram", "--identifier", "wh_a", "--yes"); code != ExitOK {
		t.Fatalf("delete = %d %s", code, r.stderr.String())
	}
	if del := api.calls(http.MethodDelete, webhookAPath); len(del) != 1 || del[0].IfMatch != etagV1 {
		t.Fatalf("delete calls = %+v", del)
	}
	var out uncertainResult
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.Outcome != outcomeDeleted || out.WebhookIdentifier != "wh_a" {
		t.Errorf("stdout = %s", r.stdout.String())
	}

	api.routes["DELETE "+webhookAPath] = respond(http.StatusConflict, `{"error":{"code":"endpoint_must_be_disabled","message":"disable first","request_id":"r"}}`)
	r = newTestRun(srv.URL)
	if code := r.run("webhook", "delete", "--type", "telegram", "--identifier", "wh_a", "--yes"); code != ExitError ||
		!strings.Contains(r.stderr.String(), "webhook disable --type telegram --identifier wh_a --yes") {
		t.Errorf("enabled = %d %s", code, r.stderr.String())
	}

	for name, tc := range map[string]struct {
		reread  http.HandlerFunc
		outcome string
	}{
		"absent":  {respond(http.StatusNotFound, `{"error":{"code":"webhook_endpoint_not_found","message":"no","request_id":"r"}}`), outcomeDesiredStateObserved},
		"present": {tagged(http.StatusOK, etagV1, endpointVersionJSON("wh_a", false, 1)), outcomeUncertain},
	} {
		api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
			"GET " + webhookAPath:    sequence(tagged(http.StatusOK, etagV1, endpointVersionJSON("wh_a", false, 1)), tc.reread),
			"DELETE " + webhookAPath: respond(http.StatusServiceUnavailable, `{"error":{"code":"dependency_unavailable","message":"uncertain","request_id":"r"}}`),
		}}
		srv := httptest.NewServer(api)
		r := newTestRun(srv.URL)
		code := r.run("webhook", "delete", "--type", "telegram", "--identifier", "wh_a", "--yes")
		srv.Close()
		var out uncertainResult
		if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.Outcome != tc.outcome || code != ExitError {
			t.Errorf("%s: exit %d stdout %s", name, code, r.stdout.String())
		}
		if len(api.calls(http.MethodDelete, webhookAPath)) != 1 {
			t.Errorf("%s: the deletion was retried", name)
		}
	}
	r.assertNoSecrets(t)
}

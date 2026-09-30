package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAdminAuditList pins the query, the JSON passthrough, and the table.
func TestAdminAuditList(t *testing.T) {
	page := `{"items":[{"stream_id":"1-0","event_id":"e1","timestamp_ms":1740000000000,"actor":"admin_session",` +
		`"operation":"dead_letter_payload_viewed","target":"m1","request_id":"r1","outcome":"success"}],"next_cursor":"abc"}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET /admin/v1/audit": respond(http.StatusOK, page)}}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run("audit", "list", "--limit", "5", "--cursor", "xyz"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	if q := api.queries[0]; !strings.Contains(q, "limit=5") || !strings.Contains(q, "cursor=xyz") {
		t.Errorf("query = %q", q)
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["next_cursor"] != "abc" || len(out["items"].([]any)) != 1 {
		t.Errorf("stdout = %s (%v)", r.stdout.String(), err)
	}
	r = newTestRun(srv.URL)
	r.tty = true
	r.run("audit", "list")
	if !strings.Contains(r.stdout.String(), "dead_letter_payload_viewed") || !strings.Contains(r.stdout.String(), "admin_session") ||
		!strings.Contains(r.stdout.String(), "next_cursor") {
		t.Errorf("table = %s", r.stdout.String())
	}
	r.assertNoSecrets(t)
	if code := newTestRun(srv.URL).run("audit"); code != ExitUsage {
		t.Errorf("audit without list = %d", code)
	}
	api.routes["GET /admin/v1/audit"] = respond(http.StatusServiceUnavailable, apiErrorBody("dependency_unavailable", "x"))
	if code := newTestRun(srv.URL).run("audit", "list"); code != ExitError {
		t.Errorf("unavailable = %d", code)
	}
}

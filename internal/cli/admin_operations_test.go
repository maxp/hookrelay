package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestAdminOperationsSummary pins the JSON passthrough and the table.
func TestAdminOperationsSummary(t *testing.T) {
	body := `{"generated_ms":1,"readiness":{"ready":true,"accepting_webhooks":true,"startup_reconciliation":"complete"},` +
		`"valkey":{"used_memory_bytes":10,"maxmemory_bytes":20},"queues":{"queued_messages":3,"ready_recipients":1,"leased_recipients":1,` +
		`"retry_wait_recipients":0,"blocked_recipients":0,"earliest_lease_expires_ms":99},"dead_letters":{"count":2,` +
		`"oldest_dead_lettered_ms":5,"newest_dead_lettered_ms":6},"webhook_endpoints":{"count":1},"admin_sessions":{"indexed":0},` +
		`"audit":{"length":7},"links":{"grafana_url":"https://g/d"}}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET /admin/v1/operations/summary": respond(http.StatusOK, body)}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run("operations", "summary"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["queues"].(map[string]any)["queued_messages"] != 3.0 {
		t.Errorf("stdout = %s (%v)", r.stdout.String(), err)
	}
	r = newTestRun(srv.URL)
	r.tty = true
	r.run("operations", "summary")
	for _, want := range []string{"queued_messages", "earliest_lease_expires_ms", "oldest_dead_lettered_ms", "https://g/d", "complete"} {
		if !strings.Contains(r.stdout.String(), want) {
			t.Errorf("table lacks %s: %s", want, r.stdout.String())
		}
	}
	if strings.Contains(r.stdout.String(), "earliest_retry_at_ms") {
		t.Errorf("zero retry time printed: %s", r.stdout.String())
	}
	r.assertNoSecrets(t)
	if code := newTestRun(srv.URL).run("operations"); code != ExitUsage {
		t.Errorf("operations without summary = %d", code)
	}
}

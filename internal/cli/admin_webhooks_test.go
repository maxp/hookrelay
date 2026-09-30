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

package administration_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// createWebhook creates one endpoint through the API and returns its
// identifier.
func createWebhook(t *testing.T, h http.Handler, id, botID string, enabled bool) {
	t.Helper()
	body := `{"webhook_type":"telegram","webhook_identifier":"` + id + `","bot_id":"` + botID + `",` +
		`"credential":{"kind":"secret_token","value":"telegram-secret-001"},"enabled":` + map[bool]string{true: "true", false: "false"}[enabled] + `}`
	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", body); rec.Code != http.StatusCreated {
		t.Fatalf("create %s = %d %s", id, rec.Code, rec.Body)
	}
}

// TestWebhookListOverRealValkey pages every endpoint through the composed
// handler, service, store, and Valkey, newest first, without repeats or
// gaps even when creation times tie.
func TestWebhookListOverRealValkey(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)
	h := composedHandler(t, a)
	want := map[string]bool{}
	for _, id := range []string{"wh_1", "wh_2", "wh_3", "wh_4", "wh_5"} {
		createWebhook(t, h, id, "123456789", true)
		want[id] = true
	}
	seen := map[string]bool{}
	var lastCreated int64 = 1 << 62
	cursor := ""
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatal("pagination does not terminate")
		}
		path := "/admin/v1/webhooks?limit=2"
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		rec := doJSON(t, h, http.MethodGet, path, "admin-secret-value-016", "")
		if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "telegram-secret-001") {
			t.Fatalf("list = %d %s", rec.Code, rec.Body)
		}
		var page struct {
			Items []struct {
				WebhookIdentifier string `json:"webhook_identifier"`
				CreatedMs         int64  `json:"created_ms"`
			} `json:"items"`
			NextCursor string `json:"next_cursor"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		for _, it := range page.Items {
			if seen[it.WebhookIdentifier] {
				t.Fatalf("%s listed twice", it.WebhookIdentifier)
			}
			if it.CreatedMs > lastCreated {
				t.Fatalf("list is not newest first at %s", it.WebhookIdentifier)
			}
			seen[it.WebhookIdentifier], lastCreated = true, it.CreatedMs
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if len(seen) != len(want) {
		t.Errorf("listed %v, want %v", seen, want)
	}
}

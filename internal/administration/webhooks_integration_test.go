package administration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/valkey"
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

// webhookIngestion composes the webhook route over the same Valkey.
func webhookIngestion(t *testing.T, a *valkey.Adapter) http.Handler {
	t.Helper()
	types, err := ingestion.Builtin(ingestion.BuiltinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := ingestion.NewHandler(ingestion.HandlerDeps{
		Registry: types, Endpoints: valkey.NewEndpointLookup(a),
		Acceptor: valkey.NewMessageAcceptor(a, valkey.AcceptLimits{MaxQueuedMessages: 100, MaxQueuedMessagesPerRecipient: 10, MaxDedupRecords: 100, DedupRetention: time.Hour}),
		Gen:      gen.Crypto{}, Clock: gen.SystemClock{},
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// sendUpdate posts one signed Telegram update to the endpoint.
func sendUpdate(h http.Handler, id string, updateID int) int {
	body := `{"update_id":` + strconv.Itoa(updateID) + `,"message":{"message_id":1,"date":1700000000,"chat":{"id":-5}}}`
	req := httptest.NewRequest(http.MethodPost, "/webhook/telegram/"+id, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Telegram-Bot-Api-Secret-Token", "telegram-secret-001")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// patchEnabled sends PATCH with If-Match and returns the response.
func patchEnabled(t *testing.T, h http.Handler, id, etag string, enabled bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPatch, "/admin/v1/webhooks/telegram/"+id, strings.NewReader(`{"enabled":`+strconv.FormatBool(enabled)+`}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer admin-secret-value-016")
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestWebhookEnableDisableOverRealValkey drives disable → the webhook route
// answers 404 like an unknown endpoint → re-enable → accepted again, with
// ETags advancing, a stale ETag refused, and one audit entry per change.
func TestWebhookEnableDisableOverRealValkey(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)
	admin := composedHandler(t, a)
	hooks := webhookIngestion(t, a)
	createWebhook(t, admin, "wh_toggle", "123456789", true)
	if code := sendUpdate(hooks, "wh_toggle", 1); code != http.StatusOK {
		t.Fatalf("enabled webhook = %d", code)
	}

	etag := doJSON(t, admin, http.MethodGet, "/admin/v1/webhooks/telegram/wh_toggle", "admin-secret-value-016", "").Header().Get("ETag")
	if rec := patchEnabled(t, admin, "wh_toggle", "", false); rec.Code != http.StatusPreconditionRequired {
		t.Fatalf("without If-Match = %d", rec.Code)
	}
	rec := patchEnabled(t, admin, "wh_toggle", etag, false)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") == etag || !strings.HasSuffix(rec.Header().Get("ETag"), `:2"`) {
		t.Fatalf("disable = %d %s etag %s", rec.Code, rec.Body, rec.Header().Get("ETag"))
	}
	disabledTag := rec.Header().Get("ETag")
	if code := sendUpdate(hooks, "wh_toggle", 2); code != http.StatusNotFound {
		t.Errorf("disabled webhook = %d, want 404", code)
	}
	if rec := patchEnabled(t, admin, "wh_toggle", etag, true); rec.Code != http.StatusPreconditionFailed {
		t.Errorf("stale enable = %d", rec.Code)
	}
	if rec := patchEnabled(t, admin, "wh_toggle", disabledTag, true); rec.Code != http.StatusOK {
		t.Fatalf("enable = %d %s", rec.Code, rec.Body)
	}
	if code := sendUpdate(hooks, "wh_toggle", 3); code != http.StatusOK {
		t.Errorf("re-enabled webhook = %d", code)
	}
	var ops []string
	for _, e := range auditTail(t, a, 100) {
		ops = append(ops, e["operation"])
	}
	if got := strings.Join(ops, ","); got != "webhook_endpoint_created,webhook_endpoint_disabled,webhook_endpoint_enabled" {
		t.Errorf("audit operations = %s", got)
	}
}

// deleteWebhook sends DELETE with an optional If-Match.
func deleteWebhook(t *testing.T, h http.Handler, id, etag string) int {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/admin/v1/webhooks/telegram/"+id, nil)
	req.Header.Set("Authorization", "Bearer admin-secret-value-016")
	if etag != "" {
		req.Header.Set("If-Match", etag)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

// TestWebhookDeleteOverRealValkey drives the retirement flow: an enabled
// endpoint refuses deletion, a disabled one is deleted with one audit entry
// and disappears from reads, the listing, and the webhook route; a repeat
// is a 204 without audit; recreating the identifier starts a new
// generation, so the old ETag is refused.
func TestWebhookDeleteOverRealValkey(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)
	admin := composedHandler(t, a)
	hooks := webhookIngestion(t, a)
	createWebhook(t, admin, "wh_old", "123456789", true)
	get := func() *httptest.ResponseRecorder {
		return doJSON(t, admin, http.MethodGet, "/admin/v1/webhooks/telegram/wh_old", "admin-secret-value-016", "")
	}
	etag := get().Header().Get("ETag")
	if code := deleteWebhook(t, admin, "wh_old", etag); code != http.StatusConflict {
		t.Fatalf("delete enabled = %d", code)
	}
	disabled := patchEnabled(t, admin, "wh_old", etag, false).Header().Get("ETag")
	if code := deleteWebhook(t, admin, "wh_old", disabled); code != http.StatusNoContent {
		t.Fatalf("delete = %d", code)
	}
	if rec := get(); rec.Code != http.StatusNotFound {
		t.Errorf("read after delete = %d", rec.Code)
	}
	if rec := doJSON(t, admin, http.MethodGet, "/admin/v1/webhooks", "admin-secret-value-016", ""); rec.Body.String() != `{"items":[]}`+"\n" {
		t.Errorf("listing after delete = %s", rec.Body)
	}
	if code := sendUpdate(hooks, "wh_old", 1); code != http.StatusNotFound {
		t.Errorf("webhook after delete = %d", code)
	}
	if code := deleteWebhook(t, admin, "wh_old", ""); code != http.StatusNoContent {
		t.Errorf("repeat delete = %d", code)
	}
	var ops []string
	for _, e := range auditTail(t, a, 100) {
		ops = append(ops, e["operation"])
	}
	if got := strings.Join(ops, ","); got != "webhook_endpoint_created,webhook_endpoint_disabled,webhook_endpoint_deleted" {
		t.Errorf("audit operations = %s", got)
	}

	createWebhook(t, admin, "wh_old", "123456789", false)
	if code := deleteWebhook(t, admin, "wh_old", disabled); code != http.StatusPreconditionFailed {
		t.Errorf("earlier-generation ETag = %d, want 412", code)
	}
	if code := deleteWebhook(t, admin, "wh_old", get().Header().Get("ETag")); code != http.StatusNoContent {
		t.Errorf("delete recreated = %d", code)
	}
}

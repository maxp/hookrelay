package administration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

const adminSecret = "admin-secret-value-016"

// webhookService composes the Admin API over repo.
func webhookService(t *testing.T, repo *fakeRepo) (http.Handler, *bytes.Buffer) {
	t.Helper()
	logs := &bytes.Buffer{}
	svc, err := NewService(ServiceDeps{
		Repo: repo, Catalog: fakeCatalog{}, Audit: &fakeAudit{}, AdminSecret: adminSecret, Gen: fixedGen{},
		Logger: observability.NewTestLogger("info", logs), Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return Handler(svc), logs
}

func storedEndpoint(id string, createdMs int64, enabled bool) *Endpoint {
	return &Endpoint{Type: "telegram", Identifier: id, BotID: "42", Enabled: enabled, CredentialKind: "secret_token",
		CredentialValue: "super-secret-credential", GenerationID: "0195c4d8-0000-7000-8000-00000000000" + id[len(id)-1:],
		CreatedMs: createdMs, UpdatedMs: createdMs + 1, ConfigVersion: 2}
}

func listing(id string, createdMs int64) EndpointListing {
	return EndpointListing{Member: "telegram:" + id, CreatedMs: createdMs, Endpoint: storedEndpoint(id, createdMs, true)}
}

type listPage struct {
	Items []struct {
		WebhookIdentifier string `json:"webhook_identifier"`
		BotPlatform       string `json:"bot_platform"`
		ConfigVersion     int64  `json:"config_version"`
		WebhookPath       string `json:"webhook_path"`
	} `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

// TestListWebhooksContract pins the list route: newest-first safe items,
// the credential value never returned, a cursor over created_ms and member
// that resumes strictly after the last item, no cursor on the last page,
// and orphan members skipped with one event.
func TestListWebhooksContract(t *testing.T) {
	repo := newFakeRepo()
	repo.listings = []EndpointListing{
		listing("wh_c", 300),
		{Member: "telegram:wh_gone", CreatedMs: 250, Orphan: OrphanMissing},
		listing("wh_b2", 200),
		listing("wh_b1", 200),
		listing("wh_a", 100),
	}
	h, logs := webhookService(t, repo)

	rec := doJSON(t, h, http.MethodGet, "/admin/v1/webhooks?limit=3", adminSecret, "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "super-secret-credential") {
		t.Fatal("the credential value leaked into the list")
	}
	var page listPage
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].WebhookIdentifier != "wh_c" || page.Items[1].WebhookIdentifier != "wh_b2" {
		t.Fatalf("page 1 = %s", rec.Body)
	}
	if it := page.Items[0]; it.BotPlatform != "telegram" || it.ConfigVersion != 2 || it.WebhookPath != "/webhook/telegram/wh_c" {
		t.Errorf("item = %+v", it)
	}
	if repo.listCalls[0].limit != 4 {
		t.Errorf("repository limit = %d, want limit+1", repo.listCalls[0].limit)
	}
	if page.NextCursor == nil {
		t.Fatal("no next_cursor while more remain")
	}
	raw, _ := base64.RawURLEncoding.DecodeString(*page.NextCursor)
	if string(raw) != `{"created_ms":200,"id":"telegram:wh_b2"}` {
		t.Errorf("cursor = %s", raw)
	}
	if n := strings.Count(logs.String(), `"event":"webhook_index_orphan"`); n != 1 || !strings.Contains(logs.String(), `"member":"telegram:wh_gone"`) {
		t.Errorf("orphan events = %d: %s", n, logs)
	}

	rec = doJSON(t, h, http.MethodGet, "/admin/v1/webhooks?limit=3&cursor="+*page.NextCursor, adminSecret, "")
	page = listPage{}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 2 || page.Items[0].WebhookIdentifier != "wh_b1" || page.Items[1].WebhookIdentifier != "wh_a" || page.NextCursor != nil {
		t.Fatalf("page 2 = %s", rec.Body)
	}

	// Default limit and an empty listing.
	repo.listings = nil
	rec = doJSON(t, h, http.MethodGet, "/admin/v1/webhooks", adminSecret, "")
	if rec.Code != http.StatusOK || rec.Body.String() != `{"items":[]}`+"\n" {
		t.Errorf("empty list = %d %s", rec.Code, rec.Body)
	}
	if got := repo.listCalls[len(repo.listCalls)-1].limit; got != 51 {
		t.Errorf("default repository limit = %d", got)
	}
}

// TestListWebhooksValidation pins the bounded refusals.
func TestListWebhooksValidation(t *testing.T) {
	h, _ := webhookService(t, newFakeRepo())
	for query, code := range map[string]string{
		"limit=0":           "invalid_request",
		"limit=201":         "invalid_request",
		"limit=x":           "invalid_request",
		"cursor=!!!":        "invalid_cursor",
		"cursor=bm90anNvbg": "invalid_cursor",
		"cursor=" + base64.RawURLEncoding.EncodeToString([]byte(`{"created_ms":1}`)): "invalid_cursor",
	} {
		rec := doJSON(t, h, http.MethodGet, "/admin/v1/webhooks?"+query, adminSecret, "")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"code":"`+code+`"`) {
			t.Errorf("%s = %d %s", query, rec.Code, rec.Body)
		}
	}
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/webhooks", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated list = %d", rec.Code)
	}
}

// doPatch sends a PATCH with an optional If-Match header.
func doPatch(t *testing.T, h http.Handler, path, ifMatch, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPatch, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminSecret)
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errCode(rec *httptest.ResponseRecorder) string {
	var env struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &env)
	return env.Error.Code
}

// TestPatchWebhookContract pins enable/disable: 200 with the new ETag and
// the full safe representation, the audit copy and feature event, a no-op
// for the current value (unchanged ETag, no audit), and the bounded
// refusals in precedence order.
func TestPatchWebhookContract(t *testing.T) {
	repo := newFakeRepo()
	repo.endpoints["telegram:wh_a"] = storedEndpoint("wh_a", 100, true)
	h, logs := webhookService(t, repo)
	const path = "/admin/v1/webhooks/telegram/wh_a"
	etag := `"0195c4d8-0000-7000-8000-00000000000a:2"`

	rec := doPatch(t, h, path, etag, `{"enabled":false}`)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != `"0195c4d8-0000-7000-8000-00000000000a:3"` {
		t.Fatalf("disable = %d %s etag %s", rec.Code, rec.Body, rec.Header().Get("ETag"))
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["enabled"] != false || body["config_version"] != 3.0 || body["bot_platform"] != "telegram" ||
		body["credential"].(map[string]any)["configured"] != true || strings.Contains(rec.Body.String(), "super-secret") {
		t.Errorf("body = %s", rec.Body)
	}
	if c := repo.mutations[0]; c.target != "telegram:wh_a" || c.expected == nil || c.expected.ConfigVersion != 2 || c.eventID == "" || c.requestID == "" {
		t.Errorf("mutation call = %+v", c)
	}
	for _, want := range []string{`"event":"webhook_endpoint_disabled"`, `"operation":"webhook_endpoint_disabled"`, `"credential_kind":"secret_token"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %s: %s", want, logs)
		}
	}
	if strings.Contains(logs.String(), "super-secret") {
		t.Error("the credential leaked into logs")
	}

	// The same value again: unchanged ETag, no audit, no event.
	logs.Reset()
	newTag := `"0195c4d8-0000-7000-8000-00000000000a:3"`
	rec = doPatch(t, h, path, newTag, `{"enabled":false}`)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != newTag || logs.Len() != 0 {
		t.Errorf("no-op = %d etag %s logs %s", rec.Code, rec.Header().Get("ETag"), logs)
	}
	if rec := doPatch(t, h, path, newTag, `{"enabled":true}`); rec.Code != http.StatusOK || !strings.Contains(logs.String(), `"event":"webhook_endpoint_enabled"`) {
		t.Errorf("enable = %d %s", rec.Code, logs)
	}

	for name, tc := range map[string]struct {
		path, ifMatch, body string
		status              int
		code                string
	}{
		"stale":            {path, etag, `{"enabled":false}`, 412, "precondition_failed"},
		"missing if-match": {path, "", `{"enabled":false}`, 428, "precondition_required"},
		"missing endpoint": {"/admin/v1/webhooks/telegram/wh_x", "", `{"enabled":false}`, 404, "webhook_endpoint_not_found"},
		"unknown type":     {"/admin/v1/webhooks/other/wh_a", etag, `{"enabled":false}`, 404, "webhook_endpoint_not_found"},
		"bad identifier":   {"/admin/v1/webhooks/telegram/wh.a", etag, `{"enabled":false}`, 404, "webhook_endpoint_not_found"},
		"weak tag":         {path, "W/" + etag, `{"enabled":false}`, 400, "invalid_request"},
		"wildcard":         {path, "*", `{"enabled":false}`, 400, "invalid_request"},
		"tag list":         {path, etag + ", " + etag, `{"enabled":false}`, 400, "invalid_request"},
		"unquoted":         {path, "0195:2", `{"enabled":false}`, 400, "invalid_request"},
		"missing enabled":  {path, etag, `{}`, 400, "invalid_request"},
		"unknown field":    {path, etag, `{"enabled":false,"bot_id":"1"}`, 400, "invalid_request"},
	} {
		rec := doPatch(t, h, tc.path, tc.ifMatch, tc.body)
		if rec.Code != tc.status || errCode(rec) != tc.code {
			t.Errorf("%s = %d %s, want %d %s", name, rec.Code, rec.Body, tc.status, tc.code)
		}
	}

	for result, code := range map[SetEnabledResult]string{
		SetEnabledWrongType: "dependency_unavailable", SetEnabledUncertain: "dependency_unavailable", SetEnabledUnavailable: "dependency_unavailable",
	} {
		repo.setEnabledResult = result
		if rec := doPatch(t, h, path, etag, `{"enabled":false}`); rec.Code != http.StatusServiceUnavailable || errCode(rec) != code {
			t.Errorf("%s = %d %s", result, rec.Code, rec.Body)
		}
	}
}

// doDelete sends a DELETE with an optional If-Match header.
func doDelete(t *testing.T, h http.Handler, path, ifMatch string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	req.Header.Set("Authorization", "Bearer "+adminSecret)
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDeleteWebhookContract pins delete: 204 with the audit copy and the
// feature event for a disabled endpoint, 204 without any audit for an
// absent one, and the bounded refusals.
func TestDeleteWebhookContract(t *testing.T) {
	repo := newFakeRepo()
	repo.endpoints["telegram:wh_a"] = storedEndpoint("wh_a", 100, false)
	repo.endpoints["telegram:wh_on"] = storedEndpoint("wh_on", 100, true)
	h, logs := webhookService(t, repo)
	const path = "/admin/v1/webhooks/telegram/wh_a"
	etag := `"0195c4d8-0000-7000-8000-00000000000a:2"`

	for name, tc := range map[string]struct {
		path, ifMatch string
		status        int
		code          string
	}{
		"missing if-match": {path, "", 428, "precondition_required"},
		"stale":            {path, `"0195c4d8-0000-7000-8000-00000000000a:1"`, 412, "precondition_failed"},
		"weak":             {path, "W/" + etag, 400, "invalid_request"},
		"enabled":          {"/admin/v1/webhooks/telegram/wh_on", `"0195c4d8-0000-7000-8000-00000000000n:2"`, 409, "endpoint_must_be_disabled"},
	} {
		rec := doDelete(t, h, tc.path, tc.ifMatch)
		if rec.Code != tc.status || errCode(rec) != tc.code {
			t.Errorf("%s = %d %s, want %d %s", name, rec.Code, rec.Body, tc.status, tc.code)
		}
	}
	if logs.Len() != 0 {
		t.Errorf("refusals logged: %s", logs)
	}

	rec := doDelete(t, h, path, etag)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("delete = %d %s", rec.Code, rec.Body)
	}
	for _, want := range []string{`"event":"webhook_endpoint_deleted"`, `"operation":"webhook_endpoint_deleted"`, `"generation_id":"0195c4d8-0000-7000-8000-00000000000a"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %s: %s", want, logs)
		}
	}
	if strings.Contains(logs.String(), "super-secret") {
		t.Error("the credential leaked into logs")
	}

	// Absent: 204 again, with or without If-Match, and no audit.
	logs.Reset()
	for _, p := range []string{path, "/admin/v1/webhooks/other/wh_a", "/admin/v1/webhooks/telegram/wh.bad"} {
		if rec := doDelete(t, h, p, ""); rec.Code != http.StatusNoContent {
			t.Errorf("absent %s = %d %s", p, rec.Code, rec.Body)
		}
	}
	if rec := doDelete(t, h, path, etag); rec.Code != http.StatusNoContent {
		t.Errorf("repeat = %d", rec.Code)
	}
	if logs.Len() != 0 {
		t.Errorf("absent deletes logged: %s", logs)
	}

	for _, result := range []DeleteResult{DeleteWrongType, DeleteUncertain, DeleteUnavailable} {
		repo.deleteResult = result
		if rec := doDelete(t, h, path, etag); rec.Code != http.StatusServiceUnavailable || errCode(rec) != "dependency_unavailable" {
			t.Errorf("%s = %d %s", result, rec.Code, rec.Body)
		}
	}
}

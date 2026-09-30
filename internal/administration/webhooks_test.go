package administration

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
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

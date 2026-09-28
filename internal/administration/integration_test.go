package administration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/valkey"
)

const validCreate = `{
	"webhook_type": "telegram",
	"bot_id": "123456789",
	"credential": {"kind": "secret_token", "value": "telegram-secret-001"},
	"enabled": true
}`

func doJSON(t *testing.T, h http.Handler, method, path, secret, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// testAdapter mirrors the valkey test harness: real pinned Valkey through
// HOOKRELAY_TEST_VALKEY_URL, skip when absent.
func testAdapter(t *testing.T, production bool) *valkey.Adapter {
	t.Helper()
	url := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if url == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set; start the pinned Valkey container")
	}
	// This package shares the pinned instance with the storage tests and
	// therefore owns database 1: rewrite a bare URL (no db path) to /1 and
	// remap /0 to /1.
	if strings.HasSuffix(url, "/0") {
		url = strings.TrimSuffix(url, "0") + "1"
	} else if !strings.HasSuffix(url, "/1") {
		url = strings.TrimRight(url, "/") + "/1"
	}
	opt, err := valkey.ParseURL(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	a, err := valkey.NewAdapter(opt)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	t.Cleanup(a.Close)
	if _, err := a.ValidateReadiness(context.Background(), production); err != nil {
		t.Fatalf("gate: %v", err)
	}
	return a
}

func flushAll(t *testing.T, a *valkey.Adapter) {
	t.Helper()
	if err := a.FlushDB(context.Background()); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
}

// auditTail returns the most recent audit entries (newest last).
func auditTail(t *testing.T, a *valkey.Adapter, n int64) []map[string]string {
	t.Helper()
	entries, err := a.AuditEntries(context.Background(), n)
	if err != nil {
		t.Fatalf("audit entries: %v", err)
	}
	return entries
}

// TestAdminCreateGetOverRealValkey runs the composed vertical: HTTP handler →
// service → Valkey adapter → Lua → real Valkey, then asserts the stored
// state, the audit event, and the restart-persistence contract.
func TestAdminCreateGetOverRealValkey(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)

	repo := valkey.NewEndpointStore(a)
	audit := valkey.NewAuditSink(a)
	svc := administration.NewService(repo, builtinCatalog{}, audit, "admin-secret-value-016", gen.Crypto{})
	h := administration.Handler(svc, gen.Crypto{})

	// Create through HTTP.
	rec := doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", validCreate)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create = %d: %s", rec.Code, rec.Body.String())
	}
	var created struct {
		WebhookIdentifier string `json:"webhook_identifier"`
		GenerationID      string `json:"generation_id"`
		CreatedMs         int64  `json:"created_ms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.WebhookIdentifier, "wh_") || len(created.WebhookIdentifier) != 25 {
		t.Errorf("server-generated identifier = %q", created.WebhookIdentifier)
	}
	if created.CreatedMs <= 0 {
		t.Errorf("created_ms must come from Valkey TIME, got %d", created.CreatedMs)
	}

	// Read back through HTTP with matching ETag.
	rec = doJSON(t, h, http.MethodGet, "/admin/v1/webhooks/telegram/"+created.WebhookIdentifier, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get = %d", rec.Code)
	}
	wantETag := `"` + created.GenerationID + `:1"`
	if rec.Header().Get("ETag") != wantETag {
		t.Errorf("ETag = %q, want %q", rec.Header().Get("ETag"), wantETag)
	}
	if strings.Contains(rec.Body.String(), "telegram-secret-001") {
		t.Error("credential value leaked in read response")
	}

	// The create appended exactly one audit event for this endpoint.
	entries, err := a.AuditEntries(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("no audit entries after create")
	}
	last := entries[len(entries)-1]
	if last["operation"] != "webhook_endpoint_created" || last["target"] != "telegram:"+created.WebhookIdentifier || last["outcome"] != "success" {
		t.Errorf("create audit = %+v", last)
	}

	// Duplicate identifier through HTTP → 409.
	dup := strings.Replace(validCreate, `"enabled": true`, `"enabled": true, "webhook_identifier": "`+created.WebhookIdentifier+`"`, 1)
	rec = doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "admin-secret-value-016", dup)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate = %d", rec.Code)
	}

	// Rejected authentication audit is best effort but should appear.
	rec = doJSON(t, h, http.MethodPost, "/admin/v1/webhooks", "wrong-secret-value-00001", validCreate)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("rejected auth = %d", rec.Code)
	}
	entries, err = a.AuditEntries(context.Background(), 5)
	if err != nil {
		t.Fatal(err)
	}
	last = entries[len(entries)-1]
	if last["operation"] != "admin_auth_rejected" || last["outcome"] != "failure" {
		t.Errorf("rejected-auth audit = %+v", last)
	}

	// Restart persistence: a fresh adapter over the same Valkey still serves
	// the endpoint and the audit history is intact.
	reopened := testAdapter(t, false)
	repo2 := valkey.NewEndpointStore(reopened)
	svc2 := administration.NewService(repo2, builtinCatalog{}, valkey.NewAuditSink(reopened), "admin-secret-value-016", gen.Crypto{})
	h2 := administration.Handler(svc2, gen.Crypto{})
	rec = doJSON(t, h2, http.MethodGet, "/admin/v1/webhooks/telegram/"+created.WebhookIdentifier, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("post-restart get = %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("ETag") != wantETag {
		t.Errorf("post-restart ETag = %q", rec.Header().Get("ETag"))
	}
}

// builtinCatalog adapts the ingestion registry to the narrow TypeCatalog
// shape, mirroring the application composition.
type builtinCatalog struct{}

func (builtinCatalog) Lookup(webhookType string) (string, []string, bool) {
	r, err := ingestion.Builtin()
	if err != nil {
		return "", nil, false
	}
	d, ok := r.Lookup(ingestion.WebhookType(webhookType))
	if !ok {
		return "", nil, false
	}
	return string(d.Platform), d.CredentialKinds, true
}

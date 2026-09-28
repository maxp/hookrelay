package cli

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/valkey"
)

// realAdminHandler composes the Admin API over the pinned Valkey from
// HOOKRELAY_TEST_VALKEY_URL, skipping when absent. This package owns
// database 2 of the shared instance.
func realAdminHandler(t *testing.T) (http.Handler, *valkey.Adapter) {
	t.Helper()
	raw := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if raw == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set; start the pinned Valkey container")
	}
	raw = strings.TrimSuffix(strings.TrimSuffix(raw, "/0"), "/") + "/2"
	opt, err := valkey.ParseURL(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	a, err := valkey.NewAdapter(opt)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	t.Cleanup(a.Close)
	ctx := context.Background()
	if err := a.FlushDB(ctx); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	if _, err := a.ValidateReadiness(ctx, false); err != nil {
		t.Fatalf("gate: %v", err)
	}
	svc, err := administration.NewService(administration.ServiceDeps{
		Repo:        valkey.NewEndpointStore(a),
		Catalog:     typeCatalog{registry: types},
		Audit:       valkey.NewAuditSink(a),
		AdminSecret: testAdminSecret,
		Gen:         gen.Crypto{},
	})
	if err != nil {
		t.Fatalf("service: %v", err)
	}
	return administration.Handler(svc), a
}

// TestAdminCLIOverRealValkey drives the CLI against the real Admin API:
// create, read back, a definite conflict, and a lost create response that
// is reconciled by reading the known identity without a second POST.
func TestAdminCLIOverRealValkey(t *testing.T) {
	h, a := realAdminHandler(t)

	// dropNextCreate lets the real handler apply the create, then drops the
	// connection so the client never sees the response.
	var dropNextCreate atomic.Bool
	var posts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			posts.Add(1)
			if dropNextCreate.CompareAndSwap(true, false) {
				h.ServeHTTP(httptest.NewRecorder(), r)
				dropConnection(w, r)
				return
			}
		}
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run(createArgs...); code != ExitOK {
		t.Fatalf("create exit = %d: %s", code, r.stderr.String())
	}
	r = newTestRun(srv.URL)
	if code := r.run("webhook", "get", "--type", "telegram", "--identifier", testWebhookID); code != ExitOK {
		t.Fatalf("get exit = %d: %s", code, r.stderr.String())
	}
	var got webhookResponse
	if err := json.Unmarshal(r.stdout.Bytes(), &got); err != nil || got.BotPlatform != "telegram" || got.BotID != "123456789" {
		t.Fatalf("get result = %+v (%v)", got, err)
	}

	r = newTestRun(srv.URL)
	if code := r.run(createArgs...); code != ExitError || !strings.Contains(r.stderr.String(), "webhook_identifier_conflict") {
		t.Fatalf("duplicate create: exit %d, stderr %s", code, r.stderr.String())
	}

	// Lost response after a real create.
	dropNextCreate.Store(true)
	postsBefore := posts.Load()
	r = newTestRun(srv.URL)
	if code := r.run(append(createArgs, "--identifier", "wh_lost")...); code != ExitError {
		t.Fatalf("lost-response create exit = %d, want 1", code)
	}
	if n := posts.Load() - postsBefore; n != 1 {
		t.Errorf("lost-response create sent %d POSTs, want exactly 1", n)
	}
	var out uncertainResult
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out.Outcome != outcomeDesiredStateObserved || out.WebhookIdentifier != "wh_lost" {
		t.Fatalf("lost-response result = %+v (%v)", out, err)
	}
	entries, err := a.AuditEntries(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	created := 0
	for _, e := range entries {
		if e["target"] == "telegram:wh_lost" && e["operation"] == "webhook_endpoint_created" {
			created++
		}
	}
	if created != 1 {
		t.Errorf("audit events for the lost create = %d, want exactly 1", created)
	}
	r.assertNoSecrets(t)
}

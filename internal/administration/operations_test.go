package administration

import (
	"context"
	"net/http"
	"testing"
	"time"
)

type fakeOperations struct {
	snap OperationsSnapshot
	err  error
}

func (f *fakeOperations) OperationsSnapshot(context.Context) (OperationsSnapshot, error) {
	return f.snap, f.err
}

type fakeReadiness struct {
	ready, accepting bool
	state            string
}

func (f fakeReadiness) Ready() bool                 { return f.ready }
func (f fakeReadiness) AcceptingWebhooks() bool     { return f.accepting }
func (f fakeReadiness) ReconciliationState() string { return f.state }

// TestOperationsSummaryContract pins the summary body: readiness, memory,
// queue depths with optional times omitted when zero, the DLQ, counts, the
// Grafana link, and a wrong-type or failed snapshot as 503.
func TestOperationsSummaryContract(t *testing.T) {
	ops := &fakeOperations{snap: OperationsSnapshot{QueuedMessages: 12, ReadyRecipients: 3, LeasedRecipients: 2, RetryWaitRecipients: 1,
		EarliestLeaseExpiresMs: 1740000030000, DeadLetters: 4, OldestDeadLetteredMs: 1739000000000, NewestDeadLetteredMs: 1739900000000,
		WebhookEndpoints: 5, AdminSessions: 1, AuditLength: 250, UsedMemoryBytes: 1048576, MaxMemoryBytes: 268435456}}
	svc, err := NewService(ServiceDeps{Repo: newFakeRepo(), Operations: ops, Readiness: fakeReadiness{false, false, "in_progress"},
		GrafanaURL: "https://grafana.example/d/hookrelay", Now: func() time.Time { return time.UnixMilli(1740000000000) },
		Catalog: fakeCatalog{}, Audit: &fakeAudit{}, AdminSecret: "admin-secret-value-016", Gen: fixedGen{}})
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/operations/summary", "admin-secret-value-016", "")
	want := `{"generated_ms":1740000000000,` +
		`"readiness":{"ready":false,"accepting_webhooks":false,"startup_reconciliation":"in_progress"},` +
		`"valkey":{"used_memory_bytes":1048576,"maxmemory_bytes":268435456},` +
		`"queues":{"queued_messages":12,"ready_recipients":3,"leased_recipients":2,"retry_wait_recipients":1,"blocked_recipients":0,` +
		`"earliest_lease_expires_ms":1740000030000},` +
		`"dead_letters":{"count":4,"oldest_dead_lettered_ms":1739000000000,"newest_dead_lettered_ms":1739900000000},` +
		`"webhook_endpoints":{"count":5},"admin_sessions":{"indexed":1},"audit":{"length":250},` +
		`"links":{"grafana_url":"https://grafana.example/d/hookrelay"}}` + "\n"
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Errorf("summary = %d\n%s\nwant\n%s", rec.Code, rec.Body.String(), want)
	}

	ops.snap = OperationsSnapshot{}
	svc2, _ := NewService(ServiceDeps{Repo: newFakeRepo(), Operations: ops, Now: func() time.Time { return time.UnixMilli(1) },
		Catalog: fakeCatalog{}, Audit: &fakeAudit{}, AdminSecret: "admin-secret-value-016", Gen: fixedGen{}})
	rec = doJSON(t, Handler(svc2), http.MethodGet, "/admin/v1/operations/summary", "admin-secret-value-016", "")
	want = `{"generated_ms":1,"valkey":{"used_memory_bytes":0,"maxmemory_bytes":0},` +
		`"queues":{"queued_messages":0,"ready_recipients":0,"leased_recipients":0,"retry_wait_recipients":0,"blocked_recipients":0},` +
		`"dead_letters":{"count":0},"webhook_endpoints":{"count":0},"admin_sessions":{"indexed":0},"audit":{"length":0},"links":{}}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("empty summary = %s", rec.Body.String())
	}

	for _, e := range []error{ErrStoredWrongType, context.DeadlineExceeded} {
		ops.err = e
		if rec := doJSON(t, h, http.MethodGet, "/admin/v1/operations/summary", "admin-secret-value-016", ""); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%v: %d", e, rec.Code)
		}
	}
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/operations/summary", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated = %d", rec.Code)
	}
}

package administration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

const dlqID = "01950000-0000-7000-8000-000000000001"

type replayCall struct {
	messageID, resolution, eventID, requestID string
}

type fakeDeadLetters struct {
	items     []DeadLetter
	listAfter []*DeadLetterCursor
	listLimit []int
	record    *DeadLetter
	replay    Replay
	replays   []replayCall
	payload   Payload
	views     []replayCall // messageID, actor (as resolution), eventID, requestID
	deletion  DeleteDLQ
	deletes   []deleteCall
}

type deleteCall struct {
	messageID string
	expected  *DeadLetterVersion
	actor     string
	eventID   string
}

func (f *fakeDeadLetters) ListDeadLetters(_ context.Context, limit int, after *DeadLetterCursor) ([]DeadLetter, error) {
	f.listLimit = append(f.listLimit, limit)
	f.listAfter = append(f.listAfter, after)
	return f.items[:min(limit, len(f.items))], nil
}

func (f *fakeDeadLetters) GetDeadLetter(_ context.Context, messageID string) (*DeadLetter, error) {
	if f.record == nil || f.record.MessageID != messageID {
		return nil, nil
	}
	return f.record, nil
}

func (f *fakeDeadLetters) ReplayDeadLetter(_ context.Context, messageID, resolution, eventID, requestID string) Replay {
	f.replays = append(f.replays, replayCall{messageID, resolution, eventID, requestID})
	return f.replay
}

func (f *fakeDeadLetters) ViewPayload(_ context.Context, messageID, actor, eventID, requestID string) Payload {
	f.views = append(f.views, replayCall{messageID, actor, eventID, requestID})
	return f.payload
}

func (f *fakeDeadLetters) DeleteDeadLetter(_ context.Context, messageID string, expected *DeadLetterVersion, actor, eventID, _ string) DeleteDLQ {
	f.deletes = append(f.deletes, deleteCall{messageID, expected, actor, eventID})
	return f.deletion
}

func deadLetterService(t *testing.T, f *fakeDeadLetters) (http.Handler, *bytes.Buffer, *prometheus.Registry) {
	t.Helper()
	logs := &bytes.Buffer{}
	reg := prometheus.NewRegistry()
	svc, err := NewService(ServiceDeps{
		Repo: newFakeRepo(), DeadLetters: f, Catalog: fakeCatalog{}, Audit: &fakeAudit{},
		AdminSecret: "admin-secret-value-016", Gen: fixedGen{},
		Logger: observability.NewTestLogger("info", logs), Registerer: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return Handler(svc), logs, reg
}

// TestListDeadLettersContract pins the list route: safe metadata with
// structured Recipient fields, newest-first cursors over the index score
// and member, and skipping a member without its record.
func TestListDeadLettersContract(t *testing.T) {
	f := &fakeDeadLetters{items: []DeadLetter{
		{MessageID: "m9", DeadLetteredMs: 900, RecordMissing: true},
		{MessageID: dlqID, RecipientIdentity: "telegram:42:chat:-100", DeadLetteredMs: 800, Reason: "nack_exhausted", DeliveryCycle: 1},
		{MessageID: "m7", RecipientIdentity: "telegram:42:user:7", DeadLetteredMs: 700, Reason: "expiry_exhausted", DeliveryCycle: 2},
	}}
	h, _, _ := deadLetterService(t, f)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters?limit=2", "admin-secret-value-016", "")
	var body struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &body) != nil {
		t.Fatalf("list = %d %s", rec.Code, rec.Body.String())
	}
	if f.listLimit[0] != 3 || f.listAfter[0] != nil {
		t.Errorf("list call limit %d after %v", f.listLimit[0], f.listAfter[0])
	}
	if len(body.Items) != 1 || body.Next == "" {
		t.Fatalf("body = %s", rec.Body.String())
	}
	item, _ := json.Marshal(body.Items[0])
	if string(item) != `{"dead_letter_reason":"nack_exhausted","dead_lettered_ms":800,"delivery_cycle":1,"message_id":"`+dlqID+
		`","recipient":{"bot_id":"42","bot_platform":"telegram","chat_id":"-100","scope":"chat"}}` {
		t.Errorf("item = %s", item)
	}
	if strings.Contains(rec.Body.String(), "telegram:42:") {
		t.Error("internal Recipient identity exposed")
	}
	rec = doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters?limit=2&cursor="+body.Next, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK || f.listAfter[1] == nil || f.listAfter[1].Score != 800 || f.listAfter[1].Member != dlqID {
		t.Errorf("continuation = %d, after %+v", rec.Code, f.listAfter[1])
	}

	for name, q := range map[string]string{"limit zero": "limit=0", "limit too large": "limit=201", "bad cursor": "cursor=!!"} {
		if rec := doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters?"+q, "admin-secret-value-016", ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

// TestGetDeadLetterContract pins the safe get with attempt history and
// 404 dead_letter_not_found for an absent or malformed identifier.
func TestGetDeadLetterContract(t *testing.T) {
	f := &fakeDeadLetters{record: &DeadLetter{
		MessageID: dlqID, RecipientIdentity: "telegram:42:bot", DeadLetteredMs: 800, Reason: "nack_exhausted", DeliveryCycle: 3,
		Attempts: []AttemptEntry{{DeliveryCycle: 3, Attempt: 1, ClaimedMs: 1, LeaseExpiresMs: 2, CompletedMs: 2, Outcome: "nack", ReasonCode: "boom"}},
		Archived: &ArchivedCycles{ArchivedCycles: 1, ArchivedAttempts: 4, FirstArchivedMs: 1, LastArchivedMs: 2},
	}}
	h, _, _ := deadLetterService(t, f)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters/"+dlqID, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"reason_code":"boom"`) ||
		!strings.Contains(rec.Body.String(), `"archived_cycles_summary":{"archived_cycles":1`) || !strings.Contains(rec.Body.String(), `"scope":"bot"`) {
		t.Errorf("get = %d %s", rec.Code, rec.Body.String())
	}
	for _, id := range []string{"01950000-0000-7000-8000-000000000002", "not-a-uuid"} {
		rec := doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters/"+id, "admin-secret-value-016", "")
		if rec.Code != http.StatusNotFound || !strings.Contains(rec.Body.String(), "dead_letter_not_found") {
			t.Errorf("%s: %d %s", id, rec.Code, rec.Body.String())
		}
	}
}

// TestReplayDeadLetterContract pins the replayed response, the audit and
// feature events, the replay metric, the default reject resolution, and
// the error mapping including the uncertain outcome.
func TestReplayDeadLetterContract(t *testing.T) {
	f := &fakeDeadLetters{replay: Replay{Result: ReplayReplayed, DeliveryCycle: 2, QueuePosition: "after_active_head", ReplayedMs: 1740000000000,
		DeduplicationResolution: "not_conflicting", RecipientIdentity: "telegram:42:chat:-100"}}
	h, logs, reg := deadLetterService(t, f)
	rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "admin-secret-value-016", "")
	want := `{"status":"replayed","message_id":"` + dlqID + `","delivery_cycle":2,"queue_position":"after_active_head",` +
		`"replayed_ms":1740000000000,"deduplication_resolution":"not_conflicting"}` + "\n"
	if rec.Code != http.StatusOK || rec.Body.String() != want {
		t.Fatalf("replay = %d %s", rec.Code, rec.Body.String())
	}
	if c := f.replays[0]; c.messageID != dlqID || c.resolution != "reject" || c.eventID == "" || c.requestID == "" {
		t.Errorf("replay call = %+v", c)
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "dead_letter_replayed", "outcome": "success"}); got != 1 {
		t.Errorf("audit events = %v", got)
	}
	if got := counterValue(t, reg, "hookrelay_dead_letter_replays_total", map[string]string{"outcome": "replayed"}); got != 1 {
		t.Errorf("replays = %v", got)
	}
	if !strings.Contains(logs.String(), `"event":"delivery_replayed"`) || !strings.Contains(logs.String(), `"chat_id":"-100"`) ||
		!strings.Contains(logs.String(), `"queue_position":"after_active_head"`) {
		t.Errorf("logs = %s", logs.String())
	}

	rec = doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "admin-secret-value-016", `{"deduplication_conflict_resolution":"keep_current"}`)
	if rec.Code != http.StatusOK || f.replays[1].resolution != "keep_current" {
		t.Errorf("keep_current = %d, call %+v", rec.Code, f.replays[1])
	}
	for name, body := range map[string]string{
		"unknown resolution": `{"deduplication_conflict_resolution":"repoint"}`,
		"unknown field":      `{"resolution":"keep_current"}`,
	} {
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "admin-secret-value-016", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	if len(f.replays) != 2 {
		t.Error("an invalid request reached the store")
	}

	for result, want := range map[ReplayResult]struct {
		status  int
		code    string
		outcome string
	}{
		ReplayNotFound:              {404, "dead_letter_not_found", "not_found"},
		ReplayRecipientBlocked:      {409, "recipient_blocked", "recipient_blocked"},
		ReplayDeduplicationConflict: {409, "deduplication_conflict", "deduplication_conflict"},
		ReplayMessageMissing:        {503, "dependency_unavailable", "message_missing"},
		ReplayWrongType:             {503, "dependency_unavailable", "failed"},
		ReplayUnavailable:           {503, "dependency_unavailable", "failed"},
		ReplayUncertain:             {503, "dependency_unavailable", "uncertain"},
	} {
		f := &fakeDeadLetters{replay: Replay{Result: result}}
		h, _, reg := deadLetterService(t, f)
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "admin-secret-value-016", "")
		if rec.Code != want.status || !strings.Contains(rec.Body.String(), `"code":"`+want.code+`"`) {
			t.Errorf("%s: %d %s", result, rec.Code, rec.Body.String())
		}
		if got := counterValue(t, reg, "hookrelay_dead_letter_replays_total", map[string]string{"outcome": want.outcome}); got != 1 {
			t.Errorf("%s: replays{outcome=%s} = %v", result, want.outcome, got)
		}
		if result == ReplayUncertain && !strings.Contains(rec.Body.String(), "uncertain") {
			t.Errorf("uncertain replay not reported as uncertain: %s", rec.Body.String())
		}
	}

	f = &fakeDeadLetters{}
	h, _, _ = deadLetterService(t, f)
	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/not-a-uuid/replay", "admin-secret-value-016", ""); rec.Code != http.StatusNotFound || len(f.replays) != 0 {
		t.Errorf("malformed id = %d, calls %d", rec.Code, len(f.replays))
	}
	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "wrong-secret", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated replay = %d", rec.Code)
	}
}

// TestDeadLetterPayloadContract pins the payload route: the stored
// Canonical Message embedded verbatim, the actor passed to the audited
// store operation, no-store caching, the audit and feature events without
// payload, the metric, and the error mapping that never discloses.
func TestDeadLetterPayloadContract(t *testing.T) {
	msg := `{"message_id":"` + dlqID + `","payload":{"text":"secret-ish"}}`
	f := &fakeDeadLetters{payload: Payload{Result: PayloadDisclosed, Message: json.RawMessage(msg), DeliveryCycle: 2,
		DeadLetteredMs: 1740000000000, RecipientIdentity: "telegram:42:chat:-100"}}
	h, logs, reg := deadLetterService(t, f)
	for _, body := range []string{"", "{}"} {
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/payload", "admin-secret-value-016", body)
		want := `{"message_id":"` + dlqID + `","delivery_cycle":2,"dead_lettered_ms":1740000000000,"message":` + msg + "}\n"
		if rec.Code != http.StatusOK || rec.Body.String() != want {
			t.Fatalf("payload (body %q) = %d %s", body, rec.Code, rec.Body.String())
		}
		if rec.Header().Get("Cache-Control") != "no-store" {
			t.Errorf("Cache-Control = %q", rec.Header().Get("Cache-Control"))
		}
	}
	if c := f.views[0]; c.messageID != dlqID || c.resolution != "admin_bearer" || c.eventID == "" || c.requestID == "" {
		t.Errorf("view call = %+v", c)
	}
	if got := counterValue(t, reg, "hookrelay_dead_letter_payload_views_total", map[string]string{"outcome": "disclosed"}); got != 2 {
		t.Errorf("views = %v", got)
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "dead_letter_payload_viewed", "outcome": "success"}); got != 2 {
		t.Errorf("audit events = %v", got)
	}
	if !strings.Contains(logs.String(), `"event":"dead_letter_payload_viewed"`) || !strings.Contains(logs.String(), `"recipient_scope":"chat"`) {
		t.Errorf("logs = %s", logs.String())
	}
	if strings.Contains(logs.String(), "secret-ish") {
		t.Error("payload logged")
	}

	for name, body := range map[string]string{"unknown field": `{"x":1}`, "not an object": `[]`} {
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/payload", "admin-secret-value-016", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	if len(f.views) != 2 {
		t.Error("an invalid request reached the store")
	}

	for result, want := range map[PayloadResult]struct {
		status  int
		code    string
		outcome string
	}{
		PayloadNotFound:       {404, "dead_letter_not_found", "not_found"},
		PayloadMessageMissing: {503, "dependency_unavailable", "unavailable"},
		PayloadWrongType:      {503, "dependency_unavailable", "unavailable"},
		PayloadUnavailable:    {503, "dependency_unavailable", "unavailable"},
	} {
		f := &fakeDeadLetters{payload: Payload{Result: result, Message: json.RawMessage(msg)}}
		h, _, reg := deadLetterService(t, f)
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/payload", "admin-secret-value-016", "")
		if rec.Code != want.status || !strings.Contains(rec.Body.String(), `"code":"`+want.code+`"`) || strings.Contains(rec.Body.String(), "secret-ish") {
			t.Errorf("%s: %d %s", result, rec.Code, rec.Body.String())
		}
		if got := counterValue(t, reg, "hookrelay_dead_letter_payload_views_total", map[string]string{"outcome": want.outcome}); got != 1 {
			t.Errorf("%s: views{outcome=%s} = %v", result, want.outcome, got)
		}
	}

	f = &fakeDeadLetters{}
	h, _, _ = deadLetterService(t, f)
	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/not-a-uuid/payload", "admin-secret-value-016", ""); rec.Code != http.StatusNotFound || len(f.views) != 0 {
		t.Errorf("malformed id = %d, calls %d", rec.Code, len(f.views))
	}
	if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/payload", "wrong-secret", ""); rec.Code != http.StatusUnauthorized || len(f.views) != 0 {
		t.Errorf("unauthenticated payload = %d", rec.Code)
	}
}

func doDLQDelete(t *testing.T, h http.Handler, path string, ifMatch ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	req.Header.Set("Authorization", "Bearer admin-secret-value-016")
	for _, v := range ifMatch {
		req.Header.Add("If-Match", v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestDeleteDeadLetterContract pins the ETag on the dead-letter read, the
// strong If-Match passed to the audited store deletion, the audit and
// feature events, the deletion metric, 204 for absence without audit, and
// the error mapping.
func TestDeleteDeadLetterContract(t *testing.T) {
	record := &DeadLetter{MessageID: dlqID, RecipientIdentity: "telegram:42:chat:-100", DeadLetteredMs: 1740000000000, Reason: "nack_exhausted", DeliveryCycle: 2}
	f := &fakeDeadLetters{record: record, deletion: DeleteDLQ{Result: DeleteDLQDeleted, DeletedMs: 1740000000001,
		RecipientIdentity: "telegram:42:chat:-100", Reason: "nack_exhausted"}}
	h, logs, reg := deadLetterService(t, f)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/dead-letters/"+dlqID, "admin-secret-value-016", "")
	tag := rec.Header().Get("ETag")
	if tag != `"2:1740000000000"` {
		t.Fatalf("ETag = %q", tag)
	}
	rec = doDLQDelete(t, h, "/admin/v1/dead-letters/"+dlqID, tag)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("delete = %d %s", rec.Code, rec.Body.String())
	}
	if c := f.deletes[0]; c.messageID != dlqID || c.expected == nil || *c.expected != (DeadLetterVersion{2, 1740000000000}) ||
		c.actor != "admin_bearer" || c.eventID == "" {
		t.Errorf("delete call = %+v", c)
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "dead_letter_deleted", "outcome": "success"}); got != 1 {
		t.Errorf("audit events = %v", got)
	}
	if got := counterValue(t, reg, "hookrelay_dead_letter_deletions_total", map[string]string{"outcome": "deleted"}); got != 1 {
		t.Errorf("deletions = %v", got)
	}
	if !strings.Contains(logs.String(), `"event":"dead_letter_deleted"`) || !strings.Contains(logs.String(), `"chat_id":"-100"`) {
		t.Errorf("logs = %s", logs.String())
	}

	// The store decides absence and the missing precondition.
	for result, want := range map[DeleteDLQResult]struct {
		status  int
		code    string
		outcome string
	}{
		DeleteDLQAbsent:               {204, "", "absent"},
		DeleteDLQPreconditionRequired: {428, "precondition_required", "refused"},
		DeleteDLQPreconditionFailed:   {412, "precondition_failed", "refused"},
		DeleteDLQRecipientBlocked:     {409, "recipient_blocked", "refused"},
		DeleteDLQWrongType:            {503, "dependency_unavailable", "unavailable"},
		DeleteDLQUnavailable:          {503, "dependency_unavailable", "unavailable"},
		DeleteDLQUncertain:            {503, "dependency_unavailable", "unavailable"},
	} {
		f := &fakeDeadLetters{deletion: DeleteDLQ{Result: result}}
		h, _, reg := deadLetterService(t, f)
		rec := doDLQDelete(t, h, "/admin/v1/dead-letters/"+dlqID)
		if rec.Code != want.status || (want.code != "" && !strings.Contains(rec.Body.String(), `"code":"`+want.code+`"`)) {
			t.Errorf("%s: %d %s", result, rec.Code, rec.Body.String())
		}
		if len(f.deletes) != 1 || f.deletes[0].expected != nil {
			t.Errorf("%s: calls %+v", result, f.deletes)
		}
		if got := counterValue(t, reg, "hookrelay_dead_letter_deletions_total", map[string]string{"outcome": want.outcome}); got != 1 {
			t.Errorf("%s: deletions{outcome=%s} = %v", result, want.outcome, got)
		}
		if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "dead_letter_deleted", "outcome": "success"}); got != 0 {
			t.Errorf("%s: audit counted", result)
		}
		if result == DeleteDLQUncertain && !strings.Contains(rec.Body.String(), "uncertain") {
			t.Errorf("uncertain deletion not reported as uncertain: %s", rec.Body.String())
		}
	}

	// A malformed tag is 400 for an existing entry and 204 for an absent
	// one; an identifier that cannot name a dead letter is absent.
	for _, bad := range [][]string{{`W/"2:1740000000000"`}, {"*"}, {`"2:1740000000000", "3:1"`}, {`"2:x"`}, {`"2:1"`, `"2:1"`}} {
		f := &fakeDeadLetters{record: record}
		h, _, _ := deadLetterService(t, f)
		if rec := doDLQDelete(t, h, "/admin/v1/dead-letters/"+dlqID, bad...); rec.Code != http.StatusBadRequest || len(f.deletes) != 0 {
			t.Errorf("If-Match %q on an existing entry = %d, calls %d", bad, rec.Code, len(f.deletes))
		}
		f = &fakeDeadLetters{deletion: DeleteDLQ{Result: DeleteDLQAbsent}}
		h, _, _ = deadLetterService(t, f)
		if rec := doDLQDelete(t, h, "/admin/v1/dead-letters/"+dlqID, bad...); rec.Code != http.StatusNoContent || len(f.deletes) != 1 || f.deletes[0].expected != nil {
			t.Errorf("If-Match %q on an absent entry = %d, calls %+v", bad, rec.Code, f.deletes)
		}
	}
	f = &fakeDeadLetters{}
	h, _, reg = deadLetterService(t, f)
	if rec := doDLQDelete(t, h, "/admin/v1/dead-letters/not-a-uuid", "*"); rec.Code != http.StatusNoContent || len(f.deletes) != 0 {
		t.Errorf("malformed id = %d, calls %d", rec.Code, len(f.deletes))
	}
	if got := counterValue(t, reg, "hookrelay_dead_letter_deletions_total", map[string]string{"outcome": "absent"}); got != 1 {
		t.Errorf("malformed id deletions = %v", got)
	}
	req := httptest.NewRequest(http.MethodDelete, "/admin/v1/dead-letters/"+dlqID, nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || len(f.deletes) != 0 {
		t.Errorf("unauthenticated delete = %d", rec.Code)
	}
}

type fakeAuditLog struct {
	items   []AuditEntry
	err     error
	befores []string
	limits  []int
}

func (f *fakeAuditLog) ListAudit(_ context.Context, limit int, before string) ([]AuditEntry, error) {
	f.limits = append(f.limits, limit)
	f.befores = append(f.befores, before)
	return f.items[:min(limit, len(f.items))], f.err
}

// TestListAuditContract pins the audit route: newest-first pages with an
// opaque stream-ID cursor, the empty list, the limit and cursor checks,
// and a wrong-type Stream as 503.
func TestListAuditContract(t *testing.T) {
	log := &fakeAuditLog{items: []AuditEntry{
		{StreamID: "3-0", EventID: "e3", TimestampMs: 3, Actor: "admin_bearer", Operation: "dead_letter_deleted", Target: "m", Outcome: "success"},
		{StreamID: "2-0", EventID: "e2"},
		{StreamID: "1-0", EventID: "e1"},
	}}
	svc, err := NewService(ServiceDeps{Repo: newFakeRepo(), AuditLog: log, Catalog: fakeCatalog{}, Audit: &fakeAudit{},
		AdminSecret: "admin-secret-value-016", Gen: fixedGen{}})
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/audit?limit=2", "admin-secret-value-016", "")
	var page struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &page) != nil || len(page.Items) != 2 || page.Next == "" {
		t.Fatalf("list = %d %s", rec.Code, rec.Body.String())
	}
	first, _ := json.Marshal(page.Items[0])
	if string(first) != `{"actor":"admin_bearer","event_id":"e3","operation":"dead_letter_deleted","outcome":"success","stream_id":"3-0","target":"m","timestamp_ms":3}` {
		t.Errorf("item = %s", first)
	}
	if log.limits[0] != 3 || log.befores[0] != "" {
		t.Errorf("call = %d %q", log.limits[0], log.befores[0])
	}
	rec = doJSON(t, h, http.MethodGet, "/admin/v1/audit?limit=2&cursor="+page.Next, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK || log.befores[1] != "2-0" {
		t.Errorf("continuation = %d, before %q", rec.Code, log.befores[1])
	}
	rec = doJSON(t, h, http.MethodGet, "/admin/v1/audit", "admin-secret-value-016", "")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), "next_cursor") {
		t.Errorf("last page = %s", rec.Body.String())
	}
	for name, q := range map[string]string{
		"limit zero": "limit=0", "limit too large": "limit=201", "bad cursor": "cursor=!!",
		"cursor without id": "cursor=" + encodeCursor(AuditCursor{ID: "x"}),
	} {
		if rec := doJSON(t, h, http.MethodGet, "/admin/v1/audit?"+q, "admin-secret-value-016", ""); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	log.items, log.err = nil, nil
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/audit", "admin-secret-value-016", ""); rec.Code != http.StatusOK ||
		rec.Body.String() != `{"items":[]}`+"\n" {
		t.Errorf("empty = %s", rec.Body.String())
	}
	log.err = ErrStoredWrongType
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/audit", "admin-secret-value-016", ""); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("wrong type = %d", rec.Code)
	}
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/audit", "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated = %d", rec.Code)
	}
}

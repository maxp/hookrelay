package administration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
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

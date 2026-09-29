package administration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

type fakeRecipients struct {
	items      []RecipientStateItem
	listCalls  []listCall
	inspection BlockInspection
	inspected  []string
	clear      ClearBlockResult
	clearInfo  string
	clears     []clearCall
}

type listCall struct {
	status RecipientStatus
	limit  int
	after  *RecipientCursor
}

type clearCall struct {
	rid, reason, eventID, requestID string
	detected                        int64
}

func (f *fakeRecipients) ListRecipientStates(_ context.Context, status RecipientStatus, limit int, after *RecipientCursor) ([]RecipientStateItem, error) {
	f.listCalls = append(f.listCalls, listCall{status, limit, after})
	return f.items[:min(limit, len(f.items))], nil
}

func (f *fakeRecipients) InspectBlock(_ context.Context, rid string) (BlockInspection, error) {
	f.inspected = append(f.inspected, rid)
	return f.inspection, nil
}

func (f *fakeRecipients) ClearBlock(_ context.Context, rid string, detected int64, reason, eventID, requestID string) (ClearBlockResult, string) {
	f.clears = append(f.clears, clearCall{rid, reason, eventID, requestID, detected})
	return f.clear, f.clearInfo
}

func recipientService(t *testing.T, f *fakeRecipients) (http.Handler, *bytes.Buffer, *prometheus.Registry) {
	t.Helper()
	logs := &bytes.Buffer{}
	reg := prometheus.NewRegistry()
	svc, err := NewService(ServiceDeps{
		Repo: newFakeRepo(), Recipients: f, Catalog: fakeCatalog{}, Audit: &fakeAudit{},
		AdminSecret: "admin-secret-value-016", Gen: fixedGen{},
		Logger: observability.NewTestLogger("info", logs), Registerer: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	return Handler(svc), logs, reg
}

const chatRecipient = `{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-100"}`

// TestListRecipientStatesContract pins the list route: the status filter
// maps to its index, structured Recipient fields replace the internal
// identity, the per-status score field, the next cursor when more remain,
// and cursor continuation.
func TestListRecipientStatesContract(t *testing.T) {
	f := &fakeRecipients{items: []RecipientStateItem{
		{RecipientIdentity: "telegram:42:chat:-100", Score: 1740000000000},
		{RecipientIdentity: "telegram:42:user:7", Score: 1740000000500},
		{RecipientIdentity: "telegram:42:bot", Score: 1740000000900},
	}}
	h, _, _ := recipientService(t, f)
	rec := doJSON(t, h, http.MethodGet, "/admin/v1/recipient-states?status=leased&limit=2", "admin-secret-value-016", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list = %d %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if f.listCalls[0].status != StatusLeased || f.listCalls[0].limit != 3 || f.listCalls[0].after != nil {
		t.Errorf("list call = %+v, want leased with limit+1 and no cursor", f.listCalls[0])
	}
	if len(body.Items) != 2 || body.Next == "" {
		t.Fatalf("body = %s", rec.Body.String())
	}
	first, _ := json.Marshal(body.Items[0])
	if string(first) != `{"lease_expires_ms":1740000000000,"recipient":{"bot_id":"42","bot_platform":"telegram","chat_id":"-100","scope":"chat"},"status":"leased"}` {
		t.Errorf("item = %s", first)
	}
	if strings.Contains(rec.Body.String(), "telegram:42:") {
		t.Error("internal Recipient identity exposed")
	}

	rec = doJSON(t, h, http.MethodGet, "/admin/v1/recipient-states?status=leased&limit=2&cursor="+body.Next, "admin-secret-value-016", "")
	if rec.Code != http.StatusOK || f.listCalls[1].after == nil || f.listCalls[1].after.Score != 1740000000500 ||
		f.listCalls[1].after.Member != "telegram:42:user:7" {
		t.Errorf("continuation = %d, call %+v", rec.Code, f.listCalls[1])
	}

	for status, field := range map[string]string{"ready": "ready_sequence", "retry_wait": "retry_at_ms", "blocked": "detected_ms"} {
		f := &fakeRecipients{items: []RecipientStateItem{{RecipientIdentity: "telegram:42:relay", Score: 9, DetectedMs: 9, ReasonCode: "head_state_missing"}}}
		h, _, _ := recipientService(t, f)
		rec := doJSON(t, h, http.MethodGet, "/admin/v1/recipient-states?status="+status, "admin-secret-value-016", "")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"`+field+`":9`) || strings.Contains(rec.Body.String(), "next_cursor") ||
			f.listCalls[0].limit != 51 {
			t.Errorf("%s: %d %s (limit %d)", status, rec.Code, rec.Body.String(), f.listCalls[0].limit)
		}
		if status == "blocked" != strings.Contains(rec.Body.String(), `"reason_code":"head_state_missing"`) {
			t.Errorf("%s: reason_code presence wrong: %s", status, rec.Body.String())
		}
	}
}

// TestListRecipientStatesValidation pins the bounded query checks.
func TestListRecipientStatesValidation(t *testing.T) {
	h, _, _ := recipientService(t, &fakeRecipients{})
	for name, tc := range map[string]struct {
		query string
		code  string
	}{
		"missing status":   {"", "invalid_request"},
		"unknown status":   {"status=dead", "invalid_request"},
		"limit zero":       {"status=ready&limit=0", "invalid_request"},
		"limit too large":  {"status=ready&limit=201", "invalid_request"},
		"limit not number": {"status=ready&limit=x", "invalid_request"},
		"bad cursor":       {"status=ready&cursor=!!", "invalid_cursor"},
		"cursor not json":  {"status=ready&cursor=" + base64.RawURLEncoding.EncodeToString([]byte("x")), "invalid_cursor"},
	} {
		rec := doJSON(t, h, http.MethodGet, "/admin/v1/recipient-states?"+tc.query, "admin-secret-value-016", "")
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), `"code":"`+tc.code+`"`) {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

// TestInspectBlockContract pins the inspection body: marker, bounded head,
// memberships, and violated invariants, for the structured Recipient.
func TestInspectBlockContract(t *testing.T) {
	f := &fakeRecipients{inspection: BlockInspection{
		Marker:             &BlockMarker{DetectedMs: 55, ReasonCode: "head_message_missing"},
		QueueLength:        2,
		Head:               &BlockHead{MessageID: "m1", Status: "leased", DeliveryCycle: 1, Attempt: 2, LeaseExpiresMs: 99},
		HeadMessagePresent: true,
		Memberships:        map[string]bool{"ready": false, "leases": false, "retries": false, "blocked": true},
		ViolatedInvariants: []string{"head_message_missing"},
	}}
	h, _, _ := recipientService(t, f)
	rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/inspect", "admin-secret-value-016", `{"recipient":`+chatRecipient+`}`)
	if rec.Code != http.StatusOK || len(f.inspected) != 1 || f.inspected[0] != "telegram:42:chat:-100" {
		t.Fatalf("inspect = %d %s (%v)", rec.Code, rec.Body.String(), f.inspected)
	}
	want := `{"marker":{"detected_ms":55,"reason_code":"head_message_missing"},"queue_length":2,` +
		`"head":{"message_id":"m1","status":"leased","delivery_cycle":1,"attempt":2,"lease_expires_ms":99},` +
		`"head_message_present":true,"memberships":{"blocked":true,"leases":false,"ready":false,"retries":false},` +
		`"violated_invariants":["head_message_missing"]}` + "\n"
	if rec.Body.String() != want {
		t.Errorf("body = %s\nwant %s", rec.Body.String(), want)
	}

	f.inspection = BlockInspection{Memberships: map[string]bool{}}
	rec = doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/inspect", "admin-secret-value-016", `{"recipient":`+chatRecipient+`}`)
	if !strings.Contains(rec.Body.String(), `"marker":null`) || !strings.Contains(rec.Body.String(), `"head":null`) ||
		!strings.Contains(rec.Body.String(), `"violated_invariants":[]`) {
		t.Errorf("empty inspection = %s", rec.Body.String())
	}

	for name, body := range map[string]string{
		"missing recipient":   `{}`,
		"chat without id":     `{"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42"}}`,
		"user with chat id":   `{"recipient":{"scope":"user","bot_platform":"telegram","bot_id":"42","chat_id":"1","user_id":"2"}}`,
		"unknown scope":       `{"recipient":{"scope":"group","bot_platform":"telegram","bot_id":"42"}}`,
		"colon in identifier": `{"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"a:b"}}`,
	} {
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/inspect", "admin-secret-value-016", body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
}

// TestClearBlockContract pins 204 with the audit event and feature event,
// the error mapping, and the required preconditions.
func TestClearBlockContract(t *testing.T) {
	body := `{"recipient":` + chatRecipient + `,"expected_detected_ms":55,"expected_reason_code":"head_message_missing"}`
	f := &fakeRecipients{clear: ClearCleared, clearInfo: "leases"}
	h, logs, reg := recipientService(t, f)
	rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/clear", "admin-secret-value-016", body)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("clear = %d %s", rec.Code, rec.Body.String())
	}
	c := f.clears[0]
	if c.rid != "telegram:42:chat:-100" || c.detected != 55 || c.reason != "head_message_missing" || c.eventID == "" || c.requestID == "" {
		t.Errorf("clear call = %+v", c)
	}
	if got := counterValue(t, reg, "hookrelay_audit_events_total", map[string]string{"operation": "recipient_block_cleared", "outcome": "success"}); got != 1 {
		t.Errorf("audit events = %v", got)
	}
	if !strings.Contains(logs.String(), `"event":"recipient_block_cleared"`) || !strings.Contains(logs.String(), `"restored_index":"leases"`) {
		t.Errorf("logs = %s", logs.String())
	}

	for result, want := range map[ClearBlockResult]struct {
		status int
		code   string
	}{
		ClearNotFound:           {404, "recipient_block_not_found"},
		ClearPreconditionFailed: {412, "precondition_failed"},
		ClearAmbiguous:          {409, "recipient_state_ambiguous"},
		ClearWrongType:          {503, "dependency_unavailable"},
		ClearUnavailable:        {503, "dependency_unavailable"},
		ClearUncertain:          {503, "dependency_unavailable"},
	} {
		f := &fakeRecipients{clear: result, clearInfo: "head_message_missing"}
		h, _, _ := recipientService(t, f)
		rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/clear", "admin-secret-value-016", body)
		if rec.Code != want.status || !strings.Contains(rec.Body.String(), `"code":"`+want.code+`"`) {
			t.Errorf("%s: %d %s", result, rec.Code, rec.Body.String())
		}
		if result == ClearAmbiguous && !strings.Contains(rec.Body.String(), "head_message_missing") {
			t.Errorf("ambiguous without the invariant: %s", rec.Body.String())
		}
		if result == ClearUncertain && !strings.Contains(rec.Body.String(), "uncertain") {
			t.Errorf("uncertain clear not reported as uncertain: %s", rec.Body.String())
		}
	}

	f = &fakeRecipients{}
	h, _, _ = recipientService(t, f)
	for name, b := range map[string]string{
		"no detection time": `{"recipient":` + chatRecipient + `,"expected_reason_code":"x"}`,
		"no reason":         `{"recipient":` + chatRecipient + `,"expected_detected_ms":55}`,
	} {
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/clear", "admin-secret-value-016", b); rec.Code != http.StatusPreconditionRequired ||
			!strings.Contains(rec.Body.String(), "precondition_required") {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	if len(f.clears) != 0 {
		t.Error("clear ran without preconditions")
	}
}

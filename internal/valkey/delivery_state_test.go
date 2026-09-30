package valkey

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
)

// enqueueCanonical accepts a message whose blob carries its structured
// Recipient, as real Canonical Messages do (chat scope, chat -1 or -2).
func enqueueCanonical(t *testing.T, a *Adapter, messageID, rid string) {
	t.Helper()
	r := acceptReq(messageID, "d-"+messageID, "b-"+messageID)
	r.RecipientIdentity = rid
	chat := rid[strings.LastIndexByte(rid, ':')+1:]
	r.MessageJSON = []byte(`{"message_id":"` + messageID + `","received_ms":1740000000000,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"` +
		chat + `"},"platform_event_type":"message","payload":{}}`)
	if res := NewMessageAcceptor(a, testLimits()).Accept(context.Background(), r); res.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("enqueue %s = %+v", messageID, res)
	}
}

func deliveryState(t *testing.T, a *Adapter, messageID string) administration.DeliveryState {
	t.Helper()
	st, err := NewMessageStateStore(a).DeliveryState(context.Background(), messageID)
	if err != nil {
		t.Fatalf("delivery state %s: %v", messageID, err)
	}
	return st
}

func assertState(t *testing.T, a *Adapter, messageID, state string, cycle int64, position string) {
	t.Helper()
	want := administration.DeliveryState{Found: true, State: state, DeliveryCycle: cycle, QueuePosition: position}
	if got := deliveryState(t, a, messageID); got != want {
		t.Errorf("%s: delivery state = %+v, want %+v", messageID, got, want)
	}
}

// TestDeliveryStateClassifiesEveryState walks one message through the
// states the read reports: queued behind the head, queued head, leased,
// retry_wait, dead_lettered, replayed behind an active head in a newer
// cycle, and acknowledged; an unknown message is not found.
func TestDeliveryStateClassifiesEveryState(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueCanonical(t, a, "m0", ridA)
	enqueueCanonical(t, a, "m1", ridA)
	assertState(t, a, "m1", "queued", 1, "behind_head")
	assertState(t, a, "m0", "queued", 1, "head")

	claimNext(t, s, "op-0", "dlv_0")
	assertState(t, a, "m0", "leased", 1, "head")
	ackOK(t, s, "dlv_0")
	assertState(t, a, "m0", "acknowledged", 1, "")
	assertState(t, a, "m1", "queued", 1, "head")

	claimNext(t, s, "op-1", "dlv_1")
	if n := s.Nack(ctx, nackReq("dlv_1", "")); n.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("nack = %+v", n)
	}
	assertState(t, a, "m1", "retry_wait", 1, "head")

	activateRetry(t, a, ridA)
	claimNext(t, s, "op-2", "dlv_2")
	req := nackReq("dlv_2", "")
	req.RetryDelaysMs, req.MaxAttempts = []int64{1000}, 2
	if n := s.Nack(ctx, req); n.Outcome != delivery.NackDeadLettered {
		t.Fatalf("dead-letter = %+v", n)
	}
	assertState(t, a, "m1", "dead_lettered", 1, "")

	enqueueCanonical(t, a, "m2", ridA)
	claimNext(t, s, "op-3", "dlv_3")
	if r := replay(a, "m1", "reject"); r.QueuePosition != "after_active_head" {
		t.Fatalf("replay = %+v", r)
	}
	assertState(t, a, "m1", "queued", 2, "behind_head")
	ackOK(t, s, "dlv_3")
	assertState(t, a, "m1", "queued", 2, "head")

	if st := deliveryState(t, a, "nope"); st != (administration.DeliveryState{}) {
		t.Errorf("unknown message = %+v", st)
	}
}

// TestDeliveryStateInconsistent pins the bounded inconsistency reasons and
// that the read never writes.
func TestDeliveryStateInconsistent(t *testing.T) {
	for reason, setup := range map[string]func(t *testing.T, a *Adapter){
		"queued_delivery_state_missing": func(t *testing.T, a *Adapter) {
			a.testDo(t, "RPUSH", "hr1:a:m2", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}`)
		},
		"queued_delivery_state_invalid": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:mi:m2", "pending_attempt", "1") },
		"message_not_queued":            func(t *testing.T, a *Adapter) { a.testDo(t, "LREM", "hr1:r:"+ridA+":q", "0", "m2") },
		"message_invalid":               func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:m:m2", `{"message_id":"m2"}`) },
		"unsupported_key_type":          func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:dl:m2", "x") },
		"dead_letter_record_invalid":    func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:dl:m2", "delivery_cycle", "0") },
	} {
		t.Run(reason, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueCanonical(t, a, "m1", ridA)
			enqueueCanonical(t, a, "m2", ridA)
			setup(t, a)
			before := snapshot(t, a)
			if st := deliveryState(t, a, "m2"); st.Inconsistent != reason || st.Found {
				t.Errorf("state = %+v, want inconsistent %s", st, reason)
			}
			assertUnchanged(t, a, before, "delivery-state read")
		})
	}

	// Head-state inconsistencies for the queue head.
	a, _ := claimSetup(t)
	enqueueCanonical(t, a, "m1", ridA)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "m9")
	if st := deliveryState(t, a, "m1"); st.Inconsistent != "queue_head_mismatch" {
		t.Errorf("head mismatch = %+v", st)
	}
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "m1", "status", "odd")
	if st := deliveryState(t, a, "m1"); st.Inconsistent != "head_state_missing" {
		t.Errorf("unknown status = %+v", st)
	}
}

// TestDeliveryStateScriptArgumentsAndReload pins argument rejection and the
// EVAL reload after SCRIPT FLUSH.
func TestDeliveryStateScriptArgumentsAndReload(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	for _, args := range [][]string{{"", ridA, "hr1"}, {"m1", ridA, ""}, {"m1", ridA}} {
		if _, err := a.RunScript(ctx, "delivery_state_v1", nil, args); err == nil {
			t.Errorf("args %q accepted", args)
		}
	}
	if _, err := a.RunScript(ctx, "delivery_state_v1", []string{"hr1:ready"}, []string{"m1", ridA, "hr1"}); err == nil {
		t.Error("unexpected key accepted")
	}
	enqueueCanonical(t, a, "m1", ridA)
	a.testDo(t, "SCRIPT", "FLUSH")
	assertState(t, a, "m1", "queued", 1, "head")
}

// TestDeliveryStateRouteOverRealValkey drives the Admin route through the
// real handler, service, and store.
func TestDeliveryStateRouteOverRealValkey(t *testing.T) {
	a, _ := claimSetup(t)
	const id = "01950000-0000-7000-8000-000000000001"
	enqueueCanonical(t, a, id, ridA)
	svc, err := administration.NewService(administration.ServiceDeps{
		Repo: NewEndpointStore(a), Messages: NewMessageStateStore(a), Audit: NewAuditSink(a),
		AdminSecret: "admin-secret-value-016", Gen: gen.Crypto{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := administration.Handler(svc)
	get := func(id string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/messages/"+id+"/delivery-state", nil)
		req.Header.Set("Authorization", "Bearer admin-secret-value-016")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	if w := get(id); w.Code != http.StatusOK ||
		w.Body.String() != `{"message_id":"`+id+`","delivery_cycle":1,"state":"queued","queue_position":"head"}`+"\n" {
		t.Errorf("delivery state = %d %s", w.Code, w.Body.String())
	}
	if w := get("01950000-0000-7000-8000-000000000002"); w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "message_not_found") {
		t.Errorf("unknown = %d %s", w.Code, w.Body.String())
	}
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "odd")
	if w := get(id); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "recipient_state_ambiguous") {
		t.Errorf("inconsistent = %d %s", w.Code, w.Body.String())
	}
}

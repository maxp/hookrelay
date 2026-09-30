package valkey

import (
	"context"
	"strings"
	"testing"
)

func runMessage(t *testing.T, a *Adapter, mode, id, rid, position string) *Result {
	t.Helper()
	res, err := a.RunScript(context.Background(), "reconcile_message_v1",
		[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
		[]string{mode, id, rid, position, "604800000", "hr1"})
	if err != nil {
		t.Fatalf("reconcile message: %v", err)
	}
	return res
}

func field(t *testing.T, res *Result, i int) string {
	t.Helper()
	v, err := res.Fields[i].ToString()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestReconcileMessageLifecycleStatesAndReload(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "queued", ridA)
	if res := runMessage(t, a, "inspect", "queued", ridA, "head"); res.Status != "consistent" || field(t, res, 0) != "queued" {
		t.Fatalf("queued = %+v", res)
	}

	aDead, sDead := claimSetup(t)
	enqueueJSON(t, aDead, "dead", ridB)
	deadLetterOne(t, sDead, "op-dead", "dlv_dead")
	if res := runMessage(t, aDead, "inspect", "dead", "", "none"); res.Status != "consistent" || field(t, res, 0) != "dead_lettered" {
		t.Fatalf("dead-lettered = %s/%s", res.Status, field(t, res, 0))
	}

	a2, s2 := claimSetup(t)
	enqueueJSON(t, a2, "acked", ridA)
	s2.Claim(context.Background(), claimReq("op-ack", "args", "dlv_ack"))
	if got := s2.Ack(context.Background(), ackReq("dlv_ack")); got.Outcome != "acknowledged" {
		t.Fatalf("ack = %+v", got)
	}
	if res := runMessage(t, a2, "inspect", "acked", "", "none"); res.Status != "consistent" || field(t, res, 0) != "acknowledged" {
		t.Fatalf("acknowledged = %+v", res)
	}

	a2.testDo(t, "SCRIPT", "FLUSH")
	if res := runMessage(t, a2, "inspect", "acked", "", "none"); res.Status != "consistent" {
		t.Fatalf("after SCRIPT FLUSH = %+v", res)
	}
}

func TestReconcileMessageBlocksQueueLocalCorruption(t *testing.T) {
	cases := []struct {
		name   string
		poison func(*testing.T, *Adapter)
		reason string
	}{
		{"invalid canonical message", func(t *testing.T, a *Adapter) {
			a.testDo(t, "SET", "hr1:m:m1", `{"message_id":"other","received_ms":1,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-1"},"platform_event_type":"test","payload":{}}`)
		}, "message_invalid"},
		{"metadata invalid", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:mi:m1", "surprise", "x")
		}, "metadata_invalid"},
		{"history invalid", func(t *testing.T, a *Adapter) {
			a.testDo(t, "RPUSH", "hr1:a:m1", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":3,"lease_expires_ms":2,"completed_ms":4,"outcome":"nack"}`)
		}, "history_invalid"},
		{"dedup invalid", func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:d:"+messageDedupDigest("m1"))
			a.testDo(t, "SET", "hr1:d:"+messageDedupDigest("m1"), "poison")
		}, "dedup_record_invalid"},
		{"success overlap", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:success:m1", "recipient_scope", "chat", "bot_platform", "telegram", "received_ms", "1", "acknowledged_ms", "2", "delivery_cycle", "1", "attempt_count", "1")
			a.testDo(t, "PEXPIRE", "hr1:success:m1", "60000")
		}, "success_overlap"},
		{"dead-letter overlap", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:dl:m1", "bot_platform", "telegram", "bot_id", "42", "recipient_scope", "chat", "chat_id", "-1", "recipient_identity", ridA, "dead_lettered_ms", "2", "dead_letter_reason", "nack_exhausted", "delivery_cycle", "1")
		}, "dead_letter_overlap"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			tc.poison(t, a)
			res := runMessage(t, a, "inspect", "m1", ridA, "head")
			if res.Status != "blocked" || field(t, res, 0) != tc.reason {
				t.Fatalf("result = %+v", res)
			}
			if hget(t, a, "hr1:q:"+ridA, "reason_code") != "message_lifecycle_inconsistent" {
				t.Error("bounded marker was not created")
			}
			if _, ok := score(t, a, "hr1:blocked", ridA); !ok {
				t.Error("blocked member missing")
			}
		})
	}
}

func TestReconcileMessageHoldsGlobalCorruptionWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*testing.T, *Adapter, *DeliveryStore)
		id     string
		reason string
	}{
		{"lone blob", func(t *testing.T, a *Adapter, _ *DeliveryStore) {
			enqueueJSON(t, a, "m1", ridA)
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q", "hr1:r:"+ridA+":s", "hr1:ready")
		}, "m1", "message_orphan"},
		{"invalid success", func(t *testing.T, a *Adapter, _ *DeliveryStore) {
			a.testDo(t, "HSET", "hr1:success:m1", "recipient_scope", "chat")
			a.testDo(t, "PEXPIRE", "hr1:success:m1", "60000")
		}, "m1", "success_invalid"},
		{"dead-letter invalid", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			a.testDo(t, "HSET", "hr1:dl:m1", "recipient_identity", ridB)
		}, "m1", "dead_letter_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, s := claimSetup(t)
			tc.setup(t, a, s)
			before := snapshot(t, a)
			res := runMessage(t, a, "inspect", tc.id, "", "none")
			if res.Status != "inconsistent" || field(t, res, 0) != tc.reason {
				t.Fatalf("result = %+v", res)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

func TestReconcileMessageRemovesOnlyOrphanMetadataAndHistory(t *testing.T) {
	a, _ := claimSetup(t)
	a.testDo(t, "HSET", "hr1:mi:orphan", "dedup_identity_digest", messageDedupDigest("orphan"))
	a.testDo(t, "RPUSH", "hr1:a:orphan", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}`)
	res := runMessage(t, a, "inspect", "orphan", "", "none")
	if res.Status != "orphan_records" {
		t.Fatalf("inspect = %+v", res)
	}
	res = runMessage(t, a, "delete_orphans", "orphan", "", "none")
	if res.Status != "removed_orphans" || exists(t, a, "hr1:mi:orphan") || exists(t, a, "hr1:a:orphan") {
		t.Fatalf("delete = %+v", res)
	}
}

func TestReconcileMessageLegacyAndMarkerValidation(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "legacy", ridA)
	a.testDo(t, "DEL", "hr1:mi:legacy")
	if res := runMessage(t, a, "inspect", "legacy", ridA, "head"); res.Status != "legacy" || field(t, res, 0) != "queued" {
		t.Fatalf("legacy = %+v", res)
	}

	a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "bad", "reason_code", "surprise")
	if res := runMessage(t, a, "inspect", "legacy", ridA, "head"); res.Status != "inconsistent" || field(t, res, 0) != "marker_invalid" {
		t.Fatalf("marker = %+v", res)
	}
}

func TestReconcileMessagePendingAndSuccessTTLValidation(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	a.testDo(t, "RPUSH", "hr1:a:m2", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}`)
	if res := runMessage(t, a, "inspect", "m2", ridA, "behind_head"); res.Status != "blocked" || field(t, res, 0) != "pending_state_missing" {
		t.Fatalf("pending missing = %+v", res)
	}

	a2, s2 := claimSetup(t)
	enqueueJSON(t, a2, "acked", ridA)
	s2.Claim(context.Background(), claimReq("op-ack", "args", "dlv_ack"))
	s2.Ack(context.Background(), ackReq("dlv_ack"))
	a2.testDo(t, "PERSIST", "hr1:success:acked")
	if res := runMessage(t, a2, "inspect", "acked", "", "none"); res.Status != "inconsistent" || field(t, res, 0) != "success_invalid" {
		t.Fatalf("success TTL = %+v", res)
	}
}

func TestReconcileScansMessageFamiliesInBatchesAndIsIdempotent(t *testing.T) {
	a, _ := claimSetup(t)
	for i := 0; i < 20; i++ {
		id := "orphan-" + itoa(i)
		a.testDo(t, "HSET", "hr1:mi:"+id, "dedup_identity_digest", messageDedupDigest(id))
	}
	rep, err := a.Reconcile(context.Background(), ReconcileOptions{BatchSize: 3, MessageCheckBound: 10})
	if err != nil || rep.Findings["message_orphans_removed"] != 20 || rep.Hold() != "" {
		t.Fatalf("first = %+v, %v", rep, err)
	}
	again, err := a.Reconcile(context.Background(), ReconcileOptions{BatchSize: 3, MessageCheckBound: 10})
	if err != nil || again.Findings["message_orphans_removed"] != 0 {
		t.Fatalf("second = %+v, %v", again, err)
	}
}

func TestReconcileMessageScriptReturnsNoPayload(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	res := runMessage(t, a, "inspect", "m1", ridA, "head")
	if strings.Contains(strings.Join([]string{res.Status, field(t, res, 0)}, " "), "payload") {
		t.Fatalf("result exposed payload: %+v", res)
	}
}

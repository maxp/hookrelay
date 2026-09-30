package valkey

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

func deleteDLQ(a *Adapter, messageID string, expected *administration.DeadLetterVersion) administration.DeleteDLQ {
	return NewDeadLetterStore(a).DeleteDeadLetter(context.Background(), messageID, expected, "admin_session", "evt-"+messageID, "req-"+messageID)
}

// dlqVersion reads the stored entity version of a dead letter.
func dlqVersion(t *testing.T, a *Adapter, messageID string) *administration.DeadLetterVersion {
	t.Helper()
	m := hgetall(t, a, "hr1:dl:"+messageID)
	cycle, _ := strconv.ParseInt(m["delivery_cycle"], 10, 64)
	dead, _ := strconv.ParseInt(m["dead_lettered_ms"], 10, 64)
	return &administration.DeadLetterVersion{DeliveryCycle: cycle, DeadLetteredMs: dead}
}

// TestDLQDeleteDeletesEverything pins the deleted tuple and every affected
// key: record, blob, metadata, history, and DLQ member go, one audit event
// without payload is appended, and the deduplication record and the
// Recipient's queued messages stay.
func TestDLQDeleteDeletesEverything(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	dedup := hgetall(t, a, "hr1:d:"+messageDedupDigest("m1"))

	d := deleteDLQ(a, "m1", dlqVersion(t, a, "m1"))
	if d.Result != administration.DeleteDLQDeleted || d.RecipientIdentity != ridA || d.Reason != "nack_exhausted" || d.DeletedMs <= 0 {
		t.Fatalf("delete = %+v", d)
	}
	for _, k := range []string{"hr1:dl:m1", "hr1:m:m1", "hr1:mi:m1", "hr1:a:m1"} {
		if exists(t, a, k) {
			t.Errorf("%s kept", k)
		}
	}
	if _, ok := score(t, a, "hr1:dlq", "m1"); ok {
		t.Error("dlq member kept")
	}
	if got := hgetall(t, a, "hr1:d:"+messageDedupDigest("m1")); len(got) == 0 || got["message_id"] != dedup["message_id"] {
		t.Errorf("dedup record = %v, want %v", got, dedup)
	}
	if !exists(t, a, "hr1:m:m2") || lrange(t, a, "hr1:r:"+ridA+":q")[0] != "m2" {
		t.Error("the Recipient's queued message was touched")
	}
	entries, _ := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Build()).ToArray()
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(entries))
	}
	fields, _ := streamEntryFields(entries[0])
	want := map[string]string{
		"event_id": "evt-m1", "timestamp_ms": itoa64(d.DeletedMs), "actor": "admin_session", "operation": "dead_letter_deleted",
		"target": "m1", "request_id": "req-m1", "outcome": "success", "reason": "nack_exhausted",
	}
	if len(fields) != len(want) {
		t.Errorf("audit fields = %v", fields)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("audit %s = %q, want %q", k, fields[k], v)
		}
	}

	// Repeating is harmless: absent, no audit.
	before := snapshot(t, a)
	if d := deleteDLQ(a, "m1", nil); d.Result != administration.DeleteDLQAbsent {
		t.Errorf("repeat = %+v", d)
	}
	assertUnchanged(t, a, before, "repeat")
}

// TestDLQDeleteStaleTagAfterReplay pins the entity tag: after a replay and
// a second dead-lettering, the tag read before the replay is refused with
// the current pair, and the current tag deletes.
func TestDLQDeleteStaleTagAfterReplay(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	old := dlqVersion(t, a, "m1")
	if r := replay(a, "m1", "reject"); r.Result != administration.ReplayReplayed {
		t.Fatalf("replay = %+v", r)
	}
	deadLetterOne(t, s, "op-2", "dlv_2")
	current := dlqVersion(t, a, "m1")
	if current.DeliveryCycle != 2 {
		t.Fatalf("current = %+v", current)
	}
	before := snapshot(t, a)
	d := deleteDLQ(a, "m1", old)
	if d.Result != administration.DeleteDLQPreconditionFailed || d.Current != *current {
		t.Errorf("stale tag = %+v, want current %+v", d, current)
	}
	assertUnchanged(t, a, before, "stale tag")
	if d := deleteDLQ(a, "m1", current); d.Result != administration.DeleteDLQDeleted {
		t.Errorf("current tag = %+v", d)
	}
}

// TestDLQDeleteRefusals pins every refusal with no mutation and the absent
// path removing only an orphan index member.
func TestDLQDeleteRefusals(t *testing.T) {
	ctx := context.Background()
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	v := dlqVersion(t, a, "m1")
	before := snapshot(t, a)
	if d := deleteDLQ(a, "m1", nil); d.Result != administration.DeleteDLQPreconditionRequired {
		t.Errorf("no tag = %+v", d)
	}
	assertUnchanged(t, a, before, "precondition_required")
	for _, bad := range []administration.DeadLetterVersion{
		{DeliveryCycle: v.DeliveryCycle + 1, DeadLetteredMs: v.DeadLetteredMs},
		{DeliveryCycle: v.DeliveryCycle, DeadLetteredMs: v.DeadLetteredMs + 1},
	} {
		if d := deleteDLQ(a, "m1", &bad); d.Result != administration.DeleteDLQPreconditionFailed || d.Current != *v {
			t.Errorf("tag %+v = %+v", bad, d)
		}
	}
	assertUnchanged(t, a, before, "precondition_failed")

	a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "head_message_missing")
	before = snapshot(t, a)
	if d := deleteDLQ(a, "m1", v); d.Result != administration.DeleteDLQRecipientBlocked {
		t.Errorf("blocked = %+v", d)
	}
	assertUnchanged(t, a, before, "recipient_blocked")
	a.testDo(t, "DEL", "hr1:q:"+ridA)

	a.testDo(t, "ZADD", "hr1:dlq", "1000", "orphan")
	before = snapshot(t, a)
	if d := deleteDLQ(a, "orphan", v); d.Result != administration.DeleteDLQAbsent {
		t.Errorf("orphan = %+v", d)
	}
	if _, ok := score(t, a, "hr1:dlq", "orphan"); ok {
		t.Error("orphan member kept")
	}
	a.testDo(t, "ZADD", "hr1:dlq", "1000", "orphan")
	assertUnchanged(t, a, before, "absent")

	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"record not a hash":          func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dl:m1"); a.testDo(t, "SET", "hr1:dl:m1", "x") },
		"record without cycle":       func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "delivery_cycle") },
		"record without recipient":   func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "recipient_identity") },
		"audit not a stream":         func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:audit", "x") },
		"dlq index not a sorted set": func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dlq"); a.testDo(t, "SET", "hr1:dlq", "x") },
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			v := dlqVersion(t, a, "m1")
			setup(t, a)
			before := snapshot(t, a)
			if d := deleteDLQ(a, "m1", v); d.Result != administration.DeleteDLQWrongType {
				t.Errorf("delete = %+v", d)
			}
			assertUnchanged(t, a, before, name)
		})
	}

	// Arguments, keys, and the EVAL reload after SCRIPT FLUSH.
	a, s = claimSetup(t)
	keys := []string{"hr1:dlq", "hr1:audit"}
	valid := []string{"m1", "1", "1000", "admin_bearer", "evt", "req", "hr1"}
	for _, i := range []int{0, 3, 4, 5, 6} {
		args := append([]string(nil), valid...)
		args[i] = ""
		if _, err := a.RunScript(ctx, "dlq_delete_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("empty argument %d accepted: %v", i+1, err)
		}
	}
	for name, pair := range map[string][2]string{"half pair": {"1", ""}, "zero cycle": {"0", "1000"}, "negative": {"1", "-5"}} {
		args := append([]string(nil), valid...)
		args[1], args[2] = pair[0], pair[1]
		if _, err := a.RunScript(ctx, "dlq_delete_v1", keys, args); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	bad := append([]string(nil), valid...)
	bad[3] = "maintenance"
	if _, err := a.RunScript(ctx, "dlq_delete_v1", keys, bad); err == nil {
		t.Error("unknown actor accepted")
	}
	if _, err := a.RunScript(ctx, "dlq_delete_v1", []string{"hr1:dlq", "other:audit"}, valid); err == nil {
		t.Error("foreign key accepted")
	}
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "SCRIPT", "FLUSH")
	if d := deleteDLQ(a, "m1", dlqVersion(t, a, "m1")); d.Result != administration.DeleteDLQDeleted {
		t.Errorf("delete after SCRIPT FLUSH = %+v", d)
	}
}

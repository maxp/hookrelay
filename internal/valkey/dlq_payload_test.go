package valkey

import (
	"context"
	"errors"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

func viewPayload(a *Adapter, messageID string) administration.Payload {
	return NewDeadLetterStore(a).ViewPayload(context.Background(), messageID, "admin_session", "evt-"+messageID, "req-"+messageID)
}

// TestDLQPayloadDisclosesAfterAudit pins the disclosed tuple: the stored
// blob verbatim with the record's cycle, time, and Recipient, one audit
// event without payload, and no other change.
func TestDLQPayloadDisclosesAfterAudit(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	dead := deadLetterOne(t, s, "op-1", "dlv_1")
	blob, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:m:m1").Build()).ToString()
	before := snapshot(t, a)

	p := viewPayload(a, "m1")
	if p.Result != administration.PayloadDisclosed || string(p.Message) != blob || p.DeliveryCycle != 1 ||
		p.DeadLetteredMs != dead || p.RecipientIdentity != ridA {
		t.Fatalf("payload = %+v", p)
	}
	entries, _ := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Build()).ToArray()
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(entries))
	}
	fields, _ := streamEntryFields(entries[0])
	want := map[string]string{
		"event_id": "evt-m1", "actor": "admin_session", "operation": "dead_letter_payload_viewed", "target": "m1",
		"request_id": "req-m1", "outcome": "success",
	}
	if len(fields) != len(want)+1 || fields["timestamp_ms"] == "" {
		t.Errorf("audit fields = %v", fields)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("audit %s = %q, want %q", k, fields[k], v)
		}
	}
	a.testDo(t, "DEL", "hr1:audit")
	assertUnchanged(t, a, before, "disclosure")
}

// TestDLQPayloadRefusals pins every refusal: nothing disclosed and no
// state change, including no audit event.
func TestDLQPayloadRefusals(t *testing.T) {
	ctx := context.Background()
	a, _ := claimSetup(t)
	before := snapshot(t, a)
	if p := viewPayload(a, "absent"); p.Result != administration.PayloadNotFound || p.Message != nil {
		t.Errorf("absent = %+v", p)
	}
	assertUnchanged(t, a, before, "not_found")

	for name, tc := range map[string]struct {
		setup func(t *testing.T, a *Adapter)
		want  administration.PayloadResult
	}{
		"message missing":        {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:m:m1") }, administration.PayloadMessageMissing},
		"message not a string":   {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:m:m1"); a.testDo(t, "RPUSH", "hr1:m:m1", "x") }, administration.PayloadWrongType},
		"record not a hash":      {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dl:m1"); a.testDo(t, "SET", "hr1:dl:m1", "x") }, administration.PayloadWrongType},
		"record without cycle":   {func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "delivery_cycle") }, administration.PayloadWrongType},
		"record without time":    {func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:dl:m1", "dead_lettered_ms", "x") }, administration.PayloadWrongType},
		"record without rcpt":    {func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "recipient_identity") }, administration.PayloadWrongType},
		"audit not a stream":     {func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:audit", "x") }, administration.PayloadWrongType},
		"blob not a JSON object": {func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:m:m1", "not json") }, administration.PayloadWrongType},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			tc.setup(t, a)
			before := snapshot(t, a)
			p := viewPayload(a, "m1")
			if p.Result != tc.want || p.Message != nil {
				t.Errorf("payload = %+v, want %s", p, tc.want)
			}
			if name == "blob not a JSON object" {
				// The script appended the audit before Go rejected the blob:
				// the access is recorded although nothing was disclosed.
				a.testDo(t, "DEL", "hr1:audit")
			}
			assertUnchanged(t, a, before, name)
		})
	}

	// Arguments, keys, and the EVAL reload after SCRIPT FLUSH.
	a, s := claimSetup(t)
	keys := []string{"hr1:audit"}
	valid := []string{"m1", "admin_bearer", "evt", "req", "hr1"}
	for i := range valid {
		args := append([]string(nil), valid...)
		args[i] = ""
		if _, err := a.RunScript(ctx, "dlq_payload_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("empty argument %d accepted: %v", i+1, err)
		}
	}
	bad := append([]string(nil), valid...)
	bad[1] = "consumer"
	if _, err := a.RunScript(ctx, "dlq_payload_v1", keys, bad); err == nil {
		t.Error("unknown actor accepted")
	}
	if _, err := a.RunScript(ctx, "dlq_payload_v1", []string{"other:audit"}, valid); err == nil {
		t.Error("foreign key accepted")
	}
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "SCRIPT", "FLUSH")
	if p := viewPayload(a, "m1"); p.Result != administration.PayloadDisclosed {
		t.Errorf("payload after SCRIPT FLUSH = %+v", p)
	}
}

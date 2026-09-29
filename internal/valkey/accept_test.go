package valkey

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/ingestion"
)

const testRecipient = "telegram:42:chat:-100"

func testLimits() AcceptLimits {
	return AcceptLimits{
		MaxQueuedMessages:             100,
		MaxQueuedMessagesPerRecipient: 10,
		MaxDedupRecords:               100,
		DedupRetention:                time.Hour,
	}
}

func acceptReq(messageID, dedup, body string) ingestion.AcceptRequest {
	return ingestion.AcceptRequest{
		MessageID:           messageID,
		DedupIdentityDigest: dedup,
		BodyDigest:          body,
		ReceivedMs:          1740000000000,
		MessageJSON:         []byte(`{"message_id":"` + messageID + `"}`),
		RecipientIdentity:   testRecipient,
	}
}

// snapshot captures every key and its type so tests can assert that a
// refused transition changed nothing.
func snapshot(t *testing.T, a *Adapter) map[string]string {
	t.Helper()
	ctx := context.Background()
	msg, err := a.client.Do(ctx, a.client.B().Keys().Pattern("*").Build()).ToMessage()
	if err != nil {
		t.Fatal(err)
	}
	keys, _ := msg.AsStrSlice()
	sort.Strings(keys)
	out := map[string]string{}
	for _, k := range keys {
		dump, err := a.client.Do(ctx, a.client.B().Dump().Key(k).Build()).ToString()
		if err != nil {
			t.Fatalf("dump %s: %v", k, err)
		}
		out[k] = dump
	}
	return out
}

func assertUnchanged(t *testing.T, a *Adapter, before map[string]string, what string) {
	t.Helper()
	after := snapshot(t, a)
	if len(after) != len(before) {
		t.Errorf("%s changed the key set: %d keys, want %d", what, len(after), len(before))
		return
	}
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s mutated %s", what, k)
		}
	}
}

func hget(t *testing.T, a *Adapter, key, field string) string {
	t.Helper()
	v, err := a.client.Do(context.Background(), a.client.B().Hget().Key(key).Field(field).Build()).ToString()
	if err != nil {
		t.Fatalf("hget %s %s: %v", key, field, err)
	}
	return v
}

// TestAcceptAcceptedAndKeys pins the accepted tuple and every affected key:
// dedup record + TTL + age member, blob, queue, head state and ready member
// created only for the first message, and the queued counter.
func TestAcceptAcceptedAndKeys(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	acc := NewMessageAcceptor(a, testLimits())

	r1 := acc.Accept(ctx, acceptReq("m1", "d1", "b1"))
	if r1.Outcome != ingestion.AcceptAccepted || r1.MessageID != "m1" || r1.AcceptedMs <= 0 {
		t.Fatalf("first accept = %+v", r1)
	}

	if got := hget(t, a, "hr1:d:d1", "message_id"); got != "m1" {
		t.Errorf("dedup message_id = %q", got)
	}
	if got := hget(t, a, "hr1:d:d1", "body_digest"); got != "b1" {
		t.Errorf("dedup body_digest = %q", got)
	}
	ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key("hr1:d:d1").Build()).AsInt64()
	if ttl <= 0 || ttl > time.Hour.Milliseconds() {
		t.Errorf("dedup TTL = %d ms, want within the retention", ttl)
	}
	if s, _ := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:dedup_age").Member("d1").Build()).AsInt64(); s != r1.AcceptedMs {
		t.Errorf("dedup_age score = %d, want accepted_ms %d", s, r1.AcceptedMs)
	}
	if blob, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:m:m1").Build()).ToString(); blob != `{"message_id":"m1"}` {
		t.Errorf("blob = %q", blob)
	}
	state := "hr1:r:" + testRecipient + ":s"
	for field, want := range map[string]string{"status": "ready", "head_message_id": "m1", "delivery_cycle": "1", "attempt": "1"} {
		if got := hget(t, a, state, field); got != want {
			t.Errorf("state %s = %q, want %q", field, got, want)
		}
	}
	seq1, _ := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:ready").Member(testRecipient).Build()).AsInt64()
	if seq1 != 1 {
		t.Errorf("ready score = %d, want the first fairness sequence 1", seq1)
	}

	// A second message queues behind the head without touching head state
	// or the ready membership.
	if r := acc.Accept(ctx, acceptReq("m2", "d2", "b2")); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("second accept = %+v", r)
	}
	queue, _ := a.client.Do(ctx, a.client.B().Lrange().Key("hr1:r:"+testRecipient+":q").Start(0).Stop(-1).Build()).AsStrSlice()
	if strings.Join(queue, ",") != "m1,m2" {
		t.Errorf("queue = %v, want m1,m2", queue)
	}
	if got := hget(t, a, state, "head_message_id"); got != "m1" {
		t.Errorf("head moved to %q", got)
	}
	if seq, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:ready_seq").Build()).AsInt64(); seq != 1 {
		t.Errorf("ready_seq = %d, want 1 (no new allocation)", seq)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 2 {
		t.Errorf("queued counter = %d, want 2", n)
	}

	// A leased head keeps accepting behind it.
	if _, err := a.client.Do(ctx, a.client.B().Hset().Key(state).FieldValue().FieldValue("status", "leased").Build()).ToMessage(); err != nil {
		t.Fatal(err)
	}
	if r := acc.Accept(ctx, acceptReq("m3", "d3", "b3")); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("accept behind a leased head = %+v", r)
	}
	if got := hget(t, a, state, "status"); got != "leased" {
		t.Errorf("leased head state changed to %q", got)
	}
}

// TestAcceptDuplicates pins duplicate and duplicate_conflict: the original
// message id is returned and nothing changes.
func TestAcceptDuplicates(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	acc := NewMessageAcceptor(a, testLimits())

	if r := acc.Accept(ctx, acceptReq("m1", "d1", "b1")); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("accept = %+v", r)
	}
	before := snapshot(t, a)
	if r := acc.Accept(ctx, acceptReq("m2", "d1", "b1")); r.Outcome != ingestion.AcceptDuplicate || r.MessageID != "m1" {
		t.Errorf("duplicate = %+v, want duplicate of m1", r)
	}
	assertUnchanged(t, a, before, "duplicate")
	if r := acc.Accept(ctx, acceptReq("m3", "d1", "other-body")); r.Outcome != ingestion.AcceptDuplicateConflict || r.MessageID != "m1" {
		t.Errorf("conflict = %+v, want duplicate_conflict of m1", r)
	}
	assertUnchanged(t, a, before, "duplicate_conflict")
}

// TestAcceptRefusalsCreateNothing pins every refusal status and the absence
// of any mutation.
func TestAcceptRefusalsCreateNothing(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()

	cases := []struct {
		name   string
		limits func(*AcceptLimits)
		setup  func()
		want   ingestion.AcceptOutcome
	}{
		{"blocked recipient", nil, func() {
			a.client.Do(ctx, a.client.B().Hset().Key("hr1:q:"+testRecipient).FieldValue().FieldValue("reason_code", "queue_head_mismatch").Build())
		}, ingestion.AcceptRecipientBlocked},
		{"recipient capacity", func(l *AcceptLimits) { l.MaxQueuedMessagesPerRecipient = 1 }, func() {
			NewMessageAcceptor(a, testLimits()).Accept(ctx, acceptReq("m0", "d0", "b0"))
		}, ingestion.AcceptRecipientCapacity},
		{"global capacity", func(l *AcceptLimits) { l.MaxQueuedMessages = 1 }, func() {
			r := acceptReq("m0", "d0", "b0")
			r.RecipientIdentity = "telegram:42:chat:other"
			NewMessageAcceptor(a, testLimits()).Accept(ctx, r)
		}, ingestion.AcceptGlobalCapacity},
		{"dedup capacity", func(l *AcceptLimits) { l.MaxDedupRecords = 1 }, func() {
			r := acceptReq("m0", "d0", "b0")
			r.RecipientIdentity = "telegram:42:chat:other"
			NewMessageAcceptor(a, testLimits()).Accept(ctx, r)
		}, ingestion.AcceptDedupCapacity},
	}
	for _, key := range []string{"hr1:d:d1", "hr1:dedup_age", "hr1:r:" + testRecipient + ":q", "hr1:r:" + testRecipient + ":s", "hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:stats:queued_messages"} {
		key := key
		poison := "SET"
		if key == "hr1:ready_seq" || key == "hr1:stats:queued_messages" {
			poison = "HSET"
		}
		cases = append(cases, struct {
			name   string
			limits func(*AcceptLimits)
			setup  func()
			want   ingestion.AcceptOutcome
		}{"wrong type " + key, nil, func() {
			if poison == "SET" {
				a.client.Do(ctx, a.client.B().Set().Key(key).Value("poison").Build())
			} else {
				a.client.Do(ctx, a.client.B().Hset().Key(key).FieldValue().FieldValue("x", "y").Build())
			}
		}, ingestion.AcceptInternalFailure})
	}
	cases = append(cases, struct {
		name   string
		limits func(*AcceptLimits)
		setup  func()
		want   ingestion.AcceptOutcome
	}{"head state without a queue", nil, func() {
		a.client.Do(ctx, a.client.B().Hset().Key("hr1:r:"+testRecipient+":s").FieldValue().FieldValue("status", "leased").Build())
	}, ingestion.AcceptInternalFailure})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			flushAll(t, a)
			gate(t, a, false)
			tc.setup()
			limits := testLimits()
			if tc.limits != nil {
				tc.limits(&limits)
			}
			before := snapshot(t, a)
			if r := NewMessageAcceptor(a, limits).Accept(ctx, acceptReq("m1", "d1", "b1")); r.Outcome != tc.want {
				t.Fatalf("outcome = %+v, want %s", r, tc.want)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestAcceptScriptRejectsInvalidArguments pins argument and key-identity
// validation before any read or write.
func TestAcceptScriptRejectsInvalidArguments(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)

	keys := []string{"hr1:d:d1", "hr1:dedup_age", "hr1:m:m1", "hr1:r:" + testRecipient + ":q", "hr1:r:" + testRecipient + ":s",
		"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:q:" + testRecipient, "hr1:stats:queued_messages"}
	valid := []string{"m1", "d1", "b1", "1740000000000", "", "{}", testRecipient, "100", "10", "100", "3600000"}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	if _, err := a.RunScript(ctx, "accept_v1", keys, valid); err != nil {
		t.Fatalf("valid call failed: %v", err)
	}
	flushAll(t, a)
	for name, args := range map[string][]string{
		"too few arguments":      valid[:10],
		"empty message_id":       with(0, ""),
		"non-numeric limit":      with(7, "many"),
		"zero retention":         with(10, "0"),
		"bad occurred_ms":        with(4, "-5"),
		"recipient key mismatch": with(6, "telegram:42:chat:other"),
		"message key mismatch":   with(0, "m2"),
	} {
		_, err := a.RunScript(ctx, "accept_v1", keys, args)
		if err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v, want a script error", name, err)
		}
	}
	if n, _ := a.client.Do(ctx, a.client.B().Dbsize().Build()).ToMessage(); nInt(n) != 0 {
		t.Errorf("rejected arguments left %d keys behind", nInt(n))
	}
}

// TestEndpointLookup pins the ingestion endpoint reader.
func TestEndpointLookup(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	if _, _, r := a.CreateEndpoint(ctx, endpointFixture(), "e", "webhook_endpoint_created", "r"); r != CreateOK {
		t.Fatalf("create = %s", r)
	}
	l := NewEndpointLookup(a)
	e, err := l.LookupEndpoint(ctx, "telegram", "wh_test1")
	if err != nil || e == nil || e.BotID != "123456789" || !e.Enabled || e.CredentialValue != "test-credential-value" {
		t.Fatalf("lookup = %+v, %v", e, err)
	}
	if e, err := l.LookupEndpoint(ctx, "telegram", "wh_missing"); e != nil || err != nil {
		t.Errorf("missing lookup = %+v, %v", e, err)
	}
	a.client.Do(ctx, a.client.B().Set().Key("hr1:wh:telegram:poison").Value("x").Build())
	if _, err := l.LookupEndpoint(ctx, "telegram", "poison"); !errors.Is(err, ingestion.ErrStoredWrongType) {
		t.Errorf("poisoned lookup err = %v", err)
	}
}

// TestAcceptDedupCapacityCountsLiveRecords pins that expired dedup index
// members neither block acceptance nor accumulate: the capacity counts live
// members only and an acceptance prunes expired ones.
func TestAcceptDedupCapacityCountsLiveRecords(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	limits := testLimits()
	limits.MaxDedupRecords = 1
	limits.DedupRetention = 50 * time.Millisecond
	acc := NewMessageAcceptor(a, limits)

	if r := acc.Accept(ctx, acceptReq("m1", "d1", "b1")); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("first = %+v", r)
	}
	second := acceptReq("m2", "d2", "b2")
	second.RecipientIdentity = "telegram:42:chat:other"
	if r := acc.Accept(ctx, second); r.Outcome != ingestion.AcceptDedupCapacity {
		t.Fatalf("second within retention = %+v, want dedup_capacity", r)
	}
	time.Sleep(120 * time.Millisecond)
	if r := acc.Accept(ctx, second); r.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("second after expiry = %+v, want accepted", r)
	}
	members, _ := a.client.Do(ctx, a.client.B().Zrange().Key("hr1:dedup_age").Min("0").Max("-1").Build()).AsStrSlice()
	if len(members) != 1 || members[0] != "d2" {
		t.Errorf("dedup_age = %v, want the expired member pruned", members)
	}
}

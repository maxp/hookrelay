package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
)

func replay(a *Adapter, messageID, resolution string) administration.Replay {
	return NewDeadLetterStore(a).ReplayDeadLetter(context.Background(), messageID, resolution, "admin_bearer", "evt-"+messageID, "req-"+messageID)
}

func headState(t *testing.T, a *Adapter, rid string) map[string]string {
	t.Helper()
	return hgetall(t, a, "hr1:r:"+rid+":s")
}

func assertHead(t *testing.T, a *Adapter, rid, messageID, status, cycle, attempt string) {
	t.Helper()
	st := headState(t, a, rid)
	if st["head_message_id"] != messageID || st["status"] != status || st["delivery_cycle"] != cycle || st["attempt"] != attempt {
		t.Errorf("head state = %v, want %s %s cycle %s attempt %s", st, messageID, status, cycle, attempt)
	}
}

// assertPending pins a queued message's saved pending pair ("" for none).
func assertPending(t *testing.T, a *Adapter, messageID, cycle, attempt string) {
	t.Helper()
	m := hgetall(t, a, "hr1:mi:"+messageID)
	if m["pending_delivery_cycle"] != cycle || m["pending_attempt"] != attempt {
		t.Errorf("hr1:mi:%s = %v, want pending %q/%q", messageID, m, cycle, attempt)
	}
}

func claimNext(t *testing.T, s *DeliveryStore, op, token string) delivery.Delivery {
	t.Helper()
	c := s.Claim(context.Background(), claimReq(op, "args", token))
	if c.Outcome != delivery.ClaimClaimed {
		t.Fatalf("claim %s = %+v", op, c)
	}
	return c.Delivery
}

func ackOK(t *testing.T, s *DeliveryStore, token string) {
	t.Helper()
	if r := s.Ack(context.Background(), ackReq(token)); r.Outcome != delivery.AckAcknowledged {
		t.Fatalf("ack %s = %+v", token, r)
	}
}

// TestReplayIntoEmptyQueue pins the replayed tuple and every affected key
// for a Recipient whose queue drained: the message becomes the ready head
// in a new cycle, the record and DLQ member go, the counter grows, history
// and metadata stay, and the audit event is appended atomically.
func TestReplayIntoEmptyQueue(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")

	r := replay(a, "m1", "reject")
	if r.Result != administration.ReplayReplayed || r.DeliveryCycle != 2 || r.QueuePosition != "head" ||
		r.DeduplicationResolution != "not_conflicting" || r.RecipientIdentity != ridA || r.ReplayedMs <= 0 {
		t.Fatalf("replay = %+v", r)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); len(q) != 1 || q[0] != "m1" {
		t.Errorf("queue = %v", q)
	}
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	if _, ok := score(t, a, "hr1:ready", ridA); !ok {
		t.Error("ready member missing")
	}
	if exists(t, a, "hr1:dl:m1") {
		t.Error("dead-letter record kept")
	}
	if _, ok := score(t, a, "hr1:dlq", "m1"); ok {
		t.Error("dlq member kept")
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 1 {
		t.Errorf("counter = %d, want 1", n)
	}
	if h := lrange(t, a, "hr1:a:m1"); len(h) != 1 {
		t.Errorf("history = %v, want the cycle-1 attempt kept", h)
	}
	if !exists(t, a, "hr1:m:m1") || hget(t, a, "hr1:mi:m1", "dedup_identity_digest") != messageDedupDigest("m1") {
		t.Error("blob or metadata not kept")
	}
	assertPending(t, a, "m1", "", "")

	entries, _ := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Build()).ToArray()
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(entries))
	}
	fields, _ := streamEntryFields(entries[0])
	for k, want := range map[string]string{
		"event_id": "evt-m1", "actor": "admin_bearer", "operation": "dead_letter_replayed", "target": "m1",
		"request_id": "req-m1", "outcome": "success", "reason": "not_conflicting", "timestamp_ms": itoa64(r.ReplayedMs),
	} {
		if fields[k] != want {
			t.Errorf("audit %s = %q, want %q", k, fields[k], want)
		}
	}

	// The replayed message is claimed in the new cycle and can dead-letter
	// again with its history accumulated.
	d := claimNext(t, s, "op-2", "dlv_2")
	if d.MessageID != "m1" || d.DeliveryCycle != 2 || d.Attempt != 1 {
		t.Errorf("claim after replay = %+v", d)
	}
	req := nackReq("dlv_2", "")
	req.RetryDelaysMs, req.MaxAttempts = nil, 1
	if n := s.Nack(ctx, req); n.Outcome != delivery.NackDeadLettered || n.DeliveryCycle != 2 {
		t.Errorf("second dead-letter = %+v", n)
	}
	if got := hget(t, a, "hr1:dl:m1", "delivery_cycle"); got != "2" {
		t.Errorf("dl delivery_cycle = %s", got)
	}
	if h := lrange(t, a, "hr1:a:m1"); len(h) != 2 {
		t.Errorf("history = %v, want both cycles", h)
	}
}

// TestReplayBehindLeasedHead pins after_active_head: the leased head is not
// interrupted, the replayed message waits behind it with its new cycle
// saved as pending state, and acknowledgement of the head restores it.
func TestReplayBehindLeasedHead(t *testing.T) {
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	enqueue(t, a, "m2", ridA)
	claimNext(t, s, "op-2", "dlv_2")
	lease := headState(t, a, ridA)

	r := replay(a, "m1", "reject")
	if r.Result != administration.ReplayReplayed || r.QueuePosition != "after_active_head" || r.DeliveryCycle != 2 {
		t.Fatalf("replay = %+v", r)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m2,m1" {
		t.Errorf("queue = %v", q)
	}
	if st := headState(t, a, ridA); st["delivery_token"] != lease["delivery_token"] || st["status"] != "leased" {
		t.Errorf("leased head changed: %v", st)
	}
	assertPending(t, a, "m1", "2", "1")

	ackOK(t, s, "dlv_2")
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	assertPending(t, a, "m1", "", "")
	if hget(t, a, "hr1:mi:m1", "dedup_identity_digest") != messageDedupDigest("m1") {
		t.Error("restoring the pending pair dropped the dedup digest")
	}
	if d := claimNext(t, s, "op-3", "dlv_3"); d.MessageID != "m1" || d.DeliveryCycle != 2 || d.Attempt != 1 {
		t.Errorf("claim = %+v", d)
	}
	ackOK(t, s, "dlv_3")
	if exists(t, a, "hr1:mi:m1") || exists(t, a, "hr1:a:m1") || exists(t, a, "hr1:m:m1") {
		t.Error("acknowledged replayed message kept blob, metadata, or history")
	}
}

// TestReplayBehindRetryWaitHead pins after_active_head for a head waiting
// for its retry and the dead-letter transition of that head exposing the
// replayed message with its restored cycle.
func TestReplayBehindRetryWaitHead(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	enqueue(t, a, "m2", ridA)
	claimNext(t, s, "op-2", "dlv_2")
	if n := s.Nack(ctx, nackReq("dlv_2", "")); n.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("nack = %+v", n)
	}

	if r := replay(a, "m1", "reject"); r.Result != administration.ReplayReplayed || r.QueuePosition != "after_active_head" {
		t.Fatalf("replay = %+v", r)
	}
	assertHead(t, a, ridA, "m2", "retry_wait", "1", "2")
	if _, ok := score(t, a, "hr1:retries", ridA); !ok {
		t.Error("retries member dropped")
	}
	if _, ok := score(t, a, "hr1:ready", ridA); ok {
		t.Error("a waiting head became ready")
	}
	assertPending(t, a, "m1", "2", "1")

	activateRetry(t, a, ridA)
	if d := claimNext(t, s, "op-3", "dlv_3"); d.MessageID != "m2" || d.Attempt != 2 {
		t.Fatalf("retry claim = %+v", d)
	}
	req := nackReq("dlv_3", "")
	req.RetryDelaysMs, req.MaxAttempts = []int64{1000}, 2
	if n := s.Nack(ctx, req); n.Outcome != delivery.NackDeadLettered {
		t.Fatalf("dead-letter = %+v", n)
	}
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	assertPending(t, a, "m1", "", "")
	if d := claimNext(t, s, "op-4", "dlv_4"); d.MessageID != "m1" || d.DeliveryCycle != 2 || d.Attempt != 1 {
		t.Errorf("claim = %+v", d)
	}
}

// TestReplayBehindExpiringHead pins the expiry dead-letter path exposing a
// replayed message with its restored cycle.
func TestReplayBehindExpiringHead(t *testing.T) {
	a, s := expirySetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	enqueue(t, a, "m2", ridA)
	claimAndLapse(t, s, "op-2", "dlv_2", "")
	if r := replay(a, "m1", "reject"); r.QueuePosition != "after_active_head" {
		t.Fatalf("replay = %+v", r)
	}
	if e := s.ExpireLease(context.Background(), ridA, nil, 1); e.Outcome != delivery.ExpiryDeadLettered {
		t.Fatalf("expiry = %+v", e)
	}
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	assertPending(t, a, "m1", "", "")
}

// TestReplayPreemptsReadyRetry pins head replay over a ready head whose
// attempt is already greater than one: the preempted head keeps its
// cycle/attempt as pending state and gets it back when it is head again.
func TestReplayPreemptsReadyRetry(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	enqueue(t, a, "m2", ridA)
	claimNext(t, s, "op-2", "dlv_2")
	s.Nack(ctx, nackReq("dlv_2", ""))
	activateRetry(t, a, ridA)
	before, _ := score(t, a, "hr1:ready", ridA)

	r := replay(a, "m1", "reject")
	if r.Result != administration.ReplayReplayed || r.QueuePosition != "head" {
		t.Fatalf("replay = %+v", r)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m1,m2" {
		t.Errorf("queue = %v", q)
	}
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	assertPending(t, a, "m2", "1", "2")
	if after, _ := score(t, a, "hr1:ready", ridA); after == before {
		t.Error("ready score not refreshed")
	}

	claimNext(t, s, "op-3", "dlv_3")
	ackOK(t, s, "dlv_3")
	assertHead(t, a, ridA, "m2", "ready", "1", "2")
	assertPending(t, a, "m2", "", "")
	if d := claimNext(t, s, "op-4", "dlv_4"); d.MessageID != "m2" || d.Attempt != 2 {
		t.Errorf("claim = %+v, want the retry's attempt 2", d)
	}
}

// TestReplayPreemptsLegacyHead pins that a preempted head accepted before
// the metadata key existed gets a metadata key holding only its pending
// pair, which disappears once the pair is restored.
func TestReplayPreemptsLegacyHead(t *testing.T) {
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	enqueue(t, a, "m2", ridA)
	a.testDo(t, "DEL", "hr1:mi:m2")

	if r := replay(a, "m1", "reject"); r.QueuePosition != "head" {
		t.Fatalf("replay = %+v", r)
	}
	if m := hgetall(t, a, "hr1:mi:m2"); len(m) != 2 || m["pending_delivery_cycle"] != "1" || m["pending_attempt"] != "1" {
		t.Errorf("legacy head metadata = %v", m)
	}
	claimNext(t, s, "op-2", "dlv_2")
	ackOK(t, s, "dlv_2")
	assertHead(t, a, ridA, "m2", "ready", "1", "1")
	if exists(t, a, "hr1:mi:m2") {
		t.Error("empty legacy metadata key kept")
	}
}

// TestMultipleReplays pins that replays wait in replay order (first in,
// first out) behind an active head, that a later replay never overwrites
// another queued message's pending state, and that each is restored in
// queue order.
func TestMultipleReplays(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	deadLetterOne(t, s, "op-2", "dlv_2")
	enqueueJSON(t, a, "m3", ridA)
	claimNext(t, s, "op-3", "dlv_3")

	if r := replay(a, "m1", "reject"); r.QueuePosition != "after_active_head" {
		t.Fatalf("first replay = %+v", r)
	}
	if r := replay(a, "m2", "reject"); r.QueuePosition != "after_pending_replay" {
		t.Fatalf("second replay = %+v", r)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m3,m1,m2" {
		t.Errorf("queue = %v", q)
	}
	assertPending(t, a, "m1", "2", "1")
	assertPending(t, a, "m2", "2", "1")
	if rep := reconcile(t, a, true); rep.Findings["blocked"] != 0 {
		t.Errorf("reconcile after replays = %+v", rep.Findings)
	}

	ackOK(t, s, "dlv_3")
	assertHead(t, a, ridA, "m1", "ready", "2", "1")
	assertPending(t, a, "m2", "2", "1")
	claimNext(t, s, "op-4", "dlv_4")
	ackOK(t, s, "dlv_4")
	assertHead(t, a, ridA, "m2", "ready", "2", "1")
}

// TestReplayOrderIgnoresClaims pins that replaying dead letters oldest
// first restores their original order whether or not a Consumer claims the
// first replayed message before the second replay: a waiting-replay ready
// head is not preempted (after_pending_replay), and a leased one is not
// interrupted (after_active_head). The preempted original head keeps its
// pending pair behind both.
func TestReplayOrderIgnoresClaims(t *testing.T) {
	for _, claimFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "leased"}[claimFirst], func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			enqueueJSON(t, a, "m2", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			deadLetterOne(t, s, "op-2", "dlv_2")
			enqueueJSON(t, a, "m3", ridA)

			if r := replay(a, "m1", "reject"); r.QueuePosition != "head" {
				t.Fatalf("first replay = %+v", r)
			}
			want := "after_pending_replay"
			if claimFirst {
				claimNext(t, s, "op-3", "dlv_3")
				want = "after_active_head"
			}
			if r := replay(a, "m2", "reject"); r.QueuePosition != want {
				t.Fatalf("second replay = %+v, want %s", r, want)
			}
			if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m1,m2,m3" {
				t.Errorf("queue = %v", q)
			}
			assertPending(t, a, "m2", "2", "1")
			assertPending(t, a, "m3", "1", "1")
			if rep := reconcile(t, a, true); rep.Findings["blocked"] != 0 {
				t.Errorf("reconcile after replays = %+v", rep.Findings)
			}

			if !claimFirst {
				claimNext(t, s, "op-3", "dlv_3")
			}
			ackOK(t, s, "dlv_3")
			assertHead(t, a, ridA, "m2", "ready", "2", "1")
			claimNext(t, s, "op-4", "dlv_4")
			ackOK(t, s, "dlv_4")
			assertHead(t, a, ridA, "m3", "ready", "1", "1")
		})
	}
}

// TestReplayPreemptsStartedReplay pins that a replayed ready head that has
// already started its cycle (attempt above 1) is not a waiting replay: a
// later replay preempts it like any ready retry.
func TestReplayPreemptsStartedReplay(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	enqueue(t, a, "m2", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	deadLetterOne(t, s, "op-2", "dlv_2")
	replay(a, "m1", "reject")
	claimNext(t, s, "op-3", "dlv_3")
	s.Nack(ctx, nackReq("dlv_3", ""))
	activateRetry(t, a, ridA)

	if r := replay(a, "m2", "reject"); r.QueuePosition != "head" {
		t.Fatalf("replay = %+v", r)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m2,m1" {
		t.Errorf("queue = %v", q)
	}
	assertPending(t, a, "m1", "2", "2")
}

// TestReplayDeduplicationResolution pins the conflict check: a mapping to
// another message refuses the default replay without mutation, and
// keep_current replays while leaving that mapping unchanged.
func TestReplayDeduplicationResolution(t *testing.T) {
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "HSET", "hr1:d:"+messageDedupDigest("m1"), "message_id", "m9")

	before := snapshot(t, a)
	if r := replay(a, "m1", "reject"); r.Result != administration.ReplayDeduplicationConflict {
		t.Fatalf("reject = %+v", r)
	}
	assertUnchanged(t, a, before, "conflicting replay")

	r := replay(a, "m1", "keep_current")
	if r.Result != administration.ReplayReplayed || r.DeduplicationResolution != "kept_current" {
		t.Fatalf("keep_current = %+v", r)
	}
	if got := hget(t, a, "hr1:d:"+messageDedupDigest("m1"), "message_id"); got != "m9" {
		t.Errorf("dedup mapping = %s, want the newer m9 kept", got)
	}

	// A message dead-lettered without a dedup digest (accepted before the
	// metadata key existed) never conflicts.
	enqueue(t, a, "m2", ridB)
	a.testDo(t, "DEL", "hr1:mi:m2")
	claimNext(t, s, "op-2", "dlv_2") // ridA's m1 is head: claim it first
	ackOK(t, s, "dlv_2")
	deadLetterOne(t, s, "op-3", "dlv_3")
	if got := hget(t, a, "hr1:dl:m2", "dedup_identity_digest"); got != "" {
		t.Fatalf("legacy dedup digest = %q", got)
	}
	a.testDo(t, "HSET", "hr1:d:"+messageDedupDigest("m2"), "message_id", "m9")
	if r := replay(a, "m2", "reject"); r.Result != administration.ReplayReplayed || r.DeduplicationResolution != "not_conflicting" {
		t.Errorf("legacy replay = %+v", r)
	}
}

// TestReplayRefusals pins every refusal with no mutation.
func TestReplayRefusals(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(t *testing.T, a *Adapter)
		want  administration.ReplayResult
	}{
		"not dead-lettered":    {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dl:m1") }, administration.ReplayNotFound},
		"message missing":      {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:m:m1") }, administration.ReplayMessageMissing},
		"recipient blocked":    {func(t *testing.T, a *Adapter) { block(t, a, ridA, "55", "queue_head_mismatch") }, administration.ReplayRecipientBlocked},
		"record not a hash":    {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dl:m1"); a.testDo(t, "SET", "hr1:dl:m1", "x") }, administration.ReplayWrongType},
		"record without cycle": {func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "delivery_cycle") }, administration.ReplayWrongType},
		"blob not a string":    {func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:m:m1"); a.testDo(t, "RPUSH", "hr1:m:m1", "x") }, administration.ReplayWrongType},
		"dedup record not a hash": {func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:d:"+messageDedupDigest("m1"))
			a.testDo(t, "SET", "hr1:d:"+messageDedupDigest("m1"), "x")
		}, administration.ReplayWrongType},
		"audit not a stream":       {func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:audit", "x") }, administration.ReplayWrongType},
		"counter malformed":        {func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:stats:queued_messages", "x") }, administration.ReplayWrongType},
		"ready sequence exhausted": {func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready_seq", "9223372036854775807") }, administration.ReplayWrongType},
		"malformed history":        {func(t *testing.T, a *Adapter) { a.testDo(t, "RPUSH", "hr1:a:m1", "{") }, administration.ReplayWrongType},
		"pending already saved":    {func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:mi:m1", "pending_attempt", "1") }, administration.ReplayWrongType},
		"state without queue":      {func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "ready") }, administration.ReplayWrongType},
		"already queued": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			a.testDo(t, "RPUSH", "hr1:r:"+ridA+":q", "m1")
		}, administration.ReplayWrongType},
		"queue without state": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			a.testDo(t, "DEL", "hr1:r:"+ridA+":s")
		}, administration.ReplayWrongType},
		"head mismatch": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "m9")
		}, administration.ReplayWrongType},
		"preempted head has pending state": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			a.testDo(t, "HSET", "hr1:mi:m2", "pending_delivery_cycle", "1", "pending_attempt", "1")
		}, administration.ReplayWrongType},
		"waiting replay with a partial pending pair": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			enqueue(t, a, "m3", ridA)
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "retry_wait")
			a.testDo(t, "HSET", "hr1:mi:m3", "pending_attempt", "1")
		}, administration.ReplayWrongType},
		"scanned metadata not a hash": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			enqueue(t, a, "m3", ridA)
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "retry_wait")
			a.testDo(t, "DEL", "hr1:mi:m3")
			a.testDo(t, "SET", "hr1:mi:m3", "x")
		}, administration.ReplayWrongType},
		"preempted head metadata not a hash": {func(t *testing.T, a *Adapter) {
			enqueue(t, a, "m2", ridA)
			a.testDo(t, "DEL", "hr1:mi:m2")
			a.testDo(t, "SET", "hr1:mi:m2", "x")
		}, administration.ReplayWrongType},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueue(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			tc.setup(t, a)
			before := snapshot(t, a)
			if r := replay(a, "m1", "reject"); r.Result != tc.want {
				t.Fatalf("replay = %+v, want %s", r, tc.want)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestReplayArchivesBeyondTenCycles pins that the new cycle counts toward
// the latest 10 retained cycles: replaying cycle 10 folds cycle 1.
func TestReplayArchivesBeyondTenCycles(t *testing.T) {
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "DEL", "hr1:a:m1")
	for c := 1; c <= 10; c++ {
		a.testDo(t, "RPUSH", "hr1:a:m1", `{"kind":"attempt","delivery_cycle":`+itoa(c)+`,"attempt":1,"claimed_ms":`+itoa(c*100)+
			`,"lease_expires_ms":`+itoa(c*100+60)+`,"completed_ms":`+itoa(c*100+50)+`,"outcome":"nack"}`)
	}
	a.testDo(t, "HSET", "hr1:dl:m1", "delivery_cycle", "10")

	if r := replay(a, "m1", "reject"); r.DeliveryCycle != 11 {
		t.Fatalf("replay = %+v", r)
	}
	h := lrange(t, a, "hr1:a:m1")
	if len(h) != 10 {
		t.Fatalf("history = %d entries, want summary + 9", len(h))
	}
	sum := decodeEntry(t, h[0])
	if sum["kind"] != "archived_cycles_summary" || sum["archived_cycles"] != 1.0 || sum["archived_attempts"] != 1.0 ||
		sum["first_archived_ms"] != 100.0 || sum["last_archived_ms"] != 150.0 {
		t.Errorf("summary = %v", sum)
	}
	if e := decodeEntry(t, h[1]); e["delivery_cycle"] != 2.0 {
		t.Errorf("first kept entry = %v", e)
	}
}

// TestReplayArgumentsAndReload pins argument rejection and the EVAL reload
// after SCRIPT FLUSH.
func TestReplayArgumentsAndReload(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:dlq", "hr1:ready", "hr1:ready_seq", "hr1:retries", "hr1:leases", "hr1:blocked", "hr1:stats:queued_messages", "hr1:audit"}
	valid := []string{"m1", "reject", "evt", "req", "admin_session", "hr1"}
	for i := range valid {
		args := append([]string(nil), valid...)
		args[i] = ""
		if _, err := a.RunScript(ctx, "replay_dlq_v3", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("empty argument %d accepted: %v", i+1, err)
		}
	}
	bad := append([]string(nil), valid...)
	bad[1] = "repoint"
	if _, err := a.RunScript(ctx, "replay_dlq_v3", keys, bad); err == nil {
		t.Error("unknown resolution accepted")
	}
	bad = append([]string(nil), valid...)
	bad[4] = "maintenance"
	if _, err := a.RunScript(ctx, "replay_dlq_v3", keys, bad); err == nil {
		t.Error("unknown actor accepted")
	}
	if _, err := a.RunScript(ctx, "replay_dlq_v3", keys[:7], valid); err == nil {
		t.Error("missing key accepted")
	}
	wrong := append([]string(nil), keys...)
	wrong[7] = "other:audit"
	if _, err := a.RunScript(ctx, "replay_dlq_v3", wrong, valid); err == nil {
		t.Error("foreign key accepted")
	}

	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := replay(a, "m1", "reject"); r.Result != administration.ReplayReplayed {
		t.Errorf("replay after SCRIPT FLUSH = %+v", r)
	}
}

// TestHeadAdvanceRefusesMissingPendingState pins that ack, nack, and expiry
// refuse to expose a next head whose attempt history has no valid saved
// pair — before any write, never resetting it to 1/1.
func TestHeadAdvanceRefusesMissingPendingState(t *testing.T) {
	damage := map[string]func(t *testing.T, a *Adapter){
		"history without pending pair": func(t *testing.T, a *Adapter) {
			a.testDo(t, "RPUSH", "hr1:a:m2", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}`)
		},
		"partial pending pair": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:mi:m2", "pending_attempt", "2") },
		"malformed pending pair": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:mi:m2", "pending_delivery_cycle", "0", "pending_attempt", "1")
		},
		"metadata not a hash": func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:mi:m2"); a.testDo(t, "SET", "hr1:mi:m2", "x") },
	}
	for name, setup := range damage {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			a, s := claimSetup(t)
			enqueue(t, a, "m1", ridA)
			enqueue(t, a, "m2", ridA)
			claimNext(t, s, "op-1", "dlv_1")
			setup(t, a)

			before := snapshot(t, a)
			if r := s.Ack(ctx, ackReq("dlv_1")); r.Outcome != delivery.AckInternalFailure {
				t.Errorf("ack = %+v", r)
			}
			assertUnchanged(t, a, before, "ack")
			req := nackReq("dlv_1", "")
			req.RetryDelaysMs, req.MaxAttempts = nil, 1
			if r := s.Nack(ctx, req); r.Outcome != delivery.NackInternalFailure {
				t.Errorf("nack = %+v", r)
			}
			assertUnchanged(t, a, before, "nack")

			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1")
			before = snapshot(t, a)
			if r := s.ExpireLease(ctx, ridA, nil, 1); r.Outcome != delivery.ExpiryInternalFailure {
				t.Errorf("expiry = %+v", r)
			}
			assertUnchanged(t, a, before, "expiry")
		})
	}
}

// TestReconcileQueuedDeliveryState pins reconcile_recipient_v3 and the
// block operations on damaged pending state behind a healthy head: the
// Recipient is blocked with the matching reason, inspection reports it,
// clear refuses while the damage remains, and succeeds after repair.
func TestReconcileQueuedDeliveryState(t *testing.T) {
	ctx := context.Background()
	for reason, setup := range map[string]func(t *testing.T, a *Adapter){
		"queued_delivery_state_missing": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HDEL", "hr1:mi:m1", "pending_delivery_cycle", "pending_attempt")
		},
		"queued_delivery_state_invalid": func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:mi:m1", "pending_attempt") },
	} {
		t.Run(reason, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			enqueueJSON(t, a, "m2", ridA)
			claimNext(t, s, "op-2", "dlv_2")
			replay(a, "m1", "reject")
			if rep := reconcile(t, a, true); rep.Findings["blocked"] != 0 {
				t.Fatalf("healthy replay state blocked: %+v", rep.BlockReasons)
			}

			setup(t, a)
			rep := reconcile(t, a, true)
			if rep.BlockReasons[reason] != 1 {
				t.Fatalf("reconcile = %+v", rep.BlockReasons)
			}
			if m := hgetall(t, a, "hr1:mi:m1"); m["dedup_identity_digest"] != messageDedupDigest("m1") {
				t.Errorf("reconciliation rewrote metadata: %v", m)
			}
			store := testRecipientStore(a)
			in, err := store.InspectBlock(ctx, ridA)
			if err != nil || len(in.ViolatedInvariants) != 1 || in.ViolatedInvariants[0] != reason || in.Marker == nil {
				t.Fatalf("inspection = %+v, %v", in, err)
			}
			result, detail := store.ClearBlock(ctx, ridA, in.Marker.DetectedMs, reason, "evt", "req")
			if result != administration.ClearAmbiguous || detail != reason {
				t.Errorf("clear = %s %s", result, detail)
			}

			a.testDo(t, "HSET", "hr1:mi:m1", "pending_delivery_cycle", "2", "pending_attempt", "1")
			if result, _ := store.ClearBlock(ctx, ridA, in.Marker.DetectedMs, reason, "evt", "req"); result != administration.ClearCleared {
				t.Errorf("clear after repair = %s", result)
			}
		})
	}
}

// TestDeadLetterStoreListAndGet pins newest-first paging with cursors, a
// member without its record, and the safe get with attempt history.
func TestDeadLetterStoreListAndGet(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	for _, id := range []string{"m1", "m2", "m3"} {
		enqueue(t, a, id, ridA)
		deadLetterOne(t, s, "op-"+id, "dlv_"+id)
	}
	// Equal scores order by descending member.
	for _, id := range []string{"m1", "m2", "m3"} {
		a.testDo(t, "ZADD", "hr1:dlq", "500", id)
		a.testDo(t, "HSET", "hr1:dl:"+id, "dead_lettered_ms", "500")
	}
	a.testDo(t, "ZADD", "hr1:dlq", "900", "m9") // no record
	store := NewDeadLetterStore(a)

	page, err := store.ListDeadLetters(ctx, 2, nil)
	if err != nil || len(page) != 2 || page[0].MessageID != "m9" || !page[0].RecordMissing || page[1].MessageID != "m3" {
		t.Fatalf("page 1 = %+v, %v", page, err)
	}
	if page[1].RecipientIdentity != ridA || page[1].Reason != "nack_exhausted" || page[1].DeliveryCycle != 1 || page[1].DeadLetteredMs != 500 {
		t.Errorf("item = %+v", page[1])
	}
	page, err = store.ListDeadLetters(ctx, 2, &administration.DeadLetterCursor{Score: 500, Member: "m3"})
	if err != nil || len(page) != 2 || page[0].MessageID != "m2" || page[1].MessageID != "m1" {
		t.Fatalf("page 2 = %+v, %v", page, err)
	}

	d, err := store.GetDeadLetter(ctx, "m1")
	if err != nil || d == nil || len(d.Attempts) != 1 || d.Attempts[0].Outcome != "nack" || d.Attempts[0].DeliveryCycle != 1 || d.Archived != nil {
		t.Fatalf("get = %+v, %v", d, err)
	}
	if d, err := store.GetDeadLetter(ctx, "m9"); err != nil || d != nil {
		t.Errorf("get without record = %+v, %v", d, err)
	}
}

// TestDeadLetterRoutesOverRealValkey drives list → get → replay through the
// real Admin API handler, service, and store.
func TestDeadLetterRoutesOverRealValkey(t *testing.T) {
	a, s := claimSetup(t)
	const id = "01950000-0000-7000-8000-000000000001"
	enqueue(t, a, id, ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	svc, err := administration.NewService(administration.ServiceDeps{
		Repo: NewEndpointStore(a), DeadLetters: NewDeadLetterStore(a), Audit: NewAuditSink(a),
		AdminSecret: "admin-secret-value-016", Gen: gen.Crypto{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := administration.Handler(svc)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-secret-value-016")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}

	w := call(http.MethodGet, "/admin/v1/dead-letters", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"message_id":"`+id+`"`) ||
		!strings.Contains(w.Body.String(), `"chat_id":"-1"`) || strings.Contains(w.Body.String(), "d-"+id) {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	if w := call(http.MethodGet, "/admin/v1/dead-letters/"+id, ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"outcome":"nack"`) {
		t.Errorf("get = %d %s", w.Code, w.Body.String())
	}

	a.testDo(t, "HSET", "hr1:d:"+messageDedupDigest(id), "message_id", "other")
	if w := call(http.MethodPost, "/admin/v1/dead-letters/"+id+"/replay", ""); w.Code != http.StatusConflict ||
		!strings.Contains(w.Body.String(), "deduplication_conflict") {
		t.Errorf("conflicting replay = %d %s", w.Code, w.Body.String())
	}
	w = call(http.MethodPost, "/admin/v1/dead-letters/"+id+"/replay", `{"deduplication_conflict_resolution":"keep_current"}`)
	var out administration.ReplayView
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &out) != nil || out.Status != "replayed" || out.DeliveryCycle != 2 ||
		out.QueuePosition != "head" || out.DeduplicationResolution != "kept_current" {
		t.Fatalf("replay = %d %s", w.Code, w.Body.String())
	}
	if w := call(http.MethodPost, "/admin/v1/dead-letters/"+id+"/replay", ""); w.Code != http.StatusNotFound ||
		!strings.Contains(w.Body.String(), "dead_letter_not_found") {
		t.Errorf("second replay = %d %s", w.Code, w.Body.String())
	}
	if d := claimNext(t, s, "op-2", "dlv_2"); d.MessageID != id || d.DeliveryCycle != 2 {
		t.Errorf("claim after replay = %+v", d)
	}
}

package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/maxp/hookrelay/internal/delivery"
)

func ackReq(token string) delivery.AckRequest {
	sum := sha256.Sum256([]byte(token))
	return delivery.AckRequest{Token: token, TokenDigest: hex.EncodeToString(sum[:])}
}

func exists(t *testing.T, a *Adapter, key string) bool {
	t.Helper()
	n, err := a.client.Do(context.Background(), a.client.B().Exists().Key(key).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// enqueueJSON accepts a message with a realistic blob so ack can read
// received_ms.
func enqueueJSON(t *testing.T, a *Adapter, messageID, rid string) {
	t.Helper()
	r := acceptReq(messageID, "d-"+messageID, "b-"+messageID)
	r.RecipientIdentity = rid
	r.MessageJSON = []byte(`{"message_id":"` + messageID + `","received_ms":1740000000123,"payload":{"n":12345678901234567890}}`)
	NewMessageAcceptor(a, testLimits()).Accept(context.Background(), r)
}

// TestAckWithNextHead pins the success path with a remaining message: the
// next head becomes ready with a fresh sequence, success metadata and the
// terminal tombstone remain, and every plaintext token copy is gone.
func TestAckWithNextHead(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	req := claimReq("op-1", "args", "dlv_token1")
	req.ConsumerInstanceID = "worker-1"
	claimed := s.Claim(ctx, req)

	res := s.Ack(ctx, ackReq("dlv_token1"))
	if res.Outcome != delivery.AckAcknowledged || res.MessageID != "m1" || res.RecipientIdentity != ridA || res.DeliveryCycle != 1 || res.Attempt != 1 || res.AcknowledgedMs < claimed.Delivery.ClaimedMs {
		t.Fatalf("ack = %+v", res)
	}

	success := "hr1:success:m1"
	for field, want := range map[string]string{
		"recipient_scope": "chat", "bot_platform": "telegram", "received_ms": "1740000000123",
		"acknowledged_ms": itoa64(res.AcknowledgedMs), "delivery_cycle": "1", "attempt_count": "1", "consumer_instance_id": "worker-1",
	} {
		if got := hget(t, a, success, field); got != want {
			t.Errorf("success %s = %q, want %q", field, got, want)
		}
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key(success).Build()).AsInt64(); ttl <= 0 || ttl > SuccessTTL.Milliseconds() {
		t.Errorf("success TTL = %d", ttl)
	}
	if exists(t, a, "hr1:m:m1") || exists(t, a, "hr1:a:m1") {
		t.Error("acknowledged blob or history kept")
	}
	queue, _ := a.client.Do(ctx, a.client.B().Lrange().Key("hr1:r:"+ridA+":q").Start(0).Stop(-1).Build()).AsStrSlice()
	if len(queue) != 1 || queue[0] != "m2" {
		t.Errorf("queue = %v", queue)
	}
	state := "hr1:r:" + ridA + ":s"
	for field, want := range map[string]string{"status": "ready", "head_message_id": "m2", "delivery_cycle": "1", "attempt": "1"} {
		if got := hget(t, a, state, field); got != want {
			t.Errorf("state %s = %q, want %q", field, got, want)
		}
	}
	for _, f := range []string{"delivery_token", "lease_expires_ms", "claimed_ms", "consumer_instance_id"} {
		if n, _ := a.client.Do(ctx, a.client.B().Hexists().Key(state).Field(f).Build()).AsInt64(); n != 0 {
			t.Errorf("state kept %s", f)
		}
	}
	if seq, _ := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:ready").Member(ridA).Build()).AsInt64(); seq != 2 {
		t.Errorf("ready score = %d, want the fresh sequence 2", seq)
	}
	if exists(t, a, "hr1:leases") {
		t.Error("lease member kept")
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 1 {
		t.Errorf("counter = %d", n)
	}
	tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
	if got := hget(t, a, tomb, "state"); got != "acknowledged" {
		t.Errorf("tombstone state = %q", got)
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key(tomb).Build()).AsInt64(); ttl <= 0 || ttl > TombstoneTTL.Milliseconds() {
		t.Errorf("tombstone TTL = %d", ttl)
	}
	if got := hget(t, a, "hr1:op:op-1", "state"); got != "no_longer_active" {
		t.Errorf("op state = %q", got)
	}
	// No plaintext token remains anywhere.
	for _, k := range []string{tomb, "hr1:op:op-1", state} {
		if n, _ := a.client.Do(ctx, a.client.B().Hexists().Key(k).Field("delivery_token").Build()).AsInt64(); n != 0 {
			t.Errorf("%s still holds the token", k)
		}
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key("hr1:op:op-1").Build()).AsInt64(); ttl <= 0 {
		t.Errorf("op record lost its TTL: %d", ttl)
	}

	// Claim replay now reports the completed attempt; the next head is
	// claimable.
	if r := s.Claim(ctx, claimReq("op-1", "args", "dlv_x")); r.Outcome != delivery.ClaimNoLongerActive {
		t.Errorf("claim replay after ack = %+v", r)
	}
	if r := s.Claim(ctx, claimReq("op-2", "args", "dlv_token2")); r.Outcome != delivery.ClaimClaimed || r.Delivery.MessageID != "m2" {
		t.Errorf("next claim = %+v", r)
	}
}

// TestAckDrainsQueue pins the empty-queue branch and the idempotent repeat.
func TestAckDrainsQueue(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	first := s.Ack(ctx, ackReq("dlv_token1"))
	if first.Outcome != delivery.AckAcknowledged {
		t.Fatalf("ack = %+v", first)
	}
	for _, k := range []string{"hr1:r:" + ridA + ":q", "hr1:r:" + ridA + ":s", "hr1:ready", "hr1:leases"} {
		if exists(t, a, k) {
			t.Errorf("%s kept after draining", k)
		}
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 0 {
		t.Errorf("counter = %d", n)
	}

	before := snapshot(t, a)
	repeat := s.Ack(ctx, ackReq("dlv_token1"))
	if repeat.Outcome != delivery.AckAlreadyAcknowledged || repeat.MessageID != "m1" || repeat.AcknowledgedMs != first.AcknowledgedMs {
		t.Errorf("repeat = %+v, want the recorded result", repeat)
	}
	assertUnchanged(t, a, before, "repeated ack")
}

// TestAckRefusals pins not_found, stale, recipient_blocked, and wrong_type
// with no mutation.
func TestAckRefusals(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(a *Adapter)
		token  string
		want   delivery.AckOutcome
	}{
		{"unknown token", nil, "dlv_unknown", delivery.AckNotFound},
		{"superseded token", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token", "dlv_other") }, "dlv_token1", delivery.AckStale},
		{"lease expired", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1") }, "dlv_token1", delivery.AckStale},
		{"head moved", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "m9") }, "dlv_token1", delivery.AckStale},
		{"blocked recipient", func(a *Adapter) {
			a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "queue_head_mismatch")
		}, "dlv_token1", delivery.AckRecipientBlocked},
		{"wrong type", func(a *Adapter) { a.testDo(t, "SET", "hr1:success:m1", "x") }, "dlv_token1", delivery.AckInternalFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s := claimSetup(t)
			ctx := context.Background()
			enqueueJSON(t, a, "m1", ridA)
			s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
			if tc.mutate != nil {
				tc.mutate(a)
			}
			before := snapshot(t, a)
			if r := s.Ack(ctx, ackReq(tc.token)); r.Outcome != tc.want {
				t.Fatalf("ack = %+v, want %s", r, tc.want)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestAckArguments pins argument and key validation.
func TestAckArguments(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:t:d", "hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:stats:queued_messages"}
	valid := []string{"dlv_t", "d", "86400000", "3600000", "hr1"}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":         valid[:4],
		"empty token":     with(0, ""),
		"zero ttl":        with(3, "0"),
		"digest mismatch": with(1, "other"),
	} {
		if _, err := a.RunScript(ctx, "ack_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

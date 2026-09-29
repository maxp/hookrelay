package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/ingestion"
)

const (
	ridA = "telegram:42:chat:-1"
	ridB = "telegram:42:chat:-2"
)

func claimLimits() ClaimLimits {
	return ClaimLimits{MaxActiveLeases: 10, InitialLeaseDuration: time.Minute}
}

func claimReq(op, args, token string) delivery.ClaimRequest {
	sum := sha256.Sum256([]byte(token))
	return delivery.ClaimRequest{OperationID: op, ArgsDigest: args, Token: token, TokenDigest: hex.EncodeToString(sum[:]), RecordEmpty: true}
}

// enqueue accepts one message for a recipient through accept_v1.
func enqueue(t *testing.T, a *Adapter, messageID, rid string) {
	t.Helper()
	r := acceptReq(messageID, "d-"+messageID, "b-"+messageID)
	r.RecipientIdentity = rid
	r.MessageJSON = []byte(`{"message_id":"` + messageID + `"}`)
	if res := NewMessageAcceptor(a, testLimits()).Accept(context.Background(), r); res.Outcome != ingestion.AcceptAccepted {
		t.Fatalf("enqueue %s = %+v", messageID, res)
	}
}

func (a *Adapter) testDo(t *testing.T, args ...string) {
	t.Helper()
	if _, err := a.client.Do(context.Background(), a.client.B().Arbitrary(args...).Build()).ToMessage(); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
}

func claimSetup(t *testing.T) (*Adapter, *DeliveryStore) {
	t.Helper()
	a := testAdapter(t, false)
	flushAll(t, a)
	gate(t, a, false)
	return a, NewDeliveryStore(a, claimLimits())
}

// TestClaimClaimedAndKeys pins the claimed tuple and every affected key:
// leased head state, lease index, ready removal, operation and token
// records with their TTLs.
func TestClaimClaimedAndKeys(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	enqueue(t, a, "m2", ridA)

	req := claimReq("op-1", "args-1", "dlv_token1")
	req.ConsumerInstanceID = "worker-1"
	res := s.Claim(ctx, req)
	d := res.Delivery
	if res.Outcome != delivery.ClaimClaimed || d.Token != "dlv_token1" || d.MessageID != "m1" || d.DeliveryCycle != 1 || d.Attempt != 1 {
		t.Fatalf("claim = %+v", res)
	}
	if d.LeaseExpiresMs != d.ClaimedMs+time.Minute.Milliseconds() || string(res.MessageJSON) != `{"message_id":"m1"}` {
		t.Errorf("lease/message = %+v %s", d, res.MessageJSON)
	}

	state := "hr1:r:" + ridA + ":s"
	for field, want := range map[string]string{
		"status": "leased", "delivery_token": "dlv_token1", "head_message_id": "m1",
		"claimed_ms": itoa64(d.ClaimedMs), "lease_expires_ms": itoa64(d.LeaseExpiresMs), "consumer_instance_id": "worker-1",
	} {
		if got := hget(t, a, state, field); got != want {
			t.Errorf("state %s = %q, want %q", field, got, want)
		}
	}
	if score, _ := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:leases").Member(ridA).Build()).AsInt64(); score != d.LeaseExpiresMs {
		t.Errorf("lease score = %d", score)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Zcard().Key("hr1:ready").Build()).AsInt64(); n != 0 {
		t.Errorf("ready members = %d, want the leased recipient removed", n)
	}
	for field, want := range map[string]string{
		"kind": "claim", "args_digest": "args-1", "state": "active", "delivery_token": "dlv_token1", "message_id": "m1",
		"recipient_identity": ridA, "delivery_cycle": "1", "attempt": "1", "lease_expires_ms": itoa64(d.LeaseExpiresMs),
	} {
		if got := hget(t, a, "hr1:op:op-1", field); got != want {
			t.Errorf("op %s = %q, want %q", field, got, want)
		}
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key("hr1:op:op-1").Build()).AsInt64(); ttl <= 0 || ttl > ClaimOpTTL.Milliseconds() {
		t.Errorf("op TTL = %d", ttl)
	}
	tokenKey := "hr1:t:" + req.TokenDigest
	for field, want := range map[string]string{"state": "active", "recipient_identity": ridA, "message_id": "m1", "operation_id": "op-1"} {
		if got := hget(t, a, tokenKey, field); got != want {
			t.Errorf("token %s = %q, want %q", field, got, want)
		}
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key(tokenKey).Build()).AsInt64(); ttl <= ClaimOpTTL.Milliseconds() || ttl > (ClaimOpTTL+time.Minute).Milliseconds() {
		t.Errorf("token TTL = %d, want 10 min plus the lease", ttl)
	}

	// The next message of the leased recipient is not claimable.
	if r := s.Claim(ctx, claimReq("op-2", "args-2", "dlv_token2")); r.Outcome != delivery.ClaimEmpty {
		t.Errorf("second claim = %+v, want empty while the head is leased", r)
	}
}

// TestClaimReplay pins replay_active (same token, no mutation), operation
// conflict, replay_empty, record_empty=0, and claim_no_longer_active.
func TestClaimReplay(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)

	first := s.Claim(ctx, claimReq("op-1", "args-1", "dlv_token1"))
	before := snapshot(t, a)
	replay := s.Claim(ctx, claimReq("op-1", "args-1", "dlv_other"))
	if replay.Outcome != delivery.ClaimReplayActive || replay.Delivery != first.Delivery || string(replay.MessageJSON) != string(first.MessageJSON) {
		t.Errorf("replay = %+v, want the recorded attempt", replay)
	}
	if r := s.Claim(ctx, claimReq("op-1", "args-other", "dlv_other")); r.Outcome != delivery.ClaimOperationConflict {
		t.Errorf("changed args = %+v", r)
	}
	assertUnchanged(t, a, before, "replay and conflict")

	// Empty outcome recorded only when asked.
	quiet := claimReq("op-quiet", "args", "dlv_q")
	quiet.RecordEmpty = false
	if r := s.Claim(ctx, quiet); r.Outcome != delivery.ClaimEmpty {
		t.Fatalf("quiet empty = %+v", r)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Exists().Key("hr1:op:op-quiet").Build()).AsInt64(); n != 0 {
		t.Error("record_empty=0 recorded an operation")
	}
	if r := s.Claim(ctx, claimReq("op-empty", "args", "dlv_e")); r.Outcome != delivery.ClaimEmpty {
		t.Fatalf("empty = %+v", r)
	}
	enqueue(t, a, "m9", ridB)
	if r := s.Claim(ctx, claimReq("op-empty", "args", "dlv_e2")); r.Outcome != delivery.ClaimReplayEmpty {
		t.Errorf("repeated empty operation = %+v, want replay_empty even though work appeared", r)
	}

	// The attempt ends (token superseded, lease expired, or op marked done).
	for name, mutate := range map[string]func(){
		"token superseded": func() { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token", "dlv_new") },
		"lease expired":    func() { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1") },
		"op completed":     func() { a.testDo(t, "HSET", "hr1:op:op-1", "state", "no_longer_active") },
	} {
		a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token", "dlv_token1", "lease_expires_ms", itoa64(first.Delivery.LeaseExpiresMs))
		a.testDo(t, "HSET", "hr1:op:op-1", "state", "active")
		mutate()
		if r := s.Claim(ctx, claimReq("op-1", "args-1", "dlv_x")); r.Outcome != delivery.ClaimNoLongerActive {
			t.Errorf("%s: replay = %+v, want claim_no_longer_active", name, r)
		}
	}
}

// TestClaimLeaseLimit pins the work-pool-wide limit over unexpired leases.
func TestClaimLeaseLimit(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	s := NewDeliveryStore(a, ClaimLimits{MaxActiveLeases: 1, InitialLeaseDuration: time.Minute})
	enqueue(t, a, "m1", ridA)
	enqueue(t, a, "m2", ridB)

	if r := s.Claim(ctx, claimReq("op-1", "a", "dlv_1")); r.Outcome != delivery.ClaimClaimed {
		t.Fatalf("first = %+v", r)
	}
	before := snapshot(t, a)
	if r := s.Claim(ctx, claimReq("op-2", "a", "dlv_2")); r.Outcome != delivery.ClaimLimitExceeded {
		t.Fatalf("over limit = %+v", r)
	}
	assertUnchanged(t, a, before, "limit_exceeded")

	// An expired lease no longer counts.
	a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
	if r := s.Claim(ctx, claimReq("op-3", "a", "dlv_3")); r.Outcome != delivery.ClaimClaimed || r.Delivery.MessageID != "m2" {
		t.Errorf("after expiry = %+v", r)
	}
}

// TestClaimBlocksInconsistentCandidates pins skip-and-continue: every
// inconsistent candidate is blocked (marker + blocked index, removed from
// ready and leases) and the claim proceeds to the next valid head; stale
// derived entries are dropped without a marker.
func TestClaimBlocksInconsistentCandidates(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(a *Adapter)
		reason string // "" means dropped without a marker
	}{
		{"existing marker", func(a *Adapter) {
			a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "5", "reason_code", "head_message_missing")
		}, "head_message_missing"},
		{"queue wrong type", func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q")
			a.testDo(t, "SET", "hr1:r:"+ridA+":q", "x")
		}, "unsupported_key_type"},
		{"head state missing", func(a *Adapter) { a.testDo(t, "DEL", "hr1:r:"+ridA+":s") }, "head_state_missing"},
		{"head mismatch", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "other") }, "queue_head_mismatch"},
		{"unknown status", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "retry_wait") }, "queue_head_mismatch"},
		{"head blob missing", func(a *Adapter) { a.testDo(t, "DEL", "hr1:m:m1") }, "head_message_missing"},
		{"drained queue", func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q", "hr1:r:"+ridA+":s")
		}, ""},
		{"leased head in ready", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "status", "leased") }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s := claimSetup(t)
			ctx := context.Background()
			enqueue(t, a, "m1", ridA) // ready sequence 1: scanned first
			enqueue(t, a, "m2", ridB)
			tc.break_(a)

			res := s.Claim(ctx, claimReq("op", "a", "dlv_t"))
			if res.Outcome != delivery.ClaimClaimed || res.Delivery.MessageID != "m2" {
				t.Fatalf("claim = %+v, want the next valid head m2", res)
			}
			if in, _ := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:ready").Member(ridA).Build()).AsInt64(); in != 0 {
				t.Error("inconsistent candidate still ready")
			}
			markerType, _ := a.client.Do(ctx, a.client.B().Type().Key("hr1:q:"+ridA).Build()).ToString()
			if tc.reason == "" {
				if markerType != "none" || res.BlockedDetected != 0 {
					t.Errorf("stale entry blocked: marker %s, detected %d", markerType, res.BlockedDetected)
				}
				return
			}
			if got := hget(t, a, "hr1:q:"+ridA, "reason_code"); got != tc.reason {
				t.Errorf("reason = %q, want %q", got, tc.reason)
			}
			if _, err := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:blocked").Member(ridA).Build()).AsInt64(); err != nil {
				t.Errorf("blocked index member missing: %v", err)
			}
			wantNew := int64(1)
			if tc.name == "existing marker" {
				wantNew = 0
			}
			if res.BlockedDetected != wantNew {
				t.Errorf("blocked_detected = %d, want %d", res.BlockedDetected, wantNew)
			}
		})
	}
}

// TestClaimOrderAndReload pins fairness order by ready sequence and the
// EVAL reload after SCRIPT FLUSH.
func TestClaimOrderAndReload(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridB)
	enqueue(t, a, "m2", ridA)
	if r := s.Claim(ctx, claimReq("op-1", "a", "dlv_1")); r.Delivery.MessageID != "m1" {
		t.Errorf("first claim = %+v, want the lowest ready sequence", r)
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := s.Claim(ctx, claimReq("op-2", "a", "dlv_2")); r.Outcome != delivery.ClaimClaimed || r.Delivery.MessageID != "m2" {
		t.Errorf("claim after SCRIPT FLUSH = %+v", r)
	}
}

// TestClaimWrongTypeAndArguments pins the global key-type refusal and
// argument validation.
func TestClaimWrongTypeAndArguments(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	for _, key := range []string{"hr1:ready", "hr1:leases", "hr1:blocked", "hr1:op:op-1"} {
		flushAll(t, a)
		a.testDo(t, "SET", key, "x")
		before := snapshot(t, a)
		if r := s.Claim(ctx, claimReq("op-1", "a", "dlv_1")); r.Outcome != delivery.ClaimInternalFailure {
			t.Errorf("%s poisoned: %+v", key, r)
		}
		assertUnchanged(t, a, before, key)
	}

	flushAll(t, a)
	keys := []string{"hr1:ready", "hr1:leases", "hr1:blocked"}
	valid := []string{"op", "args", "10", "600000", "60000", "dlv_t", "digest", "", "1", "hr1"}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":         valid[:9],
		"empty operation": with(0, ""),
		"zero limit":      with(2, "0"),
		"bad flag":        with(8, "yes"),
		"prefix mismatch": with(9, "hr2"),
	} {
		if _, err := a.RunScript(ctx, "claim_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if n, _ := a.client.Do(ctx, a.client.B().Dbsize().Build()).ToMessage(); nInt(n) != 0 {
		t.Errorf("rejected arguments left %d keys", nInt(n))
	}
}

// TestDeliveryStats pins the gauge sources.
func TestDeliveryStats(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueue(t, a, "m1", ridA)
	enqueue(t, a, "m2", ridB)
	s.Claim(ctx, claimReq("op", "a", "dlv_t"))
	a.testDo(t, "ZADD", "hr1:blocked", "1", "telegram:42:chat:-9")
	st, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st != (delivery.Stats{ActiveLeases: 1, ReadyRecipients: 1, BlockedRecipients: 1, QueuedMessages: 2}) {
		t.Errorf("stats = %+v", st)
	}
}

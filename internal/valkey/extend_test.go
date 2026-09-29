package valkey

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/delivery"
)

// extendSetup returns a store with one-minute leases, 30 s extensions, and
// a 100 s maximum lease lifetime.
func extendSetup(t *testing.T) (*Adapter, *DeliveryStore) {
	t.Helper()
	a, _ := claimSetup(t)
	return a, NewDeliveryStore(a, ClaimLimits{
		MaxActiveLeases: 10, InitialLeaseDuration: time.Minute,
		LeaseExtension: 30 * time.Second, MaxLeaseLifetime: 100 * time.Second,
	})
}

func extendReq(token, op, args string) delivery.ExtendRequest {
	r := ackReq(token)
	return delivery.ExtendRequest{Token: r.Token, TokenDigest: r.TokenDigest, OperationID: op, ArgsDigest: args}
}

// TestExtendPushesTheDeadline pins the extended tuple and every affected
// key (state, lease score, token record deadline and TTL, the extend
// operation record), the cap at the maximum lifetime, idempotent replay,
// and operation conflicts.
func TestExtendPushesTheDeadline(t *testing.T) {
	a, s := extendSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	claimed := s.Claim(ctx, claimReq("op-claim", "args", "dlv_token1")).Delivery
	maxLease := claimed.ClaimedMs + 100_000
	claimOp := snapshot(t, a)["hr1:op:op-claim"]

	res := s.Extend(ctx, extendReq("dlv_token1", "ext-1", "ext-args-1"))
	if res.Outcome != delivery.ExtendExtended || res.MessageID != "m1" || res.LeaseExpiresMs != claimed.LeaseExpiresMs+30_000 ||
		res.MaxLeaseExpiresMs != maxLease || res.DeliveryCycle != 1 || res.Attempt != 1 || res.RecipientIdentity != ridA {
		t.Fatalf("extend = %+v", res)
	}
	state := "hr1:r:" + ridA + ":s"
	if got := hget(t, a, state, "lease_expires_ms"); got != itoa64(res.LeaseExpiresMs) {
		t.Errorf("state lease = %s", got)
	}
	if sc, _ := score(t, a, "hr1:leases", ridA); sc != res.LeaseExpiresMs {
		t.Errorf("lease score = %d", sc)
	}
	tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
	if got := hget(t, a, tomb, "lease_expires_ms"); got != itoa64(res.LeaseExpiresMs) || hget(t, a, tomb, "state") != "active" {
		t.Errorf("token record = %v", hgetall(t, a, tomb))
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key(tomb).Build()).AsInt64(); ttl < ClaimOpTTL.Milliseconds()+60_000 {
		t.Errorf("token TTL = %d, want at least the claim op TTL plus the remaining lease", ttl)
	}
	assertHash(t, a, "hr1:op:ext-1", map[string]string{
		"kind": "extend", "args_digest": "ext-args-1", "state": "completed", "message_id": "m1",
		"lease_expires_ms": itoa64(res.LeaseExpiresMs), "max_lease_expires_ms": itoa64(maxLease),
	})
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key("hr1:op:ext-1").Build()).AsInt64(); ttl <= 0 || ttl > ClaimOpTTL.Milliseconds() {
		t.Errorf("extend op TTL = %d", ttl)
	}
	if snapshot(t, a)["hr1:op:op-claim"] != claimOp {
		t.Error("the claim operation record changed")
	}

	// Replay returns the recorded deadline without extending again; other
	// arguments or another operation kind conflict.
	before := snapshot(t, a)
	if r := s.Extend(ctx, extendReq("dlv_token1", "ext-1", "ext-args-1")); r.Outcome != delivery.ExtendReplay ||
		r.LeaseExpiresMs != res.LeaseExpiresMs || r.MaxLeaseExpiresMs != maxLease || r.MessageID != "m1" {
		t.Errorf("replay = %+v", r)
	}
	for name, req := range map[string]delivery.ExtendRequest{
		"other arguments": extendReq("dlv_token1", "ext-1", "other-args"),
		"claim operation": extendReq("dlv_token1", "op-claim", "ext-args-x"),
	} {
		if r := s.Extend(ctx, req); r.Outcome != delivery.ExtendOperationConflict {
			t.Errorf("%s = %+v, want operation_conflict", name, r)
		}
	}
	assertUnchanged(t, a, before, "replay and conflicts")

	// The next extension is capped at the maximum lifetime; after that the
	// lease cannot grow.
	if r := s.Extend(ctx, extendReq("dlv_token1", "ext-2", "ext-args-2")); r.Outcome != delivery.ExtendExtended || r.LeaseExpiresMs != maxLease {
		t.Errorf("capped extension = %+v, want the maximum %d", r, maxLease)
	}
	before = snapshot(t, a)
	if r := s.Extend(ctx, extendReq("dlv_token1", "ext-3", "ext-args-3")); r.Outcome != delivery.ExtendMaximumLifetimeReached {
		t.Errorf("extension at the maximum = %+v", r)
	}
	assertUnchanged(t, a, before, "extension at the maximum")

	// The extended lease is acknowledged normally.
	if r := s.Ack(ctx, ackReq("dlv_token1")); r.Outcome != delivery.AckAcknowledged {
		t.Errorf("ack = %+v", r)
	}
}

// TestExtendLeaseWithoutAttemptStart pins the claim_v1-era lease: the
// maximum lifetime is measured from claimed_ms.
func TestExtendLeaseWithoutAttemptStart(t *testing.T) {
	a, s := extendSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	claimed := s.Claim(context.Background(), claimReq("op-claim", "args", "dlv_token1")).Delivery
	a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "attempt_started_ms")
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendExtended ||
		r.MaxLeaseExpiresMs != claimed.ClaimedMs+100_000 {
		t.Errorf("extend = %+v", r)
	}
}

// TestExtendRefusals pins every refusal status and the absence of mutation.
func TestExtendRefusals(t *testing.T) {
	state := "hr1:r:" + ridA + ":s"
	cases := []struct {
		name   string
		mutate func(t *testing.T, a *Adapter, s *DeliveryStore)
		token  string
		want   delivery.ExtendOutcome
	}{
		{"unknown token", nil, "dlv_unknown", delivery.ExtendNotFound},
		{"acknowledged", func(t *testing.T, a *Adapter, s *DeliveryStore) { s.Ack(context.Background(), ackReq("dlv_token1")) }, "dlv_token1", delivery.ExtendStale},
		{"nacked", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			s.Nack(context.Background(), nackReq("dlv_token1", ""))
		}, "dlv_token1", delivery.ExtendStale},
		{"lease past its deadline", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "HSET", state, "lease_expires_ms", "1") }, "dlv_token1", delivery.ExtendStale},
		{"superseded token", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "HSET", state, "delivery_token", "dlv_other")
		}, "dlv_token1", delivery.ExtendStale},
		{"head moved", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "HSET", state, "head_message_id", "m9") }, "dlv_token1", delivery.ExtendStale},
		{"blocked recipient", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "queue_head_mismatch")
		}, "dlv_token1", delivery.ExtendRecipientBlocked},
		{"op record wrong type", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "SET", "hr1:op:ext-1", "x") }, "dlv_token1", delivery.ExtendInternalFailure},
		{"leases wrong type", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "DEL", "hr1:leases")
			a.testDo(t, "SET", "hr1:leases", "x")
		}, "dlv_token1", delivery.ExtendInternalFailure},
		{"state wrong type", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "DEL", state)
			a.testDo(t, "SET", state, "x")
		}, "dlv_token1", delivery.ExtendInternalFailure},
		{"malformed attempt start", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "HSET", state, "attempt_started_ms", "x")
		}, "dlv_token1", delivery.ExtendInternalFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s := extendSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			s.Claim(context.Background(), claimReq("op-claim", "args", "dlv_token1"))
			if tc.mutate != nil {
				tc.mutate(t, a, s)
			}
			before := snapshot(t, a)
			if r := s.Extend(context.Background(), extendReq(tc.token, "ext-1", "a")); r.Outcome != tc.want {
				t.Fatalf("extend = %+v, want %s", r, tc.want)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestExtendRacingExpiry pins both orders of extension and expiry: a lease
// past its deadline cannot be extended (expiry still applies), and an
// extended lease is no longer due.
func TestExtendRacingExpiry(t *testing.T) {
	a, s := expirySetup(t)
	enqueueJSON(t, a, "m1", ridA)
	claimAndLapse(t, s, "op-claim", "dlv_token1", "")
	before := snapshot(t, a)
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendStale {
		t.Errorf("extend after the deadline = %+v, want stale", r)
	}
	assertUnchanged(t, a, before, "extend after the deadline")
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryRetryScheduled {
		t.Errorf("expiry after a refused extension = %+v", r)
	}

	a, s = extendSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(context.Background(), claimReq("op-claim", "args", "dlv_token1"))
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendExtended {
		t.Fatalf("extend = %+v", r)
	}
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryNotDue {
		t.Errorf("expiry of an extended lease = %+v, want not_due", r)
	}
}

// TestExtendArgumentsAndReload pins argument validation and the EVAL
// reload after SCRIPT FLUSH.
func TestExtendArgumentsAndReload(t *testing.T) {
	a, s := extendSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:t:d", "hr1:op:op", "hr1:leases"}
	valid := []string{"dlv_t", "d", "op", "args", "30000", "300000", "600000", "hr1"}
	if _, err := a.RunScript(ctx, "extend_v1", keys, valid); err != nil {
		t.Fatalf("valid call failed: %v", err)
	}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":            valid[:7],
		"empty token":        with(0, ""),
		"digest mismatch":    with(1, "other"),
		"operation mismatch": with(2, "other"),
		"empty args digest":  with(3, ""),
		"zero extension":     with(4, "0"),
		"zero lifetime":      with(5, "0"),
		"zero op ttl":        with(6, "0"),
		"prefix mismatch":    with(7, "hr2"),
	} {
		if _, err := a.RunScript(ctx, "extend_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "extend_v1", keys[:2], valid); err == nil || errors.Is(err, ErrNotDispatched) {
		t.Errorf("too few keys: err = %v", err)
	}

	flushAll(t, a)
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(ctx, claimReq("op-claim", "args", "dlv_token1"))
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := s.Extend(ctx, extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendExtended {
		t.Errorf("extend after SCRIPT FLUSH = %+v", r)
	}
}

// TestExtendAfterExpiryRan pins extension after maintenance already expired
// the lease: the token is in its expired phase, so the attempt is stale.
func TestExtendAfterExpiryRan(t *testing.T) {
	a, s := expirySetup(t)
	enqueueJSON(t, a, "m1", ridA)
	claimAndLapse(t, s, "op-claim", "dlv_token1", "")
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryRetryScheduled {
		t.Fatalf("expire = %+v", r)
	}
	before := snapshot(t, a)
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendStale {
		t.Errorf("extend after expiry = %+v, want stale", r)
	}
	assertUnchanged(t, a, before, "extend after expiry")
}

// TestExtendReplayOfCorruptRecord pins that a malformed recorded extension
// is wrong_type, never a successful replay with empty values, and that
// replay does not depend on the token record or lease index.
func TestExtendReplayOfCorruptRecord(t *testing.T) {
	a, s := extendSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(context.Background(), claimReq("op-claim", "args", "dlv_token1"))
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendExtended {
		t.Fatalf("extend = %+v", r)
	}
	a.testDo(t, "DEL", "hr1:leases")
	a.testDo(t, "SET", "hr1:leases", "x")
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendReplay {
		t.Errorf("replay beside a corrupt lease index = %+v", r)
	}
	a.testDo(t, "HDEL", "hr1:op:ext-1", "lease_expires_ms")
	if r := s.Extend(context.Background(), extendReq("dlv_token1", "ext-1", "a")); r.Outcome != delivery.ExtendInternalFailure {
		t.Errorf("replay of a corrupt record = %+v, want internal failure", r)
	}
}

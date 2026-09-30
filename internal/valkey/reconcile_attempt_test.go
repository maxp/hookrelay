package valkey

import (
	"bytes"
	"context"
	"reflect"
	"testing"

	"github.com/maxp/hookrelay/internal/observability"
)

func claimedAttempt(t *testing.T) (*Adapter, *DeliveryStore, string) {
	t.Helper()
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	req := claimReq("op-1", "args-1", "dlv_token1")
	if got := s.Claim(context.Background(), req); got.Outcome != "claimed" {
		t.Fatalf("claim = %+v", got)
	}
	return a, s, req.TokenDigest
}

func runAttemptFor(t *testing.T, a *Adapter, rid, mode, digest, reason string) *Result {
	t.Helper()
	res, err := a.RunScript(context.Background(), "reconcile_attempt_v1",
		[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
		[]string{mode, rid, digest, reason, "600000", "hr1"})
	if err != nil {
		t.Fatalf("reconcile attempt: %v", err)
	}
	return res
}

func runAttempt(t *testing.T, a *Adapter, mode, digest, reason string) *Result {
	t.Helper()
	return runAttemptFor(t, a, ridA, mode, digest, reason)
}

func TestReconcileAttemptScriptConsistentRepairLegacyAndReload(t *testing.T) {
	a, _, digest := claimedAttempt(t)
	if res := runAttempt(t, a, "verify", digest, ""); res.Status != "consistent" {
		t.Fatalf("consistent = %+v", res)
	}

	a.testDo(t, "ZREM", "hr1:leases", ridA)
	if res := runAttempt(t, a, "verify", digest, ""); res.Status != "repaired" {
		t.Fatalf("repair = %+v", res)
	}
	want := hget(t, a, "hr1:r:"+ridA+":s", "lease_expires_ms")
	if got, ok := score(t, a, "hr1:leases", ridA); !ok || itoa64(got) != want {
		t.Fatalf("lease score = %d %v, want %s", got, ok, want)
	}

	a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token_digest")
	before := snapshot(t, a)
	if res := runAttempt(t, a, "verify", digest, ""); res.Status != "legacy" {
		t.Fatalf("legacy = %+v", res)
	}
	assertUnchanged(t, a, before, "legacy attempt")

	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token_digest", digest)
	a.testDo(t, "ZREM", "hr1:leases", ridA)
	a.testDo(t, "SCRIPT", "FLUSH")
	if res := runAttempt(t, a, "verify", digest, ""); res.Status != "repaired" {
		t.Fatalf("after SCRIPT FLUSH = %+v", res)
	}
}

func TestReconcileAttemptScriptBlocksCrossLinkFailures(t *testing.T) {
	cases := []struct {
		name   string
		poison func(*testing.T, *Adapter, string)
		reason string
	}{
		{"queue head mismatch", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "LPUSH", "hr1:r:"+ridA+":q", "other")
		}, "active_attempt_structure"},
		{"malformed leased state", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "attempt", "zero")
		}, "active_attempt_state"},
		{"plaintext token missing", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token")
		}, "active_attempt_state"},
		{"digest malformed", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token_digest", "bad")
		}, "active_attempt_state"},
		{"token missing", func(t *testing.T, a *Adapter, digest string) {
			a.testDo(t, "DEL", "hr1:t:"+digest)
		}, "token_missing"},
		{"token wrong type", func(t *testing.T, a *Adapter, digest string) {
			a.testDo(t, "DEL", "hr1:t:"+digest)
			a.testDo(t, "SET", "hr1:t:"+digest, "poison")
		}, "token_type"},
		{"token mismatch", func(t *testing.T, a *Adapter, digest string) {
			a.testDo(t, "HSET", "hr1:t:"+digest, "message_id", "other")
		}, "token_mismatch"},
		{"claim operation missing", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "DEL", "hr1:op:op-1")
		}, "claim_operation_missing"},
		{"claim operation wrong type", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "DEL", "hr1:op:op-1")
			a.testDo(t, "SET", "hr1:op:op-1", "poison")
		}, "claim_operation_type"},
		{"claim operation mismatch", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "HSET", "hr1:op:op-1", "attempt", "2")
		}, "claim_operation_mismatch"},
		{"token ttl missing", func(t *testing.T, a *Adapter, digest string) {
			a.testDo(t, "PERSIST", "hr1:t:"+digest)
		}, "token_ttl"},
		{"token ttl out of bounds", func(t *testing.T, a *Adapter, digest string) {
			a.testDo(t, "PEXPIRE", "hr1:t:"+digest, "1000000")
		}, "token_ttl"},
		{"claim operation ttl missing", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "PERSIST", "hr1:op:op-1")
		}, "claim_operation_ttl"},
		{"claim operation ttl out of bounds", func(t *testing.T, a *Adapter, _ string) {
			a.testDo(t, "PEXPIRE", "hr1:op:op-1", "700000")
		}, "claim_operation_ttl"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _, digest := claimedAttempt(t)
			tc.poison(t, a, digest)
			stateBefore := snapshot(t, a)
			res := runAttempt(t, a, "verify", digest, "")
			if res.Status != "blocked" {
				t.Fatalf("result = %+v", res)
			}
			gotReason, _ := res.Fields[0].ToString()
			if gotReason != tc.reason {
				t.Errorf("reason = %q, want %q", gotReason, tc.reason)
			}
			if got := hget(t, a, "hr1:q:"+ridA, "reason_code"); got != "active_attempt_inconsistent" {
				t.Errorf("marker reason = %q", got)
			}
			after := snapshot(t, a)
			for _, key := range []string{"hr1:r:" + ridA + ":s", "hr1:t:" + digest, "hr1:op:op-1"} {
				if !reflect.DeepEqual(stateBefore[key], after[key]) {
					t.Errorf("authoritative active-attempt record %s was mutated", key)
				}
			}
			for _, index := range []string{"hr1:ready", "hr1:leases", "hr1:retries"} {
				if _, ok := score(t, a, index, ridA); ok {
					t.Errorf("blocked Recipient remains in %s", index)
				}
			}
			if _, ok := score(t, a, "hr1:blocked", ridA); !ok {
				t.Error("blocked index member missing")
			}
		})
	}
}

func TestReconcileAttemptScriptStatusEdges(t *testing.T) {
	a, _, digest := claimedAttempt(t)
	if res := runAttemptFor(t, a, ridB, "verify", digest, ""); res.Status != "not_leased" {
		t.Fatalf("not_leased = %+v", res)
	}
	other := digest[:63] + "0"
	if other == digest {
		other = digest[:63] + "1"
	}
	if res := runAttempt(t, a, "verify", other, ""); res.Status != "changed" {
		t.Fatalf("changed = %+v", res)
	}
	if res := runAttempt(t, a, "block", "", "token_digest_mismatch"); res.Status != "blocked" {
		t.Fatalf("block mode = %+v", res)
	}
	if res := runAttempt(t, a, "verify", digest, ""); res.Status != "already_blocked" {
		t.Fatalf("already_blocked = %+v", res)
	}
	if _, err := a.RunScript(context.Background(), "reconcile_attempt_v1",
		[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
		[]string{"bogus", ridA, digest, "", "600000", "hr1"}); err == nil {
		t.Error("invalid mode was accepted")
	}
}

func TestReconcileAttemptScriptRefusesWrongGlobalIndexWithoutMutation(t *testing.T) {
	a, _, digest := claimedAttempt(t)
	a.testDo(t, "DEL", "hr1:leases")
	a.testDo(t, "SET", "hr1:leases", "poison")
	before := snapshot(t, a)
	res := runAttempt(t, a, "verify", digest, "")
	if res.Status != "wrong_type" {
		t.Fatalf("result = %+v", res)
	}
	reason, _ := res.Fields[0].ToString()
	if reason != "lease_index_type" {
		t.Fatalf("reason = %q", reason)
	}
	assertUnchanged(t, a, before, "wrong-type active-attempt index")
}

func TestReconcileValidatesActiveAttemptAndKeepsOtherRecipientsServiceable(t *testing.T) {
	a, s := consistentState(t)
	digest := hget(t, a, "hr1:r:"+ridA+":s", "delivery_token_digest")
	a.testDo(t, "HSET", "hr1:t:"+digest, "message_id", "other")

	rep := reconcile(t, a, true)
	if rep.Findings["blocked"] != 1 || rep.BlockReasons["active_attempt_inconsistent"] != 1 ||
		rep.Findings["active_attempt_token_mismatch"] != 1 || rep.Hold() != "" {
		t.Fatalf("report = %+v %+v", rep.Findings, rep.BlockReasons)
	}
	if got := hget(t, a, "hr1:q:"+ridA, "reason_code"); got != "active_attempt_inconsistent" {
		t.Fatalf("marker reason = %q", got)
	}
	if got := hget(t, a, "hr1:r:"+ridB+":s", "status"); got != "ready" {
		t.Fatalf("unrelated Recipient status = %q", got)
	}
	if claim := s.Claim(context.Background(), claimReq("op-2", "args-2", "dlv_token2")); claim.Delivery.MessageID != "b1" {
		t.Fatalf("unrelated Recipient claim = %+v", claim)
	}

	again := reconcile(t, a, true)
	if again.Findings["blocked"] != 0 || again.Findings["already_blocked"] != 1 {
		t.Fatalf("second pass = %+v", again.Findings)
	}
}

func TestReconcileBlocksPlaintextTokenDigestMismatchWithoutLoggingToken(t *testing.T) {
	a, _, _ := claimedAttempt(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_token", "dlv_tampered")
	var logs bytes.Buffer
	rep, err := a.Reconcile(context.Background(), ReconcileOptions{
		Full: true, MessageCheckBound: 10, BatchSize: 7, Logger: observability.NewTestLogger("info", &logs),
	})
	if err != nil || rep.Findings["active_attempt_token_digest_mismatch"] != 1 ||
		hget(t, a, "hr1:q:"+ridA, "reason_code") != "active_attempt_inconsistent" {
		t.Fatalf("report = %+v, err = %v", rep.Findings, err)
	}
	if bytes.Contains(logs.Bytes(), []byte("dlv_tampered")) || bytes.Contains(logs.Bytes(), []byte("dlv_token1")) {
		t.Fatalf("Delivery Token leaked to logs: %s", logs.String())
	}
}

func TestReconcileHoldsWrongTypedAttemptGlobalIndex(t *testing.T) {
	a, _, _ := claimedAttempt(t)
	a.testDo(t, "DEL", "hr1:leases")
	a.testDo(t, "SET", "hr1:leases", "poison")
	rep, err := a.Reconcile(context.Background(), ReconcileOptions{Full: true, MessageCheckBound: 10, BatchSize: 7})
	if err == nil && rep.Hold() != "unhandled_inconsistency" {
		t.Fatalf("report = %+v, err = %v", rep, err)
	}
}

func TestReconcileAcceptsLegacyLeasedAttempt(t *testing.T) {
	a, _, _ := claimedAttempt(t)
	a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token_digest")
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "" || rep.Findings["blocked"] != 0 {
		t.Fatalf("report = %+v", rep)
	}
	assertUnchanged(t, a, before, "legacy leased attempt")
}

package valkey

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func reconcile(t *testing.T, a *Adapter, full bool) ReconcileReport {
	t.Helper()
	rep, err := a.Reconcile(context.Background(), ReconcileOptions{Full: full, MessageCheckBound: 10, BatchSize: 7})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return rep
}

func score(t *testing.T, a *Adapter, key, member string) (int64, bool) {
	t.Helper()
	v, err := a.client.Do(context.Background(), a.client.B().Zscore().Key(key).Member(member).Build()).AsInt64()
	if err != nil {
		return 0, false
	}
	return v, true
}

// consistentState builds a ready recipient (ridA, two messages) and a leased
// recipient (ridB) through the real transitions.
func consistentState(t *testing.T) (*Adapter, *DeliveryStore) {
	t.Helper()
	a, s := claimSetup(t)
	enqueueJSON(t, a, "a1", ridA)
	enqueueJSON(t, a, "b1", ridB)
	enqueueJSON(t, a, "a2", ridA)
	// ridA is scanned first by claim: claim it, then accept ridB's order.
	if r := s.Claim(context.Background(), claimReq("op-1", "x", "dlv_1")); r.Delivery.MessageID != "a1" {
		t.Fatalf("setup claim = %+v", r)
	}
	return a, s
}

// TestReconcileConsistentStateIsNoOp pins the restart smoke: a consistent
// persisted state survives a full pass unchanged and allows readiness.
func TestReconcileConsistentStateIsNoOp(t *testing.T) {
	a, _ := consistentState(t)
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "" || rep.Recipients != 2 {
		t.Errorf("report = %+v", rep)
	}
	for kind, n := range rep.Findings {
		if n != 0 {
			t.Errorf("finding %s = %d on consistent state", kind, n)
		}
	}
	assertUnchanged(t, a, before, "consistent reconcile")
}

// TestReconcileRepairsDerivedStructures pins each derived repair.
func TestReconcileRepairsDerivedStructures(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(a *Adapter)
		check  func(t *testing.T, a *Adapter, rep ReconcileReport)
	}{
		{"missing ready member", func(a *Adapter) { a.testDo(t, "ZREM", "hr1:ready", ridB) }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if s, ok := score(t, a, "hr1:ready", ridB); !ok || s <= 1 {
				t.Errorf("ready member not restored with a fresh sequence: %d %v", s, ok)
			}
		}},
		{"stale ready for leased head", func(a *Adapter) { a.testDo(t, "ZADD", "hr1:ready", "99", ridA) }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if _, ok := score(t, a, "hr1:ready", ridA); ok {
				t.Error("leased head still ready")
			}
		}},
		{"missing lease member", func(a *Adapter) { a.testDo(t, "ZREM", "hr1:leases", ridA) }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			want := hget(t, a, "hr1:r:"+ridA+":s", "lease_expires_ms")
			if s, ok := score(t, a, "hr1:leases", ridA); !ok || itoa64(s) != want {
				t.Errorf("lease member = %d %v, want score %s", s, ok, want)
			}
		}},
		{"wrong lease score", func(a *Adapter) { a.testDo(t, "ZADD", "hr1:leases", "4102444800000", ridA) }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			want := hget(t, a, "hr1:r:"+ridA+":s", "lease_expires_ms")
			if s, _ := score(t, a, "hr1:leases", ridA); itoa64(s) != want {
				t.Errorf("lease score = %d, want %s", s, want)
			}
		}},
		{"stale lease for ready head", func(a *Adapter) { a.testDo(t, "ZADD", "hr1:leases", "4102444800000", ridB) }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if _, ok := score(t, a, "hr1:leases", ridB); ok {
				t.Error("ready head still leased")
			}
		}},
		{"drained recipient in indexes", func(a *Adapter) {
			a.testDo(t, "ZADD", "hr1:ready", "5", "telegram:42:chat:-9")
			a.testDo(t, "ZADD", "hr1:blocked", "5", "telegram:42:chat:-9")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["drained"] != 1 {
				t.Errorf("drained = %d", rep.Findings["drained"])
			}
			for _, k := range []string{"hr1:ready", "hr1:blocked"} {
				if _, ok := score(t, a, k, "telegram:42:chat:-9"); ok {
					t.Errorf("stale %s member kept", k)
				}
			}
		}},
		{"marker without blocked member", func(a *Adapter) {
			a.testDo(t, "HSET", "hr1:q:"+ridB, "detected_ms", "123", "reason_code", "queue_head_mismatch")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if s, ok := score(t, a, "hr1:blocked", ridB); !ok || s != 123 {
				t.Errorf("blocked member = %d %v", s, ok)
			}
			if _, ok := score(t, a, "hr1:ready", ridB); ok {
				t.Error("blocked recipient still ready")
			}
			if rep.Findings["already_blocked"] != 1 || rep.Hold() != "" {
				t.Errorf("report = %+v", rep)
			}
		}},
		{"counter drift", func(a *Adapter) { a.testDo(t, "SET", "hr1:stats:queued_messages", "42") }, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if n, _ := a.client.Do(context.Background(), a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 3 {
				t.Errorf("counter = %d, want 3", n)
			}
			if rep.Findings["counter_repaired"] != 1 {
				t.Errorf("report = %+v", rep.Findings)
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := consistentState(t)
			tc.break_(a)
			rep := reconcile(t, a, true)
			tc.check(t, a, rep)
			// A second pass finds nothing left to repair.
			again := reconcile(t, a, true)
			for _, k := range []string{"repaired", "drained", "blocked", "counter_repaired"} {
				if again.Findings[k] != 0 {
					t.Errorf("second pass %s = %d", k, again.Findings[k])
				}
			}
		})
	}
}

// TestReconcileBlocksEachReason pins marker creation for every bounded
// reason while other recipients stay serviceable.
func TestReconcileBlocksEachReason(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(a *Adapter)
		reason string
	}{
		{"queue wrong type", func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridB+":q")
			a.testDo(t, "SET", "hr1:r:"+ridB+":q", "x")
		}, "unsupported_key_type"},
		{"state wrong type", func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridB+":s")
			a.testDo(t, "SET", "hr1:r:"+ridB+":s", "x")
		}, "unsupported_key_type"},
		{"state missing", func(a *Adapter) { a.testDo(t, "DEL", "hr1:r:"+ridB+":s") }, "head_state_missing"},
		{"head mismatch", func(a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridB+":s", "head_message_id", "zz") }, "queue_head_mismatch"},
		{"state without queue", func(a *Adapter) { a.testDo(t, "DEL", "hr1:r:"+ridB+":q") }, "queue_head_mismatch"},
		{"head blob missing", func(a *Adapter) { a.testDo(t, "DEL", "hr1:m:b1") }, "head_message_missing"},
		{"queued blob missing", func(a *Adapter) {
			enqueueJSON(t, a, "b2", ridB)
			a.testDo(t, "DEL", "hr1:m:b2")
		}, "head_message_missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s := consistentState(t)
			tc.break_(a)
			rep := reconcile(t, a, true)
			if rep.BlockReasons[tc.reason] != 1 || rep.Hold() != "" {
				t.Fatalf("report = %+v %+v", rep.Findings, rep.BlockReasons)
			}
			if got := hget(t, a, "hr1:q:"+ridB, "reason_code"); got != tc.reason {
				t.Errorf("marker reason = %q", got)
			}
			if _, ok := score(t, a, "hr1:blocked", ridB); !ok {
				t.Error("blocked member missing")
			}
			if _, ok := score(t, a, "hr1:ready", ridB); ok {
				t.Error("blocked recipient still ready")
			}
			// The leased recipient is untouched and stays deliverable.
			if hget(t, a, "hr1:r:"+ridA+":s", "status") != "leased" {
				t.Error("other recipient disturbed")
			}
			if r := s.Ack(context.Background(), ackReq("dlv_1")); r.Outcome != "acknowledged" {
				t.Errorf("other recipient ack = %+v", r)
			}
		})
	}
}

// TestReconcileRefusals pins the due-lease hold without mutation and the
// unhandled refusal.
func TestReconcileRefusals(t *testing.T) {
	a, _ := consistentState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1")
	a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
	before := snapshot(t, a)
	rep := reconcile(t, a, false)
	if rep.Hold() != "due_lease" || rep.Findings["due_lease"] != 1 {
		t.Errorf("due lease report = %+v", rep.Findings)
	}
	assertUnchanged(t, a, before, "due lease")

	a, _ = consistentState(t)
	a.testDo(t, "SET", "hr1:q:"+ridB, "not-a-hash")
	rep = reconcile(t, a, true)
	if rep.Hold() != "unhandled_inconsistency" || rep.Findings["counter_unverified"] != 1 {
		t.Errorf("unhandled report = %+v", rep.Findings)
	}
}

// TestReconcileDedup pins expired-record removal, index restoration from
// accepted_ms, and orphan index members, across more than one batch.
func TestReconcileDedup(t *testing.T) {
	a, _ := claimSetup(t)
	now := time.Now().UnixMilli()
	for i := 0; i < 20; i++ {
		d := fmt.Sprintf("live%02d", i)
		a.testDo(t, "HSET", "hr1:d:"+d, "message_id", "m", "body_digest", "b", "accepted_ms", itoa64(now-int64(i)), "expires_ms", itoa64(now+3600000))
	}
	a.testDo(t, "HSET", "hr1:d:expired", "message_id", "m", "body_digest", "b", "accepted_ms", "1", "expires_ms", "2")
	a.testDo(t, "ZADD", "hr1:dedup_age", "1", "expired")
	a.testDo(t, "ZADD", "hr1:dedup_age", "5", "orphan")

	rep := reconcile(t, a, false)
	if rep.Findings["dedup_restored"] != 20 || rep.Findings["dedup_expired_removed"] != 1 || rep.Findings["dedup_orphans_removed"] != 1 {
		t.Errorf("findings = %+v", rep.Findings)
	}
	if exists(t, a, "hr1:d:expired") {
		t.Error("expired record kept")
	}
	if s, ok := score(t, a, "hr1:dedup_age", "live05"); !ok || s != now-5 {
		t.Errorf("restored score = %d %v", s, ok)
	}
	if _, ok := score(t, a, "hr1:dedup_age", "orphan"); ok {
		t.Error("orphan member kept")
	}
}

// TestReconcileBatchesManyRecipients pins a full pass over more recipients
// than one scan page, and the lightweight pass leaving the counter alone.
func TestReconcileBatchesManyRecipients(t *testing.T) {
	a, _ := claimSetup(t)
	for i := 0; i < 30; i++ {
		enqueueJSON(t, a, fmt.Sprintf("m%02d", i), fmt.Sprintf("telegram:42:chat:%d", i+1))
	}
	a.testDo(t, "DEL", "hr1:ready")
	a.testDo(t, "SET", "hr1:stats:queued_messages", "7")

	light := reconcile(t, a, false)
	if light.Recipients != 30 || light.Findings["repaired"] != 30 {
		t.Errorf("light pass = %d recipients, %+v", light.Recipients, light.Findings)
	}
	if n, _ := a.client.Do(context.Background(), a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 7 {
		t.Error("lightweight pass touched the counter")
	}
	if n, _ := a.client.Do(context.Background(), a.client.B().Zcard().Key("hr1:ready").Build()).AsInt64(); n != 30 {
		t.Errorf("ready members = %d", n)
	}
	reconcile(t, a, true)
	if n, _ := a.client.Do(context.Background(), a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 30 {
		t.Errorf("counter after full pass = %d", n)
	}
	// Repairs were audited best effort.
	entries, _ := a.AuditEntries(context.Background(), 100)
	repairs := 0
	for _, e := range entries {
		if e["actor"] == "reconciliation" {
			repairs++
		}
	}
	if repairs < 30 {
		t.Errorf("audited repairs = %d", repairs)
	}
}

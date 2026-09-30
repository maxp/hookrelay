package valkey

import (
	"context"
	"errors"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

// TestExpireSessions pins the expiry batch: due members and their Hashes
// go, oldest first up to the limit, the remaining count is reported, and a
// wrong-typed index is refused unchanged.
func TestExpireSessions(t *testing.T) {
	ctx := context.Background()
	a, s := sessionSetup(t)
	for i := range 3 {
		createSession(t, s, digestOf(i))
	}
	for i, score := range []string{"10", "20", "30"} {
		a.testDo(t, "ZADD", adminSessionsKey, score, digestOf(i+10))
		a.testDo(t, "HSET", "hr1:admin_session:"+digestOf(i+10), "created_ms", "1")
	}
	n, indexed, err := s.ExpireSessions(ctx, 2)
	if err != nil || n != 2 || indexed != 4 {
		t.Fatalf("expire = %d %d %v", n, indexed, err)
	}
	if exists(t, a, "hr1:admin_session:"+digestOf(10)) || exists(t, a, "hr1:admin_session:"+digestOf(11)) ||
		!exists(t, a, "hr1:admin_session:"+digestOf(12)) {
		t.Error("not the oldest two removed")
	}
	if n, indexed, _ := s.ExpireSessions(ctx, 100); n != 1 || indexed != 3 {
		t.Errorf("second = %d %d", n, indexed)
	}
	before := snapshot(t, a)
	if n, indexed, err := s.ExpireSessions(ctx, 100); n != 0 || indexed != 3 || err != nil {
		t.Errorf("nothing due = %d %d %v", n, indexed, err)
	}
	assertUnchanged(t, a, before, "nothing due")

	a.testDo(t, "DEL", adminSessionsKey)
	a.testDo(t, "SET", adminSessionsKey, "x")
	if _, _, err := s.ExpireSessions(ctx, 100); !errors.Is(err, administration.ErrStoredWrongType) {
		t.Errorf("wrong type = %v", err)
	}
	for _, bad := range [][]string{{"0", "hr1"}, {"1001", "hr1"}, {"1", ""}} {
		if _, err := a.RunScript(ctx, "expire_sessions_v1", []string{adminSessionsKey}, bad); err == nil {
			t.Errorf("%v accepted", bad)
		}
	}
}

// TestReconcileSessions pins session reconciliation: an orphan member is
// removed, unusable Hashes are deleted, a valid unindexed or mis-scored
// session is restored from its Hash, a consistent one is untouched, and a
// wrong-typed structure fails the pass.
func TestReconcileSessions(t *testing.T) {
	ctx := context.Background()
	a, s := sessionSetup(t)
	valid := createSession(t, s, digestOf(1))
	createSession(t, s, digestOf(2)) // consistent
	createSession(t, s, digestOf(3))
	a.testDo(t, "ZREM", adminSessionsKey, digestOf(3)) // unindexed
	createSession(t, s, digestOf(4))
	a.testDo(t, "ZADD", adminSessionsKey, "99999999999999", digestOf(5)) // orphan member
	createSession(t, s, digestOf(6))
	a.testDo(t, "HSET", "hr1:admin_session:"+digestOf(6), "generation_id", "stale")
	createSession(t, s, digestOf(7))
	a.testDo(t, "HDEL", "hr1:admin_session:"+digestOf(7), "csrf_token")
	a.testDo(t, "ZREM", adminSessionsKey, digestOf(7)) // malformed and unindexed
	// Set after every login: a login's own cleanup removes due members.
	a.testDo(t, "ZADD", adminSessionsKey, "5", digestOf(4)) // wrong score
	two := hgetall(t, a, "hr1:admin_session:"+digestOf(2))

	rep, err := a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecret})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Findings["session_orphans_removed"] != 1 || rep.Findings["sessions_removed"] != 2 || rep.Findings["session_index_restored"] != 2 {
		t.Errorf("findings = %v", rep.Findings)
	}
	for _, d := range []int{3, 4} {
		sc, ok := score(t, a, adminSessionsKey, digestOf(d))
		if !ok || int64(sc) < valid.CreatedMs || hget(t, a, "hr1:admin_session:"+digestOf(d), "idle_expires_ms") != itoa64(int64(sc)) {
			t.Errorf("session %d score = %v %v, want its idle expiry", d, sc, ok)
		}
	}
	for _, d := range []int{5, 6, 7} {
		if exists(t, a, "hr1:admin_session:"+digestOf(d)) {
			t.Errorf("session %d kept", d)
		}
		if _, ok := score(t, a, adminSessionsKey, digestOf(d)); ok {
			t.Errorf("session %d member kept", d)
		}
	}
	if got := hgetall(t, a, "hr1:admin_session:"+digestOf(2)); len(got) != len(two) || got["idle_expires_ms"] != two["idle_expires_ms"] {
		t.Errorf("consistent session touched: %v", got)
	}
	var repairs int
	for _, e := range auditOps(t, a) {
		if e["operation"] == "session_index_repaired" {
			repairs++
		}
	}
	if repairs != 5 {
		t.Errorf("repair audit events = %d, want 5", repairs)
	}
	if rep, err := a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecret}); err != nil ||
		rep.Findings["session_orphans_removed"]+rep.Findings["sessions_removed"]+rep.Findings["session_index_restored"] != 0 {
		t.Errorf("second pass = %v, %v", rep.Findings, err)
	}

	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"index not a zset": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", adminSessionsKey)
			a.testDo(t, "SET", adminSessionsKey, "x")
		},
		"session not a hash": func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:admin_session:"+digestOf(9), "x") },
		"malformed session id": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:admin_session:NOT-A-DIGEST", "created_ms", "1")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := sessionSetup(t)
			setup(t, a)
			if _, err := a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecret}); err == nil {
				t.Error("the pass succeeded")
			}
		})
	}
}

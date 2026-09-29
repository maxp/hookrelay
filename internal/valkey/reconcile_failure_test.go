package valkey

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/delivery"
)

// retryWaitState builds consistentState and nacks ridA's lease with a long
// delay: ridA waits in retry_wait, ridB is ready.
func retryWaitState(t *testing.T) (*Adapter, *DeliveryStore) {
	t.Helper()
	a, s := consistentState(t)
	req := nackReq("dlv_1", "")
	req.RetryDelaysMs = []int64{600000, 600000, 600000}
	if r := s.Nack(context.Background(), req); r.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("nack = %+v", r)
	}
	return a, s
}

// TestReconcileAcceptsRetryWait pins retry_wait as a valid head status: a
// consistent waiting head is left alone and its derived memberships are
// repaired to exactly one retries member at retry_at_ms.
func TestReconcileAcceptsRetryWait(t *testing.T) {
	a, _ := retryWaitState(t)
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "" || len(rep.DueRetries) != 0 {
		t.Errorf("report = %+v", rep)
	}
	for kind, n := range rep.Findings {
		if n != 0 {
			t.Errorf("finding %s = %d on a consistent retry_wait head", kind, n)
		}
	}
	assertUnchanged(t, a, before, "consistent retry_wait")

	for name, break_ := range map[string]func(a *Adapter){
		"missing retries member": func(a *Adapter) { a.testDo(t, "ZREM", "hr1:retries", ridA) },
		"wrong retries score":    func(a *Adapter) { a.testDo(t, "ZADD", "hr1:retries", "4102444800000", ridA) },
		"stale ready member":     func(a *Adapter) { a.testDo(t, "ZADD", "hr1:ready", "99", ridA) },
		"stale lease member":     func(a *Adapter) { a.testDo(t, "ZADD", "hr1:leases", "4102444800000", ridA) },
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := retryWaitState(t)
			break_(a)
			rep := reconcile(t, a, true)
			if rep.Findings["repaired"] != 1 || rep.Hold() != "" {
				t.Errorf("report = %+v", rep.Findings)
			}
			if sc, ok := score(t, a, "hr1:retries", ridA); !ok || itoa64(sc) != retryAtOf(t, a) {
				t.Errorf("retries member = %d %v", sc, ok)
			}
			for _, idx := range []string{"hr1:ready", "hr1:leases"} {
				if _, ok := score(t, a, idx, ridA); ok {
					t.Errorf("%s member kept for a retry_wait head", idx)
				}
			}
		})
	}
}

func retryAtOf(t *testing.T, a *Adapter) string {
	t.Helper()
	return hget(t, a, "hr1:r:"+ridA+":s", "retry_at_ms")
}

// TestReconcileBlocksRetryWaitAndDropsItsRetryMember pins that isolating a
// retry_wait Recipient removes its retries member, for a new marker and an
// existing one; a malformed retry_at_ms is head_state_missing.
func TestReconcileBlocksRetryWaitAndDropsItsRetryMember(t *testing.T) {
	a, _ := retryWaitState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "retry_at_ms", "soon")
	rep := reconcile(t, a, true)
	if rep.BlockReasons["head_state_missing"] != 1 {
		t.Errorf("report = %+v", rep.BlockReasons)
	}
	if _, ok := score(t, a, "hr1:retries", ridA); ok {
		t.Error("blocked recipient kept its retries member")
	}

	a, _ = retryWaitState(t)
	a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "5", "reason_code", "queue_head_mismatch")
	rep = reconcile(t, a, true)
	if rep.Findings["already_blocked"] != 1 {
		t.Errorf("report = %+v", rep.Findings)
	}
	if _, ok := score(t, a, "hr1:retries", ridA); ok {
		t.Error("already-blocked recipient kept its retries member")
	}
}

// TestReconcileReportsDueWorkWithoutMutation pins that a due lease and a due
// retry are reported for execution, change nothing, and no longer hold
// readiness.
func TestReconcileReportsDueWorkWithoutMutation(t *testing.T) {
	a, _ := consistentState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1")
	a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "" || rep.Findings["due_lease"] != 1 || len(rep.DueLeases) != 1 || rep.DueLeases[0] != ridA {
		t.Errorf("due lease report = %+v", rep)
	}
	assertUnchanged(t, a, before, "due lease")

	a, _ = retryWaitState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "retry_at_ms", "1")
	a.testDo(t, "ZADD", "hr1:retries", "1", ridA)
	before = snapshot(t, a)
	rep = reconcile(t, a, true)
	if rep.Hold() != "" || rep.Findings["due_retry"] != 1 || len(rep.DueRetries) != 1 || rep.DueRetries[0] != ridA {
		t.Errorf("due retry report = %+v", rep)
	}
	assertUnchanged(t, a, before, "due retry")
}

// TestReconcileRetryIndex pins the retries index as a Recipient source (a
// stale member of a drained Recipient is removed) and its type check.
func TestReconcileRetryIndex(t *testing.T) {
	a, _ := consistentState(t)
	a.testDo(t, "ZADD", "hr1:retries", "5", "telegram:42:chat:-9")
	rep := reconcile(t, a, true)
	if rep.Findings["drained"] != 1 {
		t.Errorf("report = %+v", rep.Findings)
	}
	if _, ok := score(t, a, "hr1:retries", "telegram:42:chat:-9"); ok {
		t.Error("stale retries member kept")
	}
}

// deadLetterOne dead-letters the head of rid through nack_v2 with a
// single-attempt policy and returns its dead_lettered_ms.
func deadLetterOne(t *testing.T, s *DeliveryStore, op, token string) int64 {
	t.Helper()
	ctx := context.Background()
	if c := s.Claim(ctx, claimReq(op, "args", token)); c.Outcome != delivery.ClaimClaimed {
		t.Fatalf("claim = %+v", c)
	}
	req := nackReq(token, "")
	req.RetryDelaysMs, req.MaxAttempts = nil, 1
	r := s.Nack(ctx, req)
	if r.Outcome != delivery.NackDeadLettered {
		t.Fatalf("nack = %+v", r)
	}
	return r.DeadLetteredMs
}

// TestReconcileDLQ pins the DLQ checks: consistent entries are left alone,
// an orphan member is removed, a missing or wrong member is restored at
// dead_lettered_ms, across several batches.
func TestReconcileDLQ(t *testing.T) {
	a, s := claimSetup(t)
	for i := 0; i < 10; i++ {
		id := "m" + itoa64(int64(i))
		enqueueJSON(t, a, id, "telegram:42:chat:"+itoa64(int64(-100-i)))
		deadLetterOne(t, s, "op-"+id, "dlv_"+id)
	}
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "" {
		t.Errorf("hold = %q", rep.Hold())
	}
	for kind, n := range rep.Findings {
		if n != 0 {
			t.Errorf("finding %s = %d on a consistent DLQ", kind, n)
		}
	}
	assertUnchanged(t, a, before, "consistent DLQ")

	dlMs := hget(t, a, "hr1:dl:m3", "dead_lettered_ms")
	a.testDo(t, "ZREM", "hr1:dlq", "m3")
	a.testDo(t, "ZADD", "hr1:dlq", "1", "m4")
	a.testDo(t, "ZADD", "hr1:dlq", "7", "m-orphan")
	rep = reconcile(t, a, true)
	if rep.Findings["dlq_restored"] != 2 || rep.Findings["dlq_orphans_removed"] != 1 || rep.Hold() != "" {
		t.Errorf("report = %+v", rep.Findings)
	}
	if sc, ok := score(t, a, "hr1:dlq", "m3"); !ok || itoa64(sc) != dlMs {
		t.Errorf("m3 member = %d %v, want %s", sc, ok, dlMs)
	}
	if sc, _ := score(t, a, "hr1:dlq", "m4"); itoa64(sc) != hget(t, a, "hr1:dl:m4", "dead_lettered_ms") {
		t.Errorf("m4 score = %d", sc)
	}
	if _, ok := score(t, a, "hr1:dlq", "m-orphan"); ok {
		t.Error("orphan member kept")
	}
}

// TestReconcileDLQMissingMessageHoldsReadiness pins the missing DLQ blob: it
// is reported with its message id and holds readiness; no Recipient block
// marker is created and nothing is mutated, even when the Recipient's
// active queue is empty.
func TestReconcileDLQMissingMessageHoldsReadiness(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "DEL", "hr1:m:m1")
	before := snapshot(t, a)
	rep := reconcile(t, a, true)
	if rep.Hold() != "dead_letter_message_missing" || rep.Findings["dlq_message_missing"] != 1 ||
		len(rep.DeadLetterMessagesMissing) != 1 || rep.DeadLetterMessagesMissing[0] != "m1" {
		t.Errorf("report = %+v", rep)
	}
	a.testDo(t, "DEL", auditKey) // best-effort audit of the ambiguity; delivery state unchanged
	assertUnchanged(t, a, before, "missing DLQ blob")
	if exists(t, a, "hr1:q:"+ridA) {
		t.Error("a Recipient block marker was created for a DLQ integrity failure")
	}
	found := false
	for _, issue := range rep.ConsistencyIssues() {
		if issue.Kind == "dead_letter_message_missing" && issue.Resolution == "held" && issue.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Errorf("issues = %+v", rep.ConsistencyIssues())
	}
}

// TestReconcileDLQInvalidRecordHolds pins a malformed dead-letter record as
// an unhandled inconsistency without mutation.
func TestReconcileDLQInvalidRecordHolds(t *testing.T) {
	for name, poison := range map[string]func(a *Adapter){
		"record wrong type": func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:dl:m1")
			a.testDo(t, "SET", "hr1:dl:m1", "x")
		},
		"malformed dead_lettered_ms": func(a *Adapter) { a.testDo(t, "HSET", "hr1:dl:m1", "dead_lettered_ms", "soon") },
		"dlq wrong type": func(a *Adapter) {
			a.testDo(t, "DEL", "hr1:dlq")
			a.testDo(t, "SET", "hr1:dlq", "x")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			poison(a)
			before := snapshot(t, a)
			rep, err := a.Reconcile(context.Background(), ReconcileOptions{Full: true, MessageCheckBound: 10, BatchSize: 7})
			if err == nil && rep.Hold() != "unhandled_inconsistency" {
				t.Errorf("report = %+v", rep)
			}
			a.testDo(t, "DEL", auditKey) // best-effort audit of the ambiguity
			assertUnchanged(t, a, before, name)
		})
	}
}

// reconcileWithMaintenance runs the startup composition: a pass, the due
// work through a real delivery.Maintenance, and a verifying pass.
func reconcileWithMaintenance(t *testing.T, a *Adapter, s *DeliveryStore) ReconcileReport {
	t.Helper()
	attempts, err := delivery.NewAttemptMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	m, err := delivery.NewMaintenance(delivery.MaintenanceDeps{
		Retries: s, Leases: s, Attempts: attempts,
		RetryPolicy: delivery.RetryPolicy{MaxAttempts: 4, Delays: []time.Duration{time.Minute, time.Minute, time.Minute}, JitterMin: 1, JitterMax: 1},
		Config:      delivery.MaintenanceConfig{Interval: time.Second, BatchSize: 100, MaxContinuousBatches: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	rep, err := a.ReconcileAndProcessDue(context.Background(), ReconcileOptions{Full: true, MessageCheckBound: 10, BatchSize: 7}, m.ProcessDue)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return rep
}

// TestReconcileAndProcessDueExpiresBeforeReadiness pins the startup
// composition: an overdue lease is expired and a due retry activated
// before readiness, the verifying pass finds nothing due, and readiness is
// not held.
func TestReconcileAndProcessDueExpiresBeforeReadiness(t *testing.T) {
	a, s := expirySetup(t)
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridB)
	claimAndLapse(t, s, "op-1", "dlv_token1", "") // ridA: overdue lease
	c := s.Claim(context.Background(), claimReq("op-2", "args", "dlv_token2"))
	if c.Delivery.MessageID != "m2" {
		t.Fatalf("claim = %+v", c)
	}
	req := nackReq("dlv_token2", "")
	req.RetryDelaysMs = []int64{1, 1, 1}
	s.Nack(context.Background(), req) // ridB: due retry
	time.Sleep(10 * time.Millisecond)

	rep := reconcileWithMaintenance(t, a, s)
	if rep.Hold() != "" || rep.Findings["due_leases_processed"] != 1 || rep.Findings["due_retries_processed"] != 1 ||
		len(rep.DueLeases) != 0 || len(rep.DueRetries) != 0 {
		t.Errorf("report = %+v", rep)
	}
	if got := hget(t, a, "hr1:r:"+ridA+":s", "status"); got != "retry_wait" {
		t.Errorf("overdue lease state = %q, want retry_wait", got)
	}
	if got := hget(t, a, "hr1:r:"+ridB+":s", "status"); got != "ready" {
		t.Errorf("due retry state = %q, want ready", got)
	}
	if got := hget(t, a, "hr1:t:"+ackReq("dlv_token1").TokenDigest, "state"); got != "expired" {
		t.Errorf("expired token phase = %q", got)
	}
}

// TestReconcileAndProcessDueHoldsOnMissingDLQMessage pins that the
// composition keeps the DLQ integrity hold and creates no block marker.
func TestReconcileAndProcessDueHoldsOnMissingDLQMessage(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	a.testDo(t, "DEL", "hr1:m:m1")
	rep := reconcileWithMaintenance(t, a, s)
	if rep.Hold() != "dead_letter_message_missing" || exists(t, a, "hr1:q:"+ridA) {
		t.Errorf("report = %+v, marker %v", rep, exists(t, a, "hr1:q:"+ridA))
	}
}

// TestReconcileDueRepairsItsLocator pins that a due head whose derived
// locator is missing gets it back (so maintenance can still find it if the
// startup transition fails), while the head state stays untouched.
func TestReconcileDueRepairsItsLocator(t *testing.T) {
	a, _ := consistentState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "1")
	a.testDo(t, "ZREM", "hr1:leases", ridA)
	rep := reconcile(t, a, true)
	if rep.Findings["due_lease"] != 1 || len(rep.DueLeases) != 1 {
		t.Errorf("report = %+v", rep.Findings)
	}
	if sc, ok := score(t, a, "hr1:leases", ridA); !ok || sc != 1 {
		t.Errorf("lease locator = %d %v, want restored at the deadline", sc, ok)
	}
	if hget(t, a, "hr1:r:"+ridA+":s", "status") != "leased" {
		t.Error("due head state changed")
	}

	a, _ = retryWaitState(t)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "retry_at_ms", "1")
	a.testDo(t, "ZREM", "hr1:retries", ridA)
	a.testDo(t, "ZADD", "hr1:ready", "99", ridA)
	rep = reconcile(t, a, true)
	if rep.Findings["due_retry"] != 1 {
		t.Errorf("report = %+v", rep.Findings)
	}
	if sc, ok := score(t, a, "hr1:retries", ridA); !ok || sc != 1 {
		t.Errorf("retry locator = %d %v", sc, ok)
	}
	if _, ok := score(t, a, "hr1:ready", ridA); ok {
		t.Error("stale ready member kept for a due retry")
	}
}

// TestReconcileAndProcessDueCountsANewBlockOnce pins the merged report: a
// Recipient blocked by the first pass is not counted again as an existing
// block by the verifying pass.
func TestReconcileAndProcessDueCountsANewBlockOnce(t *testing.T) {
	a, s := expirySetup(t)
	enqueueJSON(t, a, "m1", ridA)
	claimAndLapse(t, s, "op-1", "dlv_token1", "") // due work forces two passes
	enqueueJSON(t, a, "m2", ridB)
	a.testDo(t, "DEL", "hr1:r:"+ridB+":s") // ridB: head_state_missing
	rep := reconcileWithMaintenance(t, a, s)
	if rep.BlockReasons["head_state_missing"] != 1 || rep.Findings["already_blocked"] != 0 {
		t.Errorf("report = %+v %+v", rep.Findings, rep.BlockReasons)
	}
	for _, issue := range rep.ConsistencyIssues() {
		if issue.Kind == "existing_block" {
			t.Errorf("new block also counted as existing: %+v", rep.ConsistencyIssues())
		}
	}
}

// TestReconcileDLQValidatesRecordFields pins the required dead-letter
// fields; an invalid record seen from both the index and the key scan is
// counted once, and is audited.
func TestReconcileDLQValidatesRecordFields(t *testing.T) {
	for name, poison := range map[string][]string{
		"missing bot_id":             {"HDEL", "hr1:dl:m1", "bot_id"},
		"unknown reason":             {"HSET", "hr1:dl:m1", "dead_letter_reason", "bored"},
		"malformed delivery_cycle":   {"HSET", "hr1:dl:m1", "delivery_cycle", "x"},
		"chat scope without chat_id": {"HDEL", "hr1:dl:m1", "chat_id"},
		"missing recipient_identity": {"HDEL", "hr1:dl:m1", "recipient_identity"},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			deadLetterOne(t, s, "op-1", "dlv_1")
			a.testDo(t, poison...)
			rep := reconcile(t, a, true)
			if rep.Findings["dlq_invalid"] != 1 || rep.Hold() != "unhandled_inconsistency" {
				t.Errorf("report = %+v", rep.Findings)
			}
		})
	}
}

// TestReconcileDLQAuditsRepairsAndAmbiguity pins best-effort audit events
// for DLQ repairs and the missing-message ambiguity.
func TestReconcileDLQAuditsRepairsAndAmbiguity(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridB)
	deadLetterOne(t, s, "op-1", "dlv_1")
	deadLetterOne(t, s, "op-2", "dlv_2")
	a.testDo(t, "ZREM", "hr1:dlq", "m1")
	a.testDo(t, "DEL", "hr1:m:m2")
	reconcile(t, a, true)
	entries, err := a.AuditEntries(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, e := range entries {
		seen[e["reason"]] = e["target"]
	}
	if seen["dlq_restored"] != "m1" || seen["dead_letter_message_missing"] != "m2" {
		t.Errorf("audit entries = %+v", entries)
	}
}

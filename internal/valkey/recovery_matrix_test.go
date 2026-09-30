package valkey

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/observability"
)

func processDueDirect(s *DeliveryStore) func(context.Context, []string, []string) {
	return func(ctx context.Context, leases, retries []string) {
		for _, rid := range leases {
			s.ExpireLease(ctx, rid, []int64{600000, 600000, 600000}, 4)
		}
		for _, rid := range retries {
			s.ActivateRetry(ctx, rid)
		}
	}
}

func createRecoveryEndpoint(t *testing.T, a *Adapter) {
	t.Helper()
	if _, _, got := a.CreateEndpoint(context.Background(), endpointFixture(), "endpoint-event", "webhook_endpoint_created", "endpoint-request"); got != CreateOK {
		t.Fatalf("endpoint create = %s", got)
	}
}

func TestCompleteModelStartupRecoveryMatrix(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	createRecoveryEndpoint(t, a)
	if _, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessionStore(a)
	validSession := digestOf(81)
	if got := createSession(t, sessions, validSession); got.Result != "created" {
		t.Fatalf("session = %+v", got)
	}

	const (
		readyRID   = "telegram:42:chat:-101"
		leaseRID   = "telegram:42:chat:-102"
		retryRID   = "telegram:42:chat:-103"
		blockedRID = "telegram:42:chat:-104"
		replayRID  = "telegram:42:chat:-105"
		dlqRID     = "telegram:42:chat:-106"
		ackRID     = "telegram:42:chat:-107"
		dueLease   = "telegram:42:chat:-108"
		dueRetry   = "telegram:42:chat:-109"
	)
	for _, pair := range [][2]string{
		{"leased", leaseRID}, {"retry", retryRID}, {"replay", replayRID}, {"dlq", dlqRID},
		{"acked", ackRID}, {"due-lease", dueLease}, {"due-retry", dueRetry}, {"ready", readyRID}, {"blocked", blockedRID},
	} {
		enqueueJSON(t, a, pair[0], pair[1])
	}
	claimWant := func(op, token, want string) {
		t.Helper()
		for i := 0; i < 20; i++ {
			c := s.Claim(ctx, claimReq(op, "args", token))
			if c.Outcome != delivery.ClaimClaimed {
				t.Fatalf("claim %s = %+v", want, c)
			}
			if c.Delivery.MessageID == want {
				return
			}
			t.Fatalf("claim order reached %s before %s", c.Delivery.MessageID, want)
		}
	}
	claimWant("op-lease", "dlv_lease", "leased")
	claimWant("op-retry", "dlv_retry", "retry")
	req := nackReq("dlv_retry", "")
	req.RetryDelaysMs = []int64{600000, 600000, 600000}
	if n := s.Nack(ctx, req); n.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("retry nack = %+v", n)
	}
	a.testDo(t, "HSET", "hr1:q:"+blockedRID, "detected_ms", "1", "reason_code", "queue_head_mismatch")
	a.testDo(t, "ZADD", "hr1:blocked", "1", blockedRID)
	a.testDo(t, "ZREM", "hr1:ready", blockedRID)

	claimWant("op-replay", "dlv_replay", "replay")
	dlqReq := nackReq("dlv_replay", "")
	dlqReq.RetryDelaysMs, dlqReq.MaxAttempts = nil, 1
	if n := s.Nack(ctx, dlqReq); n.Outcome != delivery.NackDeadLettered {
		t.Fatalf("replay dead-letter = %+v", n)
	}
	claimWant("op-dlq", "dlv_dlq", "dlq")
	dlqReq = nackReq("dlv_dlq", "")
	dlqReq.RetryDelaysMs, dlqReq.MaxAttempts = nil, 1
	if n := s.Nack(ctx, dlqReq); n.Outcome != delivery.NackDeadLettered {
		t.Fatalf("dlq nack = %+v", n)
	}
	claimWant("op-ack", "dlv_ack", "acked")
	if ack := s.Ack(ctx, ackReq("dlv_ack")); ack.Outcome != delivery.AckAcknowledged {
		t.Fatalf("ack = %+v", ack)
	}
	claimWant("op-due-lease", "dlv_due_lease", "due-lease")
	a.testDo(t, "HSET", "hr1:r:"+dueLease+":s", "claimed_ms", "1", "attempt_started_ms", "1", "lease_expires_ms", "2")
	a.testDo(t, "HSET", "hr1:t:"+ackReq("dlv_due_lease").TokenDigest, "claimed_ms", "1", "lease_expires_ms", "2")
	a.testDo(t, "HSET", "hr1:op:op-due-lease", "claimed_ms", "1", "lease_expires_ms", "2")
	a.testDo(t, "ZADD", "hr1:leases", "2", dueLease)
	claimWant("op-due-retry", "dlv_due_retry", "due-retry")
	req = nackReq("dlv_due_retry", "")
	req.RetryDelaysMs = []int64{1, 1, 1}
	if n := s.Nack(ctx, req); n.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("due retry nack = %+v", n)
	}
	time.Sleep(10 * time.Millisecond)
	if r := replay(a, "replay", "reject"); r.Result != "replayed" {
		t.Fatalf("replay = %+v", r)
	}

	// Cross-slice recoverable damage in the same pass.
	a.testDo(t, "SREM", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_test1")
	a.testDo(t, "ZREM", "hr1:webhooks", "telegram:wh_test1")
	a.testDo(t, "ZREM", "hr1:ready", readyRID)
	a.testDo(t, "ZADD", adminSessionsKey, "99999999999999", digestOf(82))
	a.testDo(t, "SCRIPT", "FLUSH")

	var logs bytes.Buffer
	rep, err := a.ReconcileAndProcessDue(ctx, ReconcileOptions{
		Full: true, BatchSize: 3, MessageCheckBound: 20, DedupRetention: testLimits().DedupRetention,
		AdminSecret: testAdminSecret, Logger: observability.NewTestLogger("info", &logs),
	}, processDueDirect(s))
	if err != nil || rep.Hold() != "" {
		t.Fatalf("recovery = %+v, %v", rep, err)
	}
	for kind, want := range map[string]int{
		"endpoint_bot_restored": 1, "endpoint_listing_restored": 1, "repaired": 1,
		"session_orphans_removed": 1, "due_leases_processed": 1, "due_retries_processed": 1,
	} {
		if rep.Findings[kind] != want {
			t.Errorf("%s = %d, want %d; all=%v", kind, rep.Findings[kind], want, rep.Findings)
		}
	}
	if hget(t, a, "hr1:r:"+dueLease+":s", "status") != "retry_wait" || hget(t, a, "hr1:r:"+dueRetry+":s", "status") != "ready" {
		t.Errorf("due states: lease=%s retry=%s", hget(t, a, "hr1:r:"+dueLease+":s", "status"), hget(t, a, "hr1:r:"+dueRetry+":s", "status"))
	}
	if hget(t, a, "hr1:q:"+blockedRID, "reason_code") != "queue_head_mismatch" || !exists(t, a, "hr1:dl:dlq") || !exists(t, a, "hr1:success:acked") {
		t.Error("mixed lifecycle state was not preserved")
	}
	audit := fmt.Sprint(auditOps(t, a))
	for _, secret := range []string{endpointFixture().CredentialValue, testCSRF, "dlv_lease", "dlv_due_lease", `"payload"`} {
		if strings.Contains(logs.String(), secret) || strings.Contains(audit, secret) {
			t.Errorf("sensitive value leaked to logs/audit: %q", secret)
		}
	}
	allowedIssues := map[[2]string]bool{
		{"endpoint_bot_index_missing", "restored"}: true, {"endpoint_listing_missing", "restored"}: true,
		{"derived_index_drift", "repaired"}: true, {"session_index_orphan", "removed"}: true,
		{"existing_block", "kept"}: true,
	}
	for _, issue := range rep.ConsistencyIssues() {
		if issue.Count <= 0 || !allowedIssues[[2]string{issue.Kind, issue.Resolution}] {
			t.Errorf("unbounded/unexpected consistency issue: %+v", issue)
		}
	}

	again, err := a.ReconcileAndProcessDue(ctx, ReconcileOptions{
		Full: true, BatchSize: 3, MessageCheckBound: 20, DedupRetention: testLimits().DedupRetention, AdminSecret: testAdminSecret,
	}, processDueDirect(s))
	if err != nil || again.Hold() != "" || again.Findings["endpoint_bot_restored"] != 0 || again.Findings["repaired"] != 0 ||
		again.Findings["due_leases_processed"] != 0 || again.Findings["due_retries_processed"] != 0 {
		t.Fatalf("second pass = %+v, %v", again, err)
	}
}

func TestLightweightRecoveryRunsAfterGateReload(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	createRecoveryEndpoint(t, a)
	enqueueJSON(t, a, "m1", ridA)
	if first, err := a.Reconcile(ctx, ReconcileOptions{Full: true, BatchSize: 3}); err != nil || first.Hold() != "" {
		t.Fatalf("initial pass = %+v, %v", first, err)
	}

	// Dependency recovery reloads scripts through the gate, then a lightweight
	// reconciliation repairs state before the application may mark ready.
	a.testDo(t, "SCRIPT", "FLUSH")
	a.testDo(t, "SREM", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_test1")
	a.testDo(t, "ZREM", "hr1:webhooks", "telegram:wh_test1")
	a.testDo(t, "ZREM", "hr1:ready", ridA)
	if gate, err := a.ValidateReadiness(ctx, false); err != nil || !gate.OK() {
		t.Fatalf("recovery gate = %+v, %v", gate, err)
	}
	recovered, err := a.Reconcile(ctx, ReconcileOptions{Full: false, BatchSize: 2})
	if err != nil || recovered.Hold() != "" || recovered.Findings["endpoint_bot_restored"] != 1 ||
		recovered.Findings["endpoint_listing_restored"] != 1 || recovered.Findings["repaired"] != 1 || recovered.Findings["counter_repaired"] != 0 {
		t.Fatalf("lightweight recovery = %+v, %v", recovered, err)
	}
}

func TestRecoveryRotatesAdminSecretAndCleansMixedSessions(t *testing.T) {
	a, s := sessionSetup(t)
	ctx := context.Background()
	valid := digestOf(91)
	createSession(t, s, valid)
	// Not indexed: survives the bounded rotation script, then reconciliation
	// removes it because its generation is stale.
	staleUnindexed := digestOf(92)
	createSession(t, s, staleUnindexed)
	a.testDo(t, "ZREM", adminSessionsKey, staleUnindexed)
	// Indexed orphan and due disposable session.
	a.testDo(t, "ZADD", adminSessionsKey, "99999999999999", digestOf(93))
	expired := digestOf(94)
	createSession(t, s, expired)
	a.testDo(t, "HSET", "hr1:admin_session:"+expired, "idle_expires_ms", "1")
	a.testDo(t, "ZADD", adminSessionsKey, "1", expired)

	rep, err := a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecretOther, BatchSize: 2})
	if err != nil || rep.Hold() != "" || rep.Findings["admin_auth_rotated"] != 1 {
		t.Fatalf("rotation = %+v, %v", rep, err)
	}
	for _, digest := range []string{valid, staleUnindexed, expired} {
		if exists(t, a, "hr1:admin_session:"+digest) {
			t.Errorf("session %s survived rotation/reconciliation", digest)
		}
	}
	if _, ok := score(t, a, adminSessionsKey, digestOf(93)); ok {
		t.Error("orphan session member survived")
	}
}

func TestRecoveryClassifiesPartialClaimWithoutBlindRetry(t *testing.T) {
	a, s, _ := claimedAttempt(t)
	_ = s
	ctx := context.Background()
	digest := hget(t, a, "hr1:r:"+ridA+":s", "delivery_token_digest")
	// Representative claim_v3 partial outcome: leased state and derived
	// locator survived, but the token record did not. Reconciliation must
	// isolate; it must not run claim again or invent a token/op record.
	a.testDo(t, "DEL", "hr1:t:"+digest)
	opBefore := hgetall(t, a, "hr1:op:op-1")
	var logs bytes.Buffer
	rep, err := a.Reconcile(ctx, ReconcileOptions{Full: true, Logger: observability.NewTestLogger("info", &logs)})
	if err != nil || rep.Hold() != "" || rep.Findings["active_attempt_token_missing"] != 1 ||
		hget(t, a, "hr1:q:"+ridA, "reason_code") != "active_attempt_inconsistent" {
		t.Fatalf("partial claim = %+v, %v", rep, err)
	}
	if exists(t, a, "hr1:t:"+digest) || hgetall(t, a, "hr1:op:op-1")["delivery_token"] != opBefore["delivery_token"] {
		t.Error("reconciliation retried or rewrote the uncertain claim")
	}
	if strings.Contains(logs.String(), "dlv_token1") {
		t.Error("Delivery Token leaked while classifying partial claim")
	}
}

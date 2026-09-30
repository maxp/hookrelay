package valkey

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/delivery"
)

const testDLQRetention = time.Hour

// agedDeadLetter dead-letters the next head and backdates it to
// dead_lettered_ms=1000 so its retention has passed.
func agedDeadLetter(t *testing.T, a *Adapter, s *DeliveryStore, id string) {
	t.Helper()
	deadLetterOne(t, s, "op-"+id, "dlv_"+id)
	a.testDo(t, "HSET", "hr1:dl:"+id, "dead_lettered_ms", "1000")
	a.testDo(t, "ZADD", "hr1:dlq", "1000", id)
}

// TestExpireDLQDeletesEverything pins the expired tuple and every affected
// key: record, blob, metadata, history, and DLQ member are deleted, and one
// audit event without payload is appended; queued messages stay.
func TestExpireDLQDeletesEverything(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	agedDeadLetter(t, a, s, "m1")

	batch, err := s.DueDeadLetters(ctx, 10, testDLQRetention)
	if err != nil || len(batch.Entries) != 1 || batch.Entries[0].MessageID != "m1" || batch.Entries[0].DueMs != 1000+testDLQRetention.Milliseconds() ||
		batch.NowMs <= 0 {
		t.Fatalf("due = %+v, %v", batch, err)
	}
	res := s.ExpireDeadLetter(ctx, "m1", testDLQRetention, "evt-1")
	if res.Outcome != delivery.DLQExpiryExpired || res.RecipientIdentity != ridA || res.Reason != "nack_exhausted" ||
		res.DeadLetteredMs != 1000 || res.ExpiredMs < batch.NowMs {
		t.Fatalf("expire = %+v", res)
	}
	for _, k := range []string{"hr1:dl:m1", "hr1:m:m1", "hr1:mi:m1", "hr1:a:m1"} {
		if exists(t, a, k) {
			t.Errorf("%s kept", k)
		}
	}
	if _, ok := score(t, a, "hr1:dlq", "m1"); ok {
		t.Error("dlq member kept")
	}
	if !exists(t, a, "hr1:m:m2") || lrange(t, a, "hr1:r:"+ridA+":q")[0] != "m2" {
		t.Error("the Recipient's queued message was touched")
	}

	entries, _ := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Build()).ToArray()
	if len(entries) != 1 {
		t.Fatalf("audit entries = %d, want 1", len(entries))
	}
	fields, _ := streamEntryFields(entries[0])
	want := map[string]string{
		"event_id": "evt-1", "timestamp_ms": itoa64(res.ExpiredMs), "actor": "maintenance", "operation": "dead_letter_expired",
		"target": "m1", "outcome": "success", "reason": "nack_exhausted",
	}
	if len(fields) != len(want) {
		t.Errorf("audit fields = %v", fields)
	}
	for k, v := range want {
		if fields[k] != v {
			t.Errorf("audit %s = %q, want %q", k, fields[k], v)
		}
	}
	for _, v := range fields {
		if strings.Contains(v, "received_ms") || strings.Contains(v, "payload") {
			t.Errorf("audit carries payload: %v", fields)
		}
	}
	if batch, _ := s.DueDeadLetters(ctx, 10, testDLQRetention); len(batch.Entries) != 0 {
		t.Errorf("due after expiry = %+v", batch.Entries)
	}
}

// TestExpireDLQNotDueStaleAndRefusals pins not_due and every refusal with
// no mutation, and the stale path removing only the orphan member.
func TestExpireDLQNotDueStaleAndRefusals(t *testing.T) {
	ctx := context.Background()
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	deadLetterOne(t, s, "op-1", "dlv_1")
	if batch, _ := s.DueDeadLetters(ctx, 10, testDLQRetention); len(batch.Entries) != 0 {
		t.Errorf("fresh dead letter listed as due: %+v", batch.Entries)
	}
	before := snapshot(t, a)
	if r := s.ExpireDeadLetter(ctx, "m1", testDLQRetention, "evt"); r.Outcome != delivery.DLQExpiryNotDue {
		t.Errorf("fresh = %+v", r)
	}
	assertUnchanged(t, a, before, "not_due")

	a.testDo(t, "ZADD", "hr1:dlq", "1000", "orphan")
	if r := s.ExpireDeadLetter(ctx, "orphan", testDLQRetention, "evt"); r.Outcome != delivery.DLQExpiryStale {
		t.Errorf("orphan = %+v", r)
	}
	if _, ok := score(t, a, "hr1:dlq", "orphan"); ok {
		t.Error("orphan member kept")
	}
	assertUnchanged(t, a, before, "stale")

	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"record not a hash":          func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dl:m1"); a.testDo(t, "SET", "hr1:dl:m1", "x") },
		"record without time":        func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:dl:m1", "dead_lettered_ms") },
		"audit not a stream":         func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:audit", "x") },
		"dlq index not a sorted set": func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:dlq"); a.testDo(t, "SET", "hr1:dlq", "x") },
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueue(t, a, "m1", ridA)
			agedDeadLetter(t, a, s, "m1")
			setup(t, a)
			before := snapshot(t, a)
			if r := s.ExpireDeadLetter(ctx, "m1", testDLQRetention, "evt"); r.Outcome != delivery.DLQExpiryInternalFailure {
				t.Errorf("expire = %+v", r)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestExpireDLQArgumentsAndReload pins argument rejection and the EVAL
// reload after SCRIPT FLUSH.
func TestExpireDLQArgumentsAndReload(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:dlq", "hr1:audit"}
	valid := []string{"m1", "3600000", "evt", "hr1"}
	for i := range valid {
		args := append([]string(nil), valid...)
		args[i] = ""
		if _, err := a.RunScript(ctx, "expire_dlq_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("empty argument %d accepted: %v", i+1, err)
		}
	}
	for _, bad := range []string{"0", "-1", "1.5"} {
		args := append([]string(nil), valid...)
		args[1] = bad
		if _, err := a.RunScript(ctx, "expire_dlq_v1", keys, args); err == nil {
			t.Errorf("retention %q accepted", bad)
		}
	}
	if _, err := a.RunScript(ctx, "expire_dlq_v1", keys[:1], valid); err == nil {
		t.Error("missing key accepted")
	}
	if _, err := a.RunScript(ctx, "expire_dlq_v1", []string{"hr1:dlq", "other:audit"}, valid); err == nil {
		t.Error("foreign key accepted")
	}

	enqueue(t, a, "m1", ridA)
	agedDeadLetter(t, a, s, "m1")
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := s.ExpireDeadLetter(ctx, "m1", testDLQRetention, "evt"); r.Outcome != delivery.DLQExpiryExpired {
		t.Errorf("expire after SCRIPT FLUSH = %+v", r)
	}
}

// TestMaintenanceRoundExpiresDeadLettersOverValkey runs a real maintenance
// round against Valkey: the aged dead letter is deleted, the fresh one kept.
func TestMaintenanceRoundExpiresDeadLettersOverValkey(t *testing.T) {
	a, s := claimSetup(t)
	enqueue(t, a, "m1", ridA)
	enqueue(t, a, "m2", ridA)
	agedDeadLetter(t, a, s, "m1")
	deadLetterOne(t, s, "op-m2", "dlv_m2")
	attempts, err := delivery.NewAttemptMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	policy := delivery.RetryPolicy{MaxAttempts: 4, Delays: []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}, JitterMin: 0.5, JitterMax: 1}
	m, err := delivery.NewMaintenance(delivery.MaintenanceDeps{
		Retries: s, Leases: s, DeadLetters: s, DLQRetention: testDLQRetention, RetryPolicy: policy, Attempts: attempts,
		Config: delivery.MaintenanceConfig{Interval: time.Second, BatchSize: 10, MaxContinuousBatches: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	m.RunRound(context.Background())
	if exists(t, a, "hr1:dl:m1") || !exists(t, a, "hr1:dl:m2") {
		t.Errorf("after the round: m1 kept %v, m2 kept %v", exists(t, a, "hr1:dl:m1"), exists(t, a, "hr1:dl:m2"))
	}
}

package valkey

import (
	"context"
	"errors"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

// TestOperationsSnapshot pins the snapshot over real state: an empty
// store, then queued, leased, and dead-lettered messages, a blocked
// Recipient, an endpoint, a session, and audit entries; and wrong types.
func TestOperationsSnapshot(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	ops := NewOperationsStore(a)
	snap, err := ops.OperationsSnapshot(ctx)
	if err != nil || snap.UsedMemoryBytes <= 0 {
		t.Fatalf("empty = %+v, %v", snap, err)
	}
	snap.UsedMemoryBytes, snap.MaxMemoryBytes = 0, 0
	if snap != (administration.OperationsSnapshot{}) {
		t.Errorf("empty = %+v", snap)
	}

	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	dead := deadLetterOne(t, s, "op-1", "dlv_1") // m1 dead-lettered, m2 ready
	d := claimNext(t, s, "op-2", "dlv_2")        // m2 leased
	enqueueJSON(t, a, "m3", ridB)
	a.testDo(t, "ZADD", "hr1:blocked", "5", "telegram:9:bot")
	a.testDo(t, "ZADD", "hr1:webhooks", "1", "telegram:wh_a")
	a.testDo(t, "ZADD", "hr1:admin_sessions", "1", "digest")
	a.testDo(t, "XADD", "hr1:audit", "*", "operation", "x")
	snap, err = ops.OperationsSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := administration.OperationsSnapshot{
		QueuedMessages: 2, ReadyRecipients: 1, LeasedRecipients: 1, BlockedRecipients: 1, EarliestLeaseExpiresMs: d.LeaseExpiresMs,
		DeadLetters: 1, OldestDeadLetteredMs: dead, NewestDeadLetteredMs: dead, WebhookEndpoints: 1, AdminSessions: 1, AuditLength: 1,
		UsedMemoryBytes: snap.UsedMemoryBytes, MaxMemoryBytes: snap.MaxMemoryBytes,
	}
	if snap != want {
		t.Errorf("snapshot = %+v\nwant       %+v", snap, want)
	}

	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"counter not a string": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:stats:queued_messages")
			a.testDo(t, "RPUSH", "hr1:stats:queued_messages", "x")
		},
		"counter not an integer": func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:stats:queued_messages", "-1") },
		"ready not a zset":       func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready", "x") },
		"sessions not a zset":    func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:admin_sessions", "x") },
		"audit not a stream":     func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:audit", "x") },
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := claimSetup(t)
			setup(t, a)
			if _, err := NewOperationsStore(a).OperationsSnapshot(ctx); !errors.Is(err, administration.ErrStoredWrongType) {
				t.Errorf("err = %v", err)
			}
		})
	}
	if _, err := a.RunScript(ctx, "operations_summary_v1", []string{"hr1:x"}, []string{"hr1"}); err == nil {
		t.Error("wrong key count accepted")
	}
}

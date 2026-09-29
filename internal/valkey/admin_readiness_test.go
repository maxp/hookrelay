package valkey

import (
	"context"
	"testing"
)

// TestAdminReadinessChecksPersistedStructures pins the startup/recovery gate
// against damaged endpoint Hashes, bot sets and derived listings.
func TestAdminReadinessChecksPersistedStructures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		poison func(*testing.T, *Adapter)
	}{
		{"wrong endpoint type", func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:wh:telegram:wh_test1")
			a.testDo(t, "SET", "hr1:wh:telegram:wh_test1", "poison")
		}},
		{"missing required endpoint field", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HDEL", "hr1:wh:telegram:wh_test1", "credential_value")
		}},
		{"invalid endpoint encoding", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:wh:telegram:wh_test1", "enabled", "invalid")
		}},
		{"invalid endpoint timestamp", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:wh:telegram:wh_test1", "created_ms", "1.5")
		}},
		{"invalid credential value", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:wh:telegram:wh_test1", "credential_value", "invalid secret")
		}},
		{"wrong bot set type", func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:bot:telegram:123456789:webhooks")
			a.testDo(t, "SET", "hr1:bot:telegram:123456789:webhooks", "poison")
		}},
		{"missing bot membership", func(t *testing.T, a *Adapter) {
			a.testDo(t, "SREM", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_test1")
		}},
		{"missing global listing", func(t *testing.T, a *Adapter) {
			a.testDo(t, "ZREM", "hr1:webhooks", "telegram:wh_test1")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAdapter(t, false)
			flushAll(t, a)
			gate(t, a, false)
			if _, _, res := a.CreateEndpoint(context.Background(), endpointFixture(), "event-1", "webhook_endpoint_created", "req-1"); res != CreateOK {
				t.Fatalf("setup create = %s", res)
			}
			tc.poison(t, a)
			before := snapshot(t, a)
			if _, err := a.Reconcile(context.Background(), ReconcileOptions{BatchSize: 7, Full: true}); err == nil {
				t.Fatal("startup reconciliation passed incompatible administrative state")
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestReadinessRefusesWrongGlobalIndexEvenWhenEmpty ensures a wrong-type
// global index cannot hide behind an otherwise empty keyspace.
func TestReadinessRefusesWrongGlobalIndexEvenWhenEmpty(t *testing.T) {
	for _, key := range []string{"hr1:ready", "hr1:leases", "hr1:blocked", "hr1:dedup_age"} {
		t.Run(key, func(t *testing.T) {
			a := testAdapter(t, false)
			flushAll(t, a)
			a.testDo(t, "SET", key, "poison")
			if _, err := a.ValidateReadiness(context.Background(), false); err == nil {
				t.Fatal("readiness passed wrong-type global index")
			}
			if _, err := a.Reconcile(context.Background(), ReconcileOptions{BatchSize: 7, Full: true}); err == nil {
				t.Fatal("reconciliation silently skipped wrong-type index")
			}
		})
	}
}

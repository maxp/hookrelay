package valkey

import (
	"context"
	"testing"
)

// TestEndpointIndexReconciliationRepairsDerivedState pins both directions:
// authoritative endpoint Hashes restore their indexes, while stale reverse
// members are removed without changing endpoint data.
func TestEndpointIndexReconciliationRepairsDerivedState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		break_ func(*testing.T, *Adapter)
		check  func(*testing.T, *Adapter, ReconcileReport)
	}{
		{"missing bot membership", func(t *testing.T, a *Adapter) {
			a.testDo(t, "SREM", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_test1")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_bot_restored"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
			if ok, _ := a.client.Do(context.Background(), a.client.B().Sismember().Key("hr1:bot:telegram:123456789:webhooks").Member("telegram:wh_test1").Build()).AsBool(); !ok {
				t.Error("Bot Identity membership was not restored")
			}
		}},
		{"missing global listing", func(t *testing.T, a *Adapter) {
			a.testDo(t, "ZREM", "hr1:webhooks", "telegram:wh_test1")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_listing_restored"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
		}},
		{"wrong global listing score", func(t *testing.T, a *Adapter) {
			a.testDo(t, "ZADD", "hr1:webhooks", "1", "telegram:wh_test1")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_listing_score_repaired"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
			want := hget(t, a, "hr1:wh:telegram:wh_test1", "created_ms")
			if got, ok := score(t, a, "hr1:webhooks", "telegram:wh_test1"); !ok || itoa64(got) != want {
				t.Errorf("listing score = %d %v, want %s", got, ok, want)
			}
		}},
		{"orphan bot membership", func(t *testing.T, a *Adapter) {
			a.testDo(t, "SADD", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_absent")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_index_removed"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
			if ok, _ := a.client.Do(context.Background(), a.client.B().Sismember().Key("hr1:bot:telegram:123456789:webhooks").Member("telegram:wh_absent").Build()).AsBool(); ok {
				t.Error("orphan Bot Identity member survived")
			}
		}},
		{"mismatched bot membership", func(t *testing.T, a *Adapter) {
			a.testDo(t, "SADD", "hr1:bot:telegram:987654321:webhooks", "telegram:wh_test1")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_index_removed"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
			if exists(t, a, "hr1:bot:telegram:987654321:webhooks") {
				t.Error("empty mismatched Bot Identity Set survived")
			}
		}},
		{"orphan listing member", func(t *testing.T, a *Adapter) {
			a.testDo(t, "ZADD", "hr1:webhooks", "1", "telegram:wh_absent")
		}, func(t *testing.T, a *Adapter, rep ReconcileReport) {
			if rep.Findings["endpoint_index_removed"] != 1 {
				t.Errorf("findings = %+v", rep.Findings)
			}
			if _, ok := score(t, a, "hr1:webhooks", "telegram:wh_absent"); ok {
				t.Error("orphan listing member survived")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := testAdapter(t, false)
			flushAll(t, a)
			gate(t, a, false)
			if _, _, res := a.CreateEndpoint(context.Background(), endpointFixture(), "event-1", "webhook_endpoint_created", "req-1"); res != CreateOK {
				t.Fatalf("setup create = %s", res)
			}
			tc.break_(t, a)
			rep := reconcile(t, a, true)
			if rep.Hold() != "" {
				t.Fatalf("hold = %q, findings %+v", rep.Hold(), rep.Findings)
			}
			tc.check(t, a, rep)
			again := reconcile(t, a, true)
			for _, kind := range []string{"endpoint_index_removed", "endpoint_bot_restored", "endpoint_listing_restored", "endpoint_listing_score_repaired"} {
				if again.Findings[kind] != 0 {
					t.Errorf("second pass %s = %d", kind, again.Findings[kind])
				}
			}
		})
	}
}

// TestAdminReadinessChecksPersistedStructures pins the startup/recovery gate
// against damaged authoritative endpoint Hashes and wrong-typed indexes.
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
		{"wrong listing type", func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:webhooks")
			a.testDo(t, "SET", "hr1:webhooks", "poison")
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
			rep, err := a.Reconcile(context.Background(), ReconcileOptions{BatchSize: 7, Full: true})
			if err == nil && rep.Hold() != "webhook_endpoint_inconsistent" {
				t.Fatalf("startup reconciliation passed incompatible administrative state: %+v", rep)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestReadinessRefusesWrongGlobalIndexEvenWhenEmpty ensures a wrong-type
// global index cannot hide behind an otherwise empty keyspace.
func TestEndpointIndexReconciliationBatchesManyEndpoints(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	for i := 0; i < 20; i++ {
		e := endpointFixture()
		e.Identifier = "wh_batch" + itoa(i)
		e.GenerationID = "0195c4d8-0000-7000-8000-" + pad(i+1)
		if _, _, res := a.CreateEndpoint(ctx, e, "event-"+itoa(i), "webhook_endpoint_created", "req"); res != CreateOK {
			t.Fatalf("create %d = %s", i, res)
		}
	}
	a.testDo(t, "DEL", "hr1:webhooks", "hr1:bot:telegram:123456789:webhooks", auditKey)
	rep, err := a.Reconcile(ctx, ReconcileOptions{BatchSize: 3})
	if err != nil || rep.Hold() != "" || rep.Findings["endpoint_bot_restored"] != 20 || rep.Findings["endpoint_listing_restored"] != 20 {
		t.Fatalf("reconciliation = %+v, %v", rep, err)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Scard().Key("hr1:bot:telegram:123456789:webhooks").Build()).AsInt64(); n != 20 {
		t.Errorf("Bot Identity members = %d", n)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Zcard().Key("hr1:webhooks").Build()).AsInt64(); n != 20 {
		t.Errorf("global listing members = %d", n)
	}
}

func TestReconcileEndpointScriptContract(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	if _, _, res := a.CreateEndpoint(ctx, endpointFixture(), "event-1", "webhook_endpoint_created", "req-1"); res != CreateOK {
		t.Fatalf("setup create = %s", res)
	}

	// A malformed authoritative record is a bounded refusal with no writes.
	a.testDo(t, "HDEL", "hr1:wh:telegram:wh_test1", "credential_value")
	before := snapshot(t, a)
	res, err := a.RunScript(ctx, "reconcile_endpoint_v1", []string{"hr1:webhooks"},
		[]string{"endpoint", "telegram:wh_test1", "", "", "hr1"})
	if err != nil || res.Status != "invalid" {
		t.Fatalf("invalid endpoint = %+v, %v", res, err)
	}
	assertUnchanged(t, a, before, "invalid endpoint reconciliation")

	// Caller-contract violations are errors, not bounded persisted outcomes.
	if _, err := a.RunScript(ctx, "reconcile_endpoint_v1", []string{"hr1:webhooks"},
		[]string{"bogus", "telegram:wh_test1", "", "", "hr1"}); err == nil {
		t.Error("invalid mode was accepted")
	}

	// The registered script reloads after SCRIPT FLUSH.
	a.testDo(t, "HSET", "hr1:wh:telegram:wh_test1", "credential_value", endpointFixture().CredentialValue)
	a.testDo(t, "ZREM", "hr1:webhooks", "telegram:wh_test1")
	a.testDo(t, "SCRIPT", "FLUSH")
	res, err = a.RunScript(ctx, "reconcile_endpoint_v1", []string{"hr1:webhooks"},
		[]string{"endpoint", "telegram:wh_test1", "", "", "hr1"})
	if err != nil || res.Status != "repaired" {
		t.Fatalf("after SCRIPT FLUSH = %+v, %v", res, err)
	}
}

func TestReadinessRefusesWrongGlobalIndexEvenWhenEmpty(t *testing.T) {
	for _, key := range []string{"hr1:webhooks", "hr1:ready", "hr1:leases", "hr1:blocked", "hr1:dedup_age"} {
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

package valkey

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

// createAt creates an endpoint and pins its listing score and created_ms,
// so tests control the list order.
func createAt(t *testing.T, a *Adapter, id string, createdMs string) {
	t.Helper()
	e := endpointFixture()
	e.Identifier = id
	if _, _, r := a.CreateEndpoint(context.Background(), e, "event-"+id, "webhook_endpoint_created", "req"); r != CreateOK {
		t.Fatalf("create %s = %s", id, r)
	}
	a.testDo(t, "ZADD", "hr1:webhooks", createdMs, "telegram:"+id)
	a.testDo(t, "HSET", "hr1:wh:telegram:"+id, "created_ms", createdMs)
}

// TestListEndpoints pins the listing read: descending created_ms, equal
// scores by descending member, strict-after cursors across the tie, and
// orphan members reported (missing, wrong type, malformed) rather than
// failing the page.
func TestListEndpoints(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	createAt(t, a, "wh_a", "100")
	createAt(t, a, "wh_b1", "200")
	createAt(t, a, "wh_b2", "200")
	createAt(t, a, "wh_c", "300")
	a.testDo(t, "ZADD", "hr1:webhooks", "400", "telegram:wh_gone")
	a.testDo(t, "ZADD", "hr1:webhooks", "350", "telegram:wh_str")
	a.testDo(t, "SET", "hr1:wh:telegram:wh_str", "x")
	createAt(t, a, "wh_bad", "50")
	a.testDo(t, "HDEL", "hr1:wh:telegram:wh_bad", "generation_id")
	store := NewEndpointStore(a)

	page, err := store.ListEndpoints(ctx, 4, nil)
	if err != nil || len(page) != 4 {
		t.Fatalf("page 1 = %+v, %v", page, err)
	}
	want := []struct{ member, orphan string }{
		{"telegram:wh_gone", administration.OrphanMissing},
		{"telegram:wh_str", administration.OrphanWrongType},
		{"telegram:wh_c", ""},
		{"telegram:wh_b2", ""},
	}
	for i, w := range want {
		if page[i].Member != w.member || page[i].Orphan != w.orphan || (w.orphan == "") != (page[i].Endpoint != nil) {
			t.Errorf("item %d = %+v, want %+v", i, page[i], w)
		}
	}
	if e := page[2].Endpoint; e.Identifier != "wh_c" || e.CreatedMs != 300 || e.BotID != "123456789" || e.ConfigVersion != 1 {
		t.Errorf("endpoint = %+v", e)
	}

	page, err = store.ListEndpoints(ctx, 4, &administration.EndpointCursor{CreatedMs: 200, ID: "telegram:wh_b2"})
	if err != nil || len(page) != 3 || page[0].Member != "telegram:wh_b1" || page[1].Member != "telegram:wh_a" ||
		page[2].Member != "telegram:wh_bad" || page[2].Orphan != administration.OrphanMalformed {
		t.Fatalf("page 2 = %+v, %v", page, err)
	}

	flushAll(t, a)
	if page, err := store.ListEndpoints(ctx, 10, nil); err != nil || len(page) != 0 {
		t.Errorf("empty listing = %+v, %v", page, err)
	}
}

// adminSetup creates the fixture endpoint (generation ...0001, version 1,
// enabled) on a fresh gated database.
func adminSetup(t *testing.T) (*Adapter, administration.EndpointRepository) {
	t.Helper()
	a := testAdapter(t, false)
	flushAll(t, a)
	gate(t, a, false)
	if _, _, r := a.CreateEndpoint(context.Background(), endpointFixture(), "event-0", "webhook_endpoint_created", "req-0"); r != CreateOK {
		t.Fatalf("create = %s", r)
	}
	return a, NewEndpointStore(a)
}

func ref(id string) administration.EndpointRef {
	return administration.EndpointRef{Type: "telegram", Identifier: id}
}

func fixtureVersion(v int64) *administration.EntityVersion {
	return &administration.EntityVersion{GenerationID: endpointFixture().GenerationID, ConfigVersion: v}
}

// lastAudit returns the newest audit entry.
func lastAudit(t *testing.T, a *Adapter) map[string]string {
	t.Helper()
	entries, err := a.AuditEntries(context.Background(), 100)
	if err != nil || len(entries) == 0 {
		t.Fatalf("audit: %v", err)
	}
	return entries[len(entries)-1]
}

// TestSetEndpointEnabledTuples pins endpoint_set_enabled_v1: updated with
// the safe fields, the Hash and the audit entry written together,
// unchanged without any write, and every refusal leaving state
// snapshot-equal.
func TestSetEndpointEnabledTuples(t *testing.T) {
	a, store := adminSetup(t)
	ctx := context.Background()
	before, _ := a.GetEndpoint(ctx, "telegram", "wh_test1")

	e, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), false, fixtureVersion(1), "event-1", "req-1")
	if r != administration.SetEnabledUpdated || e.Enabled || e.ConfigVersion != 2 || e.BotID != "123456789" ||
		e.CredentialKind != "secret_token" || e.CredentialValue != "" || e.CreatedMs != before.CreatedMs || e.UpdatedMs < before.UpdatedMs {
		t.Fatalf("disable = %s %+v", r, e)
	}
	stored, _ := a.GetEndpoint(ctx, "telegram", "wh_test1")
	if stored.Enabled || stored.ConfigVersion != 2 || stored.UpdatedMs != e.UpdatedMs || stored.CredentialValue != "test-credential-value" {
		t.Errorf("stored = %+v", stored)
	}
	audit := lastAudit(t, a)
	if audit["operation"] != "webhook_endpoint_disabled" || audit["event_id"] != "event-1" || audit["target"] != "telegram:wh_test1" ||
		audit["request_id"] != "req-1" || audit["actor"] != "admin_bearer" || audit["outcome"] != "success" || audit["timestamp_ms"] != strconv.FormatInt(e.UpdatedMs, 10) {
		t.Errorf("audit = %v", audit)
	}
	for _, v := range audit {
		if strings.Contains(v, "test-credential-value") {
			t.Fatal("the credential reached the audit stream")
		}
	}

	snap := snapshot(t, a)
	if e, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), false, fixtureVersion(2), "event-2", "req-2"); r != administration.SetEnabledUnchanged || e.ConfigVersion != 2 || e.Enabled {
		t.Errorf("same value = %s %+v", r, e)
	}
	assertUnchanged(t, a, snap, "unchanged")
	if e, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), true, fixtureVersion(1), "event-3", "req-3"); r != administration.SetEnabledPreconditionFailed || e != nil {
		t.Errorf("stale = %s %+v", r, e)
	}
	other := &administration.EntityVersion{GenerationID: "0195c4d8-0000-7000-8000-000000000099", ConfigVersion: 2}
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), true, other, "event-4", "req-4"); r != administration.SetEnabledPreconditionFailed {
		t.Errorf("other generation = %s", r)
	}
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), true, nil, "event-5", "req-5"); r != administration.SetEnabledPreconditionRequired {
		t.Errorf("no precondition = %s", r)
	}
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_none"), true, nil, "event-6", "req-6"); r != administration.SetEnabledNotFound {
		t.Errorf("missing = %s", r)
	}
	assertUnchanged(t, a, snap, "refusals")

	if e, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), true, fixtureVersion(2), "event-7", "req-7"); r != administration.SetEnabledUpdated || !e.Enabled || e.ConfigVersion != 3 {
		t.Errorf("enable = %s %+v", r, e)
	}
	if audit := lastAudit(t, a); audit["operation"] != "webhook_endpoint_enabled" {
		t.Errorf("enable audit = %v", audit)
	}

	// Wrong types and a malformed Hash refuse without writing.
	a.testDo(t, "HSET", "hr1:wh:telegram:wh_bad", "bot_id", "1", "enabled", "1", "config_version", "x")
	a.testDo(t, "SET", "hr1:wh:telegram:wh_str", "x")
	snap = snapshot(t, a)
	for _, id := range []string{"wh_bad", "wh_str"} {
		if _, r := store.SetEndpointEnabled(ctx, ref(id), false, fixtureVersion(1), "event-8", "req-8"); r != administration.SetEnabledWrongType {
			t.Errorf("%s = %s", id, r)
		}
	}
	a.testDo(t, "DEL", "hr1:audit")
	a.testDo(t, "SET", "hr1:audit", "x")
	snap = snapshot(t, a)
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), false, fixtureVersion(3), "event-9", "req-9"); r != administration.SetEnabledWrongType {
		t.Errorf("audit wrong type = %s", r)
	}
	assertUnchanged(t, a, snap, "wrong types")
}

// TestSetEndpointEnabledArgumentsAndReload pins argument rejection (a
// caller bug, not a bounded status) and EVAL reload after SCRIPT FLUSH.
func TestSetEndpointEnabledArgumentsAndReload(t *testing.T) {
	a, store := adminSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:wh:telegram:wh_test1", "hr1:audit"}
	for name, args := range map[string][]string{
		"too few":           {"telegram", "wh_test1", "1", "", "", "e"},
		"empty identifier":  {"telegram", "", "1", "", "", "e", "r"},
		"bad enabled":       {"telegram", "wh_test1", "yes", "", "", "e", "r"},
		"half version":      {"telegram", "wh_test1", "1", "g", "", "e", "r"},
		"malformed version": {"telegram", "wh_test1", "1", "g", "01", "e", "r"},
		"key mismatch":      {"telegram", "wh_other", "1", "", "", "e", "r"},
	} {
		if _, err := a.RunScript(ctx, "endpoint_set_enabled_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), false, fixtureVersion(1), "e", "r"); r != administration.SetEnabledUpdated {
		t.Errorf("after SCRIPT FLUSH = %s", r)
	}
}

// TestDeleteEndpointTuples pins endpoint_delete_v1: every key of a deleted
// endpoint removed with the audit entry written, the Bot Identity Set kept
// while other endpoints remain, absent as a no-op, and every refusal
// snapshot-equal.
func TestDeleteEndpointTuples(t *testing.T) {
	a, store := adminSetup(t)
	ctx := context.Background()
	second := endpointFixture()
	second.Identifier = "wh_test2"
	if _, _, r := a.CreateEndpoint(ctx, second, "event-00", "webhook_endpoint_created", "req"); r != CreateOK {
		t.Fatal(r)
	}
	const botKey = "hr1:bot:telegram:123456789:webhooks"

	snap := snapshot(t, a)
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(1), "e", "r"); r != administration.DeleteMustBeDisabled {
		t.Errorf("enabled = %s", r)
	}
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", nil, "e", "r"); r != administration.DeletePreconditionRequired {
		t.Errorf("no precondition = %s", r)
	}
	if e, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(7), "e", "r"); r != administration.DeletePreconditionFailed || e != nil {
		t.Errorf("stale = %s %+v", r, e)
	}
	assertUnchanged(t, a, snap, "refusals")

	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test1"), false, fixtureVersion(1), "e-off", "r"); r != administration.SetEnabledUpdated {
		t.Fatal(r)
	}
	e, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(2), "event-del", "req-del")
	if r != administration.DeleteDeleted || e.BotID != "123456789" || e.CredentialKind != "secret_token" || e.ConfigVersion != 2 ||
		e.GenerationID != endpointFixture().GenerationID || e.DeletedMs <= 0 {
		t.Fatalf("delete = %s %+v", r, e)
	}
	if got, _ := a.GetEndpoint(ctx, "telegram", "wh_test1"); got != nil {
		t.Error("the endpoint Hash survived")
	}
	members, _ := a.client.Do(ctx, a.client.B().Smembers().Key(botKey).Build()).AsStrSlice()
	if len(members) != 1 || members[0] != "telegram:wh_test2" {
		t.Errorf("bot set = %v", members)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Zcard().Key("hr1:webhooks").Build()).AsInt64(); n != 1 {
		t.Errorf("listing size = %d", n)
	}
	audit := lastAudit(t, a)
	if audit["operation"] != "webhook_endpoint_deleted" || audit["event_id"] != "event-del" || audit["target"] != "telegram:wh_test1" ||
		audit["timestamp_ms"] != strconv.FormatInt(e.DeletedMs, 10) {
		t.Errorf("audit = %v", audit)
	}

	snap = snapshot(t, a)
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(2), "e", "r"); r != administration.DeleteAbsent {
		t.Errorf("repeat = %s", r)
	}
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", nil, "e", "r"); r != administration.DeleteAbsent {
		t.Errorf("absent without precondition = %s", r)
	}
	assertUnchanged(t, a, snap, "absent")

	// Deleting the last endpoint of a bot removes its Set.
	if _, r := store.SetEndpointEnabled(ctx, ref("wh_test2"), false, fixtureVersion(1), "e", "r"); r != administration.SetEnabledUpdated {
		t.Fatal(r)
	}
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test2"), "telegram", fixtureVersion(2), "e", "r"); r != administration.DeleteDeleted {
		t.Fatal(r)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Exists().Key(botKey).Build()).AsInt64(); n != 0 {
		t.Error("the empty Bot Identity Set survived")
	}

	// A recreated identifier gets a new generation; the old ETag is stale.
	recreated := endpointFixture()
	recreated.GenerationID = "0195c4d8-0000-7000-8000-000000000002"
	recreated.Enabled = false
	if _, _, r := a.CreateEndpoint(ctx, recreated, "e", "webhook_endpoint_created", "r"); r != CreateOK {
		t.Fatal(r)
	}
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(1), "e", "r"); r != administration.DeletePreconditionFailed {
		t.Errorf("earlier generation = %s", r)
	}

	// Wrong types refuse without writing.
	a.testDo(t, "DEL", botKey)
	a.testDo(t, "SET", botKey, "x")
	snap = snapshot(t, a)
	v := &administration.EntityVersion{GenerationID: recreated.GenerationID, ConfigVersion: 1}
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", v, "e", "r"); r != administration.DeleteWrongType {
		t.Errorf("bot set wrong type = %s", r)
	}
	assertUnchanged(t, a, snap, "wrong type")
}

// TestDeleteEndpointArgumentsAndReload pins argument rejection and EVAL
// reload after SCRIPT FLUSH.
func TestDeleteEndpointArgumentsAndReload(t *testing.T) {
	a, store := adminSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:wh:telegram:wh_test1", "hr1:webhooks", "hr1:audit"}
	for name, args := range map[string][]string{
		"too few":        {"telegram", "wh_test1", "telegram", "", "", "e"},
		"empty platform": {"telegram", "wh_test1", "", "", "", "e", "r"},
		"half version":   {"telegram", "wh_test1", "telegram", "", "1", "e", "r"},
		"key mismatch":   {"telegram", "wh_x", "telegram", "", "", "e", "r"},
	} {
		if _, err := a.RunScript(ctx, "endpoint_delete_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if _, r := store.DeleteEndpoint(ctx, ref("wh_test1"), "telegram", fixtureVersion(1), "e", "r"); r != administration.DeleteMustBeDisabled {
		t.Errorf("after SCRIPT FLUSH = %s", r)
	}
}

// TestListBotEndpoints pins the Bot Identity read: every member with its
// record newest first, orphans reported and sorted last, an absent Set as
// an empty list, and a wrong-typed Set as the stored-type error.
func TestListBotEndpoints(t *testing.T) {
	a, store := adminSetup(t)
	ctx := context.Background()
	createAt(t, a, "wh_newer", "9999999999999")
	a.testDo(t, "SADD", "hr1:bot:telegram:123456789:webhooks", "telegram:wh_gone")
	items, err := store.ListBotEndpoints(ctx, "telegram", "123456789")
	if err != nil || len(items) != 3 {
		t.Fatalf("items = %+v, %v", items, err)
	}
	if items[0].Member != "telegram:wh_newer" || items[1].Member != "telegram:wh_test1" || items[1].Endpoint.BotID != "123456789" ||
		items[2].Member != "telegram:wh_gone" || items[2].Orphan != administration.OrphanMissing {
		t.Errorf("order = %+v", items)
	}
	if items, err := store.ListBotEndpoints(ctx, "telegram", "1"); err != nil || len(items) != 0 {
		t.Errorf("absent bot = %+v, %v", items, err)
	}
	a.testDo(t, "SET", "hr1:bot:telegram:2:webhooks", "x")
	if _, err := store.ListBotEndpoints(ctx, "telegram", "2"); !errors.Is(err, administration.ErrStoredWrongType) {
		t.Errorf("wrong type = %v", err)
	}
}

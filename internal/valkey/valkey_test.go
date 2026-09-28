package valkey

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/valkey-io/valkey-go"
)

// The integration harness runs against a real pinned Valkey addressed by
// HOOKRELAY_TEST_VALKEY_URL. Local runs skip when absent; the CI job provides
// the pinned Valkey container. A fake Valkey cannot prove script semantics.
func mustTestURLOpt(t *testing.T) valkey.ClientOption {
	t.Helper()
	url := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if url == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set")
	}
	opt, err := valkey.ParseURL(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	return opt
}

func testAdapter(t *testing.T, production bool) *Adapter {
	t.Helper()
	url := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if url == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set; start the pinned Valkey container")
	}
	opt, err := valkey.ParseURL(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	a, err := NewAdapter(opt)
	if err != nil {
		t.Fatalf("NewAdapter (eager dial is the first dependency check): %v", err)
	}
	t.Cleanup(a.Close)
	return a
}

func gate(t *testing.T, a *Adapter, production bool) {
	t.Helper()
	report, err := a.ValidateReadiness(context.Background(), production)
	if err != nil {
		t.Fatalf("readiness gate: %v (report %+v)", err, report)
	}
	if !report.OK() {
		t.Fatalf("readiness report not OK: %+v", report)
	}
}

func flushAll(t *testing.T, a *Adapter) {
	t.Helper()
	// FLUSHDB: only this database — other packages may share the instance.
	if _, err := a.client.Do(context.Background(), a.client.B().Flushdb().Build()).ToMessage(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
}

func endpointFixture() Endpoint {
	return Endpoint{
		Type:            "telegram",
		Identifier:      "wh_test1",
		BotPlatform:     "telegram",
		BotID:           "123456789",
		Enabled:         true,
		CredentialKind:  "secret_token",
		CredentialValue: "test-credential-value",
		GenerationID:    "0195c4d8-0000-7000-8000-000000000001",
	}
}

// TestScriptLoadAndNoScriptReload covers the registry contract: startup load,
// EVALSHA execution, and the single EVAL reload after SCRIPT FLUSH.
func TestScriptLoadAndNoScriptReload(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)

	// Create through the script; then flush the server cache and create
	// another endpoint — RunScript must reload via EVAL exactly once.
	if _, _, r := a.CreateEndpoint(ctx, endpointFixture(), "event-1", "webhook_endpoint_created", "req-1"); r != CreateOK {
		t.Fatalf("first create = %s", r)
	}
	if _, err := a.client.Do(ctx, a.client.B().ScriptFlush().Build()).ToMessage(); err != nil {
		t.Fatalf("script flush: %v", err)
	}
	e2 := endpointFixture()
	e2.Identifier = "wh_test2"
	e2.GenerationID = "0195c4d8-0000-7000-8000-000000000002"
	if _, _, r := a.CreateEndpoint(ctx, e2, "event-2", "webhook_endpoint_created", "req-2"); r != CreateOK {
		t.Fatalf("create after SCRIPT FLUSH (must self-heal via EVAL) = %s", r)
	}

	// Both endpoints stored.
	for _, id := range []string{"wh_test1", "wh_test2"} {
		got, err := a.GetEndpoint(ctx, "telegram", id)
		if err != nil || got == nil {
			t.Fatalf("get %s: %v %v", id, got, err)
		}
	}
}

// TestCreateEndpointTuplesAndKeys pins every status tuple variant and every
// affected key after success, including absence of mutation on failures.
func TestCreateEndpointTuplesAndKeys(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)

	createdMs, updatedMs, r := a.CreateEndpoint(ctx, endpointFixture(), "event-1", "webhook_endpoint_created", "req-1")
	if r != CreateOK {
		t.Fatalf("create = %s", r)
	}
	if createdMs <= 0 || updatedMs != createdMs {
		t.Fatalf("timestamps: created=%d updated=%d (Valkey TIME must fill both)", createdMs, updatedMs)
	}

	// Endpoint Hash fields.
	e, err := a.GetEndpoint(ctx, "telegram", "wh_test1")
	if err != nil || e == nil {
		t.Fatalf("get: %v %v", e, err)
	}
	if e.BotID != "123456789" || !e.Enabled || e.CredentialKind != "secret_token" || e.CredentialValue != "test-credential-value" {
		t.Errorf("stored fields: %+v", e)
	}
	if e.GenerationID != "0195c4d8-0000-7000-8000-000000000001" || e.ConfigVersion != 1 || e.CreatedMs != createdMs {
		t.Errorf("identity/version fields: %+v", e)
	}

	// Bot Identity SET membership.
	members, err := a.client.Do(ctx, a.client.B().Smembers().Key("hr1:bot:telegram:123456789:webhooks").Build()).ToMessage()
	if err != nil {
		t.Fatalf("smembers: %v", err)
	}
	set, _ := members.AsStrSlice()
	if len(set) != 1 || set[0] != "telegram:wh_test1" {
		t.Errorf("bot set = %v", set)
	}

	// Global listing ZSET with created_ms score.
	score, err := a.client.Do(ctx, a.client.B().Zscore().Key("hr1:webhooks").Member("telegram:wh_test1").Build()).ToMessage()
	if err != nil {
		t.Fatalf("zscore: %v", err)
	}
	if s, _ := score.AsInt64(); s != createdMs {
		t.Errorf("listing score = %d, want %d", s, createdMs)
	}

	// Audit stream: exactly one event with the bounded fields.
	entries, err := a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Count(10).Build()).ToMessage()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	flat, _ := entries.ToArray()
	if len(flat) != 1 {
		t.Fatalf("audit entries = %d, want exactly 1", len(flat))
	}
	entryFields, err := streamEntryFields(flat[0])
	if err != nil {
		t.Fatalf("audit entry shape: %v", err)
	}
	for field, want := range map[string]string{
		"actor":      "admin_bearer",
		"operation":  "webhook_endpoint_created",
		"target":     "telegram:wh_test1",
		"request_id": "req-1",
		"outcome":    "success",
	} {
		if entryFields[field] != want {
			t.Errorf("audit %s = %q, want %q", field, entryFields[field], want)
		}
	}
	if entryFields["event_id"] == "" {
		t.Error("audit event_id missing")
	}

	// Conflict: same identifier → conflict, no second audit event, no
	// mutation of the stored generation.
	e2 := endpointFixture()
	e2.GenerationID = "0195c4d8-0000-7000-8000-000000000099"
	if _, _, r := a.CreateEndpoint(ctx, e2, "event-2", "webhook_endpoint_created", "req-2"); r != CreateConflict {
		t.Fatalf("duplicate create = %s, want conflict", r)
	}
	got, _ := a.GetEndpoint(ctx, "telegram", "wh_test1")
	if got.GenerationID != "0195c4d8-0000-7000-8000-000000000001" {
		t.Errorf("conflict mutated the stored record: %+v", got)
	}
	entries, _ = a.client.Do(ctx, a.client.B().Xrange().Key("hr1:audit").Start("-").End("+").Count(10).Build()).ToMessage()
	flat, _ = entries.ToArray()
	if len(flat) != 1 {
		t.Errorf("conflict appended an audit event (%d entries), want still 1", len(flat))
	}

	// Bot endpoint limit: the 100th create is refused and nothing is stored.
	for i := 2; i <= 100; i++ {
		eN := endpointFixture()
		eN.Identifier = "wh_fill" + itoa(i)
		eN.GenerationID = "0195c4d8-0000-7000-8000-" + pad(i)
		if _, _, r := a.CreateEndpoint(ctx, eN, "event-fill"+itoa(i), "webhook_endpoint_created", "req-fill"); r != CreateOK {
			t.Fatalf("fill create %d = %s", i, r)
		}
	}
	eOver := endpointFixture()
	eOver.Identifier = "wh_over"
	eOver.GenerationID = "0195c4d8-0000-7000-8000-00000000ffff"
	if _, _, r := a.CreateEndpoint(ctx, eOver, "event-over", "webhook_endpoint_created", "req-over"); r != CreateBotLimit {
		t.Fatalf("over-limit create = %s, want bot_endpoint_limit", r)
	}
	if got, err := a.GetEndpoint(ctx, "telegram", "wh_over"); err != nil || got != nil {
		t.Errorf("over-limit endpoint stored: %v %v", got, err)
	}

	// Wrong type: poison the bot set key, expect wrong_type and no writes.
	flushAll(t, a)
	if _, err := a.client.Do(ctx, a.client.B().Set().Key("hr1:bot:telegram:123456789:webhooks").Value("poison").Build()).ToMessage(); err != nil {
		t.Fatal(err)
	}
	if _, _, r := a.CreateEndpoint(ctx, endpointFixture(), "event-3", "webhook_endpoint_created", "req-3"); r != CreateWrongType {
		t.Fatalf("poisoned key create = %s, want wrong_type", r)
	}
	if got, err := a.GetEndpoint(ctx, "telegram", "wh_test1"); err != nil || got != nil {
		t.Errorf("wrong_type path stored the endpoint: %v %v", got, err)
	}
	if n, _ := a.client.Do(ctx, a.client.B().Xlen().Key("hr1:audit").Build()).ToMessage(); nInt(n) != 0 {
		t.Errorf("wrong_type path appended audit events: %d", nInt(n))
	}
}

// TestGetEndpointAbsenceAndWrongType covers the read path statuses.
func TestGetEndpointAbsenceAndWrongType(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)

	got, err := a.GetEndpoint(ctx, "telegram", "wh_missing")
	if err != nil || got != nil {
		t.Fatalf("missing endpoint: %v %v", got, err)
	}
	if _, err := a.client.Do(ctx, a.client.B().Set().Key("hr1:wh:telegram:poison").Value("x").Build()).ToMessage(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.GetEndpoint(ctx, "telegram", "poison"); err == nil || !strings.Contains(err.Error(), "unexpected type") {
		t.Fatalf("poisoned read: %v", err)
	}
}

// TestParserRejectsUnknownStatus feeds a script result with an unknown status
// through the typed parser — the parser must reject it, not pass it through.
func TestParserRejectsUnknownStatus(t *testing.T) {
	// Construct a message shaped like a script result with an unknown status.
	client, err := valkey.NewClient(mustTestURLOpt(t))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	raw := client.B().Arbitrary("EVAL", `return {'mystery_status'}`, "0").Build()
	msg, err := client.Do(ctx, raw).ToMessage()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseResult("endpoint_create_v1", registry["endpoint_create_v1"].Statuses, msg); err == nil || !strings.Contains(err.Error(), "unknown status") {
		t.Fatalf("unknown status accepted: %v", err)
	}
}

// TestReadinessGateProductionPersistence pins the production persistence
// checks against the plain (non-AOF) test instance.
func TestReadinessGateProductionPersistence(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)

	// Development: passes without AOF.
	gate(t, a, false)

	// Production: this instance has AOF disabled, so the gate must fail with
	// a persistence error.
	_, err := a.ValidateReadiness(context.Background(), true)
	if err == nil || !strings.Contains(err.Error(), "AOF") {
		t.Fatalf("production gate passed without AOF: %v", err)
	}
}

// TestStructureValidationRejectsWrongTypes poisons a known key with the wrong
// type and pins the refusal.
func TestStructureValidationRejectsWrongTypes(t *testing.T) {
	a := testAdapter(t, false)
	flushAll(t, a)
	if _, err := a.client.Do(context.Background(), a.client.B().Set().Key("hr1:audit").Value("poison").Build()).ToMessage(); err != nil {
		t.Fatal(err)
	}
	_, err := a.ValidateReadiness(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "unexpected type") {
		t.Fatalf("poisoned structure passed the gate: %v", err)
	}
}

// streamEntryFields flattens one XRANGE entry ([id, [k, v, ...]]) into a map.
func streamEntryFields(entry valkey.ValkeyMessage) (map[string]string, error) {
	parts, err := entry.ToArray()
	if err != nil {
		return nil, err
	}
	if len(parts) != 2 {
		return nil, fmt.Errorf("stream entry has %d parts, want 2", len(parts))
	}
	pairs, err := parts[1].ToArray()
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for i := 0; i+1 < len(pairs); i += 2 {
		k, _ := pairs[i].ToString()
		v, _ := pairs[i+1].ToString()
		out[k] = v
	}
	return out, nil
}

func itoa(n int) string { return fmtInt(n) }

func fmtInt(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func pad(n int) string {
	s := fmtInt(n)
	for len(s) < 12 {
		s = "0" + s
	}
	return s
}

func nInt(m valkey.ValkeyMessage) int64 {
	n, _ := m.AsInt64()
	return n
}

package valkey

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/maxp/hookrelay/internal/gen"
)

const (
	testAdminSecret      = "admin-secret-value-016"
	testAdminSecretOther = "admin-secret-value-017"
)

func auditOps(t *testing.T, a *Adapter) []map[string]string {
	t.Helper()
	entries, err := a.AuditEntries(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// TestAdminAuthInitializeAndCurrent pins initialization of a fresh store:
// a salt, a tag that is not the secret, a UUIDv7 generation, one audit
// event; then the same secret is a no-op without another event.
func TestAdminAuthInitializeAndCurrent(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	out, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{})
	if err != nil || out.Result != "initialized" {
		t.Fatalf("first = %+v, %v", out, err)
	}
	rec := hgetall(t, a, adminAuthKey)
	salt, err := base64.RawURLEncoding.DecodeString(rec["generation_salt"])
	if err != nil || len(salt) != 32 {
		t.Errorf("salt = %q", rec["generation_salt"])
	}
	if rec["generation_tag"] != adminGenerationTag(testAdminSecret, salt) || strings.Contains(strings.Join(mapValues(rec), " "), testAdminSecret) {
		t.Errorf("record = %v", rec)
	}
	if id, err := uuid.Parse(rec["generation_id"]); err != nil || id.Version() != 7 {
		t.Errorf("generation_id = %q", rec["generation_id"])
	}
	if ms, err := strconv.ParseInt(rec["updated_ms"], 10, 64); err != nil || ms <= 0 {
		t.Errorf("updated_ms = %q", rec["updated_ms"])
	}
	events := auditOps(t, a)
	if len(events) != 1 || events[0]["operation"] != "admin_auth_initialized" || events[0]["actor"] != "startup" || events[0]["target"] != "admin_auth" {
		t.Errorf("audit = %v", events)
	}

	before := snapshot(t, a)
	if out, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil || out.Result != "current" {
		t.Errorf("second = %+v, %v", out, err)
	}
	assertUnchanged(t, a, before, "current")
}

func mapValues(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	return out
}

// TestAdminAuthRotationRevokesSessions pins rotation after a secret change:
// every indexed session Hash and the index go, an unindexed session Hash
// stays (it is invalid by generation), the salt is kept, the tag and
// generation change, and one audit event records the bucketed count.
func TestAdminAuthRotationRevokesSessions(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	if _, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil {
		t.Fatal(err)
	}
	old := hgetall(t, a, adminAuthKey)
	for _, d := range []string{"d1", "d2", "d3"} {
		a.testDo(t, "HSET", "hr1:admin_session:"+d, "generation_id", old["generation_id"])
		a.testDo(t, "ZADD", adminSessionsKey, "1", d)
	}
	a.testDo(t, "HSET", "hr1:admin_session:unindexed", "generation_id", old["generation_id"])

	out, err := a.EnsureAdminAuth(ctx, testAdminSecretOther, gen.Crypto{})
	if err != nil || out.Result != "rotated" || out.Revoked != 3 {
		t.Fatalf("rotate = %+v, %v", out, err)
	}
	for _, k := range []string{"hr1:admin_session:d1", "hr1:admin_session:d2", "hr1:admin_session:d3", adminSessionsKey} {
		if exists(t, a, k) {
			t.Errorf("%s kept", k)
		}
	}
	if !exists(t, a, "hr1:admin_session:unindexed") {
		t.Error("unindexed session deleted")
	}
	rec := hgetall(t, a, adminAuthKey)
	salt, _ := base64.RawURLEncoding.DecodeString(rec["generation_salt"])
	if rec["generation_salt"] != old["generation_salt"] || rec["generation_id"] == old["generation_id"] ||
		rec["generation_tag"] != adminGenerationTag(testAdminSecretOther, salt) {
		t.Errorf("record = %v, old %v", rec, old)
	}
	events := auditOps(t, a)
	last := events[len(events)-1]
	if len(events) != 2 || last["operation"] != "admin_secret_generation_changed" || last["reason"] != "sessions_revoked_1_10" || last["actor"] != "startup" {
		t.Errorf("audit = %v", events)
	}
	if out, err := a.EnsureAdminAuth(ctx, testAdminSecretOther, gen.Crypto{}); err != nil || out.Result != "current" {
		t.Errorf("after rotation = %+v, %v", out, err)
	}
	// Rotating back with no sessions records the zero bucket.
	if out, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil || out.Result != "rotated" || out.Revoked != 0 {
		t.Errorf("rotate back = %+v, %v", out, err)
	}
	if last := auditOps(t, a)[2]; last["reason"] != "sessions_revoked_0" {
		t.Errorf("audit = %v", last)
	}
}

// TestAdminAuthRefusals pins every state the check must not act on: no
// mutation, and an error that withholds readiness.
func TestAdminAuthRefusals(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"record not a hash": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", adminAuthKey)
			a.testDo(t, "SET", adminAuthKey, "x")
		},
		"salt missing":  func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", adminAuthKey, "generation_salt") },
		"salt short":    func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", adminAuthKey, "generation_salt", "c2hvcnQ") },
		"tag malformed": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", adminAuthKey, "generation_tag", "abc") },
		"tag not hex": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", adminAuthKey, "generation_tag", strings.Repeat("z", 64))
		},
		"tag uppercase": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", adminAuthKey, "generation_tag", strings.Repeat("A", 64))
		},
		"generation not v7":  func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", adminAuthKey, "generation_id", uuid.NewString()) },
		"updated missing":    func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", adminAuthKey, "updated_ms") },
		"updated malformed":  func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", adminAuthKey, "updated_ms", "01") },
		"index not a zset":   func(t *testing.T, a *Adapter) { a.testDo(t, "SET", adminSessionsKey, "x") },
		"audit not a stream": func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:audit"); a.testDo(t, "SET", "hr1:audit", "x") },
		"too many sessions": func(t *testing.T, a *Adapter) {
			for i := range 101 {
				a.testDo(t, "ZADD", adminSessionsKey, "1", "d"+strconv.Itoa(i))
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := claimSetup(t)
			if _, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil {
				t.Fatal(err)
			}
			setup(t, a)
			before := snapshot(t, a)
			if out, err := a.EnsureAdminAuth(ctx, testAdminSecretOther, gen.Crypto{}); !errors.Is(err, errAdminAuthInconsistent) {
				t.Errorf("check = %+v, %v", out, err)
			}
			assertUnchanged(t, a, before, name)
		})
	}
	a, _ := claimSetup(t)
	if _, err := a.EnsureAdminAuth(ctx, "", gen.Crypto{}); err == nil {
		t.Error("empty secret accepted")
	}
}

// TestAdminAuthScriptContract pins the compare-and-set results, argument
// rejection, and the EVAL reload after SCRIPT FLUSH.
func TestAdminAuthScriptContract(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	keys := []string{adminAuthKey, adminSessionsKey, "hr1:audit"}
	tag := strings.Repeat("a", 64)
	id := gen.Crypto{}.UUIDv7()
	run := func(args ...string) string {
		t.Helper()
		res, err := a.RunScript(ctx, "admin_auth_v1", keys, args)
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return res.Status
	}
	if got := run("rotate", id, id, "salt", tag, "e", "hr1"); got != "absent" {
		t.Errorf("rotate absent = %s", got)
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if got := run("initialize", "", id, "salt", tag, "e", "hr1"); got != "initialized" {
		t.Errorf("initialize = %s", got)
	}
	if got := run("initialize", "", id, "salt", tag, "e", "hr1"); got != "exists" {
		t.Errorf("initialize again = %s", got)
	}
	other := gen.Crypto{}.UUIDv7()
	before := snapshot(t, a)
	for name, args := range map[string][]string{
		"other generation": {"rotate", other, other, "salt", strings.Repeat("b", 64), "e", "hr1"},
		"other salt":       {"rotate", id, other, "pepper", strings.Repeat("b", 64), "e", "hr1"},
	} {
		if got := run(args...); got != "changed" {
			t.Errorf("%s = %s", name, got)
		}
	}
	if got := run("rotate", id, other, "salt", tag, "e", "hr1"); got != "current" {
		t.Errorf("same tag = %s", got)
	}
	assertUnchanged(t, a, before, "refusals")

	valid := []string{"rotate", id, other, "salt", strings.Repeat("b", 64), "e", "hr1"}
	for name, mutate := range map[string]func([]string){
		"unknown mode":            func(v []string) { v[0] = "reset" },
		"rotate without expected": func(v []string) { v[1] = "" },
		"initialize with expected": func(v []string) {
			v[0] = "initialize"
		},
		"empty new id":  func(v []string) { v[2] = "" },
		"empty salt":    func(v []string) { v[3] = "" },
		"uppercase tag": func(v []string) { v[4] = strings.Repeat("B", 64) },
		"short tag":     func(v []string) { v[4] = "ab" },
		"empty event":   func(v []string) { v[5] = "" },
	} {
		args := append([]string(nil), valid...)
		mutate(args)
		if _, err := a.RunScript(ctx, "admin_auth_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s accepted: %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "admin_auth_v1", []string{adminAuthKey, adminSessionsKey, "other:audit"}, valid); err == nil {
		t.Error("foreign key accepted")
	}
}

// TestReconcileRunsAdminAuthFirst pins the reconciliation hook: a fresh
// store is initialized, a changed secret rotates, and an inconsistent
// record fails the pass.
func TestReconcileRunsAdminAuthFirst(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	rep, err := a.Reconcile(ctx, ReconcileOptions{Full: true, AdminSecret: testAdminSecret})
	if err != nil || rep.Findings["admin_auth_initialized"] != 1 {
		t.Fatalf("first = %+v, %v", rep.Findings, err)
	}
	rep, err = a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecretOther})
	if err != nil || rep.Findings["admin_auth_rotated"] != 1 {
		t.Fatalf("rotated = %+v, %v", rep.Findings, err)
	}
	a.testDo(t, "HDEL", adminAuthKey, "generation_tag")
	if _, err := a.Reconcile(ctx, ReconcileOptions{AdminSecret: testAdminSecretOther}); err == nil {
		t.Error("inconsistent record allowed the pass")
	}
}

package valkey

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/gen"
)

const testCSRF = "AAAAAAAAAAAAAAAAAAAAAA"

func digestOf(i int) string {
	return strings.Repeat("0", 64-len(strconv.Itoa(i))) + strconv.Itoa(i)
}

// sessionSetup returns an initialized store with the Admin Secret
// generation in place.
func sessionSetup(t *testing.T) (*Adapter, administration.SessionStore) {
	t.Helper()
	a, _ := claimSetup(t)
	if _, err := a.EnsureAdminAuth(context.Background(), testAdminSecret, gen.Crypto{}); err != nil {
		t.Fatal(err)
	}
	a.testDo(t, "DEL", "hr1:audit")
	return a, NewSessionStore(a)
}

func createSession(t *testing.T, s administration.SessionStore, digest string) administration.SessionCreate {
	t.Helper()
	return s.CreateSession(context.Background(), digest, testCSRF, "evt-"+digest[60:], "req-"+digest[60:])
}

// TestSessionCreate pins the created tuple and every key: the Hash with the
// current generation and plaintext CSRF token only, the index score, and
// the login audit event.
func TestSessionCreate(t *testing.T) {
	a, s := sessionSetup(t)
	d := digestOf(1)
	c := createSession(t, s, d)
	if c.Result != administration.SessionCreated || c.IdleExpiresMs != c.CreatedMs+3600000 || c.AbsoluteExpiresMs != c.CreatedMs+43200000 || c.Expired != 0 {
		t.Fatalf("create = %+v", c)
	}
	h := hgetall(t, a, "hr1:admin_session:"+d)
	want := map[string]string{
		"created_ms": itoa64(c.CreatedMs), "last_seen_ms": itoa64(c.CreatedMs), "idle_expires_ms": itoa64(c.IdleExpiresMs),
		"absolute_expires_ms": itoa64(c.AbsoluteExpiresMs), "generation_id": hget(t, a, adminAuthKey, "generation_id"), "csrf_token": testCSRF,
	}
	if len(h) != len(want) {
		t.Errorf("session = %v", h)
	}
	for k, v := range want {
		if h[k] != v {
			t.Errorf("session %s = %q, want %q", k, h[k], v)
		}
	}
	if sc, ok := score(t, a, adminSessionsKey, d); !ok || int64(sc) != c.IdleExpiresMs {
		t.Errorf("index score = %v %v", sc, ok)
	}
	events := auditOps(t, a)
	if len(events) != 1 || events[0]["operation"] != "admin_login" || events[0]["actor"] != "admin_session" || events[0]["target"] != "session" ||
		events[0]["request_id"] != "req-0001" || events[0]["outcome"] != "success" {
		t.Errorf("audit = %v", events)
	}
	before := snapshot(t, a)
	if c := createSession(t, s, d); c.Result != administration.SessionCollision {
		t.Errorf("collision = %+v", c)
	}
	assertUnchanged(t, a, before, "collision")
}

// TestSessionCapacity pins the bound: the 101st login is refused without
// evicting, and an expired session is removed and reported so a new login
// fits.
func TestSessionCapacity(t *testing.T) {
	a, s := sessionSetup(t)
	for i := range administration.SessionCapacity {
		if c := createSession(t, s, digestOf(i)); c.Result != administration.SessionCreated {
			t.Fatalf("create %d = %+v", i, c)
		}
	}
	before := snapshot(t, a)
	if c := createSession(t, s, digestOf(500)); c.Result != administration.SessionCapacityExceeded || c.Expired != 0 {
		t.Errorf("over capacity = %+v", c)
	}
	assertUnchanged(t, a, before, "capacity_exceeded")

	a.testDo(t, "ZADD", adminSessionsKey, "1", digestOf(7))
	c := createSession(t, s, digestOf(500))
	if c.Result != administration.SessionCreated || c.Expired != 1 {
		t.Errorf("after expiry = %+v", c)
	}
	if exists(t, a, "hr1:admin_session:"+digestOf(7)) {
		t.Error("expired session kept")
	}
}

// TestSessionAuthenticate pins validation: a valid session with its CSRF
// token and no refresh inside the throttle, a refresh after it capped at
// the absolute expiry, and the invalid reasons with their cleanup.
func TestSessionAuthenticate(t *testing.T) {
	ctx := context.Background()
	a, s := sessionSetup(t)
	d := digestOf(1)
	c := createSession(t, s, d)
	before := snapshot(t, a)
	auth := s.AuthenticateSession(ctx, d)
	if auth.Result != administration.SessionValid || auth.CSRFToken != testCSRF || auth.IdleExpiresMs != c.IdleExpiresMs ||
		auth.AbsoluteExpiresMs != c.AbsoluteExpiresMs {
		t.Fatalf("auth = %+v", auth)
	}
	assertUnchanged(t, a, before, "no refresh inside the throttle")

	// Six minutes idle: the refresh moves the idle expiry and score.
	old := c.CreatedMs - 6*60000
	a.testDo(t, "HSET", "hr1:admin_session:"+d, "last_seen_ms", itoa64(old))
	auth = s.AuthenticateSession(ctx, d)
	h := hgetall(t, a, "hr1:admin_session:"+d)
	if auth.Result != administration.SessionValid || h["last_seen_ms"] == itoa64(old) || h["idle_expires_ms"] != itoa64(auth.IdleExpiresMs) ||
		auth.IdleExpiresMs < c.IdleExpiresMs {
		t.Errorf("refresh = %+v, %v", auth, h)
	}
	if sc, _ := score(t, a, adminSessionsKey, d); int64(sc) != auth.IdleExpiresMs {
		t.Errorf("score = %v, want %d", sc, auth.IdleExpiresMs)
	}
	// Near the absolute expiry the refresh is capped there.
	capAt := c.CreatedMs + 30*60000
	a.testDo(t, "HSET", "hr1:admin_session:"+d, "last_seen_ms", itoa64(old), "absolute_expires_ms", itoa64(capAt))
	if auth := s.AuthenticateSession(ctx, d); auth.IdleExpiresMs != capAt || auth.AbsoluteExpiresMs != capAt {
		t.Errorf("capped refresh = %+v", auth)
	}

	for name, tc := range map[string]struct {
		setup  func(t *testing.T, a *Adapter, d string)
		reason string
	}{
		"idle expired": {func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "HSET", "hr1:admin_session:"+d, "idle_expires_ms", "1000")
		}, "expired"},
		"absolute expired": {func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "HSET", "hr1:admin_session:"+d, "absolute_expires_ms", "1000")
		}, "expired"},
		"generation changed": {func(t *testing.T, a *Adapter, d string) { a.testDo(t, "HSET", adminAuthKey, "generation_id", "other") }, "generation_changed"},
		"no generation":      {func(t *testing.T, a *Adapter, d string) { a.testDo(t, "DEL", adminAuthKey) }, "generation_changed"},
		"malformed csrf": {func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "HSET", "hr1:admin_session:"+d, "csrf_token", "x")
		}, "malformed"},
		"malformed time": {func(t *testing.T, a *Adapter, d string) { a.testDo(t, "HDEL", "hr1:admin_session:"+d, "last_seen_ms") }, "malformed"},
		"absent":         {func(t *testing.T, a *Adapter, d string) { a.testDo(t, "DEL", "hr1:admin_session:"+d) }, "absent"},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := sessionSetup(t)
			createSession(t, s, d)
			tc.setup(t, a, d)
			if auth := s.AuthenticateSession(ctx, d); auth.Result != administration.SessionInvalid || auth.Reason != tc.reason || auth.CSRFToken != "" {
				t.Errorf("auth = %+v, want %s", auth, tc.reason)
			}
			if exists(t, a, "hr1:admin_session:"+d) {
				t.Error("invalid session kept")
			}
			if _, ok := score(t, a, adminSessionsKey, d); ok {
				t.Error("index member kept")
			}
		})
	}

	for name, setup := range map[string]func(t *testing.T, a *Adapter, d string){
		"session not a hash": func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "DEL", "hr1:admin_session:"+d)
			a.testDo(t, "SET", "hr1:admin_session:"+d, "x")
		},
		"index not a zset": func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "DEL", adminSessionsKey)
			a.testDo(t, "SET", adminSessionsKey, "x")
		},
		"auth not a hash": func(t *testing.T, a *Adapter, d string) {
			a.testDo(t, "DEL", adminAuthKey)
			a.testDo(t, "SET", adminAuthKey, "x")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := sessionSetup(t)
			createSession(t, s, d)
			setup(t, a, d)
			before := snapshot(t, a)
			if auth := s.AuthenticateSession(ctx, d); auth.Result != administration.SessionAuthWrongType {
				t.Errorf("auth = %+v", auth)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestSessionDelete pins logout: deletion with its audit event, a repeated
// logout as absent without audit, and wrong types refused unchanged.
func TestSessionDelete(t *testing.T) {
	ctx := context.Background()
	a, s := sessionSetup(t)
	d := digestOf(1)
	createSession(t, s, d)
	a.testDo(t, "DEL", "hr1:audit")
	if r := s.DeleteSession(ctx, d, "evt-out", "req-out"); r != administration.SessionDeleted {
		t.Fatalf("delete = %s", r)
	}
	if exists(t, a, "hr1:admin_session:"+d) || exists(t, a, adminSessionsKey) {
		t.Error("session or index member kept")
	}
	events := auditOps(t, a)
	if len(events) != 1 || events[0]["operation"] != "admin_logout" || events[0]["event_id"] != "evt-out" || events[0]["request_id"] != "req-out" {
		t.Errorf("audit = %v", events)
	}
	a.testDo(t, "ZADD", adminSessionsKey, "1", d)
	if r := s.DeleteSession(ctx, d, "evt-2", "req-2"); r != administration.SessionDeleteAbsent {
		t.Errorf("repeat = %s", r)
	}
	if _, ok := score(t, a, adminSessionsKey, d); ok || len(auditOps(t, a)) != 1 {
		t.Error("absent logout kept the stale member or audited")
	}
	a.testDo(t, "SET", "hr1:audit", "x")
	createSession(t, s, digestOf(2))
	before := snapshot(t, a)
	if r := s.DeleteSession(ctx, digestOf(2), "e", "r"); r != administration.SessionDeleteWrongType {
		t.Errorf("wrong type = %s", r)
	}
	assertUnchanged(t, a, before, "wrong_type")
}

// TestSessionScriptArguments pins argument rejection, the uninitialized
// generation, and the EVAL reload after SCRIPT FLUSH.
func TestSessionScriptArguments(t *testing.T) {
	ctx := context.Background()
	a, _ := claimSetup(t)
	store := NewSessionStore(a)
	if c := store.CreateSession(ctx, digestOf(1), testCSRF, "e", "r"); c.Result != administration.SessionAuthUninitialized {
		t.Errorf("uninitialized = %+v", c)
	}
	keys := []string{adminAuthKey, adminSessionsKey, "hr1:audit"}
	valid := []string{digestOf(1), testCSRF, "3600000", "43200000", "100", "e", "r", "hr1"}
	for name, mutate := range map[string]func([]string){
		"short digest":     func(v []string) { v[0] = "abc" },
		"uppercase digest": func(v []string) { v[0] = strings.Repeat("A", 64) },
		"short csrf":       func(v []string) { v[1] = "abc" },
		"zero idle":        func(v []string) { v[2] = "0" },
		"empty capacity":   func(v []string) { v[4] = "" },
		"empty request":    func(v []string) { v[6] = "" },
	} {
		args := append([]string(nil), valid...)
		mutate(args)
		if _, err := a.RunScript(ctx, "session_create_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("create %s accepted: %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "session_authenticate_v1", keys[:2], []string{"abc", "1", "1", "hr1"}); err == nil {
		t.Error("authenticate short digest accepted")
	}
	if _, err := a.RunScript(ctx, "session_delete_v1", []string{adminSessionsKey, "other:audit"}, []string{digestOf(1), "e", "r", "hr1"}); err == nil {
		t.Error("delete foreign key accepted")
	}
	if _, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil {
		t.Fatal(err)
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if c := store.CreateSession(ctx, digestOf(1), testCSRF, "e", "r"); c.Result != administration.SessionCreated {
		t.Errorf("create after SCRIPT FLUSH = %+v", c)
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if auth := store.AuthenticateSession(ctx, digestOf(1)); auth.Result != administration.SessionValid {
		t.Errorf("authenticate after SCRIPT FLUSH = %+v", auth)
	}
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := store.DeleteSession(ctx, digestOf(1), "e", "r"); r != administration.SessionDeleted {
		t.Errorf("delete after SCRIPT FLUSH = %s", r)
	}
}

// Package main hosts the throwaway valkey-go client spike for hookrelay
// ticket 01 (Milestone 1). The tests in this file are the spike: they verify
// the client and script/failure contracts against a real pinned Valkey.
//
// Required environment (locally provided by the spike docker containers):
//
//	HR_SPIKE_ADDR          plain Valkey host:port   (contract + failure tests)
//	HR_SPIKE_SECURE_ADDR   TLS + auth Valkey host:port
//	HR_SPIKE_SECURE_PASSWORD
//	HR_SPIKE_TLS_CA        path to the CA certificate
//	HR_SPIKE_OOM_ADDR      Valkey with small maxmemory + noeviction
//
// Tests skip when their environment is absent, matching the repo convention
// for real-Valkey integration tests.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

const (
	scriptBody = "return {KEYS[1], ARGV[1]}"
)

func envOr(t *testing.T, key, fallback string) string {
	t.Helper()
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func requireEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, k := range keys {
		if os.Getenv(k) == "" {
			if k == "HR_SPIKE_ADDR" {
				t.Skipf("HR_SPIKE_ADDR not set; start the spike Valkey container to run this test")
			}
			t.Skipf("%s not set", k)
		}
	}
}

// stripVerbatim removes the RESP3 verbatim-string prefix ("txt:" / "bin:")
// that valkey-go preserves in ToString for CLIENT LIST and similar replies.
func stripVerbatim(s string) string {
	if strings.HasPrefix(s, "txt:") || strings.HasPrefix(s, "bin:") {
		return s[4:]
	}
	return s
}

// newMainClient returns a client for the plain spike instance with the given
// option overrides applied.
func newMainClient(t *testing.T, mutate func(*valkey.ClientOption)) valkey.Client {
	t.Helper()
	requireEnv(t, "HR_SPIKE_ADDR")
	opt := valkey.ClientOption{
		InitAddress: []string{envOr(t, "HR_SPIKE_ADDR", "127.0.0.1:3039")},
	}
	if mutate != nil {
		mutate(&opt)
	}
	c, err := valkey.NewClient(opt)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustDo(t *testing.T, c valkey.Client, cmd valkey.Completed) valkey.ValkeyMessage {
	t.Helper()
	resp, err := c.Do(context.Background(), cmd).ToMessage()
	if err != nil {
		t.Fatalf("command %v: %v", cmd.Commands(), err)
	}
	return resp
}

// --- Script contracts -------------------------------------------------------

// TestScriptLoadEvalshaAndNoscriptFallback verifies SCRIPT LOAD, EVALSHA by
// digest, the NOSCRIPT error shape, and the single EVAL reload of the same
// embedded body — the contract the production adapter and its registry rely on.
func TestScriptLoadEvalshaAndNoscriptFallback(t *testing.T) {
	c := newMainClient(t, nil)
	ctx := context.Background()

	// SCRIPT LOAD returns the digest the server will accept for EVALSHA.
	load := mustDo(t, c, c.B().Arbitrary("SCRIPT", "LOAD", scriptBody).Build())
	sha, err := load.ToString()
	if err != nil || sha == "" {
		t.Fatalf("SCRIPT LOAD: %q, %v", sha, err)
	}

	// EVALSHA by digest returns the script result.
	res := mustDo(t, c, c.B().Arbitrary("EVALSHA", sha, "1", "hrspike:key", "hrspike:arg").Build())
	strs, err := res.AsStrSlice()
	if err != nil || len(strs) != 2 || strs[0] != "hrspike:key" || strs[1] != "hrspike:arg" {
		t.Fatalf("EVALSHA result: %v %v", strs, err)
	}

	// SCRIPT FLUSH removes the cached script; EVALSHA now fails with NOSCRIPT.
	mustDo(t, c, c.B().ScriptFlush().Build())
	_, err = c.Do(ctx, c.B().Arbitrary("EVALSHA", sha, "1", "k", "a").Build()).ToMessage()
	if err == nil {
		t.Fatal("expected NOSCRIPT error after SCRIPT FLUSH")
	}
	if !strings.HasPrefix(err.Error(), "NOSCRIPT") {
		t.Fatalf("expected NOSCRIPT-prefixed error, got: %v", err)
	}

	// One EVAL of the same body reloads it; EVALSHA works again.
	res = mustDo(t, c, c.B().Arbitrary("EVAL", scriptBody, "1", "k", "a").Build())
	if _, err := res.AsStrSlice(); err != nil {
		t.Fatalf("EVAL after NOSCRIPT: %v", err)
	}
	res = mustDo(t, c, c.B().Arbitrary("EVALSHA", sha, "1", "k", "a").Build())
	if _, err := res.AsStrSlice(); err != nil {
		t.Fatalf("EVALSHA after EVAL reload: %v", err)
	}
}

// TestLuaHelperAutoFallback verifies the library's Lua helper performs the
// EVALSHA-with-EVAL-fallback internally and survives SCRIPT FLUSH.
func TestLuaHelperAutoFallback(t *testing.T) {
	c := newMainClient(t, nil)
	ctx := context.Background()
	lua := valkey.NewLuaScript(scriptBody)

	res, err := lua.Exec(ctx, c, []string{"hrspike:lua"}, []string{"v1"}).ToMessage()
	if err != nil {
		t.Fatalf("first exec: %v", err)
	}
	if strs, _ := res.AsStrSlice(); len(strs) != 2 || strs[1] != "v1" {
		t.Fatalf("first exec result: %v", strs)
	}

	mustDo(t, c, c.B().ScriptFlush().Build())

	res, err = lua.Exec(ctx, c, []string{"hrspike:lua"}, []string{"v2"}).ToMessage()
	if err != nil {
		t.Fatalf("exec after SCRIPT FLUSH (helper must self-heal): %v", err)
	}
	if strs, _ := res.AsStrSlice(); len(strs) != 2 || strs[1] != "v2" {
		t.Fatalf("post-flush exec result: %v", strs)
	}
}

// TestTimeInsideScriptEffectReplication proves scripts may call TIME and that
// its result is returned to the caller — the authoritative-time mechanism the
// hookrelay transition scripts depend on.
func TestTimeInsideScriptEffectReplication(t *testing.T) {
	c := newMainClient(t, nil)
	before := time.Now().Unix() - 2

	res := mustDo(t, c, c.B().Arbitrary("EVAL", "return redis.call('TIME')[1]", "0").Build())
	sec, err := res.AsInt64()
	if err != nil {
		t.Fatalf("TIME inside script returned %v (%v) — must be callable", res, err)
	}
	after := time.Now().Unix() + 2
	if sec < before || sec > after {
		t.Fatalf("script TIME %d outside wall clock window [%d, %d]", sec, before, after)
	}

	// TIME + writes together: effect replication keeps the script atomic.
	mustDo(t, c, c.B().Arbitrary("EVAL",
		"redis.call('SET','hrspike:time',redis.call('TIME')[1]) return 1", "0").Build())
	stored := mustDo(t, c, c.B().Get().Key("hrspike:time").Build())
	if s, _ := stored.AsInt64(); s < before || s > after {
		t.Fatalf("stored script time %d outside window", s)
	}
}

// TestScriptRuntimeErrorLeavesPartialWrites pins the failure semantics the
// design relies on: a runtime error mid-script does not roll back earlier
// writes, so scripts must validate preconditions before their first write.
func TestScriptRuntimeErrorLeavesPartialWrites(t *testing.T) {
	c := newMainClient(t, nil)

	_, err := c.Do(context.Background(), c.B().Arbitrary("EVAL",
		"redis.call('SET','hrspike:partial','1') return redis.call('LPUSH','hrspike:partial','x')", "0",
	).Build()).ToMessage()
	if err == nil {
		t.Fatal("expected wrong-type runtime error")
	}
	if ret, ok := valkey.IsValkeyErr(err); !ok {
		t.Fatalf("script runtime error should be a *valkey.ValkeyError (server error class), got %T %v", err, err)
	} else if ret == nil {
		t.Fatal("IsValkeyErr returned nil *ValkeyError with ok=true")
	}

	got := mustDo(t, c, c.B().Get().Key("hrspike:partial").Build())
	if s, _ := got.ToString(); s != "1" {
		t.Fatalf("partial write missing after script runtime error: %q (rollback happened?)", s)
	}
}

// --- Error classes: server errors vs ambiguous transport errors -------------

// TestServerErrorAndTransportErrorClasses pins the distinction later tickets
// need: server-side rejections (NOSCRIPT, WRONGPASS, OOM, wrong-type) surface
// as *valkey.ValkeyError, while transport-level failures are ordinary Go
// errors — so callers can treat them differently without string matching.
func TestServerErrorAndTransportErrorClasses(t *testing.T) {
	c := newMainClient(t, nil)

	// Server error class: NOSCRIPT.
	_, err := c.Do(context.Background(), c.B().Arbitrary("EVALSHA", "ffffffffffffffffffffffffffffffffffffffff", "0").Build()).ToMessage()
	if err == nil {
		t.Fatal("expected NOSCRIPT error")
	}
	if _, ok := valkey.IsValkeyErr(err); !ok {
		t.Fatalf("NOSCRIPT should be ValkeyError class, got %T", err)
	}

	// Transport class: an unreachable address fails eagerly at NewClient —
	// the client dials at construction, so dependency unavailability is
	// detectable before the first command (and before readiness). The error
	// is an ordinary Go error, NOT a *valkey.ValkeyError server error.
	_, err = valkey.NewClient(valkey.ClientOption{
		InitAddress:      []string{"127.0.0.1:1"},
		Dialer:           net.Dialer{Timeout: 500 * time.Millisecond},
		ConnWriteTimeout: 500 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected eager dial failure against unreachable address")
	}
	if _, ok := valkey.IsValkeyErr(err); ok {
		t.Fatalf("dial failure must NOT be ValkeyError class, got %v", err)
	}

	// Transport class: context deadline exceeded mid-command is ambiguous —
	// covered further in TestContextTimeoutOutcomeIsAmbiguous.
}

// --- Timeouts ---------------------------------------------------------------

// TestContextTimeoutOutcomeIsAmbiguous proves that a context timeout while a
// write command is in flight does NOT prove non-execution: after the server
// unpauses, the command is found applied. This is the exact reason the design
// forbids blind retries of uncertain operations.
func TestContextTimeoutOutcomeIsAmbiguous(t *testing.T) {
	requireEnv(t, "HR_SPIKE_ADDR")
	victim := newMainClient(t, nil)
	pauser := newMainClient(t, nil)
	ctx := context.Background()

	// Valkey 9: CLIENT PAUSE <ms> pauses client commands; the pausing
	// connection itself is unaffected. Verified server-side in the spike.
	mustDo(t, pauser, pauser.B().ClientPause().Timeout(700).Build())

	start := time.Now()
	reqCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	_, err := victim.Do(reqCtx, victim.B().Set().Key("hrspike:paused").Value("applied").Build()).ToMessage()
	cancel()
	elapsed := time.Since(start)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context deadline error, got %v", err)
	}
	if elapsed >= 700*time.Millisecond {
		t.Fatalf("command returned only after unpause (%v) — timeout not enforced client-side", elapsed)
	}

	// Wait out the pause, then prove the timed-out command actually executed.
	time.Sleep(800 * time.Millisecond)
	got := mustDo(t, victim, victim.B().Get().Key("hrspike:paused").Build())
	if s, _ := got.ToString(); s != "applied" {
		t.Fatalf("timed-out SET was not applied: %q", s)
	}
}

// --- Reconnect ---------------------------------------------------------------

// TestReconnectAfterServerSideClientKill kills all normal client connections
// from inside the server (as a network fault would) and verifies:
//   - read-only commands are transparently retried (default DisableRetry=false);
//   - the client recovers without recreation for subsequent commands.
func TestReconnectAfterServerSideClientKill(t *testing.T) {
	requireEnv(t, "HR_SPIKE_ADDR")
	c := newMainClient(t, nil)
	ctx := context.Background()

	// Establish the pooled connections first.
	mustDo(t, c, c.B().Ping().Build())
	killer, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{envOr(t, "HR_SPIKE_ADDR", "127.0.0.1:3039")},
		// The killer must not be killed by TYPE normal; use a dedicated
		// blocking-pool connection marker via client name and exclude it.
		ClientName: "hrspike-killer",
	})
	if err != nil {
		t.Fatalf("killer NewClient: %v", err)
	}
	defer killer.Close()
	mustDo(t, killer, killer.B().Ping().Build())

	// Kill every normal connection whose name is not the killer's.
	// valkey-go pipeline connections are unnamed. The listing is a RESP3
	// verbatim string with a "txt:" prefix on the first line.
	listing := stripVerbatim(mustString(t, killer, killer.B().ClientList().Build()))
	killed := 0
	for _, line := range strings.Split(strings.TrimSpace(listing), "\n") {
		fields := map[string]string{}
		for _, f := range strings.Fields(line) {
			if k, v, ok := strings.Cut(f, "="); ok {
				fields[k] = v
			}
		}
		if fields["name"] == "hrspike-killer" {
			continue
		}
		id64, convErr := strconv.ParseInt(fields["id"], 10, 64)
		if convErr != nil {
			t.Fatalf("CLIENT LIST id %q: %v", fields["id"], convErr)
		}
		kill := killer.B().ClientKill().Id(id64).Build()
		if _, err := killer.Do(ctx, kill).ToMessage(); err != nil {
			t.Fatalf("CLIENT KILL ID %s: %v", fields["id"], err)
		}
		killed++
	}
	if killed == 0 {
		t.Fatal("expected to kill at least one pooled connection")
	}

	// Read-only command transparently retried over a fresh connection.
	_, err = c.Do(ctx, c.B().Get().Key("hrspike:killmarker").Build()).ToMessage()
	if err != nil && !valkey.IsValkeyNil(err) {
		t.Fatalf("GET after reconnect: %v", err)
	}
	if valkey.IsValkeyNil(err) {
		mustDo(t, c, c.B().Set().Key("hrspike:killmarker").Value("1").Build())
	}

	// Write after the kill: write commands are NOT auto-retried — the ring
	// may hand the command to a slot whose connection is still being
	// re-dialed, surfacing a transport-class error (EOF). This is the exact
	// ambiguity the design forbids blindly retrying for script transitions.
	// The test tolerates transport-class failures on the write, verifies they
	// are never server-error class, and asserts eventual persistence.
	var writeErr error
	for attempt := 0; attempt < 3; attempt++ {
		_, writeErr = c.Do(ctx, c.B().Set().Key("hrspike:afterkill").Value("ok").Build()).ToMessage()
		if writeErr == nil {
			break
		}
		if _, isServer := valkey.IsValkeyErr(writeErr); isServer {
			t.Fatalf("post-kill write must fail with transport class only, got server error: %v", writeErr)
		}
	}
	if writeErr != nil {
		t.Fatalf("write did not succeed within 3 attempts after reconnect: %v", writeErr)
	}
	got := mustDo(t, c, c.B().Get().Key("hrspike:afterkill").Build())
	if s, _ := got.ToString(); s != "ok" {
		t.Fatalf("write after reconnect missing: %q", s)
	}
}

// --- Connection model limits --------------------------------------------------

// TestConnectionModelLimits verifies the knobs that map to
// HOOKRELAY_VALKEY_MAX_CONNECTIONS / HOOKRELAY_VALKEY_MIN_IDLE exist and
// bound connection growth: PipelineMultiplex caps pipeline connections at
// 2^N even under concurrent load (default N=2 → 4), and BlockingPoolSize
// caps the pool shared by blocking commands (the future long-poll path).
// Connections are created lazily on demand — one conn serves sequential
// commands. BlockingPoolMinSize prewarms idle blocking connections.
func TestConnectionModelLimits(t *testing.T) {
	requireEnv(t, "HR_SPIKE_ADDR")
	ctx := context.Background()

	// CLIENT LIST sees every connection on the server; observations must be
	// filtered per client via its ClientName, which valkey-go applies to all
	// of its connections (HELLO SETNAME on every dial, pipeline and blocking).
	countByName := func(c valkey.Client, name string) int {
		listing, err := c.Do(ctx, c.B().ClientList().Build()).ToMessage()
		if err != nil {
			t.Fatalf("CLIENT LIST: %v", err)
		}
		s, _ := listing.ToString()
		n := 0
		for _, line := range strings.Split(strings.TrimSpace(stripVerbatim(s)), "\n") {
			for _, f := range strings.Fields(line) {
				if f == "name="+name {
					n++
					break
				}
			}
		}
		return n
	}

	// Default: PipelineMultiplex=2 → at most 4 connections even under
	// concurrent load (connections are created lazily, capped at 2^N).
	def := newMainClient(t, func(o *valkey.ClientOption) {
		o.ClientName = "hrspike-obs-default"
	})
	mustDo(t, def, def.B().Ping().Build())
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("hrspike:cc:%d", i)
			_ = def.Do(ctx, def.B().Set().Key(key).Value("v").Build())
			_, _ = def.Do(ctx, def.B().Get().Key(key).Build()).ToMessage()
		}(i)
	}
	wg.Wait()
	if total := countByName(def, "hrspike-obs-default"); total > 4 {
		t.Errorf("default concurrent connection count = %d, want <= 4 (PipelineMultiplex=2)", total)
	}

	// Bounded: PipelineMultiplex=0 → 1 pipeline connection; BlockingPoolSize=2
	// caps blocking-pool growth; BlockingPoolMinSize=1 prewarms one idle.
	small := newMainClient(t, func(o *valkey.ClientOption) {
		o.ClientName = "hrspike-obs-small"
		o.PipelineMultiplex = 0
		o.BlockingPoolSize = 2
		o.BlockingPoolMinSize = 1
	})
	mustDo(t, small, small.B().Ping().Build())
	if n := countByName(small, "hrspike-obs-small"); n != 1 {
		t.Errorf("PipelineMultiplex=0 idle connection count = %d, want 1 (connections are fully lazy; BlockingPoolMinSize does not prewarm eagerly)", n)
	}

	// A blocking command uses a blocking-pool connection; the pool must not
	// exceed BlockingPoolSize even under concurrent blocking calls.
	var bwg sync.WaitGroup
	for i := 0; i < 5; i++ {
		bwg.Add(1)
		go func() {
			defer bwg.Done()
			bctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			_, _ = small.Do(bctx, small.B().Blpop().Key("hrspike:nobody").Timeout(2).Build()).ToMessage()
		}()
	}
	bwg.Wait()
	time.Sleep(100 * time.Millisecond)
	if n := countByName(small, "hrspike-obs-small"); n > 1+2 {
		t.Errorf("connection count after 5 concurrent blocking calls = %d, want <= 3 (1 pipeline + BlockingPoolSize=2)", n)
	}
}

// --- Auth + TLS ---------------------------------------------------------------

// TestAuthAndTLSContract runs against the TLS+auth instance: correct
// credentials over TLS succeed; wrong password fails with a server-error-class
// WRONGPASS; a wrong CA fails the TLS handshake (transport class, no
// application error). Missing credentials surface NOAUTH on first use.
func TestAuthAndTLSContract(t *testing.T) {
	requireEnv(t, "HR_SPIKE_SECURE_ADDR", "HR_SPIKE_SECURE_PASSWORD", "HR_SPIKE_TLS_CA")
	addr := envOr(t, "HR_SPIKE_SECURE_ADDR", "127.0.0.1:3040")
	password := envOr(t, "HR_SPIKE_SECURE_PASSWORD", "hr-spike-secret-0f3a9c")
	caPath := envOr(t, "HR_SPIKE_TLS_CA", "/tmp/hr-spike-tls/ca.crt")

	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read CA: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("failed to parse CA PEM")
	}

	// Happy path: TLS + correct password.
	good, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
		Password:    password,
	})
	if err != nil {
		t.Fatalf("NewClient TLS: %v", err)
	}
	defer good.Close()
	resp, err := good.Do(context.Background(), good.B().Ping().Build()).ToMessage()
	if err != nil {
		t.Fatalf("PING over TLS+auth: %v", err)
	}
	if s, _ := resp.ToString(); s != "PONG" {
		t.Fatalf("PING over TLS+auth returned %q", s)
	}

	// Wrong password → eager handshake failure at NewClient (auth happens
	// during connection initialization, before any command).
	bad, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
		Password:    "definitely-wrong",
	})
	if err == nil {
		bad.Close()
		t.Fatal("expected eager WRONGPASS error from NewClient")
	}
	if !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("expected WRONGPASS in eager auth error, got: %v", err)
	}

	// Wrong CA → dial/TLS failure. An unrelated CA must reject the server
	// chain; the failure is an ordinary error, not a *valkey.ValkeyError.
	badCA := x509.NewCertPool()
	wrongPEM, rerr := os.ReadFile("/tmp/hr-spike-tls/wrong-ca.crt")
	if rerr != nil {
		t.Skipf("wrong-ca.crt not available: %v", rerr)
	}
	badCA.AddCert(mustParseCert(t, wrongPEM))
	_, err = valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{RootCAs: badCA, ServerName: "localhost", MinVersion: tls.VersionTLS12},
		Password:    password,
	})
	if err == nil {
		t.Fatal("expected eager TLS handshake failure with an unrelated CA")
	}
	if _, ok := valkey.IsValkeyErr(err); ok {
		t.Fatalf("TLS failure must NOT be ValkeyError class, got %v", err)
	}

	// Missing credentials → eager failure against an auth-required server.
	noauth, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{addr},
		TLSConfig:   &tls.Config{RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	})
	if err == nil {
		noauth.Close()
		t.Fatal("expected eager auth-required error without credentials")
	}
	if !strings.Contains(err.Error(), "NOAUTH") {
		t.Fatalf("expected NOAUTH in eager missing-credentials error, got: %v", err)
	}
}

func mustParseCert(t *testing.T, pem []byte) *x509.Certificate {
	t.Helper()
	certs, err := parseCertPEM(pem)
	if err != nil || len(certs) == 0 {
		t.Fatalf("parse cert: %v", err)
	}
	return certs[0]
}

// --- OOM under noeviction -----------------------------------------------------

// TestOomUnderNoeviction fills the bounded instance until writes fail with the
// OOM server error, verifies the error class, and proves the client recovers
// without recreation once space is freed.
func TestOomUnderNoeviction(t *testing.T) {
	requireEnv(t, "HR_SPIKE_OOM_ADDR")
	c := newMainClientT(t, envOr(t, "HR_SPIKE_OOM_ADDR", "127.0.0.1:3041"))
	ctx := context.Background()

	value := strings.Repeat("x", 1<<20) // 1 MiB per SET
	var oomErr error
	for i := 0; i < 64; i++ {
		_, err := c.Do(ctx, c.B().Set().Key(fmt.Sprintf("hrspike:oom:%d", i)).Value(value).Build()).ToMessage()
		if err != nil {
			if !strings.HasPrefix(err.Error(), "OOM") {
				t.Fatalf("expected OOM-prefixed error, got: %v", err)
			}
			if _, ok := valkey.IsValkeyErr(err); !ok {
				t.Fatalf("OOM must be ValkeyError class, got %T", err)
			}
			oomErr = err
			break
		}
	}
	if oomErr == nil {
		t.Fatal("never hit OOM with 8 MiB maxmemory and 1 MiB values")
	}

	// Reads still work under the OOM condition.
	if _, err := c.Do(ctx, c.B().Ping().Build()).ToMessage(); err != nil {
		t.Fatalf("PING under OOM: %v", err)
	}

	// Freeing space restores writes without client recreation.
	keys := mustDo(t, c, c.B().Keys().Pattern("hrspike:oom:*").Build())
	names, err := keys.AsStrSlice()
	if err != nil {
		t.Fatalf("KEYS: %v", err)
	}
	for _, k := range names {
		mustDo(t, c, c.B().Del().Key(k).Build())
	}
	mustDo(t, c, c.B().Set().Key("hrspike:oom-recovered").Value("ok").Build())
}

// newMainClientT builds a client for an explicit address without the main
// instance env requirement.
func newMainClientT(t *testing.T, addr string) valkey.Client {
	t.Helper()
	c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

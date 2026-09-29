package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/delivery"
)

var testDelaysMs = []int64{1000, 5000, 30000}

func nackReq(token, reason string) delivery.NackRequest {
	r := ackReq(token)
	return delivery.NackRequest{Token: r.Token, TokenDigest: r.TokenDigest, ReasonCode: reason, RetryDelaysMs: testDelaysMs, MaxAttempts: 4}
}

func lrange(t *testing.T, a *Adapter, key string) []string {
	t.Helper()
	v, err := a.client.Do(context.Background(), a.client.B().Lrange().Key(key).Start(0).Stop(-1).Build()).AsStrSlice()
	if err != nil {
		t.Fatalf("lrange %s: %v", key, err)
	}
	return v
}

func hgetall(t *testing.T, a *Adapter, key string) map[string]string {
	t.Helper()
	v, err := a.client.Do(context.Background(), a.client.B().Hgetall().Key(key).Build()).AsStrMap()
	if err != nil {
		t.Fatalf("hgetall %s: %v", key, err)
	}
	return v
}

func decodeEntry(t *testing.T, raw string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("history entry %q: %v", raw, err)
	}
	return m
}

func keysOf(m map[string]any) string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// activateRetry stands in for retry activation (ticket 03): the retry_wait
// head becomes ready again with a fresh ready membership.
func activateRetry(t *testing.T, a *Adapter, rid string) {
	t.Helper()
	a.testDo(t, "HSET", "hr1:r:"+rid+":s", "status", "ready")
	a.testDo(t, "HDEL", "hr1:r:"+rid+":s", "retry_at_ms")
	a.testDo(t, "ZREM", "hr1:retries", rid)
	a.testDo(t, "ZADD", "hr1:ready", "100", rid)
}

// TestNackSchedulesRetry pins the retry_scheduled tuple and every affected
// key: history entry, retry_wait head state, lease/retry memberships, the
// nacked token phase, and the claim operation marker; the queue, blob,
// metadata, and counter are unchanged.
func TestNackSchedulesRetry(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	req := claimReq("op-1", "args", "dlv_token1")
	req.ConsumerInstanceID = "worker-1"
	claimed := s.Claim(ctx, req)
	if claimed.Outcome != delivery.ClaimClaimed {
		t.Fatalf("claim = %+v", claimed)
	}

	res := s.Nack(ctx, nackReq("dlv_token1", "temporary_dependency_failure"))
	if res.Outcome != delivery.NackRetryScheduled || res.MessageID != "m1" || res.Attempt != 1 || res.DeliveryCycle != 1 ||
		res.RecipientIdentity != ridA || res.ClaimedMs != claimed.Delivery.ClaimedMs || res.CompletedMs < res.ClaimedMs ||
		res.RetryAtMs != res.CompletedMs+1000 {
		t.Fatalf("nack = %+v", res)
	}

	state := "hr1:r:" + ridA + ":s"
	want := map[string]string{"status": "retry_wait", "head_message_id": "m1", "delivery_cycle": "1", "attempt": "2", "retry_at_ms": itoa64(res.RetryAtMs)}
	if got := hgetall(t, a, state); len(got) != len(want) {
		t.Errorf("state = %v, want exactly %v", got, want)
	}
	for f, v := range want {
		if got := hget(t, a, state, f); got != v {
			t.Errorf("state %s = %q, want %q", f, got, v)
		}
	}
	if _, ok := score(t, a, "hr1:leases", ridA); ok {
		t.Error("lease member kept")
	}
	if _, ok := score(t, a, "hr1:ready", ridA); ok {
		t.Error("retry_wait head is ready")
	}
	if sc, ok := score(t, a, "hr1:retries", ridA); !ok || sc != res.RetryAtMs {
		t.Errorf("retries score = %d %v, want retry_at_ms %d", sc, ok, res.RetryAtMs)
	}

	history := lrange(t, a, "hr1:a:m1")
	if len(history) != 1 {
		t.Fatalf("history = %v", history)
	}
	entry := decodeEntry(t, history[0])
	if keysOf(entry) != "attempt,claimed_ms,completed_ms,consumer_instance_id,delivery_cycle,kind,lease_expires_ms,outcome,reason_code" {
		t.Errorf("history fields = %s", keysOf(entry))
	}
	for k, v := range map[string]any{
		"kind": "attempt", "delivery_cycle": 1.0, "attempt": 1.0, "claimed_ms": float64(claimed.Delivery.ClaimedMs),
		"lease_expires_ms": float64(claimed.Delivery.LeaseExpiresMs), "completed_ms": float64(res.CompletedMs),
		"outcome": "nack", "reason_code": "temporary_dependency_failure", "consumer_instance_id": "worker-1",
	} {
		if entry[k] != v {
			t.Errorf("history %s = %v, want %v", k, entry[k], v)
		}
	}
	if strings.Contains(history[0], "dlv_") {
		t.Error("history holds the token")
	}

	tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
	wantTomb := map[string]string{"state": "nacked", "result": "retry_scheduled", "message_id": "m1", "attempt": "1", "retry_at_ms": itoa64(res.RetryAtMs)}
	if got := hgetall(t, a, tomb); len(got) != len(wantTomb) {
		t.Errorf("token record = %v, want exactly %v", got, wantTomb)
	}
	for f, v := range wantTomb {
		if got := hget(t, a, tomb, f); got != v {
			t.Errorf("token %s = %q, want %q", f, got, v)
		}
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key(tomb).Build()).AsInt64(); ttl <= 0 || ttl > TombstoneTTL.Milliseconds() {
		t.Errorf("token TTL = %d", ttl)
	}
	if got := hgetall(t, a, "hr1:op:op-1"); got["state"] != "no_longer_active" || got["delivery_token"] != "" {
		t.Errorf("op record = %v", got)
	}
	if ttl, _ := a.client.Do(ctx, a.client.B().Pttl().Key("hr1:op:op-1").Build()).AsInt64(); ttl <= 0 {
		t.Errorf("op record lost its TTL: %d", ttl)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); strings.Join(q, ",") != "m1,m2" {
		t.Errorf("queue = %v", q)
	}
	if !exists(t, a, "hr1:m:m1") || !exists(t, a, "hr1:mi:m1") {
		t.Error("blob or metadata deleted")
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 2 {
		t.Errorf("counter = %d", n)
	}

	// The repeat returns the recorded result without mutation; the claim
	// replay reports the ended attempt; nothing is claimable while waiting.
	before := snapshot(t, a)
	repeat := s.Nack(ctx, nackReq("dlv_token1", "other_reason"))
	if repeat.Outcome != delivery.NackAlreadyNacked || repeat.Result != "retry_scheduled" || repeat.MessageID != "m1" ||
		repeat.Attempt != 1 || repeat.RetryAtMs != res.RetryAtMs {
		t.Errorf("repeat = %+v, want the recorded result", repeat)
	}
	assertUnchanged(t, a, before, "repeated nack")
	if r := s.Claim(ctx, claimReq("op-1", "args", "dlv_x")); r.Outcome != delivery.ClaimNoLongerActive {
		t.Errorf("claim replay after nack = %+v", r)
	}
	if r := s.Claim(ctx, claimReq("op-2", "args", "dlv_token2")); r.Outcome != delivery.ClaimEmpty {
		t.Errorf("claim during retry wait = %+v", r)
	}
}

// TestNackPicksTheDelayOfTheFailedAttempt pins delay selection by attempt,
// omission of absent optional history fields, and the temporary refusal
// of the last attempt until the dead-letter transition exists.
func TestNackPicksTheDelayOfTheFailedAttempt(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	for attempt, token := range []string{"dlv_token1", "dlv_token2", "dlv_token3"} {
		c := s.Claim(ctx, claimReq("op-"+token, "args", token))
		if c.Outcome != delivery.ClaimClaimed || c.Delivery.Attempt != int64(attempt+1) {
			t.Fatalf("claim %d = %+v", attempt+1, c)
		}
		res := s.Nack(ctx, nackReq(token, ""))
		if res.Outcome != delivery.NackRetryScheduled || res.Attempt != int64(attempt+1) || res.RetryAtMs != res.CompletedMs+testDelaysMs[attempt] {
			t.Fatalf("nack %d = %+v", attempt+1, res)
		}
		activateRetry(t, a, ridA)
	}
	history := lrange(t, a, "hr1:a:m1")
	if len(history) != 3 {
		t.Fatalf("history = %v", history)
	}
	for i, raw := range history {
		e := decodeEntry(t, raw)
		if e["attempt"] != float64(i+1) || keysOf(e) != "attempt,claimed_ms,completed_ms,delivery_cycle,kind,lease_expires_ms,outcome" {
			t.Errorf("entry %d = %s", i, raw)
		}
	}

	if c := s.Claim(ctx, claimReq("op-4", "args", "dlv_token4")); c.Delivery.Attempt != 4 {
		t.Fatalf("fourth claim = %+v", c)
	}
	before := snapshot(t, a)
	if r := s.Nack(ctx, nackReq("dlv_token4", "")); r.Outcome != delivery.NackAttemptsExhausted {
		t.Errorf("last attempt nack = %+v, want attempts_exhausted until dead-letter exists", r)
	}
	assertUnchanged(t, a, before, "last attempt nack")
}

// TestNackRefusals pins every refusal status and the absence of mutation.
func TestNackRefusals(t *testing.T) {
	state := "hr1:r:" + ridA + ":s"
	cases := []struct {
		name   string
		mutate func(t *testing.T, a *Adapter, s *DeliveryStore)
		token  string
		want   delivery.NackOutcome
	}{
		{"unknown token", nil, "dlv_unknown", delivery.NackNotFound},
		{"acknowledged", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			if r := s.Ack(context.Background(), ackReq("dlv_token1")); r.Outcome != delivery.AckAcknowledged {
				t.Fatalf("ack = %+v", r)
			}
		}, "dlv_token1", delivery.NackAlreadyAcknowledged},
		{"expired phase", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
			a.testDo(t, "DEL", tomb)
			a.testDo(t, "HSET", tomb, "state", "expired", "message_id", "m1", "expired_ms", "1")
		}, "dlv_token1", delivery.NackStale},
		{"superseded token", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "HSET", state, "delivery_token", "dlv_other")
		}, "dlv_token1", delivery.NackStale},
		{"lease expired", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "HSET", state, "lease_expires_ms", "1") }, "dlv_token1", delivery.NackStale},
		{"head moved", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "HSET", state, "head_message_id", "m9") }, "dlv_token1", delivery.NackStale},
		{"blocked recipient", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "queue_head_mismatch")
		}, "dlv_token1", delivery.NackRecipientBlocked},
		{"wrong type history", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "SET", "hr1:a:m1", "x") }, "dlv_token1", delivery.NackInternalFailure},
		{"wrong type retries", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "SET", "hr1:retries", "x") }, "dlv_token1", delivery.NackInternalFailure},
		{"malformed history entry", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "RPUSH", "hr1:a:m1", "not json") }, "dlv_token1", delivery.NackInternalFailure},
		{"history entry without attempt", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "RPUSH", "hr1:a:m1", `{"kind":"attempt","delivery_cycle":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"expired"}`)
		}, "dlv_token1", delivery.NackInternalFailure},
		{"history entry without lease_expires_ms", func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "RPUSH", "hr1:a:m1", `{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"completed_ms":2,"outcome":"expired"}`)
		}, "dlv_token1", delivery.NackInternalFailure},
		{"malformed attempt", func(t *testing.T, a *Adapter, s *DeliveryStore) { a.testDo(t, "HSET", state, "attempt", "x") }, "dlv_token1", delivery.NackInternalFailure},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, s := claimSetup(t)
			ctx := context.Background()
			enqueueJSON(t, a, "m1", ridA)
			s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
			if tc.mutate != nil {
				tc.mutate(t, a, s)
			}
			before := snapshot(t, a)
			if r := s.Nack(ctx, nackReq(tc.token, "")); r.Outcome != tc.want {
				t.Fatalf("nack = %+v, want %s", r, tc.want)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestNackSingleAttemptPolicy pins a one-attempt policy: the delay list is
// empty and the only attempt is the last one.
func TestNackSingleAttemptPolicy(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	req := nackReq("dlv_token1", "")
	req.RetryDelaysMs, req.MaxAttempts = nil, 1
	before := snapshot(t, a)
	if r := s.Nack(ctx, req); r.Outcome != delivery.NackAttemptsExhausted {
		t.Errorf("nack = %+v, want attempts_exhausted", r)
	}
	assertUnchanged(t, a, before, "single-attempt nack")
}

// TestNackArguments pins argument and key validation.
func TestNackArguments(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:t:d", "hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:dlq", "hr1:stats:queued_messages"}
	valid := []string{"dlv_t", "d", "", "1000,5000,30000", "4", "3600000", "hr1"}
	if _, err := a.RunScript(ctx, "nack_v1", keys, valid); err != nil {
		t.Fatalf("valid call failed: %v", err)
	}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":            valid[:6],
		"empty token":        with(0, ""),
		"digest mismatch":    with(1, "other"),
		"reason with space":  with(2, "bad reason"),
		"reason too long":    with(2, strings.Repeat("r", 65)),
		"delay count":        with(3, "1000,5000"),
		"zero delay":         with(3, "1000,0,30000"),
		"non-numeric delay":  with(3, "1000,x,30000"),
		"zero max attempts":  with(4, "0"),
		"zero tombstone ttl": with(5, "0"),
		"prefix mismatch":    with(6, "hr2"),
	} {
		if _, err := a.RunScript(ctx, "nack_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "nack_v1", keys[:7], valid); err == nil || errors.Is(err, ErrNotDispatched) {
		t.Errorf("too few keys: err = %v", err)
	}
}

// TestNackAfterScriptFlush pins the EVAL reload path for nack_v1.
func TestNackAfterScriptFlush(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := s.Nack(ctx, nackReq("dlv_token1", "")); r.Outcome != delivery.NackRetryScheduled {
		t.Errorf("nack after SCRIPT FLUSH = %+v", r)
	}
}

// seedHistory writes one attempt entry per (cycle, attempt) with
// claimed_ms = cycle*1000 + attempt and completed_ms = claimed_ms + 500.
func seedHistory(t *testing.T, a *Adapter, messageID string, cycles, attemptsPerCycle int) {
	t.Helper()
	for c := 1; c <= cycles; c++ {
		for n := 1; n <= attemptsPerCycle; n++ {
			claimed := int64(c*1000 + n)
			a.testDo(t, "RPUSH", "hr1:a:"+messageID, `{"kind":"attempt","delivery_cycle":`+itoa64(int64(c))+`,"attempt":`+itoa64(int64(n))+
				`,"claimed_ms":`+itoa64(claimed)+`,"lease_expires_ms":`+itoa64(claimed+60000)+`,"completed_ms":`+itoa64(claimed+500)+`,"outcome":"expired"}`)
		}
	}
}

// TestNackArchivesCyclesBeyondTen pins the 10-cycle history limit: the
// oldest cycles fold into one leading summary, which later folds merge.
func TestNackArchivesCyclesBeyondTen(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	seedHistory(t, a, "m1", 10, 2)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_cycle", "11")

	s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	if r := s.Nack(ctx, nackReq("dlv_token1", "")); r.Outcome != delivery.NackRetryScheduled || r.DeliveryCycle != 11 {
		t.Fatalf("nack = %+v", r)
	}
	history := lrange(t, a, "hr1:a:m1")
	if len(history) != 1+18+1 {
		t.Fatalf("history length = %d, want summary + cycles 2-10 (18) + the new entry", len(history))
	}
	summary := decodeEntry(t, history[0])
	for k, v := range map[string]any{"kind": "archived_cycles_summary", "archived_cycles": 1.0, "archived_attempts": 2.0, "first_archived_ms": 1001.0, "last_archived_ms": 1502.0} {
		if summary[k] != v {
			t.Errorf("summary %s = %v, want %v", k, summary[k], v)
		}
	}
	if keysOf(summary) != "archived_attempts,archived_cycles,first_archived_ms,kind,last_archived_ms" {
		t.Errorf("summary fields = %s", keysOf(summary))
	}
	if e := decodeEntry(t, history[1]); e["delivery_cycle"] != 2.0 || e["attempt"] != 1.0 {
		t.Errorf("first kept entry = %s, want cycle 2 attempt 1", history[1])
	}
	if e := decodeEntry(t, history[len(history)-1]); e["delivery_cycle"] != 11.0 || e["outcome"] != "nack" {
		t.Errorf("last entry = %s, want the new cycle-11 nack", history[len(history)-1])
	}

	// A second attempt of cycle 11 adds no cycle: nothing more is archived.
	activateRetry(t, a, ridA)
	s.Claim(ctx, claimReq("op-2", "args", "dlv_token2"))
	s.Nack(ctx, nackReq("dlv_token2", ""))
	if h := lrange(t, a, "hr1:a:m1"); len(h) != 21 || decodeEntry(t, h[0])["archived_cycles"] != 1.0 {
		t.Errorf("history after a same-cycle nack = %d entries", len(h))
	}

	// Cycle 12 folds cycle 2 into the existing summary.
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_cycle", "12", "attempt", "1", "status", "ready")
	a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "retry_at_ms")
	a.testDo(t, "ZREM", "hr1:retries", ridA)
	a.testDo(t, "ZADD", "hr1:ready", "100", ridA)
	s.Claim(ctx, claimReq("op-3", "args", "dlv_token3"))
	if r := s.Nack(ctx, nackReq("dlv_token3", "")); r.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("cycle 12 nack = %+v", r)
	}
	h := lrange(t, a, "hr1:a:m1")
	summary = decodeEntry(t, h[0])
	for k, v := range map[string]any{"archived_cycles": 2.0, "archived_attempts": 4.0, "first_archived_ms": 1001.0, "last_archived_ms": 2502.0} {
		if summary[k] != v {
			t.Errorf("merged summary %s = %v, want %v", k, summary[k], v)
		}
	}
	if len(h) != 1+16+2+1 {
		t.Errorf("history length = %d, want summary + cycles 3-10 (16) + cycle 11 (2) + cycle 12 (1)", len(h))
	}
}

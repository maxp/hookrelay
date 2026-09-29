package valkey

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/delivery"
)

// expirySetup returns a store whose leases last 20 ms.
func expirySetup(t *testing.T) (*Adapter, *DeliveryStore) {
	t.Helper()
	a, _ := claimSetup(t)
	return a, NewDeliveryStore(a, ClaimLimits{
		MaxActiveLeases: 10, InitialLeaseDuration: 20 * time.Millisecond,
		LeaseExtension: time.Minute, MaxLeaseLifetime: 5 * time.Minute,
	})
}

// claimAndLapse claims the next head and waits until its lease is due.
func claimAndLapse(t *testing.T, s *DeliveryStore, op, token, instance string) delivery.ClaimResult {
	t.Helper()
	req := claimReq(op, "args", token)
	req.ConsumerInstanceID = instance
	c := s.Claim(context.Background(), req)
	if c.Outcome != delivery.ClaimClaimed {
		t.Fatalf("claim = %+v", c)
	}
	time.Sleep(40 * time.Millisecond)
	return c
}

func expire(s *DeliveryStore, rid string) delivery.ExpiryResult {
	return s.ExpireLease(context.Background(), rid, testDelaysMs, 4)
}

// TestExpireLeaseSchedulesRetry pins the retry_scheduled tuple and every
// affected key: history outcome=expired, retry_wait head state, lease and
// retry memberships, the expired token phase, and the claim operation
// marker; ack, nack, and claim replay then see the attempt as ended.
func TestExpireLeaseSchedulesRetry(t *testing.T) {
	a, s := expirySetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	claimed := claimAndLapse(t, s, "op-1", "dlv_token1", "worker-1")

	batch, err := s.DueLeases(ctx, 10)
	if err != nil || len(batch.Entries) != 1 || batch.Entries[0] != (delivery.DueEntry{RecipientIdentity: ridA, DueMs: claimed.Delivery.LeaseExpiresMs}) {
		t.Fatalf("due leases = %+v, %v", batch, err)
	}

	res := expire(s, ridA)
	if res.Outcome != delivery.ExpiryRetryScheduled || res.MessageID != "m1" || res.Attempt != 1 || res.DeliveryCycle != 1 || res.ConsumerInstanceID != "worker-1" ||
		res.ClaimedMs != claimed.Delivery.ClaimedMs || res.ExpiredMs < claimed.Delivery.LeaseExpiresMs || res.RetryAtMs != res.ExpiredMs+1000 {
		t.Fatalf("expire = %+v", res)
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
	if sc, ok := score(t, a, "hr1:retries", ridA); !ok || sc != res.RetryAtMs {
		t.Errorf("retries score = %d %v", sc, ok)
	}

	history := lrange(t, a, "hr1:a:m1")
	if len(history) != 1 {
		t.Fatalf("history = %v", history)
	}
	entry := decodeEntry(t, history[0])
	if keysOf(entry) != "attempt,claimed_ms,completed_ms,consumer_instance_id,delivery_cycle,kind,lease_expires_ms,outcome" {
		t.Errorf("history fields = %s", keysOf(entry))
	}
	for k, v := range map[string]any{
		"kind": "attempt", "outcome": "expired", "attempt": 1.0, "delivery_cycle": 1.0, "claimed_ms": float64(claimed.Delivery.ClaimedMs),
		"lease_expires_ms": float64(claimed.Delivery.LeaseExpiresMs), "completed_ms": float64(res.ExpiredMs), "consumer_instance_id": "worker-1",
	} {
		if entry[k] != v {
			t.Errorf("history %s = %v, want %v", k, entry[k], v)
		}
	}

	tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
	wantTomb := map[string]string{"state": "expired", "message_id": "m1", "expired_ms": itoa64(res.ExpiredMs)}
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
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); len(q) != 2 {
		t.Errorf("queue = %v", q)
	}

	before := snapshot(t, a)
	if r := s.Ack(ctx, ackReq("dlv_token1")); r.Outcome != delivery.AckStale {
		t.Errorf("ack after expiry = %+v, want stale", r)
	}
	if r := s.Nack(ctx, nackReq("dlv_token1", "")); r.Outcome != delivery.NackStale {
		t.Errorf("nack after expiry = %+v, want stale", r)
	}
	if r := s.Claim(ctx, claimReq("op-1", "args", "dlv_x")); r.Outcome != delivery.ClaimNoLongerActive {
		t.Errorf("claim replay after expiry = %+v", r)
	}
	assertUnchanged(t, a, before, "refusals after expiry")
}

// TestExpireLeaseNotDueAndStaleMembers pins not_due: a lease that is not
// yet due is left untouched; a member whose state is not leased (or no
// longer exists) is removed without changing any other state.
func TestExpireLeaseNotDueAndStaleMembers(t *testing.T) {
	t.Run("not yet due", func(t *testing.T) {
		a, s := claimSetup(t) // one-minute leases
		ctx := context.Background()
		enqueueJSON(t, a, "m1", ridA)
		s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
		if batch, _ := s.DueLeases(ctx, 10); len(batch.Entries) != 0 {
			t.Errorf("not-yet-due lease listed: %+v", batch)
		}
		before := snapshot(t, a)
		if r := expire(s, ridA); r.Outcome != delivery.ExpiryNotDue {
			t.Errorf("expire = %+v, want not_due", r)
		}
		assertUnchanged(t, a, before, "not-yet-due expiry")
	})
	for name, setup := range map[string]func(t *testing.T, a *Adapter, s *DeliveryStore){
		"ready head": func(t *testing.T, a *Adapter, s *DeliveryStore) {},
		"retry_wait head": func(t *testing.T, a *Adapter, s *DeliveryStore) {
			s.Claim(context.Background(), claimReq("op-1", "args", "dlv_token1"))
			s.Nack(context.Background(), nackReq("dlv_token1", ""))
		},
		"drained queue": func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q", "hr1:r:"+ridA+":s")
			a.testDo(t, "ZREM", "hr1:ready", ridA)
		},
	} {
		t.Run("stale member for "+name, func(t *testing.T) {
			a, s := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			setup(t, a, s)
			before := snapshot(t, a)
			a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
			if r := expire(s, ridA); r.Outcome != delivery.ExpiryNotDue {
				t.Errorf("expire = %+v, want not_due", r)
			}
			assertUnchanged(t, a, before, "stale member removal")
		})
	}
}

// TestExpireLeaseRefusals pins the marker refusal (which removes the stale
// lease member and nothing else), the temporary last-attempt refusal, and
// wrong_type without mutation.
func TestExpireLeaseRefusals(t *testing.T) {
	t.Run("blocked recipient", func(t *testing.T) {
		a, s := expirySetup(t)
		enqueueJSON(t, a, "m1", ridA)
		claimAndLapse(t, s, "op-1", "dlv_token1", "")
		a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "queue_head_mismatch")
		a.testDo(t, "ZADD", "hr1:blocked", "1", ridA)
		a.testDo(t, "ZREM", "hr1:leases", ridA)
		before := snapshot(t, a)
		a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
		if r := expire(s, ridA); r.Outcome != delivery.ExpiryRecipientBlocked {
			t.Errorf("expire = %+v, want recipient_blocked", r)
		}
		assertUnchanged(t, a, before, "blocked expiry")
	})
	for name, poison := range map[string]func(t *testing.T, a *Adapter){
		"last attempt with zero counter":               func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:stats:queued_messages", "0") },
		"last attempt with dead-letter hash":           func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:dl:m1", "x", "y") },
		"last attempt with sequence overflow":          func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready_seq", "9223372036854775807") },
		"last attempt with malformed counter":          func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:stats:queued_messages", "x") },
		"last attempt with dead-letter key wrong type": func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:dl:m1", "x") },
		"last attempt with metadata wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:mi:m1")
			a.testDo(t, "SET", "hr1:mi:m1", "x")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := expirySetup(t)
			enqueueJSON(t, a, "m1", ridA)
			enqueueJSON(t, a, "m2", ridA)
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "attempt", "4")
			claimAndLapse(t, s, "op-1", "dlv_token1", "")
			poison(t, a)
			before := snapshot(t, a)
			if r := expire(s, ridA); r.Outcome != delivery.ExpiryInternalFailure {
				t.Errorf("expire = %+v, want internal failure", r)
			}
			assertUnchanged(t, a, before, name)
		})
	}
	state := "hr1:r:" + ridA + ":s"
	for name, poison := range map[string]func(t *testing.T, a *Adapter){
		"leases wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:leases")
			a.testDo(t, "SET", "hr1:leases", "x")
		},
		"state wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", state)
			a.testDo(t, "SET", state, "x")
		},
		"queue wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q")
			a.testDo(t, "SET", "hr1:r:"+ridA+":q", "x")
		},
		"history wrong type":      func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:a:m1", "x") },
		"malformed history entry": func(t *testing.T, a *Adapter) { a.testDo(t, "RPUSH", "hr1:a:m1", "not json") },
		"retries wrong type":      func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:retries", "x") },
		"dlq wrong type":          func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:dlq", "x") },
		"ready wrong type":        func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready", "x") },
		"blocked wrong type":      func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:blocked", "x") },
		"ready sequence wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:ready_seq")
			a.testDo(t, "HSET", "hr1:ready_seq", "x", "y")
		},
		"counter wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:stats:queued_messages")
			a.testDo(t, "HSET", "hr1:stats:queued_messages", "x", "y")
		},
		"claim op wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:op:op-1")
			a.testDo(t, "SET", "hr1:op:op-1", "x")
		},
		"malformed attempt":    func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", state, "attempt", "x") },
		"malformed claimed_ms": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", state, "claimed_ms", "x") },
		"head mismatch":        func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", state, "head_message_id", "m9") },
		"token record wrong type": func(t *testing.T, a *Adapter) {
			tomb := "hr1:t:" + ackReq("dlv_token1").TokenDigest
			a.testDo(t, "DEL", tomb)
			a.testDo(t, "SET", tomb, "x")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, s := expirySetup(t)
			enqueueJSON(t, a, "m1", ridA)
			claimAndLapse(t, s, "op-1", "dlv_token1", "")
			poison(t, a)
			before := snapshot(t, a)
			if r := expire(s, ridA); r.Outcome != delivery.ExpiryInternalFailure {
				t.Errorf("expire = %+v, want internal failure", r)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestExpireLeaseWithoutTokenDigest pins the claim_v2-era lease: without a
// token digest in state the lease still expires; the token record and the
// claim operation are left to their TTLs (ack and nack already see a
// non-leased head as stale).
func TestExpireLeaseWithoutTokenDigest(t *testing.T) {
	a, s := expirySetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	claimAndLapse(t, s, "op-1", "dlv_token1", "")
	a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token_digest")
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryRetryScheduled {
		t.Fatalf("expire = %+v", r)
	}
	if got := hget(t, a, "hr1:t:"+ackReq("dlv_token1").TokenDigest, "state"); got != "active" {
		t.Errorf("token record state = %q, want the active phase left to its TTL", got)
	}
	if r := s.Ack(ctx, ackReq("dlv_token1")); r.Outcome != delivery.AckStale {
		t.Errorf("ack = %+v, want stale", r)
	}
	if r := s.Claim(ctx, claimReq("op-1", "args", "dlv_x")); r.Outcome != delivery.ClaimNoLongerActive {
		t.Errorf("claim replay = %+v", r)
	}
}

// TestExpireLeaseArgumentsAndReload pins argument validation and the EVAL
// reload after SCRIPT FLUSH.
func TestExpireLeaseArgumentsAndReload(t *testing.T) {
	a, s := expirySetup(t)
	ctx := context.Background()
	keys := []string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:dlq", "hr1:stats:queued_messages"}
	valid := []string{ridA, "1000,5000,30000", "4", "3600000", "hr1"}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":            valid[:4],
		"empty recipient":    with(0, ""),
		"delay count":        with(1, "1000"),
		"zero delay":         with(1, "1000,0,30000"),
		"zero max attempts":  with(2, "0"),
		"zero tombstone ttl": with(3, "0"),
		"prefix mismatch":    with(4, "hr2"),
	} {
		if _, err := a.RunScript(ctx, "expire_lease_v2", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "expire_lease_v2", keys[:6], valid); err == nil || errors.Is(err, ErrNotDispatched) {
		t.Errorf("too few keys: err = %v", err)
	}

	enqueueJSON(t, a, "m1", ridA)
	claimAndLapse(t, s, "op-1", "dlv_token1", "")
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryRetryScheduled {
		t.Errorf("expire after SCRIPT FLUSH = %+v", r)
	}
}

// TestExpireLeaseArchivesCyclesBeyondTen pins the 10-cycle history limit on
// the expiry path: the oldest cycle folds into the leading summary.
func TestExpireLeaseArchivesCyclesBeyondTen(t *testing.T) {
	a, s := expirySetup(t)
	enqueueJSON(t, a, "m1", ridA)
	seedHistory(t, a, "m1", 10, 2)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "delivery_cycle", "11")
	claimAndLapse(t, s, "op-1", "dlv_token1", "")
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryRetryScheduled || r.DeliveryCycle != 11 {
		t.Fatalf("expire = %+v", r)
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
	if e := decodeEntry(t, history[len(history)-1]); e["delivery_cycle"] != 11.0 || e["outcome"] != "expired" {
		t.Errorf("last entry = %s", history[len(history)-1])
	}
}

// TestExpireLeaseStaleMemberBesideCorruptIndex pins the not_due order: a
// non-leased head's lease member is removed even when an unrelated global
// index has the wrong type.
func TestExpireLeaseStaleMemberBesideCorruptIndex(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	a.testDo(t, "SET", "hr1:dlq", "x")
	before := snapshot(t, a)
	a.testDo(t, "ZADD", "hr1:leases", "1", ridA)
	if r := expire(s, ridA); r.Outcome != delivery.ExpiryNotDue {
		t.Errorf("expire = %+v, want not_due", r)
	}
	assertUnchanged(t, a, before, "stale member beside a corrupt index")
}

// TestExpireLeaseDeadLettersTheLastAttempt pins the dead_lettered tuple of
// expiry: reason expiry_exhausted, the queue advanced, the expired token
// phase, the claim operation marker, and the retained history.
func TestExpireLeaseDeadLettersTheLastAttempt(t *testing.T) {
	a, s := expirySetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "attempt", "4")
	claimed := claimAndLapse(t, s, "op-1", "dlv_token1", "worker-4")

	res := expire(s, ridA)
	if res.Outcome != delivery.ExpiryDeadLettered || res.MessageID != "m1" || res.DeliveryCycle != 1 || res.Attempt != 4 ||
		res.ClaimedMs != claimed.Delivery.ClaimedMs || res.DeadLetteredMs < claimed.Delivery.LeaseExpiresMs || res.ConsumerInstanceID != "worker-4" {
		t.Fatalf("expire = %+v", res)
	}
	assertHash(t, a, "hr1:dl:m1", map[string]string{
		"bot_platform": "telegram", "bot_id": "42", "recipient_scope": "chat", "chat_id": "-1", "recipient_identity": ridA,
		"dead_lettered_ms": itoa64(res.DeadLetteredMs), "dead_letter_reason": "expiry_exhausted", "delivery_cycle": "1",
		"dedup_identity_digest": "d-m1",
	})
	if sc, ok := score(t, a, "hr1:dlq", "m1"); !ok || sc != res.DeadLetteredMs {
		t.Errorf("dlq score = %d %v", sc, ok)
	}
	if q := lrange(t, a, "hr1:r:"+ridA+":q"); len(q) != 1 || q[0] != "m2" {
		t.Errorf("queue = %v", q)
	}
	assertHash(t, a, "hr1:r:"+ridA+":s", map[string]string{"status": "ready", "head_message_id": "m2", "delivery_cycle": "1", "attempt": "1"})
	for _, idx := range []string{"hr1:leases", "hr1:retries"} {
		if _, ok := score(t, a, idx, ridA); ok {
			t.Errorf("%s member kept", idx)
		}
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 1 {
		t.Errorf("counter = %d", n)
	}
	assertHash(t, a, "hr1:t:"+ackReq("dlv_token1").TokenDigest, map[string]string{"state": "expired", "message_id": "m1", "expired_ms": itoa64(res.DeadLetteredMs)})
	if got := hget(t, a, "hr1:op:op-1", "state"); got != "no_longer_active" {
		t.Errorf("op state = %q", got)
	}
	history := lrange(t, a, "hr1:a:m1")
	if e := decodeEntry(t, history[len(history)-1]); e["outcome"] != "expired" || e["attempt"] != 4.0 {
		t.Errorf("history = %v", history)
	}
	if !exists(t, a, "hr1:m:m1") || !exists(t, a, "hr1:mi:m1") {
		t.Error("dead-lettered blob or metadata deleted")
	}
	if c := s.Claim(ctx, claimReq("op-next", "args", "dlv_next")); c.Outcome != delivery.ClaimClaimed || c.Delivery.MessageID != "m2" {
		t.Errorf("next claim = %+v", c)
	}
}

// TestExpireLeaseDeadLetterDrainsQueue pins a dead-lettering expiry of the
// only message of a user-scope Recipient: user_id in the dead-letter Hash,
// queue and state deleted, no index membership left.
func TestExpireLeaseDeadLetterDrainsQueue(t *testing.T) {
	a, s := expirySetup(t)
	ctx := context.Background()
	rid := "telegram:42:user:7"
	enqueueJSON(t, a, "m1", rid)
	a.testDo(t, "HSET", "hr1:r:"+rid+":s", "attempt", "4")
	claimAndLapse(t, s, "op-1", "dlv_token1", "")
	res := expire(s, rid)
	if res.Outcome != delivery.ExpiryDeadLettered {
		t.Fatalf("expire = %+v", res)
	}
	assertHash(t, a, "hr1:dl:m1", map[string]string{
		"bot_platform": "telegram", "bot_id": "42", "recipient_scope": "user", "user_id": "7", "recipient_identity": rid,
		"dead_lettered_ms": itoa64(res.DeadLetteredMs), "dead_letter_reason": "expiry_exhausted", "delivery_cycle": "1",
		"dedup_identity_digest": "d-m1",
	})
	for _, k := range []string{"hr1:r:" + rid + ":q", "hr1:r:" + rid + ":s", "hr1:ready", "hr1:leases", "hr1:retries"} {
		if exists(t, a, k) {
			t.Errorf("%s kept after draining", k)
		}
	}
	if n, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).AsInt64(); n != 0 {
		t.Errorf("counter = %d", n)
	}
}

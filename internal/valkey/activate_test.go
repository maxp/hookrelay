package valkey

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/delivery"
)

// nackSoon nacks the claimed token with a 1 ms retry delay and waits until
// the retry is due by Valkey time.
func nackSoon(t *testing.T, a *Adapter, s *DeliveryStore, token string) delivery.NackResult {
	t.Helper()
	req := nackReq(token, "")
	req.RetryDelaysMs = []int64{1, 1, 1}
	res := s.Nack(context.Background(), req)
	if res.Outcome != delivery.NackRetryScheduled {
		t.Fatalf("nack = %+v", res)
	}
	time.Sleep(10 * time.Millisecond)
	return res
}

// TestActivateRetryMakesTheHeadClaimable pins the activated tuple and every
// affected key: ready head state keeping cycle and the next attempt, the
// retries member replaced by a ready member with a fresh sequence; the next
// claim returns attempt 2 with a new token.
func TestActivateRetryMakesTheHeadClaimable(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridA)
	first := s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	nacked := nackSoon(t, a, s, "dlv_token1")
	seqBefore, _ := a.client.Do(ctx, a.client.B().Get().Key("hr1:ready_seq").Build()).AsInt64()

	batch, err := s.DueRetries(ctx, 10)
	if err != nil || len(batch.Entries) != 1 || batch.Entries[0].RecipientIdentity != ridA || batch.Entries[0].DueMs != nacked.RetryAtMs || batch.NowMs < nacked.RetryAtMs {
		t.Fatalf("due retries = %+v, %v", batch, err)
	}

	res := s.ActivateRetry(ctx, ridA)
	if res.Outcome != delivery.ActivationActivated || res.MessageID != "m1" || res.Attempt != 2 {
		t.Fatalf("activate = %+v", res)
	}
	state := "hr1:r:" + ridA + ":s"
	want := map[string]string{"status": "ready", "head_message_id": "m1", "delivery_cycle": "1", "attempt": "2"}
	if got := hgetall(t, a, state); len(got) != len(want) {
		t.Errorf("state = %v, want exactly %v", got, want)
	}
	for f, v := range want {
		if got := hget(t, a, state, f); got != v {
			t.Errorf("state %s = %q, want %q", f, got, v)
		}
	}
	if _, ok := score(t, a, "hr1:retries", ridA); ok {
		t.Error("retries member kept")
	}
	if sc, ok := score(t, a, "hr1:ready", ridA); !ok || sc != seqBefore+1 {
		t.Errorf("ready score = %d %v, want the fresh sequence %d", sc, ok, seqBefore+1)
	}
	if batch, _ := s.DueRetries(ctx, 10); len(batch.Entries) != 0 {
		t.Errorf("due retries after activation = %+v", batch)
	}

	second := s.Claim(ctx, claimReq("op-2", "args", "dlv_token2"))
	if second.Outcome != delivery.ClaimClaimed || second.Delivery.MessageID != "m1" || second.Delivery.Attempt != 2 ||
		second.Delivery.DeliveryCycle != 1 || second.Delivery.Token == first.Delivery.Token {
		t.Errorf("retried claim = %+v", second)
	}
}

// TestActivateRetryNotDueAndStaleMembers pins not_due: a retry that is not
// yet due is left untouched; a member whose state is not retry_wait (or no
// longer exists) is removed without changing any other state.
func TestActivateRetryNotDueAndStaleMembers(t *testing.T) {
	t.Run("not yet due", func(t *testing.T) {
		a, s := claimSetup(t)
		ctx := context.Background()
		enqueueJSON(t, a, "m1", ridA)
		s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
		s.Nack(ctx, nackReq("dlv_token1", "")) // 1 s delay
		if batch, _ := s.DueRetries(ctx, 10); len(batch.Entries) != 0 {
			t.Errorf("not-yet-due retry listed: %+v", batch)
		}
		before := snapshot(t, a)
		if r := s.ActivateRetry(ctx, ridA); r.Outcome != delivery.ActivationNotDue {
			t.Errorf("activate = %+v, want not_due", r)
		}
		assertUnchanged(t, a, before, "not-yet-due activation")
	})
	for name, setup := range map[string]func(t *testing.T, a *Adapter, s *DeliveryStore){
		"ready head": func(t *testing.T, a *Adapter, s *DeliveryStore) {},
		"leased head": func(t *testing.T, a *Adapter, s *DeliveryStore) {
			s.Claim(context.Background(), claimReq("op-1", "args", "dlv_token1"))
		},
		"drained queue": func(t *testing.T, a *Adapter, s *DeliveryStore) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q", "hr1:r:"+ridA+":s")
			a.testDo(t, "ZREM", "hr1:ready", ridA)
		},
	} {
		t.Run("stale member for "+name, func(t *testing.T) {
			a, s := claimSetup(t)
			ctx := context.Background()
			enqueueJSON(t, a, "m1", ridA)
			setup(t, a, s)
			before := snapshot(t, a)
			a.testDo(t, "ZADD", "hr1:retries", "1", ridA)
			if r := s.ActivateRetry(ctx, ridA); r.Outcome != delivery.ActivationNotDue {
				t.Errorf("activate = %+v, want not_due", r)
			}
			assertUnchanged(t, a, before, "stale member removal")
		})
	}
}

// TestActivateRetryRefusals pins the marker refusal (which removes the
// stale retries member and nothing else) and wrong_type without mutation.
func TestActivateRetryRefusals(t *testing.T) {
	t.Run("blocked recipient", func(t *testing.T) {
		a, s := claimSetup(t)
		ctx := context.Background()
		enqueueJSON(t, a, "m1", ridA)
		s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
		nackSoon(t, a, s, "dlv_token1")
		a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "1", "reason_code", "queue_head_mismatch")
		a.testDo(t, "ZADD", "hr1:blocked", "1", ridA)
		a.testDo(t, "ZREM", "hr1:retries", ridA)
		before := snapshot(t, a)
		a.testDo(t, "ZADD", "hr1:retries", "1", ridA)
		if r := s.ActivateRetry(ctx, ridA); r.Outcome != delivery.ActivationRecipientBlocked {
			t.Errorf("activate = %+v, want recipient_blocked", r)
		}
		assertUnchanged(t, a, before, "blocked activation")
	})
	for name, poison := range map[string]func(t *testing.T, a *Adapter){
		"state wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":s")
			a.testDo(t, "SET", "hr1:r:"+ridA+":s", "x")
		},
		"queue wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q")
			a.testDo(t, "SET", "hr1:r:"+ridA+":q", "x")
		},
		"malformed retry_at_ms": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "retry_at_ms", "soon") },
		"ready sequence overflow": func(t *testing.T, a *Adapter) {
			a.testDo(t, "SET", "hr1:ready_seq", "9223372036854775807")
		},
		"ready wrong type":         func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready", "x") },
		"blocked index wrong type": func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:blocked", "x") },
		"retries wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:retries")
			a.testDo(t, "SET", "hr1:retries", "x")
		},
		"ready sequence wrong type": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:ready_seq")
			a.testDo(t, "HSET", "hr1:ready_seq", "x", "y")
		},
		"malformed ready sequence": func(t *testing.T, a *Adapter) { a.testDo(t, "SET", "hr1:ready_seq", "x") },
		"malformed attempt":        func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "attempt", "x") },
		"missing head_message_id":  func(t *testing.T, a *Adapter) { a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "head_message_id") },
		"head mismatch":            func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "m9") },
		"empty queue":              func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:r:"+ridA+":q") },
	} {
		t.Run(name, func(t *testing.T) {
			a, s := claimSetup(t)
			ctx := context.Background()
			enqueueJSON(t, a, "m1", ridA)
			s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
			nackSoon(t, a, s, "dlv_token1")
			poison(t, a)
			before := snapshot(t, a)
			if r := s.ActivateRetry(ctx, ridA); r.Outcome != delivery.ActivationInternalFailure {
				t.Errorf("activate = %+v, want internal failure", r)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestActivateRetryArgumentsAndReload pins argument validation and the EVAL
// reload after SCRIPT FLUSH.
func TestActivateRetryArgumentsAndReload(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	keys := []string{"hr1:ready", "hr1:ready_seq", "hr1:retries", "hr1:blocked"}
	for name, args := range map[string][]string{
		"too few":         {ridA},
		"empty recipient": {"", "hr1"},
		"prefix mismatch": {ridA, "hr2"},
	} {
		if _, err := a.RunScript(ctx, "activate_retry_v1", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	if _, err := a.RunScript(ctx, "activate_retry_v1", keys[:3], []string{ridA, "hr1"}); err == nil || errors.Is(err, ErrNotDispatched) {
		t.Errorf("too few keys: err = %v", err)
	}

	enqueueJSON(t, a, "m1", ridA)
	s.Claim(ctx, claimReq("op-1", "args", "dlv_token1"))
	nackSoon(t, a, s, "dlv_token1")
	a.testDo(t, "SCRIPT", "FLUSH")
	if r := s.ActivateRetry(ctx, ridA); r.Outcome != delivery.ActivationActivated {
		t.Errorf("activate after SCRIPT FLUSH = %+v", r)
	}
}

// TestDueRetriesBatch pins the bounded, oldest-first due read.
func TestDueRetriesBatch(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	a.testDo(t, "ZADD", "hr1:retries", "30", "r3", "10", "r1", "20", "r2", "4102444800000", "future")
	batch, err := s.DueRetries(ctx, 2)
	if err != nil || len(batch.Entries) != 2 || batch.Entries[0] != (delivery.DueEntry{RecipientIdentity: "r1", DueMs: 10}) ||
		batch.Entries[1] != (delivery.DueEntry{RecipientIdentity: "r2", DueMs: 20}) || batch.NowMs < 1740000000000 {
		t.Errorf("batch = %+v, %v", batch, err)
	}
}

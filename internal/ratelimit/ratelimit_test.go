package ratelimit

import (
	"fmt"
	"testing"
	"time"
)

// TestTokenBucket pins burst, refill at the configured rate, Retry-After
// rounding up to whole seconds (at least one), and that a refusal takes
// no token from either bucket.
func TestTokenBucket(t *testing.T) {
	start := time.UnixMilli(1740000000000)
	l := New(Limits{GlobalRate: 0.5, GlobalBurst: 2}, start)
	for i := 0; i < 2; i++ {
		if ok, _ := l.Allow("e", start); !ok {
			t.Fatalf("burst request %d refused", i)
		}
	}
	if ok, retry := l.Allow("e", start); ok || retry != 2 {
		t.Errorf("empty bucket = %v retry %d, want refused with 2 s at 0.5/s", ok, retry)
	}
	if ok, retry := l.Allow("e", start.Add(1500*time.Millisecond)); ok || retry != 1 {
		t.Errorf("0.75 tokens = %v retry %d, want refused with 1 s", ok, retry)
	}
	if ok, _ := l.Allow("e", start.Add(2*time.Second)); !ok {
		t.Error("refilled token refused")
	}

	// Endpoint buckets are independent; a request refused by its endpoint
	// bucket does not spend a global token.
	l = New(Limits{GlobalRate: 100, GlobalBurst: 3, KeyRate: 1, KeyBurst: 1}, start)
	if ok, _ := l.Allow("a", start); !ok {
		t.Fatal("first a refused")
	}
	if ok, retry := l.Allow("a", start); ok || retry != 1 {
		t.Errorf("second a = %v retry %d", ok, retry)
	}
	for _, e := range []string{"b", "c"} {
		if ok, _ := l.Allow(e, start); !ok {
			t.Errorf("%s refused although the global bucket still had tokens", e)
		}
	}
	if ok, _ := l.Allow("d", start); ok {
		t.Error("global burst exceeded")
	}

	// Zero rates disable both limits; a zero burst still holds one token.
	l = New(Limits{}, start)
	for i := 0; i < 1000; i++ {
		if ok, _ := l.Allow("e", start); !ok {
			t.Fatal("disabled limiter refused")
		}
	}
	if l.Size() != 0 {
		t.Error("disabled endpoint limit tracked buckets")
	}
	l = New(Limits{KeyRate: 1}, start)
	if ok, _ := l.Allow("e", start); !ok {
		t.Error("zero burst refused its single token")
	}
}

// TestKeyBucketsAreBounded pins the LRU bound: the least recently used
// endpoint's bucket is dropped and restarts full.
func TestKeyBucketsAreBounded(t *testing.T) {
	start := time.UnixMilli(1740000000000)
	l := New(Limits{KeyRate: 0.001, KeyBurst: 1}, start)
	l.Allow("first", start)
	for i := 0; i < MaxKeys; i++ {
		l.Allow(fmt.Sprintf("e%d", i), start)
	}
	if l.Size() != MaxKeys {
		t.Errorf("size = %d, want %d", l.Size(), MaxKeys)
	}
	if ok, _ := l.Allow("first", start); !ok {
		t.Error("evicted endpoint did not restart with a full bucket")
	}
	if ok, _ := l.Allow("e9999", start); ok {
		t.Error("a recently used endpoint lost its bucket")
	}
}

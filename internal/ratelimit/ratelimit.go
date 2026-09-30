// Package ratelimit provides process-local token buckets: one global
// bucket and a bounded LRU of per-key buckets. Webhook ingestion keys them
// by Webhook Endpoint, administrative login by source address.
package ratelimit

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// MaxKeys bounds the per-key buckets; the least recently used key's bucket
// is dropped (it restarts full) beyond the bound.
const MaxKeys = 10_000

// Limits configures the buckets. A zero rate disables that limit; a burst
// below one holds one token. Rates are tokens per second.
type Limits struct {
	GlobalRate  float64
	GlobalBurst int
	KeyRate     float64
	KeyBurst    int
}

// bucket is a token bucket refilled continuously at rate tokens per second
// up to capacity.
type bucket struct {
	tokens float64
	last   time.Time
}

// take refills the bucket to now and takes one token. When none is
// available it returns the wait until the next token.
func (b *bucket) take(now time.Time, rate, capacity float64) (bool, time.Duration) {
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = math.Min(capacity, b.tokens+elapsed*rate)
	}
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / rate * float64(time.Second))
}

// peek reports the wait until one token without taking it.
func (b *bucket) peek(now time.Time, rate, capacity float64) time.Duration {
	tokens := b.tokens
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		tokens = math.Min(capacity, tokens+elapsed*rate)
	}
	if tokens >= 1 {
		return 0
	}
	return time.Duration((1 - tokens) / rate * float64(time.Second))
}

// Limiter holds one global bucket and a bounded LRU of per-key buckets.
type Limiter struct {
	mu     sync.Mutex
	limits Limits
	global bucket
	keys   map[string]*list.Element
	lru    *list.List // front is most recently used
}

type keyBucket struct {
	key string
	b   bucket
}

// New returns a Limiter whose buckets start full at now.
func New(l Limits, now time.Time) *Limiter {
	return &Limiter{
		limits: l,
		global: bucket{tokens: burst(l.GlobalBurst), last: now},
		keys:   map[string]*list.Element{},
		lru:    list.New(),
	}
}

func burst(n int) float64 {
	return math.Max(1, float64(n))
}

// Allow takes one token from the global and the key's bucket. A request is
// admitted only when both have a token; a refused request takes neither.
// retryAfter is the whole seconds until both could admit it, at least one.
func (r *Limiter) Allow(key string, now time.Time) (ok bool, retryAfter int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var kb *bucket
	if r.limits.KeyRate > 0 {
		kb = r.keyBucket(key, now)
	}
	var wait time.Duration
	if r.limits.GlobalRate > 0 {
		wait = max(wait, r.global.peek(now, r.limits.GlobalRate, burst(r.limits.GlobalBurst)))
	}
	if kb != nil {
		wait = max(wait, kb.peek(now, r.limits.KeyRate, burst(r.limits.KeyBurst)))
	}
	if wait > 0 {
		return false, max(1, int(math.Ceil(wait.Seconds())))
	}
	if r.limits.GlobalRate > 0 {
		r.global.take(now, r.limits.GlobalRate, burst(r.limits.GlobalBurst))
	}
	if kb != nil {
		kb.take(now, r.limits.KeyRate, burst(r.limits.KeyBurst))
	}
	return true, 0
}

// keyBucket returns the key's bucket, creating a full one and evicting the
// least recently used beyond MaxKeys.
func (r *Limiter) keyBucket(key string, now time.Time) *bucket {
	if el, ok := r.keys[key]; ok {
		r.lru.MoveToFront(el)
		return &el.Value.(*keyBucket).b
	}
	if r.lru.Len() >= MaxKeys {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.keys, oldest.Value.(*keyBucket).key)
	}
	kb := &keyBucket{key: key, b: bucket{tokens: burst(r.limits.KeyBurst), last: now}}
	r.keys[key] = r.lru.PushFront(kb)
	return &kb.b
}

// Size reports the number of tracked per-key buckets.
func (r *Limiter) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lru.Len()
}

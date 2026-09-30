package ingestion

import (
	"container/list"
	"math"
	"sync"
	"time"
)

// maxEndpointBuckets bounds the per-endpoint buckets; the least recently
// used endpoint's bucket is dropped (it restarts full) beyond the bound.
const maxEndpointBuckets = 10_000

// RateLimits configures the process-local webhook token buckets. A zero
// rate disables that limit; a burst below one holds one token.
type RateLimits struct {
	GlobalRate    float64
	GlobalBurst   int
	EndpointRate  float64
	EndpointBurst int
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

// rateLimiter holds one global bucket and a bounded LRU of per-endpoint
// buckets.
type rateLimiter struct {
	mu        sync.Mutex
	limits    RateLimits
	global    bucket
	endpoints map[string]*list.Element
	lru       *list.List // front is most recently used
}

type endpointBucket struct {
	key string
	b   bucket
}

func newRateLimiter(l RateLimits, now time.Time) *rateLimiter {
	return &rateLimiter{
		limits:    l,
		global:    bucket{tokens: burst(l.GlobalBurst), last: now},
		endpoints: map[string]*list.Element{},
		lru:       list.New(),
	}
}

func burst(n int) float64 {
	return math.Max(1, float64(n))
}

// allow takes one token from the global and the endpoint bucket. A request
// is admitted only when both have a token; a refused request takes neither.
// retryAfter is the whole seconds until both could admit it, at least one.
func (r *rateLimiter) allow(endpoint string, now time.Time) (ok bool, retryAfter int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var eb *bucket
	if r.limits.EndpointRate > 0 {
		eb = r.endpointBucket(endpoint, now)
	}
	var wait time.Duration
	if r.limits.GlobalRate > 0 {
		wait = max(wait, r.global.peek(now, r.limits.GlobalRate, burst(r.limits.GlobalBurst)))
	}
	if eb != nil {
		wait = max(wait, eb.peek(now, r.limits.EndpointRate, burst(r.limits.EndpointBurst)))
	}
	if wait > 0 {
		return false, max(1, int(math.Ceil(wait.Seconds())))
	}
	if r.limits.GlobalRate > 0 {
		r.global.take(now, r.limits.GlobalRate, burst(r.limits.GlobalBurst))
	}
	if eb != nil {
		eb.take(now, r.limits.EndpointRate, burst(r.limits.EndpointBurst))
	}
	return true, 0
}

// endpointBucket returns the endpoint's bucket, creating a full one and
// evicting the least recently used beyond maxEndpointBuckets.
func (r *rateLimiter) endpointBucket(key string, now time.Time) *bucket {
	if el, ok := r.endpoints[key]; ok {
		r.lru.MoveToFront(el)
		return &el.Value.(*endpointBucket).b
	}
	if r.lru.Len() >= maxEndpointBuckets {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.endpoints, oldest.Value.(*endpointBucket).key)
	}
	eb := &endpointBucket{key: key, b: bucket{tokens: burst(r.limits.EndpointBurst), last: now}}
	r.endpoints[key] = r.lru.PushFront(eb)
	return &eb.b
}

// size reports the number of tracked endpoint buckets.
func (r *rateLimiter) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lru.Len()
}

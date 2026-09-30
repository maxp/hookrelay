package ingestion

import (
	"time"

	"github.com/maxp/hookrelay/internal/ratelimit"
)

// RateLimits configures the process-local webhook token buckets. A zero
// rate disables that limit; a burst below one holds one token.
type RateLimits struct {
	GlobalRate    float64
	GlobalBurst   int
	EndpointRate  float64
	EndpointBurst int
}

// newRateLimiter keys the per-key buckets by Webhook Endpoint.
func newRateLimiter(l RateLimits, now time.Time) *ratelimit.Limiter {
	return ratelimit.New(ratelimit.Limits{GlobalRate: l.GlobalRate, GlobalBurst: l.GlobalBurst,
		KeyRate: l.EndpointRate, KeyBurst: l.EndpointBurst}, now)
}

package delivery

import (
	"errors"
	"time"
)

// RetryPolicy is the global Retry Policy: MaxAttempts attempts per Delivery
// Cycle including the first, and one nominal delay per retryable failure.
type RetryPolicy struct {
	MaxAttempts int
	// Delays[n-1] is the nominal delay after failed attempt n (len =
	// MaxAttempts-1; see Validate).
	Delays    []time.Duration
	JitterMin float64
	JitterMax float64
}

// Validate checks the invariants the failure transitions rely on.
func (p RetryPolicy) Validate() error {
	if p.MaxAttempts <= 0 || len(p.Delays) != p.MaxAttempts-1 {
		return errors.New("delivery: RetryPolicy needs a positive MaxAttempts and MaxAttempts-1 retry delays")
	}
	for _, d := range p.Delays {
		if d <= 0 {
			return errors.New("delivery: RetryPolicy delays must be positive")
		}
	}
	if p.JitterMin < 0 || p.JitterMax < p.JitterMin || p.JitterMax > 1 {
		return errors.New("delivery: RetryPolicy jitter must satisfy 0 <= min <= max <= 1")
	}
	return nil
}

// DrawDelaysMs draws the effective delay after each retryable attempt,
// nominal × U[JitterMin, JitterMax] in whole milliseconds (at least 1). The
// failed attempt is known only inside the atomic transition, so the whole
// list is drawn app-side and the transition picks its entry. uniform
// returns values in [0, 1).
func (p RetryPolicy) DrawDelaysMs(uniform func() float64) []int64 {
	out := make([]int64, len(p.Delays))
	for i, nominal := range p.Delays {
		factor := p.JitterMin + (p.JitterMax-p.JitterMin)*uniform()
		ms := int64(float64(nominal.Milliseconds()) * factor)
		if ms < 1 {
			ms = 1
		}
		out[i] = ms
	}
	return out
}

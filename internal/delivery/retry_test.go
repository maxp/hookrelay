package delivery

import (
	"testing"
	"time"
)

func defaultPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 4,
		Delays:      []time.Duration{time.Second, 5 * time.Second, 30 * time.Second},
		JitterMin:   0.5,
		JitterMax:   1.0,
	}
}

// TestRetryPolicyDrawsOneJitteredDelayPerRetryableAttempt pins the delay
// list passed to the failure transitions: entry n-1 is the effective delay
// after failed attempt n, nominal × U[JitterMin, JitterMax], in whole
// milliseconds.
func TestRetryPolicyDrawsOneJitteredDelayPerRetryableAttempt(t *testing.T) {
	for name, tc := range map[string]struct {
		uniform float64
		want    []int64
	}{
		"lowest jitter":  {0, []int64{500, 2500, 15000}},
		"middle jitter":  {0.5, []int64{750, 3750, 22500}},
		"highest jitter": {0.999999, []int64{999, 4999, 29999}},
	} {
		t.Run(name, func(t *testing.T) {
			got := defaultPolicy().DrawDelaysMs(func() float64 { return tc.uniform })
			if len(got) != len(tc.want) {
				t.Fatalf("delays = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("delays = %v, want %v", got, tc.want)
					break
				}
			}
		})
	}
}

// TestRetryPolicyValidate pins the policy invariants the handler relies on.
func TestRetryPolicyValidate(t *testing.T) {
	if err := defaultPolicy().Validate(); err != nil {
		t.Errorf("default policy: %v", err)
	}
	if err := (RetryPolicy{MaxAttempts: 1}).Validate(); err != nil {
		t.Errorf("single-attempt policy: %v", err)
	}
	for name, mutate := range map[string]func(*RetryPolicy){
		"zero attempts":   func(p *RetryPolicy) { p.MaxAttempts = 0 },
		"delay count":     func(p *RetryPolicy) { p.Delays = p.Delays[:2] },
		"zero delay":      func(p *RetryPolicy) { p.Delays[1] = 0 },
		"negative jitter": func(p *RetryPolicy) { p.JitterMin = -0.1 },
		"inverted jitter": func(p *RetryPolicy) { p.JitterMin, p.JitterMax = 0.9, 0.5 },
		"jitter above 1":  func(p *RetryPolicy) { p.JitterMax = 1.5 },
	} {
		p := defaultPolicy()
		mutate(&p)
		if err := p.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestRetryPolicyDelayIsAtLeastOneMillisecond keeps retry_at_ms strictly
// after the failure even for a tiny configured delay.
func TestRetryPolicyDelayIsAtLeastOneMillisecond(t *testing.T) {
	p := RetryPolicy{MaxAttempts: 2, Delays: []time.Duration{time.Microsecond}, JitterMin: 0, JitterMax: 0}
	if got := p.DrawDelaysMs(func() float64 { return 0 }); len(got) != 1 || got[0] != 1 {
		t.Errorf("delays = %v, want [1]", got)
	}
}

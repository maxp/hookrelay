package valkey

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ReadinessReport names the bounded checks behind /health/ready.
type ReadinessReport struct {
	Ping          bool
	Persistence   bool // production only: AOF everysec + noeviction
	ScriptsLoaded bool
	Structures    bool
}

// OK reports whether all applicable checks passed.
func (r ReadinessReport) OK() bool { return r.Ping && r.Persistence && r.ScriptsLoaded && r.Structures }

// ValidateReadiness runs the startup/recovery gate: connectivity, (in
// production) AOF `everysec` + `noeviction`, script loads, and basic
// known-structure validation for the keys this slice owns. Later milestones
// extend this same gate with their structures.
func (a *Adapter) ValidateReadiness(ctx context.Context, production bool) (ReadinessReport, error) {
	report := ReadinessReport{}

	if err := a.Ping(ctx); err != nil {
		return report, err
	}
	report.Ping = true

	if production {
		if err := a.checkPersistence(ctx); err != nil {
			return report, err
		}
	}
	report.Persistence = true

	if err := a.LoadScripts(ctx); err != nil {
		return report, err
	}
	report.ScriptsLoaded = true

	if err := a.checkStructures(ctx); err != nil {
		return report, err
	}
	report.Structures = true
	return report, nil
}

// checkPersistence verifies the production Valkey persistence contract.
func (a *Adapter) checkPersistence(ctx context.Context) error {
	cfg, err := a.configGet(ctx, "appendonly", "appendfsync", "maxmemory-policy")
	if err != nil {
		return err
	}
	if !strings.EqualFold(cfg["appendonly"], "yes") {
		return fmt.Errorf("valkey: production requires AOF (appendonly=%q)", cfg["appendonly"])
	}
	if !strings.EqualFold(cfg["appendfsync"], "everysec") {
		return fmt.Errorf("valkey: production requires appendfsync everysec (got %q)", cfg["appendfsync"])
	}
	if !strings.EqualFold(cfg["maxmemory-policy"], "noeviction") {
		return fmt.Errorf("valkey: production requires maxmemory-policy noeviction (got %q)", cfg["maxmemory-policy"])
	}
	return nil
}

func (a *Adapter) configGet(ctx context.Context, params ...string) (map[string]string, error) {
	args := append([]string{"CONFIG", "GET"}, params...)
	msg, err := a.client.Do(ctx, a.client.B().Arbitrary(args...).Build()).ToMessage()
	if err != nil {
		return nil, fmt.Errorf("valkey: config get: %w", err)
	}
	pairs, err := msg.AsStrMap()
	if err != nil {
		return nil, fmt.Errorf("valkey: config get shape: %w", err)
	}
	return pairs, nil
}

// checkStructures validates the expected types of the keys this slice owns.
// Missing keys are fine (fresh deployment); unexpected types prevent
// readiness.
func (a *Adapter) checkStructures(ctx context.Context) error {
	for key, allowed := range map[string][]string{
		auditKey:                    {"none", "stream"},
		"hr1:webhooks":              {"none", "zset"},
		"hr1:ready":                 {"none", "zset"},
		"hr1:leases":                {"none", "zset"},
		"hr1:blocked":               {"none", "zset"},
		"hr1:dedup_age":             {"none", "zset"},
		"hr1:ready_seq":             {"none", "string"},
		"hr1:stats:queued_messages": {"none", "string"},
	} {
		t, err := a.keyType(ctx, key)
		if err != nil {
			return err
		}
		ok := false
		for _, allowedType := range allowed {
			if t == allowedType {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("valkey: key %s has unexpected type %q", key, t)
		}
	}
	seq, err := a.client.Do(ctx, a.client.B().Get().Key("hr1:ready_seq").Build()).ToString()
	if err != nil && !isNil(err) {
		return fmt.Errorf("valkey: ready sequence: %w", err)
	}
	if err == nil {
		n, parseErr := strconv.ParseInt(seq, 10, 64)
		if parseErr != nil || n < 0 || n == math.MaxInt64 || strconv.FormatInt(n, 10) != seq {
			return fmt.Errorf("valkey: ready sequence has invalid or exhausted value")
		}
	}
	return nil
}

func (a *Adapter) keyType(ctx context.Context, key string) (string, error) {
	resp, err := a.client.Do(ctx, a.client.B().Type().Key(key).Build()).ToMessage()
	if err != nil {
		return "", fmt.Errorf("valkey: type %s: %w", key, err)
	}
	t, err := resp.ToString()
	if err != nil {
		return "", fmt.Errorf("valkey: type %s: %w", key, err)
	}
	return strings.ToLower(t), nil
}

package valkey

import (
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/valkey-io/valkey-go"
)

//go:embed scripts/*.lua
var scriptsFS embed.FS

func init() {
	registry = map[string]*Script{}
	register("endpoint_create_v1", 1, map[string]int{
		"created":            2, // created_ms, updated_ms
		"conflict":           0,
		"bot_endpoint_limit": 0,
		"wrong_type":         0,
	})
	register("accept_v2", 2, map[string]int{
		"accepted":           1, // accepted_ms
		"duplicate":          1, // original message_id
		"duplicate_conflict": 1, // original message_id
		"recipient_blocked":  0,
		"recipient_capacity": 0,
		"global_capacity":    0,
		"dedup_capacity":     0,
		"wrong_type":         0,
		"state_inconsistent": 0,
	})
	register("claim_v2", 2, map[string]int{
		// token, message_id, delivery_cycle, attempt, claimed_ms,
		// lease_expires_ms, message_json, blocked_detected
		"claimed":                8,
		"replay_active":          8,
		"replay_empty":           0,
		"claim_no_longer_active": 0,
		"operation_conflict":     0,
		"limit_exceeded":         0,
		"empty":                  1, // blocked_detected
		"wrong_type":             0,
	})
	register("ack_v3", 3, map[string]int{
		// message_id, acknowledged_ms, recipient_identity, delivery_cycle,
		// attempt, claimed_ms (the last four are empty/zero for a repeat)
		"acknowledged":         6,
		"already_acknowledged": 6,
		"already_nacked":       0,
		"not_found":            0,
		"stale":                0,
		"recipient_blocked":    0,
		"wrong_type":           0,
	})
	register("nack_v1", 1, map[string]int{
		// message_id, attempt, retry_at_ms, recipient_identity,
		// delivery_cycle, claimed_ms, completed_ms
		"retry_scheduled": 7,
		// result, message_id, attempt, retry_at_ms (recorded result)
		"already_nacked":       4,
		"already_acknowledged": 0,
		"not_found":            0,
		"stale":                0,
		"recipient_blocked":    0,
		"attempts_exhausted":   0,
		"wrong_type":           0,
	})
	register("activate_retry_v1", 1, map[string]int{
		"activated":         2, // message_id, attempt
		"not_due":           0,
		"recipient_blocked": 0,
		"wrong_type":        0,
	})
	register("reconcile_recipient_v1", 1, map[string]int{
		// repairs, queue_length, reason
		"consistent":      3,
		"repaired":        3,
		"blocked":         3,
		"already_blocked": 3,
		"drained":         3,
		"due_lease":       3,
		"unhandled":       3,
	})
	register("reconcile_dedup_v1", 1, map[string]int{
		"reconciled": 4, // expired_removed, restored, orphans_removed, skipped
		"wrong_type": 0,
	})
	register("reconcile_counter_v1", 1, map[string]int{
		"repaired":            0,
		"consistent":          0,
		"precondition_failed": 0,
		"wrong_type":          0,
	})
}

// Result is a typed script result: the bounded status code plus the
// documented positional fields for that status. The parser rejects unknown
// statuses and tuples whose field count differs from the registered arity.
type Result struct {
	Script string
	Status string
	Fields []valkey.ValkeyMessage
}

func parseResult(script string, arity map[string]int, msg valkey.ValkeyMessage) (*Result, error) {
	items, err := msg.ToArray()
	if err != nil {
		return nil, fmt.Errorf("%w: script %s: result is not an array: %w", errScriptResult, script, err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("%w: script %s: empty result array", errScriptResult, script)
	}
	status, err := items[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("%w: script %s: status is not a string: %w", errScriptResult, script, err)
	}
	want, known := arity[status]
	if !known {
		statuses := make([]string, 0, len(arity))
		for s := range arity {
			statuses = append(statuses, s)
		}
		sort.Strings(statuses)
		return nil, fmt.Errorf("%w: script %s: unknown status %q (expected one of: %s)", errScriptResult, script, status, strings.Join(statuses, ", "))
	}
	if got := len(items) - 1; got != want {
		return nil, fmt.Errorf("%w: script %s status %s: invalid shape: %d fields, want %d", errScriptResult, script, status, got, want)
	}
	return &Result{Script: script, Status: status, Fields: items[1:]}, nil
}

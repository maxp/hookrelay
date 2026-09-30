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
	register("accept_v3", 3, map[string]int{
		"accepted":           2, // accepted_ms, early_evicted
		"duplicate":          1, // original message_id
		"duplicate_conflict": 1, // original message_id
		"recipient_blocked":  0,
		"recipient_capacity": 0,
		"global_capacity":    0,
		"dedup_capacity":     0,
		"wrong_type":         0,
		"state_inconsistent": 0,
	})
	register("claim_v3", 3, map[string]int{
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
	register("ack_v4", 4, map[string]int{
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
	register("nack_v3", 3, map[string]int{
		// message_id, attempt, retry_at_ms, recipient_identity,
		// delivery_cycle, claimed_ms, completed_ms
		"retry_scheduled": 7,
		// message_id, delivery_cycle, dead_lettered_ms, recipient_identity,
		// attempt, claimed_ms
		"dead_lettered": 6,
		// result, message_id, then attempt, retry_at_ms (retry_scheduled)
		// or delivery_cycle, dead_lettered_ms (dead_lettered)
		"already_nacked":       4,
		"already_acknowledged": 0,
		"not_found":            0,
		"stale":                0,
		"recipient_blocked":    0,
		"wrong_type":           0,
	})
	register("inspect_block_v2", 2, map[string]int{
		// marker, detected_ms, reason_code, queue_length, head_message_id,
		// status, delivery_cycle, attempt, lease_expires_ms, retry_at_ms,
		// head_message_present, in_ready, in_leases, in_retries, in_blocked,
		// invariants
		"inspected": 16,
	})
	register("clear_block_v2", 2, map[string]int{
		"cleared":             1, // restored index: ready | leases | retries | none
		"not_found":           0,
		"precondition_failed": 0,
		"ambiguous":           1, // first violated invariant
		"wrong_type":          0,
	})
	register("extend_v1", 1, map[string]int{
		// message_id, lease_expires_ms, max_lease_expires_ms,
		// recipient_identity, delivery_cycle, attempt
		"extended": 6,
		// message_id, lease_expires_ms, max_lease_expires_ms (recorded)
		"replay":                         3,
		"operation_conflict":             0,
		"not_found":                      0,
		"stale":                          0,
		"recipient_blocked":              0,
		"maximum_lease_lifetime_reached": 0,
		"wrong_type":                     0,
	})
	register("activate_retry_v1", 1, map[string]int{
		"activated":         2, // message_id, attempt
		"not_due":           0,
		"recipient_blocked": 0,
		"wrong_type":        0,
	})
	register("expire_lease_v3", 3, map[string]int{
		// message_id, attempt, retry_at_ms, delivery_cycle, claimed_ms,
		// expired_ms, consumer_instance_id (empty when absent)
		"retry_scheduled": 7,
		// message_id, delivery_cycle, dead_lettered_ms, attempt, claimed_ms,
		// consumer_instance_id
		"dead_lettered":     6,
		"not_due":           0,
		"recipient_blocked": 0,
		"wrong_type":        0,
	})
	register("reconcile_recipient_v3", 3, map[string]int{
		// repairs, queue_length, reason
		"consistent":      3,
		"repaired":        3,
		"blocked":         3,
		"already_blocked": 3,
		"drained":         3,
		"due_lease":       3,
		"due_retry":       3,
		"unhandled":       3,
	})
	register("replay_dlq_v2", 2, map[string]int{
		// delivery_cycle, queue_position, replayed_ms,
		// deduplication_resolution, recipient_identity
		"replayed":               5,
		"not_found":              0,
		"message_missing":        0,
		"recipient_blocked":      0,
		"deduplication_conflict": 0,
		"wrong_type":             0,
	})
	register("delivery_state_v1", 1, map[string]int{
		"found":        3, // state, delivery_cycle, queue_position
		"not_found":    0,
		"inconsistent": 1, // reason
	})
	register("expire_dlq_v1", 1, map[string]int{
		// dead_lettered_ms, recipient_identity, dead_letter_reason, expired_ms
		"expired":    4,
		"not_due":    0,
		"stale":      0,
		"wrong_type": 0,
	})
	register("evict_dedup_v1", 1, map[string]int{
		// count, oldest_accepted_ms, live_records, stop
		"evicted":    4,
		"wrong_type": 0,
	})
	register("reconcile_dlq_v1", 1, map[string]int{
		// message id lists: orphans removed, restored, invalid, missing
		"reconciled": 4,
		"wrong_type": 0,
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

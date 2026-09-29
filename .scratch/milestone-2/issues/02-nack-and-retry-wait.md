# 02: Negative acknowledgement and retry wait

**What to build:** A consumer can `nack` a claimed message; the attempt is recorded in history and the head waits for its server-chosen retry delay. Repeats return the recorded result; ack/nack cross-conflicts are bounded.

**Blocked by:** 01.

**Status:** done

- [x] `nack_v1` per the spec (preconditions order, history entry `outcome=nack`, `retry_wait` state, `retries` membership, token `nacked` phase with recorded result, claim op `no_longer_active`); retry delay from `HOOKRELAY_RETRY_DELAYS` × jitter, `retry_at_ms` from Valkey `TIME`
- [x] `POST /v1/deliveries/nack` (`delivery_token`, optional bounded `reason_code`), 5 s deadline, `retry_scheduled` body, recorded result on repeat, `409 delivery_already_acknowledged`, `409 stale_delivery_token`, `404`, `409 recipient_blocked`
- [x] `ack` after `nack` → `409 delivery_already_nacked`
- [x] Attempt-history encoding and the 10-cycle archive summary (unit + storage tests)
- [x] `delivery_nacked` event; `delivery_attempts_total{outcome=nack}`, `delivery_attempt_duration_seconds`, `retries_waiting`
- [x] Storage-seam tests for every tuple, key, TTL, and refusal without mutation

## Comments

- 2026-09-29: Implemented. `nack_v1.lua` (preconditions in spec order; history entry with fixed field order, `reason_code` and a pattern-checked `consumer_instance_id` only when present; 10-cycle archive fold merged into one leading summary; malformed history refused before any write; `retry_wait` state with `attempt+1`; `leases` → `retries`; token `nacked` phase with the recorded result, TTL 1 h; claim op → `no_longer_active`). `ack_v3.lua` adds `already_nacked` (→ `409 delivery_already_nacked`) and returns `claimed_ms`; ticket 08's head-advance amendment is renumbered `ack_v4`. `delivery.RetryPolicy` draws one jittered delay per retryable attempt and the script picks the failed attempt's entry (spec implementation note; `expire_lease_v1` takes the same list). `POST /v1/deliveries/nack` with a 5 s deadline, `delivery_nacked` event, `delivery_attempts_total{outcome=nack}`, `delivery_attempt_duration_seconds` (nack and acknowledged, Valkey time claim → completion), `retries_waiting`; `hr1:retries` added to the readiness structure check.
- Temporary until ticket 05: failing the last attempt returns `attempts_exhausted` without mutation (HTTP `500 internal_error`).
- Known interim gap until tickets 03 and 12: nothing activates a retry yet (ticket 03), and `reconcile_recipient_v1` treats a `retry_wait` head as `head_state_missing`, so a restart during a retry wait blocks that Recipient and leaves its `retries` member beside the `blocked` one. Ticket 12's `reconcile_recipient_v2` must accept `retry_wait` and remove the `retries` member when blocking. Not deployable before the Milestone 2 chain completes.

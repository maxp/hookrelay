# 05: Dead-letter transition

**What to build:** Failing the fourth attempt (by nack or expiry) moves the message atomically to the global DLQ and exposes the Recipient's next message.

**Blocked by:** 04.

**Status:** done

- [x] Replace the temporary `attempts_exhausted` refusal in `nack_v1` and `expire_lease_v1` (tickets 02, 04) and the maintenance `deferred` result
- [x] Shared dead-letter fragment in `nack_v1` and `expire_lease_v1`: `hr1:dl` Hash with structured Recipient fields and `dedup_identity_digest` from `hr1:mi`, `dlq` membership, LPOP, counter DECR, next head ready or queue deleted, blob and history retained
- [x] `nack` response `dead_lettered`; reasons `nack_exhausted` / `expiry_exhausted`
- [x] `delivery_dead_lettered` event; `dead_letters_total{recipient_scope,reason}`, `dead_letter_messages`, `delivery_attempts_total{outcome=dead_lettered}`
- [x] Storage-seam tests for both reasons, every key, and next-head exposure

## Comments

- 2026-09-29: Implemented as `nack_v2` and `expire_lease_v2` (the v1 files keep their temporary `attempts_exhausted` refusal, unregistered; ticket 08's amendments become `nack_v3` / `expire_lease_v3`). The dead-letter fragment validates every fallible write first (no existing `hr1:dl`, Hash-or-absent `hr1:mi`, positive counter, incrementable ready sequence with a next head), then writes `hr1:dl` with the structured Recipient fields parsed from the identity and the `hr1:mi` digest (empty for M1-era messages), ZADDs `hr1:dlq`, pops the queue, decrements the counter, exposes the next head as a fresh ready head or deletes queue and state, and removes lease/retry/ready memberships; blob, history, and metadata are retained. `nack` answers `dead_lettered` (recorded for repeats); expiry records the `expired` token phase. `delivery_dead_lettered`, `dead_letters_total{recipient_scope,reason}`, `dead_letter_messages`, and `attempts{outcome=dead_lettered}` (the final attempt counts only as dead_lettered). The maintenance `deferred` result is gone; `hr1:dlq` joined the readiness structure check.
- DLQ reconciliation (`reconcile_dlq_v1`) remains ticket 12.
- Review follow-up: `delivery_dead_lettered` carries `duration_ms` (claim → dead-letter); dead-letter recording is a delivery-level `recordDeadLetter` over a `deadLetter` value (metrics stay counters only); nack results use `NackResult*` kind constants; full refusal coverage and a drained user-scope case on the expiry path.

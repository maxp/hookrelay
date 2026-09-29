# 11: Startup reconciliation and readiness gate

**What to build:** After a restart — including an unclean one — hookrelay validates every structure Milestone 1 implements, safely repairs derived indexes and counters, isolates ambiguous recipient state behind persistent block markers, refuses readiness on anything it cannot safely handle, and only then opens the public listener.

**Blocked by:** 08 (consumer claim — lease state, operation records, and tombstone active phases must exist to be reconciled).

**Status:** done

- [x] Bounded, batched scans (never a whole-keyspace `KEYS`) over recipient queues/states, block markers, and dedup records; a full pass completes in batches regardless of dataset size
- [x] Per recipient: queue is a List and state a Hash (`unsupported_key_type` → marker); non-empty queue has state (`head_state_missing` → marker); `state.head_message_id == LINDEX 0` (`queue_head_mismatch` → marker); queued message blobs exist within the per-recipient bound (`head_message_missing` → marker); markers created via `block_recipient_v1` (marker + blocked index + ready/leases removal, atomic and idempotent)
- [x] `status=leased`: `leases` membership repaired to match verified state; a due entry (score at or before authoritative Valkey time) triggers no mutation — critical metric + structured log + readiness held false for diagnosis (Milestone 2 replaces this hold with the real expiry transition)
- [x] `status=ready`: ready membership repaired (add missing, remove stale); leased and blocked recipients absent from the ready index (repaired)
- [x] Dedup: expired records removed together with their age-index members; live records missing from the age index restored from `accepted_ms`; `hr1:stats:queued_messages` recomputed from scanned queue lengths and repaired when different
- [x] Repairs touch only derived indexes/counters — authoritative state is never reconstructed or guessed; repair audit is best effort; ambiguity detection is metered and logged
- [x] Existing markers verified against the blocked index in both directions (index members without markers removed; markers without members restored)
- [x] Readiness: false until the gate completes; true with isolated blocked recipients (they stay blocked, everyone else serves); false on any unisolatable inconsistency or due-lease hold — never ready with a detected-but-unhandled inconsistency
- [x] Public listener opens only after the gate; admin listener exposes startup progress (`reconciliation_in_progress` metric, readiness body) throughout
- [x] Recovery after Valkey loss re-verifies connectivity, scripting, and (production) persistence settings and repeats lightweight reconciliation before readiness returns
- [x] Integration tests: safe repair of each derived structure, marker creation for each bounded reason code, refusal scenarios, and restart-with-persisted-volume smoke

## Comments

- 2026-09-29: Implemented. `valkey.Adapter.Reconcile` walks `hr1:r:*`, `hr1:q:*` (SCAN MATCH) and the `ready`/`leases`/`blocked` indexes (ZSCAN) in pages of 100, runs `reconcile_recipient_v1` once per distinct Recipient, reconciles `hr1:d:*` and `dedup_age` members in batches through `reconcile_dedup_v1`, and on the full (startup) pass repairs `hr1:stats:queued_messages` with `reconcile_counter_v1` compare-and-set — skipped when a queue is uncountable (wrong type) or anything is unhandled. Repairs and new markers get best-effort audit entries (`actor=reconciliation`). The app runs a pass whenever the gate succeeds while not ready (full until the public listener has served once, lightweight on recovery), before opening the public listener; a failed pass or a hold (`due_lease`, `unhandled_inconsistency`) keeps readiness false with the bounded state (`pending`/`in_progress`/`held`/`failed`/`complete`) in the readiness body; `hookrelay_reconciliation_in_progress` and `hookrelay_reconciliation_findings_total{kind}` (bounded kinds incl. `blocked_<reason>`) are exported; completion, holds, and failures are logged.
- Script consolidation recorded in the spec (see the implementation note under the Lua contracts).
- Restart with the persisted volume: covered by the consistent-state no-op test and live-verified (restart after corrupting the ready index, the counter, and one head state → ready with that Recipient blocked; a due lease → held, public listener never opened). A real container-volume restart belongs to ticket 12's Compose smoke.
- While held, the pass repeats on the one-second gate cadence and logs the hold each time; acceptable for an exceptional diagnostic state.


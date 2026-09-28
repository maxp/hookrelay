# 11: Startup reconciliation and readiness gate

**What to build:** After a restart — including an unclean one — hookrelay validates every structure Milestone 1 implements, safely repairs derived indexes and counters, isolates ambiguous recipient state behind persistent block markers, refuses readiness on anything it cannot safely handle, and only then opens the public listener.

**Blocked by:** 08 (consumer claim — lease state, operation records, and tombstone active phases must exist to be reconciled).

**Status:** ready-for-agent

- [ ] Bounded, batched scans (never a whole-keyspace `KEYS`) over recipient queues/states, block markers, and dedup records; a full pass completes in batches regardless of dataset size
- [ ] Per recipient: queue is a List and state a Hash (`unsupported_key_type` → marker); non-empty queue has state (`head_state_missing` → marker); `state.head_message_id == LINDEX 0` (`queue_head_mismatch` → marker); queued message blobs exist within the per-recipient bound (`head_message_missing` → marker); markers created via `block_recipient_v1` (marker + blocked index + ready/leases removal, atomic and idempotent)
- [ ] `status=leased`: `leases` membership repaired to match verified state; a due entry (score at or before authoritative Valkey time) triggers no mutation — critical metric + structured log + readiness held false for diagnosis (Milestone 2 replaces this hold with the real expiry transition)
- [ ] `status=ready`: ready membership repaired (add missing, remove stale); leased and blocked recipients absent from the ready index (repaired)
- [ ] Dedup: expired records removed together with their age-index members; live records missing from the age index restored from `accepted_ms`; `hr1:stats:queued_messages` recomputed from scanned queue lengths and repaired when different
- [ ] Repairs touch only derived indexes/counters — authoritative state is never reconstructed or guessed; repair audit is best effort; ambiguity detection is metered and logged
- [ ] Existing markers verified against the blocked index in both directions (index members without markers removed; markers without members restored)
- [ ] Readiness: false until the gate completes; true with isolated blocked recipients (they stay blocked, everyone else serves); false on any unisolatable inconsistency or due-lease hold — never ready with a detected-but-unhandled inconsistency
- [ ] Public listener opens only after the gate; admin listener exposes startup progress (`reconciliation_in_progress` metric, readiness body) throughout
- [ ] Recovery after Valkey loss re-verifies connectivity, scripting, and (production) persistence settings and repeats lightweight reconciliation before readiness returns
- [ ] Integration tests: safe repair of each derived structure, marker creation for each bounded reason code, refusal scenarios, and restart-with-persisted-volume smoke

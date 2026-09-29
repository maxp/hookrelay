# 01: Message index and v2 transitions

**What to build:** The storage groundwork every failure transition needs: `hr1:mi:<message_id>` written by acceptance and deleted with the blob, `attempt_started_ms` in leased state, and `retry_wait` recognized as a stale ready entry — shipped as `accept_v2`, `ack_v2`, `claim_v2` with the v1 contracts kept stable.

**Blocked by:** none (Milestone 1 complete).

**Status:** done

- [x] `accept_v2`: `accept_v1` + `HSET hr1:mi:<message_id> dedup_identity_digest` in the same writes; registry switches to v2; storage-seam tests for every tuple and the new key
- [x] `ack_v2`: `ack_v1` + `hr1:mi` deletion; every other ack behavior unchanged
- [x] `claim_v2`: writes `attempt_started_ms`; a `retry_wait` head found in `ready` is dropped as a stale derived entry (like `leased`)
- [x] Reconciliation and readiness accept both v1-era (no `hr1:mi`) and v2 data; no inconsistency for a missing `hr1:mi`
- [x] Spec notes updated if any contract detail changes during implementation

## Comments

- 2026-09-29: Implemented. `accept_v2.lua` declares the metadata key as `KEYS[12]` (type-checked; an existing metadata key for the candidate `message_id` is a caller-bug error reply like an existing blob) and writes `hr1:mi:<message_id> dedup_identity_digest` with the blob. `ack_v2.lua` resolves the metadata key in-script like the blob, type-checks it before any write, and deletes it with the blob; an M1-era message without metadata acknowledges unchanged. `claim_v2.lua` writes `attempt_started_ms = claimed_ms` and drops a `retry_wait` head found in `ready` as a stale derived entry. The registry, metrics operation map, and stores use the v2 names; the v1 files stay embedded. Reconciliation and readiness needed no change: neither reads `hr1:mi` or `attempt_started_ms`; `TestReconcileAcceptsMixedV1AndV2Data` pins a mixed v1/v2 state as finding-free, unmutated, and ready. Spec notes record the `accept_v2` `KEYS[12]` and `ack_v2` in-script resolution details.
- Review notes for later tickets: reconciliation does not type-check `hr1:mi`, so a wrong-typed metadata key on a head passes readiness while every `ack_v2` returns `wrong_type` (ticket 12 should consider it); `extend_v1` (ticket 06) must define behavior for a v1-era lease without `attempt_started_ms`; a still-running `ack_v1` process during an upgrade would orphan `hr1:mi` (single-process deployment makes this moot today).

# 01: Message index and v2 transitions

**What to build:** The storage groundwork every failure transition needs: `hr1:mi:<message_id>` written by acceptance and deleted with the blob, `attempt_started_ms` in leased state, and `retry_wait` recognized as a stale ready entry — shipped as `accept_v2`, `ack_v2`, `claim_v2` with the v1 contracts kept stable.

**Blocked by:** none (Milestone 1 complete).

**Status:** ready-for-agent

- [ ] `accept_v2`: `accept_v1` + `HSET hr1:mi:<message_id> dedup_identity_digest` in the same writes; registry switches to v2; storage-seam tests for every tuple and the new key
- [ ] `ack_v2`: `ack_v1` + `hr1:mi` deletion; every other ack behavior unchanged
- [ ] `claim_v2`: writes `attempt_started_ms`; a `retry_wait` head found in `ready` is dropped as a stale derived entry (like `leased`)
- [ ] Reconciliation and readiness accept both v1-era (no `hr1:mi`) and v2 data; no inconsistency for a missing `hr1:mi`
- [ ] Spec notes updated if any contract detail changes during implementation

# 02: Complete storage reconciliation audit

**What to build:** Publish the complete first-version key-family and transition-boundary reconciliation matrix, and amend the Milestone 5 spec with every evidence-backed missing contract before further code.

**Blocked by:** none.

**Status:** done

- [x] Inventory every accepted `hr1:` key family from `docs/design/storage.md` and every registered current transition
- [x] Classify each family as authoritative, derived, disposable, idempotency-only, or audit-only
- [x] Record creators/mutators/deleters, validation coverage, safe repair/removal/isolation/hold policy, and existing tests
- [x] Audit cross-key invariants at accept, claim, ack, nack, expiry, retry activation, replay, DLQ deletion/retention, endpoint mutations, Admin Secret rotation, and session operations
- [x] Identify detectable states left by uncertain or runtime-error Lua outcomes without assuming rollback
- [x] Add exact `KEYS`/`ARGV`/status/precondition contracts to `.scratch/milestone-5/spec.md` for every missing hardening operation selected for tickets 03–04
- [x] Do not implement speculative repairs; document rejected repairs and why authoritative data cannot be reconstructed safely

## Comments

- 2026-09-30: Published `storage-reconciliation-matrix.md`, covering all 26 accepted key families and all 31 registered transitions. The audit records authority, lifecycle ownership, current validation/repair behavior, detectable no-rollback residue, and rejected destructive/speculative repairs. It selected two bounded additions and fixed their exact contracts in the Milestone 5 spec: `reconcile_attempt_v1` for leased-head token/claim-operation cross-links (ticket 03), and `reconcile_message_v1` for semantic Canonical Message and lifecycle validation (ticket 04). Tickets 03 and 04 are now `ready-for-agent`; no code was added by this audit ticket.

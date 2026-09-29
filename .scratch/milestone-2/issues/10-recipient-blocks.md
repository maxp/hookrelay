# 10: Recipient block list, inspect, and clear

**What to build:** The operations the recovery runbook requires: list Recipients by state (including blocked), inspect a block without mutation, and clear it with exact preconditions after invariants re-verify, audited atomically.

**Blocked by:** 03 (retry state must exist for invariant checks).

**Status:** ready-for-agent

- [ ] `GET /admin/v1/recipient-states?status=…` over `ready`/`leases`/`retries`/`blocked` with structured Recipient fields and cursors
- [ ] `POST /admin/v1/recipient-blocks/inspect` (read-only Go operation per the spec)
- [ ] `clear_block_v1` and `POST /admin/v1/recipient-blocks/clear` (`recipient_block_not_found`, `recipient_state_ambiguous`, `precondition_failed` mapping)
- [ ] CLI `admin recipients list|inspect-block|clear-block`; runbook availability note removed

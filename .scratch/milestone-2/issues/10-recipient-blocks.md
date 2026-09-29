# 10: Recipient block list, inspect, and clear

**What to build:** The operations the recovery runbook requires: list Recipients by state (including blocked), inspect a block without mutation, and clear it with exact preconditions after invariants re-verify, audited atomically.

**Blocked by:** 03 (retry state must exist for invariant checks).

**Status:** done

- [x] `GET /admin/v1/recipient-states?status=ready|leased|retry_wait|blocked` over the `ready`/`leases`/`retries`/`blocked` indexes with structured Recipient fields and cursors
- [x] `POST /admin/v1/recipient-blocks/inspect` (read-only Go operation per the spec)
- [x] `clear_block_v1` and `POST /admin/v1/recipient-blocks/clear` (`recipient_block_not_found`, `recipient_state_ambiguous`, `precondition_failed` mapping)
- [x] CLI `admin recipients list|inspect-block|clear-block`; runbook availability note removed

## Comments

- 2026-09-29: Implemented. `valkey.NewRecipientStore` (cursor-paged index listing with blocked reasons; read-only `InspectBlock` computing the `reconcile_recipient_v2` invariants; `clear_block_v1.lua` with exact marker preconditions, invariant re-verification, restoration of exactly the implied index, and the atomic audit append). Admin routes `GET /admin/v1/recipient-states`, `POST /admin/v1/recipient-blocks/inspect|clear` with structured Recipient fields; CLI `admin recipients list|inspect-block|clear-block` (`--yes` required; uncertain clears are reconciled by one inspection, never retried); runbook availability note removed.
- Decision recorded during implementation: the clear's precondition errors use the Admin API's existing `428 precondition_required` / `412 precondition_failed` instead of the `409` proposed at the start of the ticket.
- The end-to-end test runs the real Admin handler, service, and store against Valkey inside the `valkey` package (the only test package that can create an inconsistent state); the CLI is covered against a fake Admin server.
- Review follow-up: inspection became the read-only `inspect_block_v1` script so inspect and clear share one rule set, order, and parser (they had drifted); marker fields are validated; the blocked list reads `detected_ms` from the marker and flags `marker_missing`. Known and kept: status/deadline agreement checks the fields required by the current status (as `reconcile_recipient_v2` does), not leftover fields of other statuses; a 503 from a clear that never dispatched is still reported by the CLI as uncertain (safe); the audit target is the internal Recipient identity, as in the endpoint precedent.

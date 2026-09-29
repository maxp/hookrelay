# 12: Reconciliation expansion

**What to build:** Startup and recovery reconciliation validate `retry_wait`, `retries`, and the DLQ, and execute overdue expiries and due retry activations before readiness instead of holding readiness on a due lease.

**Blocked by:** 04, 05.

**Status:** done

- [x] `reconcile_recipient_v2` (retry_wait valid; due lease/retry reported without mutation; blocking also removes the `retries` member — `reconcile_recipient_v1` blocks a `retry_wait` head and leaves it, see ticket 02) and `reconcile_dlq_v1` (missing DLQ blob fails readiness, never a clearable Recipient block)
- [x] A `retry_wait` head whose `retries` member was removed (e.g. `activate_retry_v1` on a blocked Recipient, later cleared) gets the member restored to `retry_at_ms`
- [x] Go reconciler runs the registered `expire_lease` (`expire_lease_v2`) / `activate_retry_v1` for due entries; the M1 due-lease hold is removed
- [x] Consistency-issue kinds/resolutions extended and documented; tests for each repair and refusal, including missing DLQ blob with empty active queue (ticket 08 adds checks for pending replay state)

## Comments

- 2026-09-29: Implemented. `reconcile_recipient_v2` (retry_wait valid with `retries` repaired to `retry_at_ms`; `due_lease`/`due_retry` reported without mutation; blocking, block alignment, and draining remove the `retries` member; malformed `retry_at_ms` → `head_state_missing`). `reconcile_dlq_v1` (orphan members removed, missing/wrong members restored at `dead_lettered_ms`, invalid records and missing blobs held — a missing blob never creates a block marker, even with an empty active queue). `Adapter.ReconcileAndProcessDue` runs pass → `delivery.Maintenance.ProcessDue` → verifying pass, so overdue leases expire and due retries activate before readiness with their normal events and metrics; the M1 due-lease hold is gone. Consistency-issue kinds extended (see the spec note); missing DLQ blobs are logged per `message_id`.
- Closes the interim gaps recorded in tickets 02–04 (restart during `retry_wait`, the due-lease readiness hold, a `retry_wait` head whose `retries` member was removed).
- Review follow-up: the due path repairs its derived locator so a failed startup transition stays visible to maintenance; the merged report counts a first-pass block once; `reconcile_dlq_v1` validates every required `hr1:dl` field, returns id lists, and invalid/missing ids are counted once across batches; DLQ repairs and ambiguities are audited and the missing-message log moved from `cli` into the reconciler with a bounded sample; `Maintenance.processOne` shared by rounds and `ProcessDue`. Left as judgement calls: the pass → process → pass orchestration stays on the adapter (testable against real Valkey with a real `delivery.Maintenance`), and findings stay a string-keyed map.

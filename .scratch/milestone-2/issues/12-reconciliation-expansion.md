# 12: Reconciliation expansion

**What to build:** Startup and recovery reconciliation validate `retry_wait`, `retries`, and the DLQ, and execute overdue expiries and due retry activations before readiness instead of holding readiness on a due lease.

**Blocked by:** 04, 05.

**Status:** ready-for-agent

- [ ] `reconcile_recipient_v2` (retry_wait valid; due lease/retry reported without mutation; blocking also removes the `retries` member — `reconcile_recipient_v1` blocks a `retry_wait` head and leaves it, see ticket 02) and `reconcile_dlq_v1` (missing DLQ blob fails readiness, never a clearable Recipient block)
- [ ] A `retry_wait` head whose `retries` member was removed (e.g. `activate_retry_v1` on a blocked Recipient, later cleared) gets the member restored to `retry_at_ms`
- [ ] Go reconciler runs `expire_lease_v1` / `activate_retry_v1` for due entries; the M1 due-lease hold is removed
- [ ] Consistency-issue kinds/resolutions extended and documented; tests for each repair and refusal, including missing DLQ blob with empty active queue (ticket 08 adds checks for pending replay state)

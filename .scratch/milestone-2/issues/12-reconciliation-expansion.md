# 12: Reconciliation expansion

**What to build:** Startup and recovery reconciliation validate `retry_wait`, `retries`, and the DLQ, and execute overdue expiries and due retry activations before readiness instead of holding readiness on a due lease.

**Blocked by:** 04, 05.

**Status:** ready-for-agent

- [ ] `reconcile_recipient_v2` (retry_wait valid; due lease/retry reported without mutation) and `reconcile_dlq_v1`
- [ ] Go reconciler runs `expire_lease_v1` / `activate_retry_v1` for due entries; the M1 due-lease hold is removed
- [ ] Consistency-issue kinds/resolutions extended and documented; tests for each repair and refusal

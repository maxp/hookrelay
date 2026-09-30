# 08: Session expiry and reconciliation

**What to build:** `expire_sessions_v1` in the maintenance loop, `reconcile_session_v1` at startup, and the `hookrelay_admin_sessions` gauge.

**Blocked by:** 06.

**Status:** done

- [x] Maintenance removes due sessions (100 per cycle) with per-session expiry audit
- [x] Startup reconciliation after the generation check: orphan index members removed, orphan/malformed/stale Hashes deleted, missing or wrong scores restored; wrong-type index fails the pass
- [x] Tests: storage seam, maintenance, composed restart integration

## Comments

- 2026-09-30: Implemented. `expire_sessions_v1` (due members oldest first up to the limit, returns removed and remaining counts) runs as a maintenance round hook through `Service.MaintainSessions`, which appends one best-effort `admin_session_expired` event per removed session and sets `hookrelay_admin_sessions`. `reconcile_session_v1` runs per digest found by `ZSCAN hr1:admin_sessions` or `SCAN hr1:admin_session:*` right after the generation check: orphan member removed, malformed/expired/stale-generation sessions deleted, valid unindexed or mis-scored sessions restored from the Hash; each repair gets a best-effort `session_index_repaired` audit and a finding (`session_orphans_removed`, `sessions_removed`, `session_index_restored`, also mapped onto `hookrelay_consistency_issues_total`). A wrong-typed index or session key, or a non-digest session key, fails the pass (spec updated). Tests: storage seam for expiry (order, limit, remaining count, wrong type, arguments), reconciliation of every case plus an idempotent second pass and the failing structures, and the service hook (audit per session, gauge, error propagation).

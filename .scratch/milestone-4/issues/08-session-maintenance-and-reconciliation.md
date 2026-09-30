# 08: Session expiry and reconciliation

**What to build:** `expire_sessions_v1` in the maintenance loop, `reconcile_session_v1` at startup, and the `hookrelay_admin_sessions` gauge.

**Blocked by:** 06.

**Status:** ready-for-agent

- [ ] Maintenance removes due sessions (100 per cycle) with per-session expiry audit
- [ ] Startup reconciliation after the generation check: orphan index members removed, orphan/malformed/stale Hashes deleted, missing or wrong scores restored; wrong-type index fails the pass
- [ ] Tests: storage seam, maintenance, composed restart integration

## Comments

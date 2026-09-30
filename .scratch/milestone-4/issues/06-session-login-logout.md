# 06: Session login, read, and logout

**What to build:** `POST|GET|DELETE /admin/v1/session` through `session_create_v1`, `session_authenticate_v1`, `session_delete_v1`, with login rate limits and capacity.

**Blocked by:** 05.

**Status:** done

- [x] Scripts per the spec, including in-script expired-session cleanup, throttled refresh, generation check
- [x] Login: exact `Origin`, per-IP and global token buckets, 16 KiB strict body, constant-time comparison, cookie attributes (`Secure` per configuration), `429 session_capacity_exceeded`, no cookie unless `created`
- [x] `GET` returns expiries and the CSRF token; `DELETE` requires CSRF/Origin for a valid session, `204` otherwise, `503` when revocation is unconfirmed
- [x] Metric `hookrelay_admin_login_attempts_total{outcome}`; best-effort failure audit
- [x] Tests: storage seam (capacity after cleanup, refresh throttle, absolute cap, generation mismatch), HTTP seam (every status, cookie attributes in both modes), rate limits

## Comments

- 2026-09-30: Implemented. `session_create_v1`, `session_authenticate_v1`, `session_delete_v1` per the spec with one change (spec updated): Lua cannot mint UUIDv7 event IDs, so scripts that remove expired sessions return the count and the service appends one best-effort `admin_session_expired` event each (expiry audit is best effort by design). `session_authenticate_v1` refuses a non-Hash session key as `wrong_type` rather than deleting it. The token bucket moved to `internal/ratelimit` (separate commit) and serves both ingestion and login (`DefaultLoginLimits`; per source address from `RemoteAddr`, never forwarded headers). `AuditSink` became one `AppendBestEffort(AuditEvent)` method used by rejected-auth, failed-login, and expiry events; `logAuditAs` carries the actor. Server wiring passes `Sessions`, the lowercase `scheme://host` of `HOOKRELAY_ADMIN_ORIGIN`, and `HOOKRELAY_ADMIN_COOKIE_SECURE`. Metrics `hookrelay_admin_login_attempts_total{outcome}` (`success`, `failure`, `rate_limited`, `capacity_exceeded`, `unavailable`) and `hookrelay_admin_csrf_rejections_total{reason}` (used by logout now, by ticket 07 next). Tests: storage seam (create keys and audit, collision, capacity without eviction and after expiry cleanup, validation with throttled refresh and absolute cap, every invalid reason with cleanup, wrong types, logout with and without audit, arguments, SCRIPT FLUSH), HTTP seam (cookie attributes in both modes, digest-only storage, every login refusal without a cookie, rate limits with Retry-After, session read, logout CSRF/Origin matrix).

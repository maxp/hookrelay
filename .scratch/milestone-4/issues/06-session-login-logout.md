# 06: Session login, read, and logout

**What to build:** `POST|GET|DELETE /admin/v1/session` through `session_create_v1`, `session_authenticate_v1`, `session_delete_v1`, with login rate limits and capacity.

**Blocked by:** 05.

**Status:** ready-for-agent

- [ ] Scripts per the spec, including in-script expired-session cleanup, throttled refresh, generation check
- [ ] Login: exact `Origin`, per-IP and global token buckets, 16 KiB strict body, constant-time comparison, cookie attributes (`Secure` per configuration), `429 session_capacity_exceeded`, no cookie unless `created`
- [ ] `GET` returns expiries and the CSRF token; `DELETE` requires CSRF/Origin for a valid session, `204` otherwise, `503` when revocation is unconfirmed
- [ ] Metric `hookrelay_admin_login_attempts_total{outcome}`; best-effort failure audit
- [ ] Tests: storage seam (capacity after cleanup, refresh throttle, absolute cap, generation mismatch), HTTP seam (every status, cookie attributes in both modes), rate limits

## Comments

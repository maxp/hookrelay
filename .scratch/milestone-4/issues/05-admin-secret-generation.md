# 05: Admin Secret generation tracking

**What to build:** `hr1:admin_auth` initialization and rotation at startup through `admin_auth_v1`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] `admin_auth_v1` per the spec (`initialized`, `exists`, `current`, `rotated`, `changed`, `absent`, `too_many_sessions`, `wrong_type`) with mandatory audit
- [ ] Tag derivation (HMAC-SHA-256 over decoded salt ‖ context) in Go; salt, tag, and secret never logged
- [ ] First step of every reconciliation pass; failures withhold readiness with `admin_auth_inconsistent`
- [ ] Tests: storage seam, reconciliation (fresh store initializes once; same secret is a no-op; changed secret rotates and revokes indexed sessions; malformed record withholds readiness)

## Comments

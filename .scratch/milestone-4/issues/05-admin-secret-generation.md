# 05: Admin Secret generation tracking

**What to build:** `hr1:admin_auth` initialization and rotation at startup through `admin_auth_v1`.

**Blocked by:** none.

**Status:** done

- [x] `admin_auth_v1` per the spec (`initialized`, `exists`, `current`, `rotated`, `changed`, `absent`, `too_many_sessions`, `wrong_type`) with mandatory audit
- [x] Tag derivation (HMAC-SHA-256 over decoded salt ‖ context) in Go; salt, tag, and secret never logged
- [x] First step of every reconciliation pass; failures withhold readiness with `admin_auth_inconsistent`
- [x] Tests: storage seam, reconciliation (fresh store initializes once; same secret is a no-op; changed secret rotates and revokes indexed sessions; malformed record withholds readiness)

## Comments

- 2026-09-30: Implemented. `admin_auth_v1.lua` per the spec (compare-and-set on the stored `generation_id` and salt; rotation deletes every indexed session Hash and the index, keeps the salt, audits `admin_secret_generation_changed` with the bucketed count). `Adapter.EnsureAdminAuth` derives the tag in Go (HMAC-SHA-256 keyed by the secret over the decoded 32-byte salt ‖ context), rereads once on `exists`/`changed`/`absent`, and treats a malformed record, `wrong_type`, `too_many_sessions`, or a second race as `errAdminAuthInconsistent`. `ReconcileOptions.AdminSecret` runs it first in every pass (the server wiring sets it); failures log `admin_auth_inconsistent` with a bounded reason and fail the pass; success adds the `admin_auth_initialized`/`admin_auth_rotated` findings and a warning event. Tests: storage seam (initialize/current, rotation revoking indexed sessions only, every refusal snapshot-equal, script compare-and-set and argument rejection, SCRIPT FLUSH) and the reconciliation hook.

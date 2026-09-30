# 07: Cookie authentication and CSRF on operational routes

**What to build:** The Bearer-or-session middleware for the operational routes, CSRF and `Origin` enforcement, actor propagation, and `replay_dlq_v3`.

**Blocked by:** 01, 02, 06.

**Status:** done

- [x] Route classes per the spec; `Authorization` present → Bearer only; cookie ignored on Bearer-only routes
- [x] Unsafe methods require exact `Origin` and constant-time `X-CSRF-Token`; `403 forbidden`; `hookrelay_admin_csrf_rejections_total{reason}`
- [x] Invalid session → `401` + clearing cookie + best-effort audit
- [x] `replay_dlq_v3` (v2 plus `actor`), payload and delete pass the context actor; `Cache-Control: no-store` on `/admin/v1/`
- [x] Tests: HTTP seam matrix (method × auth kind × Origin × CSRF), audit actor per auth kind, integration

## Comments

- 2026-09-30: Implemented. `authAdmin` wraps the operational routes (summary, recipient states, delivery state, dead letters incl. payload/replay/delete, audit); `auth` stays Bearer-only for endpoint, bot, and block routes and ignores the cookie. Both share `startRequest` (request ID, `Cache-Control: no-store`, default actor) and `bearerAllowed`. With an `Authorization` header only Bearer is evaluated. Invalid session → `401`, clearing cookie, best-effort `admin_auth_rejected` with `reason=session_<reason>`; unverifiable → `503`; unsafe methods → exact Origin then constant-time CSRF (`403 forbidden`, `hookrelay_admin_csrf_rejections_total{reason}`). `replay_dlq_v3` (v2 + `actor` ARGV) replaces v2 in the registry; payload/replay/delete stdout audit copies use the context actor. New scripts got Valkey operation labels, guarded by `TestEveryScriptHasAnOperationLabel`. Tests: middleware matrix (read with cookie, Origin/CSRF refusals, actor per auth kind, no Bearer→cookie fallback, cookie ignored on Bearer-only routes, invalid/malformed/unverifiable sessions) and a composed test over real Valkey (login → session read → cookie audit read → CSRF-guarded delete → logout → revocation by a changed secret at reconciliation).

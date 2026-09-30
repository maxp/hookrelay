# 07: Cookie authentication and CSRF on operational routes

**What to build:** The Bearer-or-session middleware for the operational routes, CSRF and `Origin` enforcement, actor propagation, and `replay_dlq_v3`.

**Blocked by:** 01, 02, 06.

**Status:** ready-for-agent

- [ ] Route classes per the spec; `Authorization` present → Bearer only; cookie ignored on Bearer-only routes
- [ ] Unsafe methods require exact `Origin` and constant-time `X-CSRF-Token`; `403 forbidden`; `hookrelay_admin_csrf_rejections_total{reason}`
- [ ] Invalid session → `401` + clearing cookie + best-effort audit
- [ ] `replay_dlq_v3` (v2 plus `actor`), payload and delete pass the context actor; `Cache-Control: no-store` on `/admin/v1/`
- [ ] Tests: HTTP seam matrix (method × auth kind × Origin × CSRF), audit actor per auth kind, integration

## Comments

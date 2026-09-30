# 09: Operational UI shell, login, and overview

**What to build:** ADR 0009's embedded UI: asset serving and headers, login/logout, the overview view.

**Blocked by:** 04, 07.

**Status:** ready-for-agent

- [ ] `internal/administration/ui/` embedded; `GET /ui/…`, `GET /` → `/ui/`; CSP and security headers per the spec
- [ ] Login and logout through the session API; `401` returns to login; CSRF token from `GET /admin/v1/session`
- [ ] Overview from `operations/summary` polled every 5 s, paused while hidden; Grafana link when configured
- [ ] Go tests: headers, no inline script/style/handlers, referenced assets exist, no `innerHTML`

## Comments

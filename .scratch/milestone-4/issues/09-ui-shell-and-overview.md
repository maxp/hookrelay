# 09: Operational UI shell, login, and overview

**What to build:** ADR 0009's embedded UI: asset serving and headers, login/logout, the overview view.

**Blocked by:** 04, 07.

**Status:** done

- [x] `internal/administration/ui/` embedded; `GET /ui/…`, `GET /` → `/ui/`; CSP and security headers per the spec
- [x] Login and logout through the session API; `401` returns to login; CSRF token from `GET /admin/v1/session`
- [x] Overview from `operations/summary` polled every 5 s, paused while hidden; Grafana link when configured
- [x] Go tests: headers, no inline script/style/handlers, referenced assets exist, no `innerHTML`

## Comments

- 2026-09-30: Implemented. `internal/administration/ui/` (`index.html`, `app.css`, `app.js`, `api.js`, `dom.js`, `views/overview.js`) embedded by `ui.go` and served at `GET /ui/…` (only when sessions are wired; `GET /` redirects there, directories and unknown files are 404) with the spec's CSP plus `nosniff`, `no-referrer`, `X-Frame-Options: DENY`, `COOP same-origin`, `Cache-Control: no-cache`. The shell bootstraps from `GET /admin/v1/session` (CSRF token kept in memory only), shows the login form on `401`, clears the secret field on submit, maps login error codes to operator messages, and logs out through `DELETE /admin/v1/session` (reporting an unconfirmed revocation). Hash routing; the overview polls `operations/summary` every 5 s and pauses while the tab is hidden; the Grafana link appears only for an http(s) URL. `dom.js` builds everything with `createElement`/`textContent`. Go tests: serving and headers, static rules (no inline script/style/handler markup, every `src`/`href` and module import embedded, no HTML-string sinks or external fetches) with a self-test proving the patterns bite. `node --check` passes for every module (dev check only; no Node dependency). Browser walkthrough is recorded with ticket 10.

# 11: Milestone 4 smoke and documentation

**What to build:** Extend the Compose smoke to Milestone 4 and close the milestone.

**Blocked by:** 01–10.

**Status:** done

- [x] Smoke: cookie-jar login, session read, cookie replay refused without CSRF and accepted with it, payload view visible in `audit list`, delete with `If-Match`, summary counts, logout, UI served with its CSP
- [x] README, `admin-api.md`, `configuration.md`, `storage.md`, `implementation-milestones.md` updated; spec status `done`

## Comments

- 2026-09-30: Implemented. The smoke adds two dead letters (the payload text is `<img src=x onerror=alert(1)>`), `admin operations summary`, a curl login with the production cookie attributes asserted (the Secure cookie is passed explicitly because curl will not send it over plain HTTP), a cookie replay refused without CSRF (`403`) and accepted with it, the audited payload, deletion refused without `If-Match` (`428`) and accepted with the read `ETag`, the summary via the cookie, `admin audit list` showing the four session actions with `actor=admin_session`, logout and `401` afterwards, `/ui/` with its CSP, `/` → `/ui/`, and the new metrics. Two green runs. README (status, Admin API, CLI, new Operational UI section, smoke description, spec pointer), `admin-api.md` (completed DLQ/summary/audit contracts, route authentication classes), `configuration.md` (Origin comparison, `HOOKRELAY_UI_GRAFANA_URL`), `storage.md` (session contract details, deletion precondition), `platform.md` (metric inventory), `implementation-milestones.md`, and `open-questions.md` (resolved items) updated. The payload metric was renamed to the already-accepted `hookrelay_dlq_payload_inspections_total`.

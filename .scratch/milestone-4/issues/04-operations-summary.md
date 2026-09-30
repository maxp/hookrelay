# 04: Operations summary

**What to build:** `GET /admin/v1/operations/summary` through `operations_summary_v1`, `HOOKRELAY_UI_GRAFANA_URL`, and `hookrelay admin operations summary`.

**Blocked by:** none.

**Status:** done

- [x] Read-only `operations_summary_v1` snapshot (counts, earliest lease and retry deadlines, DLQ oldest/newest, audit length); `wrong_type` → `503`
- [x] Handler combining readiness, `INFO memory`, the snapshot, and the optional Grafana link; served while not ready
- [x] Configuration `HOOKRELAY_UI_GRAFANA_URL` / `--ui-grafana-url` (absolute http/https), documented in `configuration.md` and `.env.example`
- [x] CLI `admin operations summary`
- [x] Tests: storage seam, HTTP seam, config validation, CLI

## Comments

- 2026-09-30: Implemented. Read-only `operations_summary_v1.lua` (13-field snapshot) plus `INFO memory` in `valkey.NewOperationsStore`; `administration.OperationsReader` and `ReadinessView` ports, `GET /admin/v1/operations/summary` registered when wired; the server wiring now creates `app.Readiness` before the admin service so the summary reports it. Zero times are omitted; `links.grafana_url` only when configured. `HOOKRELAY_UI_GRAFANA_URL` / `--ui-grafana-url` validated as an absolute http(s) URL without userinfo (the error never echoes the value); `.env.example` updated, `configuration.md` is updated with the milestone docs in ticket 11. CLI `admin operations summary`. Tests: storage seam over real state and every wrong type, HTTP seam (full and empty bodies, 503s), config, CLI.

# 04: Operations summary

**What to build:** `GET /admin/v1/operations/summary` through `operations_summary_v1`, `HOOKRELAY_UI_GRAFANA_URL`, and `hookrelay admin operations summary`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] Read-only `operations_summary_v1` snapshot (counts, earliest lease and retry deadlines, DLQ oldest/newest, audit length); `wrong_type` → `503`
- [ ] Handler combining readiness, `INFO memory`, the snapshot, and the optional Grafana link; served while not ready
- [ ] Configuration `HOOKRELAY_UI_GRAFANA_URL` / `--ui-grafana-url` (absolute http/https), documented in `configuration.md` and `.env.example`
- [ ] CLI `admin operations summary`
- [ ] Tests: storage seam, HTTP seam, config validation, CLI

## Comments

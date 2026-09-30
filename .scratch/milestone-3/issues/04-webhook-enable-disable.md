# 04: Webhook Endpoint enable/disable

**What to build:** `PATCH /admin/v1/webhooks/{type}/{identifier}` with strong `If-Match` through `endpoint_set_enabled_v1`, and `hookrelay admin webhook enable|disable`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] `endpoint_set_enabled_v1` per the spec (`updated`, `unchanged`, `not_found`, `precondition_required`, `precondition_failed`, `wrong_type`) with the mandatory audit append
- [ ] Shared strong-`If-Match` parsing (`400` for weak/`*`/lists/malformed); `428`/`412`/`404`; `200` + new `ETag`; `unchanged` keeps the `ETag` and writes no audit
- [ ] Events `webhook_endpoint_enabled` / `webhook_endpoint_disabled`; audit metrics
- [ ] CLI: `--yes` required, GET → PATCH with `If-Match`, `412` reported, lost-response reconciliation per the spec
- [ ] Tests: storage seam (every tuple, snapshot-equal refusals, recreated generation), HTTP seam, CLI, integration (disabled endpoint's webhook returns `404`, re-enabled accepts)

## Comments

# 04: Webhook Endpoint enable/disable

**What to build:** `PATCH /admin/v1/webhooks/{type}/{identifier}` with strong `If-Match` through `endpoint_set_enabled_v1`, and `hookrelay admin webhook enable|disable`.

**Blocked by:** none.

**Status:** done

- [x] `endpoint_set_enabled_v1` per the spec (`updated`, `unchanged`, `not_found`, `precondition_required`, `precondition_failed`, `wrong_type`) with the mandatory audit append
- [x] Shared strong-`If-Match` parsing (`400` for weak/`*`/lists/malformed); `428`/`412`/`404`; `200` + new `ETag`; `unchanged` keeps the `ETag` and writes no audit
- [x] Events `webhook_endpoint_enabled` / `webhook_endpoint_disabled`; audit metrics
- [x] CLI: `--yes` required, GET → PATCH with `If-Match`, `412` reported, lost-response reconciliation per the spec
- [x] Tests: storage seam (every tuple, snapshot-equal refusals, recreated generation), HTTP seam, CLI, integration (disabled endpoint's webhook returns `404`, re-enabled accepts)

## Comments

- 2026-09-30: Implemented. `endpoint_set_enabled_v1.lua` per the spec; a malformed endpoint Hash (missing field, bad `enabled`/`config_version`) is also `wrong_type`; numeric result fields are strings. Operation label `endpoint_update`. `EndpointRepository.SetEndpointEnabled`; `viewOf` now derives `credential.configured` from the credential kind because transition results never carry the value. `PATCH` handler: path shape → `404`, strict body with required `enabled`, then strong `If-Match` parsing (`400` for weak/`*`/lists/malformed), then the script (so a missing endpoint is `404` with or without `If-Match`). Events `webhook_endpoint_enabled|disabled` with safe metadata. CLI: `--yes` required, GET for the `ETag`, PATCH with `If-Match`; `412` reported without retry; a transport error or `5xx` re-reads and reports `desired_state_observed` only for the requested flag in the same generation (with a higher version, or when the read already had it), else `uncertain`, exit 1 either way. The CLI `do` gained `doHeaders` for request/response headers. Tests: storage seam (tuples, audit entry fields, snapshot-equal refusals incl. wrong types, argument rejection, SCRIPT FLUSH), HTTP seam (every refusal), CLI (flow, stale, lost response ×3), composed integration (disabled → webhook 404 → re-enabled accepts; audit sequence).

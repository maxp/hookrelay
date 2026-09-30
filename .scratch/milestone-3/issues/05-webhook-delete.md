# 05: Webhook Endpoint delete

**What to build:** `DELETE /admin/v1/webhooks/{type}/{identifier}` through `endpoint_delete_v1`, and `hookrelay admin webhook delete`.

**Blocked by:** 04 (shares `If-Match` handling).

**Status:** done

- [x] `endpoint_delete_v1` per the spec (`deleted`, `absent`, `precondition_required`, `precondition_failed`, `must_be_disabled`, `wrong_type`); removes Hash, bot Set membership (deleting an empty Set), listing member; mandatory audit
- [x] Handler: `204` on deletion and on absence (no audit), `409 endpoint_must_be_disabled`, `428`, `412`
- [x] Event `webhook_endpoint_deleted`
- [x] CLI: `--yes` required, GET → DELETE with `If-Match`, absence-observed reconciliation after a lost response
- [x] Tests: storage seam, HTTP seam, CLI, integration (recreate the same identifier gets a new generation; old `ETag` → `412`)

## Comments

- 2026-09-30: Implemented. `endpoint_delete_v1.lua` per the spec, bot Set key resolved from the Hash's `bot_id` and type-checked before the precondition; malformed Hash → `wrong_type`. Operation label `endpoint_delete`. `DELETE` handler: a path that cannot name an endpoint answers `204` (it names an absent one), otherwise strong `If-Match` parsing then the script; `absent` → `204` with no audit, event, or metric. Event `webhook_endpoint_deleted`. CLI: `--yes` required, GET for the `ETag` (a missing endpoint is an error, so a typo is not reported as success), DELETE with `If-Match`, `{"outcome":"deleted"}`; `409` adds the `disable` hint; a lost response re-reads and reports absence as `desired_state_observed` with the audit caveat, else `uncertain`, never retrying. Tests: storage seam (keys, bot Set kept then deleted when empty, audit, absent no-op, refusal snapshots, earlier generation, wrong types, arguments, SCRIPT FLUSH), HTTP seam, CLI, composed integration (enabled refusal → disable → delete → read/list/webhook all gone → repeat 204 → recreated identifier refuses the old ETag).

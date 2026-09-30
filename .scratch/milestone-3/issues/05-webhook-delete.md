# 05: Webhook Endpoint delete

**What to build:** `DELETE /admin/v1/webhooks/{type}/{identifier}` through `endpoint_delete_v1`, and `hookrelay admin webhook delete`.

**Blocked by:** 04 (shares `If-Match` handling).

**Status:** ready-for-agent

- [ ] `endpoint_delete_v1` per the spec (`deleted`, `absent`, `precondition_required`, `precondition_failed`, `must_be_disabled`, `wrong_type`); removes Hash, bot Set membership (deleting an empty Set), listing member; mandatory audit
- [ ] Handler: `204` on deletion and on absence (no audit), `409 endpoint_must_be_disabled`, `428`, `412`
- [ ] Event `webhook_endpoint_deleted`
- [ ] CLI: `--yes` required, GET → DELETE with `If-Match`, absence-observed reconciliation after a lost response
- [ ] Tests: storage seam, HTTP seam, CLI, integration (recreate the same identifier gets a new generation; old `ETag` → `412`)

## Comments

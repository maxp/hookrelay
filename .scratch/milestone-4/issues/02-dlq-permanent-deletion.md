# 02: DLQ permanent deletion

**What to build:** `ETag` on the dead-letter read, `DELETE /admin/v1/dead-letters/{message_id}` through `dlq_delete_v1`, and `hookrelay admin dlq delete`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] `GET /admin/v1/dead-letters/{message_id}` returns `ETag: "<delivery_cycle>:<dead_lettered_ms>"`
- [ ] `dlq_delete_v1` per the spec (`deleted`, `absent` with stale index repair, `precondition_required`, `precondition_failed`, `recipient_blocked`, `wrong_type`); deletes `hr1:dl`, `hr1:m`, `hr1:mi`, `hr1:a`, index member; mandatory audit; `actor` argument
- [ ] Handler: strong `If-Match`, `204` on deletion and absence (no audit), `428`, `412`, `409 recipient_blocked`, `400` malformed tag on an existing entry
- [ ] Event `dead_letter_deleted`, metric `hookrelay_dead_letter_deletions_total{outcome}`
- [ ] CLI `admin dlq delete --message-id --yes` with GET → DELETE and absence-observed reconciliation, never retrying
- [ ] Tests: storage seam, HTTP seam, CLI, integration (a replayed and re-dead-lettered entry refuses the old tag; deduplication record untouched)

## Comments

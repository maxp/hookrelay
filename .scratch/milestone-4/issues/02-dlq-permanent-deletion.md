# 02: DLQ permanent deletion

**What to build:** `ETag` on the dead-letter read, `DELETE /admin/v1/dead-letters/{message_id}` through `dlq_delete_v1`, and `hookrelay admin dlq delete`.

**Blocked by:** none.

**Status:** done

- [x] `GET /admin/v1/dead-letters/{message_id}` returns `ETag: "<delivery_cycle>:<dead_lettered_ms>"`
- [x] `dlq_delete_v1` per the spec (`deleted`, `absent` with stale index repair, `precondition_required`, `precondition_failed`, `recipient_blocked`, `wrong_type`); deletes `hr1:dl`, `hr1:m`, `hr1:mi`, `hr1:a`, index member; mandatory audit; `actor` argument
- [x] Handler: strong `If-Match`, `204` on deletion and absence (no audit), `428`, `412`, `409 recipient_blocked`, `400` malformed tag on an existing entry
- [x] Event `dead_letter_deleted`, metric `hookrelay_dead_letter_deletions_total{outcome}`
- [x] CLI `admin dlq delete --message-id --yes` with GET → DELETE and absence-observed reconciliation, never retrying
- [x] Tests: storage seam, HTTP seam, CLI, integration (a replayed and re-dead-lettered entry refuses the old tag; deduplication record untouched)

## Comments

- 2026-09-30: Implemented. `dlq_delete_v1.lua` per the spec; the Recipient block marker is resolved from the record's `recipient_identity`, any existing marker key refuses (same test as replay). `GET /admin/v1/dead-letters/{id}` sets `ETag: "<delivery_cycle>:<dead_lettered_ms>"`; `DELETE` parses one strong tag, a malformed tag is `400` only when the entry exists (the absent path runs the script without a tag and answers `204`), an invalid ID shape is `204`. Metric outcomes `deleted`/`absent`/`refused`/`unavailable` (uncertain counts as unavailable; the error body says "uncertain"). Event `dead_letter_deleted` with the Recipient fields (shared `recipientLogFields` helper, also used by replay now). CLI `admin dlq delete --message-id --yes`: GET with ETag → DELETE with If-Match; `412`/`409` explained, never retried; a lost response re-reads once and reports absence as `desired_state_observed` with the audit caveat. Tests: storage seam (every key, dedup record untouched, repeat absent, stale tag after replay + re-dead-letter, every refusal snapshot-equal, orphan member removal, arguments, SCRIPT FLUSH), HTTP seam, CLI (confirmed, refusals, uncertain matrix).

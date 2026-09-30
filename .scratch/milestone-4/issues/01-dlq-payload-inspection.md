# 01: DLQ payload inspection

**What to build:** `POST /admin/v1/dead-letters/{message_id}/payload` through `dlq_payload_v1`, and `hookrelay admin dlq payload`.

**Blocked by:** none.

**Status:** done

- [x] `dlq_payload_v1` per the spec (`disclosed`, `not_found`, `message_missing`, `wrong_type`); audit appended before the blob is returned; `actor` argument
- [x] Handler: body empty or `{}`, `200` with the stored Canonical Message, `404 dead_letter_not_found`, `503` on missing message / wrong type / uncertain outcome, `Cache-Control: no-store`
- [x] Event `dead_letter_payload_viewed`, metric `hookrelay_dead_letter_payload_views_total{outcome}`
- [x] CLI `admin dlq payload --message-id` (JSON output, audited-access note on stderr)
- [x] Tests: storage seam (wrong-type audit Stream never discloses, refusals snapshot-equal, SCRIPT FLUSH), HTTP seam, CLI, integration (payload view appears in the audit Stream)

## Comments

- 2026-09-30: Implemented. `dlq_payload_v1.lua` also returns the record's `recipient_identity` (spec updated) so the feature event can carry the Recipient scope. The store refuses a blob that is not a JSON object as `wrong_type` after the script's audit append (recorded access, nothing disclosed). The auth middleware now puts the audit actor (`admin_bearer` for now) in the request context and sets `Cache-Control: no-store` on every `/admin/v1/` response, which ticket 07 relies on. Route `POST /admin/v1/dead-letters/{message_id}/payload` (empty body or `{}`), event `dead_letter_payload_viewed`, metric `hookrelay_dead_letter_payload_views_total{outcome}`, CLI `admin dlq payload`. Tests: storage seam (tuple, audit fields, every refusal snapshot-equal with no audit, arguments, actor allowlist, SCRIPT FLUSH), HTTP seam, CLI. The storage seam over real Valkey covers the "view appears in the audit Stream" check.

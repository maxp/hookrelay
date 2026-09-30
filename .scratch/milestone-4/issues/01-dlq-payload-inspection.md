# 01: DLQ payload inspection

**What to build:** `POST /admin/v1/dead-letters/{message_id}/payload` through `dlq_payload_v1`, and `hookrelay admin dlq payload`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] `dlq_payload_v1` per the spec (`disclosed`, `not_found`, `message_missing`, `wrong_type`); audit appended before the blob is returned; `actor` argument
- [ ] Handler: body empty or `{}`, `200` with the stored Canonical Message, `404 dead_letter_not_found`, `503` on missing message / wrong type / uncertain outcome, `Cache-Control: no-store`
- [ ] Event `dead_letter_payload_viewed`, metric `hookrelay_dead_letter_payload_views_total{outcome}`
- [ ] CLI `admin dlq payload --message-id` (JSON output, audited-access note on stderr)
- [ ] Tests: storage seam (wrong-type audit Stream never discloses, refusals snapshot-equal, SCRIPT FLUSH), HTTP seam, CLI, integration (payload view appears in the audit Stream)

## Comments

# 11: Dead-letter retention

**What to build:** Maintenance deletes dead-letter entries older than `HOOKRELAY_DLQ_RETENTION` with the blob, message index, and history, appending the required audit event in the same operation.

**Blocked by:** 05, 03.

**Status:** done

- [x] `expire_dlq_v1` per the spec; maintenance batches over `dlq` by score
- [x] Storage-seam tests; audit event without payload

## Comments

- 2026-09-30: Implemented. `expire_dlq_v1` deletes a dead letter past `HOOKRELAY_DLQ_RETENTION` (record, blob, `hr1:mi`, history, `dlq` member) and appends `dead_letter_expired` (actor `maintenance`, no payload) atomically; an orphan `dlq` member is removed as `stale`. `delivery.Maintenance` runs it as the third round kind `dlq_retention` over `DueDeadLetters` (oldest first by Valkey time, bounded batches, due lag from `dead_lettered_ms + retention`) and logs `dead_letter_expired`. Storage-seam tests cover every key, the audit fields, not_due/stale, refusals without mutation, arguments, and reload; a composed test runs a real round over Valkey. Details in the spec's ticket-11 note.

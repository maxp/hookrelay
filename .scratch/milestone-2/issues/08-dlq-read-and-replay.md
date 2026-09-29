# 08: DLQ read and replay (Admin API + CLI)

**What to build:** Operators list and inspect dead letters by safe metadata and replay one ahead of its Recipient's not-yet-started messages with a new Delivery Cycle, deduplication-conflict protection, and an atomic audit append.

**Blocked by:** 05, 12 (reconciliation must understand DLQ and retry state before replay is exposed).

**Status:** ready-for-agent

- [ ] `GET /admin/v1/dead-letters` (cursor pagination, newest first) and `GET /admin/v1/dead-letters/{message_id}`
- [ ] `replay_dlq_v1` per the spec (queue position head / after_active_head, `keep_current`, cycle archive, audit); persist per-message pending cycle/attempt for a replay behind an active head and a ready head preempted by replay (including legacy messages without `hr1:mi`); `POST …/replay` responses and `404 dead_letter_not_found`, `409 deduplication_conflict`, `409 recipient_blocked`
- [ ] Register `ack_v4`, `nack_v3`, `expire_lease_v3`: restore and consume pending head state when exposing the next message, refuse missing/partial pending state when history indicates replay; extend block inspection and register `clear_block_v2` so blocks caused by damaged pending metadata cannot be cleared while damage remains; register `reconcile_recipient_v3` to check non-head pending metadata before readiness; storage-seam tests for retry attempt preservation, replay behind leased/retry-wait head, multiple replays, corrupt pending state, ack/dead-letter advancement and restart
- [ ] CLI `admin dlq list|get|replay` with the uncertain-outcome discipline
- [ ] `delivery_replayed` event, `dead_letter_replays_total{outcome}`

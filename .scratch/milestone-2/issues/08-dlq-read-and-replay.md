# 08: DLQ read and replay (Admin API + CLI)

**What to build:** Operators list and inspect dead letters by safe metadata and replay one ahead of its Recipient's not-yet-started messages with a new Delivery Cycle, deduplication-conflict protection, and an atomic audit append.

**Blocked by:** 05.

**Status:** ready-for-agent

- [ ] `GET /admin/v1/dead-letters` (cursor pagination, newest first) and `GET /admin/v1/dead-letters/{message_id}`
- [ ] `replay_dlq_v1` per the spec (queue position head / after_active_head, `keep_current`, cycle archive, audit); `POST …/replay` responses and `404 dead_letter_not_found`, `409 deduplication_conflict`, `409 recipient_blocked`
- [ ] CLI `admin dlq list|get|replay` with the uncertain-outcome discipline
- [ ] `delivery_replayed` event, `dead_letter_replays_total{outcome}`

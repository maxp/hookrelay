# 08: DLQ read and replay (Admin API + CLI)

**What to build:** Operators list and inspect dead letters by safe metadata and replay one ahead of its Recipient's not-yet-started messages with a new Delivery Cycle, deduplication-conflict protection, and an atomic audit append.

**Blocked by:** 05, 12 (reconciliation must understand DLQ and retry state before replay is exposed).

**Status:** done

- [x] `GET /admin/v1/dead-letters` (cursor pagination, newest first) and `GET /admin/v1/dead-letters/{message_id}`
- [x] `replay_dlq_v1` per the spec (queue position head / after_active_head, `keep_current`, cycle archive, audit); persist per-message pending cycle/attempt for a replay behind an active head and a ready head preempted by replay (including legacy messages without `hr1:mi`); `POST …/replay` responses and `404 dead_letter_not_found`, `409 deduplication_conflict`, `409 recipient_blocked`
- [x] Register `ack_v4`, `nack_v3`, `expire_lease_v3`: restore and consume pending head state when exposing the next message, refuse missing/partial pending state when history indicates replay; extend block inspection and register `clear_block_v2` so blocks caused by damaged pending metadata cannot be cleared while damage remains; register `reconcile_recipient_v3` to check non-head pending metadata before readiness; storage-seam tests for retry attempt preservation, replay behind leased/retry-wait head, multiple replays, corrupt pending state, ack/dead-letter advancement and restart
- [x] CLI `admin dlq list|get|replay` with the uncertain-outcome discipline
- [x] `delivery_replayed` event, `dead_letter_replays_total{outcome}`

## Comments

- 2026-09-29: Implemented. `replay_dlq_v1` (head / after_active_head with pending cycle/attempt in `hr1:mi`, preempted ready heads save theirs, dedup `reject`/`keep_current`, 10-cycle fold counting the new cycle, atomic audit). `ack_v4`, `nack_v3`, `expire_lease_v3` restore and consume the next head's pending pair and refuse history without a valid pair before any write. `reconcile_recipient_v3`, `inspect_block_v2`, `clear_block_v2` check queued non-head pending state (`queued_delivery_state_missing` / `_invalid`) within the per-Recipient bound. `valkey.NewDeadLetterStore`; Admin routes `GET /admin/v1/dead-letters`, `GET /admin/v1/dead-letters/{message_id}` (with attempt history), `POST /admin/v1/dead-letters/{message_id}/replay`; CLI `admin dlq list|get|replay` (`--yes` required; the current cycle is read first; an uncertain replay is reconciled by one dead-letter read, never retried). `delivery_replayed` event and `hookrelay_dead_letter_replays_total{outcome}`. Contract details are in the spec's ticket-08 implementation note.
- Left to ticket 09: the CLI reports a replayed message that has left the DLQ as `uncertain` until the delivery-state read can classify it.
- 2026-09-30: Milestone 2 review. Replay is now `replay_dlq_v2`, which keeps waiting replays first-in-first-out (`after_pending_replay`), so replaying oldest first restores the original order regardless of claims made between replays. It is deliberately not limited by queue capacity. See the spec's review amendment; `TestMultipleReplays`, `TestReplayOrderIgnoresClaims`, and `TestReplayPreemptsStartedReplay` pin the behaviour.

# 09: Delivery-state read

**What to build:** `GET /admin/v1/messages/{message_id}/delivery-state` and `hookrelay admin message delivery-state`, so a lost replay response can be reconciled without payloads or tokens.

**Blocked by:** 08.

**Status:** done

- [x] States `queued`, `leased`, `retry_wait`, `dead_lettered`, `acknowledged` (from success metadata while retained); safe queue-position classification
- [x] CLI replay reconciliation uses it: newer cycle outside the DLQ → desired state observed with the audit caveat; otherwise uncertain

## Comments

- 2026-09-29: Implemented. Read-only `delivery_state_v1` (the adapter reads the blob's Recipient only to locate the queue; the script classifies atomically) behind `GET /admin/v1/messages/{message_id}/delivery-state` (`404 message_not_found`, newly added to the error allowlist; `409 recipient_state_ambiguous` for unclassifiable state) and `hookrelay admin message delivery-state`. `queue_position` is `head` or `behind_head`; `acknowledged` only while the 24-hour success metadata is retained. `hookrelay admin dlq replay` now reconciles a lost response with one delivery-state read: a newer cycle in any state is `desired_state_observed` (audit caveat), otherwise `uncertain`. Details in the spec's ticket-09 implementation note.

# 09: Delivery-state read

**What to build:** `GET /admin/v1/messages/{message_id}/delivery-state` and `hookrelay admin message delivery-state`, so a lost replay response can be reconciled without payloads or tokens.

**Blocked by:** 08.

**Status:** ready-for-agent

- [ ] States `queued`, `leased`, `retry_wait`, `dead_lettered`, `acknowledged` (from success metadata while retained); safe queue-position classification
- [ ] CLI replay reconciliation uses it: newer cycle outside the DLQ → desired state observed with the audit caveat; otherwise uncertain

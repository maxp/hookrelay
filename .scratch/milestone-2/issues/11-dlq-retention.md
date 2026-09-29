# 11: Dead-letter retention

**What to build:** Maintenance deletes dead-letter entries older than `HOOKRELAY_DLQ_RETENTION` with the blob, message index, and history, appending the required audit event in the same operation.

**Blocked by:** 05, 03.

**Status:** ready-for-agent

- [ ] `expire_dlq_v1` per the spec; maintenance batches over `dlq` by score
- [ ] Storage-seam tests; audit event without payload

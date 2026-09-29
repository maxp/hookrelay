# 05: Dead-letter transition

**What to build:** Failing the fourth attempt (by nack or expiry) moves the message atomically to the global DLQ and exposes the Recipient's next message.

**Blocked by:** 04.

**Status:** ready-for-agent

- [ ] Replace the temporary `attempts_exhausted` refusal in `nack_v1` and `expire_lease_v1` (tickets 02, 04) and the maintenance `deferred` result
- [ ] Shared dead-letter fragment in `nack_v1` and `expire_lease_v1`: `hr1:dl` Hash with structured Recipient fields and `dedup_identity_digest` from `hr1:mi`, `dlq` membership, LPOP, counter DECR, next head ready or queue deleted, blob and history retained
- [ ] `nack` response `dead_lettered`; reasons `nack_exhausted` / `expiry_exhausted`
- [ ] `delivery_dead_lettered` event; `dead_letters_total{recipient_scope,reason}`, `dead_letter_messages`, `delivery_attempts_total{outcome=dead_lettered}`
- [ ] Storage-seam tests for both reasons, every key, and next-head exposure

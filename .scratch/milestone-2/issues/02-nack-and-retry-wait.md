# 02: Negative acknowledgement and retry wait

**What to build:** A consumer can `nack` a claimed message; the attempt is recorded in history and the head waits for its server-chosen retry delay. Repeats return the recorded result; ack/nack cross-conflicts are bounded.

**Blocked by:** 01.

**Status:** ready-for-agent

- [ ] `nack_v1` per the spec (preconditions order, history entry `outcome=nack`, `retry_wait` state, `retries` membership, token `nacked` phase with recorded result, claim op `no_longer_active`); retry delay from `HOOKRELAY_RETRY_DELAYS` × jitter, `retry_at_ms` from Valkey `TIME`
- [ ] `POST /v1/deliveries/nack` (`delivery_token`, optional bounded `reason_code`), 5 s deadline, `retry_scheduled` body, recorded result on repeat, `409 delivery_already_acknowledged`, `409 stale_delivery_token`, `404`, `409 recipient_blocked`
- [ ] `ack` after `nack` → `409 delivery_already_nacked`
- [ ] Attempt-history encoding and the 10-cycle archive summary (unit + storage tests)
- [ ] `delivery_nacked` event; `delivery_attempts_total{outcome=nack}`, `delivery_attempt_duration_seconds`, `retries_waiting`
- [ ] Storage-seam tests for every tuple, key, TTL, and refusal without mutation

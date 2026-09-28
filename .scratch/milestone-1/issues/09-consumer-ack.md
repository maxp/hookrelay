# 09: Consumer acknowledgement

**What to build:** A claimed message can be acknowledged by Delivery Token, completing the delivery attempt: the message leaves the queue, the next message (if any) becomes ready, compact success metadata and a terminal tombstone remain, and repeated acknowledgements return the recorded success.

**Blocked by:** 08 (consumer claim — the attempt and token records must exist).

**Status:** ready-for-agent

- [ ] `POST /v1/deliveries/ack` per the Consumer API contract: token-keyed request, same auth and body-validation discipline as claim
- [ ] `ack_v1` Lua transition locates the attempt through the tombstone-key active phase (recipient, message, claim operation record) and verifies the token against head state before any write
- [ ] Success path in one script: compact success Hash (24-hour TTL) with only safe fields; message blob and attempt-history deletion; queue LPOP; next head re-pointed (`status=ready`, fresh `delivery_cycle=1`/`attempt=1`, cleared token/lease fields) and ready-index re-add with a fresh sequence — or state and queue keys deleted when the queue empties; stats counter decremented
- [ ] Tombstone terminal phase: `state=acknowledged` + `message_id` + `acknowledged_ms` with 1-hour TTL; every plaintext token copy deleted (head state, tombstone active phase, claim op record rewritten to `claim_no_longer_active`)
- [ ] Outcomes per the contract: `200` success; idempotent repeat returns the recorded result; unknown token or expired tombstone `404 delivery_token_not_found`; superseded/stale token `409 stale_delivery_token`; blocked recipient `409 recipient_blocked` with no mutation
- [ ] M1 writes no attempt-history entries on the ack path; existing history is deleted (contract for entries is fixed for Milestone 2)
- [ ] Feature event `delivery_acknowledged`; `delivery_attempts_total{recipient_scope, outcome=acknowledged}`; `queue_messages` gauge follows the counter
- [ ] End-to-end test: claim → ack → queue empty → compact success readable → repeat ack returns the recorded result → replaying the claim operation now returns `claim_no_longer_active`

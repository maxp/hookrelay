# 09: Consumer acknowledgement

**What to build:** A claimed message can be acknowledged by Delivery Token, completing the delivery attempt: the message leaves the queue, the next message (if any) becomes ready, compact success metadata and a terminal tombstone remain, and repeated acknowledgements return the recorded success.

**Blocked by:** 08 (consumer claim — the attempt and token records must exist).

**Status:** done

- [x] `POST /v1/deliveries/ack` per the Consumer API contract: token-keyed request, same auth and body-validation discipline as claim
- [x] `ack_v1` Lua transition locates the attempt through the tombstone-key active phase (recipient, message, claim operation record) and verifies the token against head state before any write
- [x] Success path in one script: compact success Hash (24-hour TTL) with only safe fields; message blob and attempt-history deletion; queue LPOP; next head re-pointed (`status=ready`, fresh `delivery_cycle=1`/`attempt=1`, cleared token/lease fields) and ready-index re-add with a fresh sequence — or state and queue keys deleted when the queue empties; stats counter decremented
- [x] Tombstone terminal phase: `state=acknowledged` + `message_id` + `acknowledged_ms` with 1-hour TTL; every plaintext token copy deleted (head state, tombstone active phase, claim op record rewritten to `claim_no_longer_active`)
- [x] Outcomes per the contract: `200` success; idempotent repeat returns the recorded result; unknown token or expired tombstone `404 delivery_token_not_found`; superseded/stale token `409 stale_delivery_token`; blocked recipient `409 recipient_blocked` with no mutation
- [x] M1 writes no attempt-history entries on the ack path; existing history is deleted (contract for entries is fixed for Milestone 2)
- [x] Feature event `delivery_acknowledged`; `delivery_attempts_total{recipient_scope, outcome=acknowledged}`; `queue_messages` gauge follows the counter
- [x] End-to-end test: claim → ack → queue empty → compact success readable → repeat ack returns the recorded result → replaying the claim operation now returns `claim_no_longer_active`

## Comments

- 2026-09-29: Implemented. `ack_v1.lua` resolves the Recipient, message, and claim operation from the token record, checks not_found → already_acknowledged → recipient_blocked → key types → current unexpired leased head with the same token and message, then in one script writes the compact success Hash (scope and platform from the Recipient identity, `received_ms` via `cjson` from the blob, cycle, attempt count, optional instance id; 24 h TTL), deletes blob and history, pops the queue, re-points the next head to `ready` with a fresh `ready_seq` or deletes queue and state, removes the lease member, decrements the counter, rewrites the token record to its terminal phase (1 h TTL) and the op record to `no_longer_active` (remaining TTL kept). No plaintext token copy survives. `POST /v1/deliveries/ack` (5 s deadline, token shape `dlv_` + 22 base64url checked before storage), `delivery_acknowledged` event, `hookrelay_delivery_attempts_total{recipient_scope,outcome="acknowledged"}` counted once per first ack; `queue_messages` follows the counter through the gauge refresh. `model.ParseIdentity` reverses the Recipient identity serialization.
- Spec amendments recorded: KEYS/ARGV shape (per-recipient keys resolved in-script, key prefix), explicit stale conditions including the lease deadline, extra return fields for the first acknowledgement.
- Tests: storage seam (next-head and drain branches with every key, TTLs, token-copy removal, op rewrite, idempotent repeat with no mutation, not_found/stale ×3/recipient_blocked/wrong_type with no mutation, argument validation, claim replay → `claim_no_longer_active`, next head claimable); handler (success/repeat body, digest, event, metric once, error mapping, body validation); composed-app end to end (two webhooks → claim → ack → repeat → claim replay 409 → next claim → ack → drained 204 → unknown token 404). Live-verified.


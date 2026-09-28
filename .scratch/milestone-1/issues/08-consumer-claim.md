# 08: Consumer claim (wait_ms=0)

**What to build:** A queue consumer authenticated with the shared secret can claim the next available message for any recipient with an immediate check, receiving the Canonical Message plus a Delivery Token and lease deadline — with fully idempotent claim-replay semantics backed by atomic Lua state.

**Blocked by:** 05 (ingestion chat scope — queued messages and ready-index entries must exist).

**Status:** ready-for-agent

- [ ] `POST /v1/deliveries/claim` on the public listener: shared Consumer Secret Bearer auth, constant-time, uniform `401 unauthenticated`; strict body validation (16 KiB, depth 40, unknown fields rejected, UUIDv7 syntax, integers not strings); `X-Request-Id` on every response; optional bounded `Consumer-Instance-Id` header treated as diagnostics only
- [ ] `claim_v1` Lua transition: pick lowest-scored ready candidates (bounded scan of 10), skip-and-continue on blocked marker / wrong key types / status ≠ ready / head-invariant violation / missing head blob — creating the block marker + blocked-index member and removing ready/leases memberships atomically in that case (reported via a `blocked_detected` count)
- [ ] Successful claim: head state → leased with Valkey-`TIME` deadline, `leases` ZADD, ready ZREM, claim op record (10-minute TTL) with kind/args-digest/token/pointers, tombstone-key active phase; response body per the Consumer API contract with absent optional fields omitted (never `null`)
- [ ] Idempotent replay: same `operation_id` + same arguments → recorded response with the same token while the attempt is active; recorded empty outcome replays as `204`; after the attempt ends → `409 claim_no_longer_active`; changed arguments → `409 operation_conflict` (args digest over `operation_id` + `wait_ms`)
- [ ] Active-lease limit enforced work-pool-wide via unexpired `hr1:leases` count with authoritative Valkey time → `429` + `Retry-After: 1`
- [ ] Errors use the bounded Consumer error envelope; tokens never appear in URLs, logs, or error bodies
- [ ] Feature event `delivery_claimed` and metrics `delivery_claims_total{outcome}`, `active_leases`, `waiting_claims`
- [ ] Integration tests at the storage seam: every status tuple variant, key assertions after each outcome, no mutation on precondition failure, `SCRIPT FLUSH`/`NOSCRIPT` reload, parser rejection

# 08: Consumer claim (wait_ms=0)

**What to build:** A queue consumer authenticated with the shared secret can claim the next available message for any recipient with an immediate check, receiving the Canonical Message plus a Delivery Token and lease deadline — with fully idempotent claim-replay semantics backed by atomic Lua state.

**Blocked by:** 05 (ingestion chat scope — queued messages and ready-index entries must exist).

**Status:** done

- [x] `POST /v1/deliveries/claim` on the public listener: shared Consumer Secret Bearer auth, constant-time, uniform `401 unauthenticated`; strict body validation (16 KiB, depth 40, unknown fields rejected, UUIDv7 syntax, integers not strings); `X-Request-Id` on every response; optional bounded `Consumer-Instance-Id` header treated as diagnostics only
- [x] `claim_v1` Lua transition: pick lowest-scored ready candidates (bounded scan of 10), skip-and-continue on blocked marker / wrong key types / status ≠ ready / head-invariant violation / missing head blob — creating the block marker + blocked-index member and removing ready/leases memberships atomically in that case (reported via a `blocked_detected` count)
- [x] Successful claim: head state → leased with Valkey-`TIME` deadline, `leases` ZADD, ready ZREM, claim op record (10-minute TTL) with kind/args-digest/token/pointers, tombstone-key active phase; response body per the Consumer API contract with absent optional fields omitted (never `null`)
- [x] Idempotent replay: same `operation_id` + same arguments → recorded response with the same token while the attempt is active; recorded empty outcome replays as `204`; after the attempt ends → `409 claim_no_longer_active`; changed arguments → `409 operation_conflict` (args digest over `operation_id` + `wait_ms`)
- [x] Active-lease limit enforced work-pool-wide via unexpired `hr1:leases` count with authoritative Valkey time → `429` + `Retry-After: 1`
- [x] Errors use the bounded Consumer error envelope; tokens never appear in URLs, logs, or error bodies
- [x] Feature event `delivery_claimed` and metrics `delivery_claims_total{outcome}`, `active_leases`, `waiting_claims`
- [x] Integration tests at the storage seam: every status tuple variant, key assertions after each outcome, no mutation on precondition failure, `SCRIPT FLUSH`/`NOSCRIPT` reload, parser rejection

## Comments

- 2026-09-29: Implemented. `claim_v1.lua` (replay by `hr1:op`, unexpired-lease limit via `ZCOUNT leases (now +inf)`, bounded scan of 10 ready candidates with block markers for inconsistent heads, claim writes: leased head state, `leases` ZADD, `ready` ZREM, op record TTL 10 min, `hr1:t` active phase TTL 10 min + lease). New `internal/delivery` (claim use case, Consumer API transport with constant-time Bearer auth, strict body, bounded error envelope, `delivery_claimed`, metrics) and `internal/jsonbody` (strict JSON discipline now shared with the Admin API, which was refactored onto it). `valkey.DeliveryStore` implements `delivery.Claimer` and `delivery.StatsReader`; the app mounts the Consumer API under `/v1/` on the public listener and refreshes the gauges (`active_leases`, `ready_recipients`, `blocked_recipients`, `queue_messages`) on the one-second monitor.
- Contract amendments recorded in the spec: `delivery_token_digest` and `record_empty` ARGV; stale derived ready entries (drained queue, leased head) are dropped without a marker; `blocked_detected` counts newly created markers.
- Interim until ticket 10: every claim performs one atomic check and records an empty outcome, regardless of `wait_ms` (validated 0–30000). `hookrelay_waiting_claims` currently counts claims being served.
- Uncertain claims (transport error after dispatch) return `503 dependency_unavailable` with `Retry-After: 1`; repeating the same `operation_id` replays a lease the first attempt may have created.
- Tests: storage seam (claimed keys and TTLs, replay/conflict/replay_empty/record_empty=0/no-longer-active variants, lease limit incl. expired leases not counting, every block reason and both stale-entry drops, fairness order, `SCRIPT FLUSH` reload, wrong types, argument validation, stats); handler (success body, digests and defaults, instance id, outcome mapping, validation matrix, no token in logs); composed-app end to end (webhook → claim → replay → conflict → empty behind a lease, gauges). Live-verified.


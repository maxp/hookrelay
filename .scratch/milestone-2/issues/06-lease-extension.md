# 06: Lease extension

**What to build:** `POST /v1/deliveries/extend` pushes the lease deadline by the server-defined interval, idempotent by `operation_id`, capped at five minutes from the attempt start.

**Blocked by:** 01 (independent of 02–05).

**Status:** done

- [x] `extend_v1` per the spec (op record `kind=extend`, replay, conflict, stale, blocked, maximum lifetime)
- [x] Handler with 5 s deadline and contract responses; `delivery_lease_extended` event
- [x] Storage-seam tests, including extension racing expiry

## Comments

- 2026-09-29: Implemented. `extend_v1.lua` (op record replay/conflict first, then not_found, stale — including a lease past its deadline — recipient_blocked, maximum_lease_lifetime_reached; writes state, lease score, token record deadline and TTL, and the `kind=extend` op record). `POST /v1/deliveries/extend` with UUIDv7 `operation_id`, arguments digest over the operation and token digest, 5 s deadline, `delivery_lease_extended` event. Decisions: a claim_v1-era lease measures the cap from `claimed_ms`; the claim operation record keeps its recorded response. Tests: storage seam (tuples, keys, cap, replay/conflict, refusals, both orders of racing expiry, SCRIPT FLUSH), HTTP seam with fakes, real-Valkey integration (ack after the original deadline).
- Review follow-up: `delivery_lease_extended` carries `operation_id`, `delivery_cycle`, and `attempt`; replay validates the recorded result and no longer depends on the token record or lease index types; extension after `expire_lease_v2` ran is covered (stale); the store no longer re-applies configuration defaults (config owns them). Left as judgement calls: renaming `ClaimLimits`/`ClaimOpTTL` and a shared digest helper.

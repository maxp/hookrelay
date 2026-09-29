# 06: Lease extension

**What to build:** `POST /v1/deliveries/extend` pushes the lease deadline by the server-defined interval, idempotent by `operation_id`, capped at five minutes from the attempt start.

**Blocked by:** 01 (independent of 02–05).

**Status:** ready-for-agent

- [ ] `extend_v1` per the spec (op record `kind=extend`, replay, conflict, stale, blocked, maximum lifetime)
- [ ] Handler with 5 s deadline and contract responses; `delivery_lease_extended` event
- [ ] Storage-seam tests, including extension racing expiry

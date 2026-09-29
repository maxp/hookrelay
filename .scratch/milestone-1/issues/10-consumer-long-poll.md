# 10: Consumer long-poll waiting contract

**What to build:** The complete `wait_ms=0–30,000` claim contract: a waiting claim rechecks the ready index atomically on an interval until work appears, the deadline passes, or the client cancels — with Valkey remaining the source of truth throughout.

**Blocked by:** 08 (consumer claim — the loop wraps the claim transition; independent of 09).

**Status:** done

- [x] `wait_ms` optional, defaults 30,000, constrained 0–30,000; out-of-range → `400 invalid_request`
- [x] Process-side waiting loop re-runs the atomic claim script with the same `operation_id` on a 250 ms interval plus 0–50 ms uniform jitter until claimed, empty at deadline, or cancelled; every recheck is atomic and loss-recovery safe
- [x] Completed wait without work → `204 No Content` without a body; work appearing mid-wait returns immediately
- [x] Client cancellation before a lease abandons the wait; if lease creation races with disconnect, repeating the same `operation_id` recovers the recorded lease
- [x] Process-local waiting-claim limit (default 20) → `429` + `Retry-After: 1`; `hookrelay_waiting_claims` reflects it
- [x] Empty outcome recorded in the claim op record (10-minute TTL) so a repeated claim replays the empty result
- [x] Graceful shutdown cancels outstanding long polls with `503` + `Retry-After: 1`
- [x] No per-request logs for empty long-poll outcomes (metrics only); successful claims and errors logged per the feature-event contract
- [x] Tests use tolerant timing windows around the recheck interval and jitter; cancellation and deadline paths covered

## Comments

- 2026-09-29: Ticket 08 prepared the loop: `claim_v1` takes `record_empty`; pass `0` on rechecks and `1` only on the final check at the deadline so rechecks under the same `operation_id` never replay an early empty. `delivery.Handler.handleClaim` currently performs one check with `RecordEmpty: true`; `hookrelay_waiting_claims` already exists.
- 2026-09-29: Implemented in `delivery.Handler.handleClaim`: the loop runs `claim_v1` with the same operation, token, and arguments, `record_empty=0` on rechecks and `1` only on the final check at the `wait_ms` deadline (a `wait_ms=0` claim is its own final check); pauses are 250 ms + uniform 0–50 ms (`math/rand/v2`, configurable for tests); the request deadline is `wait_ms + 5 s`. Client cancellation abandons the wait without a final check. Claims with `wait_ms > 0` take one of `HOOKRELAY_MAX_WAITING_CLAIMS` process-local slots on entry (immediate claims are not limited); `hookrelay_waiting_claims` now reports occupied slots. `delivery.Handler.Shutdown` (wired through the new `app.Deps.BeforeDrain`, run after readiness and acceptance are withdrawn and before the listeners drain) ends waiting claims and refuses new ones with `503` + `Retry-After: 1`. Empty outcomes produce no log; new bounded `delivery_claims_total` outcomes: `waiting_limit_exceeded`, `cancelled`, `shutting_down`.
- Not included (Milestone 2 per the delivery design): the inline pre-poll maintenance pass over due leases/retries.
- Tests: mid-wait work with no early empty record, deadline with only the final check recording and no log, default 250–300 ms cadence, cancellation, waiting limit and gauge, shutdown (tolerant timing windows); over real Valkey: webhook arriving mid-wait, empty wait replaying immediately as 204, cancelled wait recording nothing so the same operation later claims; app shutdown order. Live-verified including SIGTERM during a 20 s wait.


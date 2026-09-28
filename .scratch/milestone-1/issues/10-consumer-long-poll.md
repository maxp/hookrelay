# 10: Consumer long-poll waiting contract

**What to build:** The complete `wait_ms=0–30,000` claim contract: a waiting claim rechecks the ready index atomically on an interval until work appears, the deadline passes, or the client cancels — with Valkey remaining the source of truth throughout.

**Blocked by:** 08 (consumer claim — the loop wraps the claim transition; independent of 09).

**Status:** ready-for-agent

- [ ] `wait_ms` optional, defaults 30,000, constrained 0–30,000; out-of-range → `400 invalid_request`
- [ ] Process-side waiting loop re-runs the atomic claim script with the same `operation_id` on a 250 ms interval plus 0–50 ms uniform jitter until claimed, empty at deadline, or cancelled; every recheck is atomic and loss-recovery safe
- [ ] Completed wait without work → `204 No Content` without a body; work appearing mid-wait returns immediately
- [ ] Client cancellation before a lease abandons the wait; if lease creation races with disconnect, repeating the same `operation_id` recovers the recorded lease
- [ ] Process-local waiting-claim limit (default 20) → `429` + `Retry-After: 1`; `hookrelay_waiting_claims` reflects it
- [ ] Empty outcome recorded in the claim op record (10-minute TTL) so a repeated claim replays the empty result
- [ ] Graceful shutdown cancels outstanding long polls with `503` + `Retry-After: 1`
- [ ] No per-request logs for empty long-poll outcomes (metrics only); successful claims and errors logged per the feature-event contract
- [ ] Tests use tolerant timing windows around the recheck interval and jitter; cancellation and deadline paths covered

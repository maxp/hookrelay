# 06: Periodic consistency-checking decision

**What to build:** Evaluate whether hookrelay needs consistency checking while serving, and record an accepted decision without silently turning startup reconciliation into an unbounded background scan.

**Blocked by:** 05.

**Status:** done

- [x] Measure full reconciliation on production-like counts for endpoints, Recipients, queued messages, dedup records, dead letters, and sessions
- [x] Document serving-time races with live transitions and which repairs remain safe under concurrency
- [x] Compare startup/recovery-only, bounded incremental background checking, and operator-triggered checking
- [x] Define the detection objective, work/latency budget, metrics, alerting, and readiness/error policy for any proposed checker
- [x] Record the decision in design docs; add an ADR only if a hard-to-reverse operational mechanism is selected
- [x] If implementation is selected, amend the Milestone 5 spec and split it into newly blocker-aware implementation tickets before coding

## Comments

- 2026-09-30: Selected startup/recovery-only complete-model checking; no periodic or online operator-triggered checker. `docs/design/consistency-checking.md` records official Valkey source constraints, four reproducible synthetic profiles (including default queued/dedup capacity), three measured passes per profile, serving-time locator/counter/block races, detection limits, five-minute drain/pass budget, readiness policy, alerts, and prerequisites for reconsideration. `internal/valkey/reconcile_cost_test.go` retains the opt-in experiment and a small regular fixture check. Research also exposed recovery's missing quiescence; ticket 08 specifies and closes that safety prerequisite, without adding a checker. No hard-to-reverse mechanism was selected, so no ADR was added.

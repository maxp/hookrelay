# 06: Periodic consistency-checking decision

**What to build:** Evaluate whether hookrelay needs consistency checking while serving, and record an accepted decision without silently turning startup reconciliation into an unbounded background scan.

**Blocked by:** 05.

**Status:** needs-info

- [ ] Measure full reconciliation on production-like counts for endpoints, Recipients, queued messages, dedup records, dead letters, and sessions
- [ ] Document serving-time races with live transitions and which repairs remain safe under concurrency
- [ ] Compare startup/recovery-only, bounded incremental background checking, and operator-triggered checking
- [ ] Define the detection objective, work/latency budget, metrics, alerting, and readiness/error policy for any proposed checker
- [ ] Record the decision in design docs; add an ADR only if a hard-to-reverse operational mechanism is selected
- [ ] If implementation is selected, amend the Milestone 5 spec and split it into newly blocker-aware implementation tickets before coding

## Comments

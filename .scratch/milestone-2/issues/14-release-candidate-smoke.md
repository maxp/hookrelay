# 14: Release-candidate smoke

**What to build:** The Compose smoke proves the failure path end to end, and CI green on it is the Milestone 2 exit criterion.

**Blocked by:** 05, 08, 12.

**Status:** done

- [x] Smoke: nack with observed retries (short configured delays) → fourth failure → DLQ via CLI → replay via CLI → claim with `delivery_cycle=2` → ack; a lease left expired across a restart is processed by reconciliation before readiness
- [x] README and design docs updated for the M2 behavior; spec status `done`

## Comments

- 2026-09-30: Implemented. `scripts/smoke.sh` keeps the Milestone 1 path and adds the failure path with `HOOKRELAY_RETRY_DELAYS=300ms,300ms,300ms` and a 5 s initial lease: three nacks with the retries claimed by waiting claims (cycle 1, attempts 1–3, `retry_wait` head in between), the fourth nack dead-letters (`nack_exhausted`, history of four), `admin dlq list|get|replay` and `admin message delivery-state` through the CLI, a claim in `delivery_cycle=2` and its ack (state `acknowledged`, cycle 2), failure-path metrics (attempts by outcome, dead letters, replays, replay audit). A lease left claimed survives a stack restart past its deadline; the `reconciliation_completed` log reports `due_leases_processed:1`, the head moves to attempt 2 without a token, and history records `outcome=expired`. Passed locally against the pinned stack; README, deployment, milestone docs, and the CI comment updated; spec status `done`.

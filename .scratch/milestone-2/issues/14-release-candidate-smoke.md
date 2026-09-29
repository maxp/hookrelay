# 14: Release-candidate smoke

**What to build:** The Compose smoke proves the failure path end to end, and CI green on it is the Milestone 2 exit criterion.

**Blocked by:** 05, 08, 12.

**Status:** ready-for-agent

- [ ] Smoke: nack with observed retries (short configured delays) → fourth failure → DLQ via CLI → replay via CLI → claim with `delivery_cycle=2` → ack; a lease left expired across a restart is processed by reconciliation before readiness
- [ ] README and design docs updated for the M2 behavior; spec status `done`

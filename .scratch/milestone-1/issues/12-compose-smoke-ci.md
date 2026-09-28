# 12: Compose smoke and complete CI

**What to build:** The whole Milestone 1 vertical slice is proven automatically: the 10-step Compose smoke runs in CI against the pinned stack, and the CI matrix is complete — this ticket's green is the milestone's definition of done.

**Blocked by:** 09 (consumer ack), 10 (long-poll waiting contract), 11 (startup reconciliation).

**Status:** ready-for-agent

- [ ] Automated Compose smoke executes the full accepted sequence: start stack → create Telegram endpoint (CLI) → send signed fixture → repeat and prove exactly one stored message → claim (`wait_ms=0`) → acknowledge → repeat acknowledgement and receive the recorded result → prove the queue empty → check metrics and health → restart the stack keeping the Valkey volume → verify readiness, the persisted endpoint, and that a repeated webhook is still deduplicated
- [ ] Smoke asserts at the storage seam where HTTP cannot see (single stored message, tombstone phases, success metadata)
- [ ] CI integration job against the pinned Valkey container is obligatory (local runs still skip without `HOOKRELAY_TEST_VALKEY_URL`); the complete suite runs under gofmt, unit, race, vet, govulncheck
- [ ] Production container build job publishes/verifies the distroless image with embedded version metadata; Compose healthcheck uses the binary `healthcheck` command
- [ ] Smoke failure fails CI; a green pipeline is the Milestone 1 exit criterion

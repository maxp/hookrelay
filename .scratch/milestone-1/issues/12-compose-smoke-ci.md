# 12: Compose smoke and complete CI

**What to build:** The whole Milestone 1 vertical slice is proven automatically: the 10-step Compose smoke runs in CI against the pinned stack, and the CI matrix is complete — this ticket's green is the milestone's definition of done.

**Blocked by:** 09 (consumer ack), 10 (long-poll waiting contract), 11 (startup reconciliation).

**Status:** done

- [x] Automated Compose smoke executes the full accepted sequence: start stack → create Telegram endpoint (CLI) → send signed fixture → repeat and prove exactly one stored message → claim (`wait_ms=0`) → acknowledge → repeat acknowledgement and receive the recorded result → prove the queue empty → check metrics and health → restart the stack keeping the Valkey volume → verify readiness, the persisted endpoint, and that a repeated webhook is still deduplicated
- [x] Smoke asserts at the storage seam where HTTP cannot see (single stored message, tombstone phases, success metadata)
- [x] CI integration job against the pinned Valkey container is obligatory (local runs still skip without `HOOKRELAY_TEST_VALKEY_URL`); the complete suite runs under gofmt, unit, race, vet, govulncheck
- [x] Production container build job publishes/verifies the distroless image with embedded version metadata; Compose healthcheck uses the binary `healthcheck` command
- [x] Smoke failure fails CI; a green pipeline is the Milestone 1 exit criterion

## Comments

- 2026-09-29: Implemented `scripts/smoke.sh`: builds the image, generates secrets with the binary into a temporary directory, and runs the stack under its own Compose project with an override (production environment with the required rate limits, peak rate, and HTTPS admin origin — so the AOF `everysec` + `noeviction` gate is exercised; free loopback ports via `ports: !override`; secrets from the temp dir). It executes the full sequence: CLI endpoint create inside the container → signed fixture twice (exactly one `hr1:m:*`, queue length 1) → claim `wait_ms=0` (tombstone `active`) → ack and repeated ack (identical body, tombstone `acknowledged`, success metadata `recipient_scope`/`attempt_count`) → empty queue (no `hr1:r:*`, no blobs, counter 0, claim `204`) → metrics (accepted, duplicate, acknowledged attempts) and health (live, accepting) → `compose down` + `up --wait` keeping the volume → readiness, `admin webhook get`, repeated webhook still deduplicated with no new message, no reconciliation findings. Any mismatch prints the hookrelay logs and exits non-zero; cleanup removes containers, volume, network, and the built image.
- CI: new `smoke` job runs the script; the `image` job now verifies the embedded version/commit/build time via `hookrelay version --json` and a non-root runtime user (distroless `65532`). The obligatory Valkey `integration` job, `gofmt`, `unit`, `race`, `vet`, and `govulncheck` jobs already cover the complete suite; the Compose healthcheck already uses `/hookrelay healthcheck`. The image is verified, not published: publishing to a registry needs a registry decision and credentials, outside this ticket's exit criterion.
- Verified locally: the smoke passes end to end; the image verification step passes with a simulated `GITHUB_SHA`. The milestone exit criterion — a green pipeline — requires pushing this work.


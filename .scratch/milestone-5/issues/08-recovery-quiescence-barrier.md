# 08: Recovery quiescence barrier

**What to build:** Prevent the existing complete-model recovery pass from racing live API and maintenance transitions. Ticket 06's concurrency review found that withdrawing readiness alone does not drain already-open listeners or stop maintenance; stale message locators can block healthy Recipients.

**Blocked by:** 05.

**Status:** done

- [x] Record the recovery barrier contract before implementation
- [x] Gate public work while unready and reject new storage-backed Admin API calls during reconciliation, while keeping health, metrics, and static UI reachable
- [x] Drain already-admitted requests and maintenance rounds before scanning; prohibit new background rounds until readiness returns
- [x] Run registered due transitions inside the exclusive recovery phase
- [x] Test the HTTP/maintenance barrier and the stale-locator race motivating it
- [x] Update operational documentation to distinguish readiness withdrawal from completed quiescence

## Comments

- 2026-09-30: Added from ticket 06's source/code review, not a new periodic checker. The current `reconcileMessages` discovery stores queue positions before script validation; ack/replay between those steps can create false isolation. Recovery therefore needs an explicit in-process quiescence barrier. Multi-process coordination remains out of scope.
- 2026-09-30: Implemented the in-process admission/drain barrier in `internal/app/recovery_barrier.go`, wrapped storage-backed HTTP routes, and wired `Readiness.RunMaintenanceRound` into the production maintenance loop. The five-minute pass deadline includes draining; cancellation never starts a scan. Tests cover admitted request/round drain, retry/correlation headers, diagnosis/health/UI availability, cancelled drain and reacquisition, skipped background rounds, and the real-Valkey stale-locator schedule motivating quiescence. Full tests/race/vet and two Compose smokes passed. No serving-time scan, counter repair, token reconstruction, or multi-process protocol was added.

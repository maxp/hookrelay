# 07: Milestone 5 smoke and documentation

**What to build:** Extend the Compose smoke with one recoverable complete-model inconsistency, document the final reconciliation policy, and close Milestone 5.

**Blocked by:** 05, 06, 08.

**Status:** done

- [x] Smoke keeps the Valkey volume, removes one endpoint Bot Identity membership and global listing member/score, restarts, and proves readiness repairs both
- [x] Smoke rechecks endpoint list/Bot Identity list plus the existing delivery, DLQ, session, health, and metrics path after recovery
- [x] Assert reconciliation findings/metrics are bounded and no secret-bearing data is printed
- [x] Update `README.md`, `storage.md`, `platform.md`, `implementation-milestones.md`, `open-questions.md`, and any runbook affected by the accepted policy
- [x] Record the periodic consistency decision and remove the corresponding open question only when it is genuinely resolved
- [x] Mark the Milestone 5 spec and all implementation tickets done after formatter, linter, tests, and two green smoke runs

## Comments

- 2026-09-30: Extended `scripts/smoke.sh` with a stopped-writer fault injection removing the remaining endpoint's Bot Identity membership and global listing, then a persisted-volume restart alongside two dead letters and a valid browser session. It proves restoration and authoritative score, persisted acknowledged delivery/session/CSRF state, both repaired-only bounded metrics, idle reconciliation, and the existing cookie replay/payload/delete/audit/logout/UI path. Diagnostics are checked for secret/token/payload leakage; payload preservation is tested without printing content. Two consecutive full green Compose runs completed after correcting the diagnostic allowlist for zero-valued bounded findings. Final checks: gofmt, `go test ./...`, `go test -race ./...`, `go vet ./...`, `git diff --check`, `bash -n scripts/smoke.sh`; integration tests used real Valkey 9.1.2, and Compose used production AOF/noeviction.

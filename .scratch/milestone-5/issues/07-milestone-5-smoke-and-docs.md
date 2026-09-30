# 07: Milestone 5 smoke and documentation

**What to build:** Extend the Compose smoke with one recoverable complete-model inconsistency, document the final reconciliation policy, and close Milestone 5.

**Blocked by:** 05, 06.

**Status:** needs-info

- [ ] Smoke keeps the Valkey volume, removes one endpoint Bot Identity membership and global listing member/score, restarts, and proves readiness repairs both
- [ ] Smoke rechecks endpoint list/Bot Identity list plus the existing delivery, DLQ, session, health, and metrics path after recovery
- [ ] Assert reconciliation findings/metrics are bounded and no secret-bearing data is printed
- [ ] Update `README.md`, `storage.md`, `platform.md`, `implementation-milestones.md`, `open-questions.md`, and any runbook affected by the accepted policy
- [ ] Record the periodic consistency decision and remove the corresponding open question only when it is genuinely resolved
- [ ] Mark the Milestone 5 spec and all implementation tickets done after formatter, linter, tests, and two green smoke runs

## Comments

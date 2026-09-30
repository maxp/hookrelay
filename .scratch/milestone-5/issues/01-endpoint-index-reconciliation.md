# 01: Webhook Endpoint index reconciliation

**What to build:** Reconcile the Bot Identity membership Sets and global Webhook Endpoint listing from authoritative endpoint Hashes through `reconcile_endpoint_v1`.

**Blocked by:** none.

**Status:** done

- [x] Register and implement `reconcile_endpoint_v1` exactly per the Milestone 5 spec, including bounded statuses/reasons and pre-write validation
- [x] Replace administrative fail-only index drift checks with bounded `SCAN`/`SSCAN`/`ZSCAN` reconciliation in the specified reverse-cleanup-then-restore order
- [x] Restore missing Bot Identity membership and global listing members/scores; remove malformed, orphan, and mismatched derived members
- [x] Keep malformed/wrong-typed authoritative endpoint state unchanged and hold readiness; enforce the 100-endpoint Bot Identity limit before restoring membership
- [x] Best-effort audit and bounded `hookrelay_consistency_issues_total` findings for repairs/removals; never expose credentials
- [x] Tests: every tuple, snapshot-equal refusals, second-pass idempotency, more than one scan page, `SCRIPT FLUSH`, full startup and lightweight recovery

## Comments

- 2026-09-30: Implemented `reconcile_endpoint_v1` and registered it as a reconciliation operation. Reconciliation scans Bot Identity Sets, the global listing, then endpoint Hashes in bounded pages; it removes malformed/orphan/mismatched reverse members, restores missing membership/listing entries, repairs listing scores, and leaves malformed authoritative records untouched under `webhook_endpoint_inconsistent`. Repairs append best-effort audit events and map to bounded consistency findings. Real-Valkey tests cover each repair/removal class, malformed snapshot-equal refusal, idempotent second pass, 20 endpoints with a scan batch of 3, and `SCRIPT FLUSH`. `go test ./...` passed both normally and against pinned Valkey 8.1.3.

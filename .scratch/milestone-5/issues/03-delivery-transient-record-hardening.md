# 03: Delivery transient-record hardening

**What to build:** Implement `reconcile_attempt_v1` and wire it after Recipient reconciliation to validate active Delivery Token and claim-operation cross-links for leased heads.

**Blocked by:** 02.

**Status:** done

- [x] Implement/register `reconcile_attempt_v1` exactly per the Milestone 5 spec
- [x] Invoke it for consistent/repaired non-due leased heads after `reconcile_recipient_v3`
- [x] Repair only the derived lease score; never rewrite/delete leased head, plaintext token, token record, or operation record
- [x] Block the named Recipient with `active_attempt_inconsistent` on safely isolatable mismatch; hold readiness on unlocatable/wrong-typed global state
- [x] Preserve accepted pre-v3 leased state without `delivery_token_digest` as `legacy` and TTL-only recovery
- [x] Tests: every tuple/reason, TTL bounds, snapshot-equal refusals, partial claim/extend residue, unrelated Recipient serviceability, second pass, `SCRIPT_FLUSH`

## Comments

- 2026-09-30: Implemented and registered `reconcile_attempt_v1`. Startup/recovery now follows a consistent or repaired non-due leased Recipient with a fenced active-attempt check, computes the plaintext-token SHA-256 only in Go, repairs only the lease score, and isolates token/claim-operation mismatches behind `active_attempt_inconsistent` without rewriting secret-bearing records. Pre-v3 leases stay `legacy`. Real-Valkey coverage pins all statuses and bounded reasons, TTL bounds, snapshot-preserved authoritative records, an unrelated Recipient claim, idempotent second pass, redacted logs, and `SCRIPT FLUSH`. `go test ./...`, `go test -race ./...`, and `go vet ./...` passed against the pinned Valkey 9.1.2 container.

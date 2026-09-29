# 04: Lease expiry

**What to build:** A lease past its deadline is expired by maintenance as a failed attempt: history `outcome=expired`, token `expired` phase, retry scheduling — so stalled consumers no longer hold a Recipient.

**Blocked by:** 03.

**Status:** done

- [x] `expire_lease_v1` per the spec; `not_due` for not-yet-due and non-leased state (stale member removal)
- [x] Maintenance processes due leases in their own batches; `ack`/`nack` after expiry → `409 stale_delivery_token`; claim replay after expiry → `claim_no_longer_active`
- [x] `delivery_lease_expired` event; `delivery_attempts_total{outcome=expired}`
- [x] Storage-seam and maintenance timing tests

## Comments

- 2026-09-29: Implemented. `claim_v3` stores `delivery_token_digest` in leased state so `expire_lease_v1` resolves the token record (Lua has no SHA-256); claim_v2-era leases expire without the token/op rewrite. `expire_lease_v1` (leases type → marker → types → not leased/not due → corrupt state → last attempt) appends `outcome=expired` history (same archive fold as nack), moves the head to `retry_wait`, the token to the `expired` phase (TTL 1 h), and the claim op to `no_longer_active`. `delivery.Maintenance` runs lease expiry before retry activation, each kind in its own batches, with drawn retry delays, the `delivery_lease_expired` event, and `attempts{outcome=expired}` via the shared `delivery.AttemptMetrics`.
- Temporary until ticket 05: a lease on the last attempt stays in place (`attempts_exhausted`, maintenance result `deferred`, one warning per batch).
- Known until ticket 12: startup reconciliation still holds readiness on a due lease (M1 behavior), and maintenance only starts after readiness, so a restart with an overdue lease stays not ready; and `reconcile_recipient_v1` blocks the `retry_wait` heads expiry now produces (see ticket 02).
- Temporary risk until ticket 05: deferred last-attempt leases stay oldest in `hr1:leases`; 500+ of them (batch 100 × 5) would starve newer due leases and pin the lease due-lag gauge.
- Review follow-up: wrong-type coverage for every key the script checks, the archive fold on the expiry path, not-leased stale-member removal before the global key types, `consumer_instance_id`/`duration_ms` on `delivery_lease_expired`, typed maintenance result labels, and `Attempts` required in both constructors.

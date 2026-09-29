# 04: Lease expiry

**What to build:** A lease past its deadline is expired by maintenance as a failed attempt: history `outcome=expired`, token `expired` phase, retry scheduling — so stalled consumers no longer hold a Recipient.

**Blocked by:** 03.

**Status:** ready-for-agent

- [ ] `expire_lease_v1` per the spec; `not_due` for not-yet-due and non-leased state (stale member removal)
- [ ] Maintenance processes due leases in their own batches; `ack`/`nack` after expiry → `409 stale_delivery_token`; claim replay after expiry → `claim_no_longer_active`
- [ ] `delivery_lease_expired` event; `delivery_attempts_total{outcome=expired}`
- [ ] Storage-seam and maintenance timing tests

# 03: Retry activation and the maintenance loop

**What to build:** Cooperative background maintenance that activates due retries in bounded batches, so a nacked message becomes claimable again with a new token and the next attempt number.

**Blocked by:** 02.

**Status:** ready-for-agent

- [ ] `activate_retry_v1` per the spec, including stale-member removal and marker refusal
- [ ] Maintenance loop: interval + jitter, batch size, max continuous batches, yield, started after readiness, stopped on shutdown, panic recovered at the seam (readiness false + shutdown)
- [ ] `maintenance_processed_total{kind,result}`, `maintenance_due_lag_seconds{kind}`, `maintenance_batch_size{kind}`, `maintenance_duration_seconds{kind}`
- [ ] Retried claim returns `attempt=2` with a new token; tests with short configured delays and tolerant windows

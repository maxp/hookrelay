# 03: Retry activation and the maintenance loop

**What to build:** Cooperative background maintenance that activates due retries in bounded batches, so a nacked message becomes claimable again with a new token and the next attempt number.

**Blocked by:** 02.

**Status:** done

- [x] `activate_retry_v1` per the spec, including stale-member removal and marker refusal
- [x] Maintenance loop: interval + jitter, batch size, max continuous batches, yield, started after readiness, stopped on shutdown, panic recovered at the seam (readiness false + shutdown)
- [x] `maintenance_processed_total{kind,result}`, `maintenance_due_lag_seconds{kind}`, `maintenance_batch_size{kind}`, `maintenance_duration_seconds{kind}`
- [x] Retried claim returns `attempt=2` with a new token; tests with short configured delays and tolerant windows

## Comments

- 2026-09-29: Implemented. `activate_retry_v1.lua` (marker → remove the stale `retries` member, `recipient_blocked`; types/encodings → `wrong_type`; state not `retry_wait` → remove the stale member, `not_due`; not yet due → `not_due` unchanged; else `ready` state keeping cycle/attempt, `retries` → `ready` with a fresh sequence). `DeliveryStore.DueRetries` reads oldest-first due members against Valkey time. `delivery.Maintenance` runs rounds (interval + jitter; batches of `BatchSize`, another only after a full one, yield after `MaxContinuousBatches`; batch work detached from the loop context so a started batch completes) with the four maintenance metrics. `app.Deps.Maintenance` starts loops after readiness, stops them at shutdown, and recovers a panic (redacted stack, readiness false, `ErrMaintenancePanic`, non-zero exit). Composed integration: claim → nack → maintenance → claim returns `attempt=2` with a new token.
- Closes ticket 02's "nothing activates a retry" gap; the reconciliation gap for a `retry_wait` head remains for ticket 12.
- Review follow-up: `activate_retry_v1` refuses a `retry_wait` state whose `head_message_id` is not the queue head (`wrong_type`, no mutation); batch timing uses the injected Clock; a failed due read counts as `failed`; maintenance stops after the listeners drain (platform order); a mutex orders the panic seam against the readiness monitor so a panic cannot be followed by restored readiness; stack redaction lives in `observability.RedactedStack` with a unit test.

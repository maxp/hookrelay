# 06: Ingestion rejection matrix and process protections

**What to build:** Every rejection path of the webhook contract and the process protections behave exactly as specified: malformed, oversized, and unsupported traffic is refused with bounded responses and correct metrics, capacity exhaustion is explicit and retryable, and no rejection ever creates state.

**Blocked by:** 05 (ingestion happy path — the route, verifier, and accept transition must exist).

**Status:** done

- [x] `413` for bodies over 256 KiB via both the declared-length precheck and the streaming limit; `400` for invalid JSON including a non-object top-level Telegram value; `415` for unsupported media types and any non-identity `Content-Encoding`
- [x] Capacity rejections in `accept_v1` → `503` + `Retry-After: 1` with nothing created: global queue cap, per-recipient queue cap, dedup record cap (bounded outcome labels: capacity rejection, dependency unavailable, internal error)
- [x] Blocked Recipient: ingestion for a blocked recipient → retryable `503`; other recipients unaffected
- [x] Process protections: in-flight webhook semaphore (default 100) acquired after route resolution and rate limiting but before body read, excess → `503` + `Retry-After: 1`; 10-second request-context deadline covering body reading
- [x] Error discipline: empty success bodies; error bodies empty or platform-minimal; no internal error envelopes, message ids, or Valkey details on webhook routes
- [x] `hookrelay_webhook_requests_total{webhook_type,outcome}` bounded outcomes asserted for every rejection class; `hookrelay_webhook_inflight` reflects the semaphore
- [x] `/health/accepting-webhooks` returns 200 while acceptance is enabled and 503 under global/per-recipient queue, dedup capacity, or Valkey unavailability; `hookrelay_accepting_webhooks` gauge matches; `/health/ready` unaffected by capacity pressure (draining stays safe)

## Comments

- 2026-09-28: Ticket 05 already returns 413/415/400/503 with bounded outcomes and `Retry-After: 1` (unit-tested with fakes). Also owned here: the `hookrelay_dedup_records` and `hookrelay_dedup_record_capacity` gauges, and replacing the readiness-following `accepting-webhooks` flag with the capacity stop conditions.
- 2026-09-28: Implemented. In-flight semaphore (`HOOKRELAY_MAX_INFLIGHT_WEBHOOKS`, default 100) acquired after endpoint resolution and before the body read, excess → `503` + `Retry-After: 1` with outcome `overloaded`; `hookrelay_webhook_inflight` gauge. Request deadline 10 s via request context plus connection read deadline, so a stalled body gets `408` (`request_timeout`); other body read failures are `400` `body_read_failed` rather than `invalid_json`. Global acceptance: `valkey.MessageAcceptor.Capacity` reads the queued counter and live dedup records (same `ZCOUNT` window as `accept_v1`); `ingestion.Handler.AcceptingWebhooks` evaluates the stop conditions and refreshes `hookrelay_dedup_records` / `hookrelay_dedup_record_capacity`; the app re-evaluates it on every one-second gate run and exports `hookrelay_accepting_webhooks`; a global or dedup capacity rejection flips acceptance off immediately. Gate failure (Valkey unavailable) turns acceptance off.
- Deviation from the ticket text, following platform.md ("A full individual Recipient likewise does not make the whole process unready; requests for that Recipient receive their own retryable capacity response"): a full per-Recipient queue rejects only that Recipient and does not flip `/health/accepting-webhooks`. The memory-percent stop remains Milestone 2 per the spec.
- New bounded outcomes `overloaded`, `request_timeout`, `body_read_failed` documented in platform.md. Tests: semaphore with a blocked acceptor, stalled-body deadline over a real TCP connection, acceptance probe and immediate stop signal, capacity (per-recipient, global, dedup) and blocked-Recipient rejections through HTTP over real Valkey with no key changes, app health/gauge behavior under a stop condition and recovery. Live-verified with `HOOKRELAY_MAX_QUEUED_MESSAGES=2`.

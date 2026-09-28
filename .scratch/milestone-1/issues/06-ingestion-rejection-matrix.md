# 06: Ingestion rejection matrix and process protections

**What to build:** Every rejection path of the webhook contract and the process protections behave exactly as specified: malformed, oversized, and unsupported traffic is refused with bounded responses and correct metrics, capacity exhaustion is explicit and retryable, and no rejection ever creates state.

**Blocked by:** 05 (ingestion happy path — the route, verifier, and accept transition must exist).

**Status:** ready-for-agent

- [ ] `413` for bodies over 256 KiB via both the declared-length precheck and the streaming limit; `400` for invalid JSON including a non-object top-level Telegram value; `415` for unsupported media types and any non-identity `Content-Encoding`
- [ ] Capacity rejections in `accept_v1` → `503` + `Retry-After: 1` with nothing created: global queue cap, per-recipient queue cap, dedup record cap (bounded outcome labels: capacity rejection, dependency unavailable, internal error)
- [ ] Blocked Recipient: ingestion for a blocked recipient → retryable `503`; other recipients unaffected
- [ ] Process protections: in-flight webhook semaphore (default 100) acquired after route resolution and rate limiting but before body read, excess → `503` + `Retry-After: 1`; 10-second request-context deadline covering body reading
- [ ] Error discipline: empty success bodies; error bodies empty or platform-minimal; no internal error envelopes, message ids, or Valkey details on webhook routes
- [ ] `hookrelay_webhook_requests_total{webhook_type,outcome}` bounded outcomes asserted for every rejection class; `hookrelay_webhook_inflight` reflects the semaphore
- [ ] `/health/accepting-webhooks` returns 200 while acceptance is enabled and 503 under global/per-recipient queue, dedup capacity, or Valkey unavailability; `hookrelay_accepting_webhooks` gauge matches; `/health/ready` unaffected by capacity pressure (draining stays safe)

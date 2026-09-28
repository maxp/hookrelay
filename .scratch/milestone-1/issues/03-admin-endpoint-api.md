# 03: Admin endpoint API vertical

**What to build:** An operator can create and read a Telegram Webhook Endpoint through the raw Admin API, with the state change and its mandatory audit append committed as one atomic Lua operation in real Valkey. This ticket also establishes the Valkey adapter core, the embedded script registry, the typed status parser, and the real-Valkey integration test harness that every later storage ticket reuses.

**Blocked by:** 02 (application scaffold).

**Status:** done

- [ ] `POST /admin/v1/webhooks` per the Admin API contract: strict 16 KiB / depth-40 / unknown-fields-rejected body validation; required `webhook_type`, `bot_id`, credential; optional identifier with server-side `wh_<base64url-128-bit>` generation; bounded Telegram credential-kind allowlist (`secret_token`, 1–256 chars); `bot_id` canonical decimal ≤ 20 digits
- [ ] `endpoint_create_v1` Lua transition: endpoint Hash + Bot Identity SET + global listing ZSET + audit Stream XADD (MAXLEN ~1,000,000) in one script; pre-write checks before any write; Valkey `TIME` for created/updated/audit timestamps
- [ ] Responses: `201` + `Location` + `ETag "<generation_id>:<config_version>"` + safe body (credential kind and `configured: true`, never the value); `409` identifier conflict; `409 bot_endpoint_limit_exceeded` at 100 endpoints per Bot Identity; bounded error envelope
- [ ] `GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}`: safe metadata + `ETag`; missing endpoint `404`
- [x] Admin Bearer authentication: constant-time comparison, uniform `401 unauthenticated` for missing and wrong secrets; rejected authentication audited best effort and never converted into access
- [x] Valkey adapter core: embedded versioned scripts (`.lua` + `go:embed`), registry with contract version and body SHA-256, startup `SCRIPT LOAD` before readiness, `EVALSHA` with one `EVAL` retry on `NOSCRIPT`, typed parser rejecting unknown statuses and shapes, ambiguous transport errors never retried blindly
- [x] Integration test harness against a real pinned Valkey (`HOOKRELAY_TEST_VALKEY_URL`; skip locally when absent, obligatory CI job): every status tuple variant, every affected key asserted after success, absence of mutation on every precondition failure, `SCRIPT FLUSH`/`NOSCRIPT` reload path, parser rejection tests
- [ ] `/health/ready` gated on Valkey availability, script load, and basic known-structure validation; admin listener starts before the gate, public listener opens only after it
- [x] Restart with the persistent Valkey volume keeps the endpoint readable; the audit event appears exactly once

## Comments

- 2026-09-28: Implemented. New packages: valkey (adapter core: eager dial, script registry with body SHA-256, EVALSHA + single EVAL on NOSCRIPT, typed status parser refusing unknown statuses/shapes, production persistence checks (AOF everysec + noeviction) and structure validation behind the readiness gate, loss monitor at a 1s cadence re-running the full gate), administration (service + HTTP transport: strict 16 KiB/depth-40/unknown-fields body decoding, bounded validation incl. Telegram secret_token 1–256 charset and bot_id canonical decimal, 201/Location/ETag/safe-body contract, uniform 401 with best-effort rejected-auth audit, conflict and bot-limit 409s), gen (UUIDv7 + crypto base64url), ingestion (Webhook Type registry: telegram → secret_token; Verifier/Converter join with the ingestion slice). The public listener now opens only after the first successful gate; loss of Valkey withdraws readiness and recovery re-gates. Integration harness: HOOKRELAY_TEST_VALKEY_URL (skip locally, obligatory CI job added) with per-package database isolation (FLUSHDB, administration owns DB 1) and tail-based audit assertions. Live-verified: create (server-generated wh_ id, Valkey TIME timestamps), ETag-stable reads across restart, 409 conflict, uniform 401, audit exactly-once (create + rejected auth), poisoned-structure state honestly blocks readiness and create with 503.
- Wiring notes: the ingestion registry satisfies the administration TypeCatalog through a narrow adapter in the composition; endpoint storage crosses the boundary via administration.EndpointRepository implemented by valkey.NewEndpointStore. google/uuid pinned for UUIDv7 behind the gen interface.

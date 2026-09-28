# 03: Admin endpoint API vertical

**What to build:** An operator can create and read a Telegram Webhook Endpoint through the raw Admin API, with the state change and its mandatory audit append committed as one atomic Lua operation in real Valkey. This ticket also establishes the Valkey adapter core, the embedded script registry, the typed status parser, and the real-Valkey integration test harness that every later storage ticket reuses.

**Blocked by:** 02 (application scaffold).

**Status:** ready-for-agent

- [ ] `POST /admin/v1/webhooks` per the Admin API contract: strict 16 KiB / depth-40 / unknown-fields-rejected body validation; required `webhook_type`, `bot_id`, credential; optional identifier with server-side `wh_<base64url-128-bit>` generation; bounded Telegram credential-kind allowlist (`secret_token`, 1–256 chars); `bot_id` canonical decimal ≤ 20 digits
- [ ] `endpoint_create_v1` Lua transition: endpoint Hash + Bot Identity SET + global listing ZSET + audit Stream XADD (MAXLEN ~1,000,000) in one script; pre-write checks before any write; Valkey `TIME` for created/updated/audit timestamps
- [ ] Responses: `201` + `Location` + `ETag "<generation_id>:<config_version>"` + safe body (credential kind and `configured: true`, never the value); `409` identifier conflict; `409 bot_endpoint_limit_exceeded` at 100 endpoints per Bot Identity; bounded error envelope
- [ ] `GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}`: safe metadata + `ETag`; missing endpoint `404`
- [ ] Admin Bearer authentication: constant-time comparison, uniform `401 unauthenticated` for missing and wrong secrets; rejected authentication audited best effort and never converted into access
- [ ] Valkey adapter core: embedded versioned scripts (`.lua` + `go:embed`), registry with contract version and body SHA-256, startup `SCRIPT LOAD` before readiness, `EVALSHA` with one `EVAL` retry on `NOSCRIPT`, typed parser rejecting unknown statuses and shapes, ambiguous transport errors never retried blindly
- [ ] Integration test harness against a real pinned Valkey (`HOOKRELAY_TEST_VALKEY_URL`; skip locally when absent, obligatory CI job): every status tuple variant, every affected key asserted after success, absence of mutation on every precondition failure, `SCRIPT FLUSH`/`NOSCRIPT` reload path, parser rejection tests
- [ ] `/health/ready` gated on Valkey availability, script load, and basic known-structure validation; admin listener starts before the gate, public listener opens only after it
- [ ] Restart with the persistent Valkey volume keeps the endpoint readable; the audit event appears exactly once

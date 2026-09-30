# Administrative API

The administrative API is exposed only on the protected administrative listener and requires the shared Admin Secret or an authenticated administrative browser session. All field names use `snake_case`. State-changing requests are audited without credentials, payloads, Authorization headers, or complete request bodies. Administrative mutations also emit feature events under the [structured-log contract](platform.md#structured-log-contract); Webhook Endpoint events may include credential kind but not credential values, lengths, or fingerprints. Critical administrative mutations require a successful Valkey audit append in the same Lua operation as the state change: this covers Webhook Endpoint creation, enablement, disablement, and deletion, administrative DLQ replay and permanent deletion, Admin Secret generation changes, and preconditioned clearing of a Recipient block. The API confirms success only after both state and audit are written. Standard-output logging remains best effort and cannot substitute for the Valkey audit record. Lua errors and lost responses can leave an uncertain outcome rather than an automatic rollback; see the [audit storage contract](storage.md#administrative-audit).

## Audit failure policies

Rejected administrative authentication is audited best effort. A failed audit write does not change the ordinary refusal response and never permits access. In particular, missing or incorrect Admin Secrets still receive the same `401 unauthenticated` response rather than success or an audit-related error.

Privileged DLQ payload inspection requires authentication and a confirmed audit append to Valkey before any payload content is sent. If audit persistence fails or cannot be confirmed, return `503` without disclosing the payload. The event describes the beginning of authorized access, not confirmed receipt of the response by the client. The event contains safe metadata only, never a copy of the payload.

Pprof is an explicit diagnostic exception: when profiling is enabled, valid Admin Bearer authentication remains mandatory, but access logging to standard output and audit persistence in Valkey are best effort. A Valkey outage or audit-write failure does not itself prevent profile collection. Browser sessions are still not accepted for profiling, and the existing concurrency and duration limits remain in force. This exception does not weaken the mandatory audit for critical administrative mutations or DLQ payload inspection.

## Browser session API

```http
POST   /admin/v1/session
GET    /admin/v1/session
DELETE /admin/v1/session
```

The administrative session cookie is named `hookrelay_admin`. In production it is set with `Secure`, `HttpOnly`, `SameSite=Strict`, `Path=/`, and no `Domain`. Local HTTP development may omit `Secure`; this mode must be explicitly configured and the administrative listener must bind only to loopback. The first version deliberately uses a normal cookie name rather than a `__Host-` prefix for consistent localhost behavior.

Login accepts a JSON body limited to 16 KiB so the configured maximum 8192-byte Admin Secret plus JSON framing always fits:

```json
{
  "admin_secret": "..."
}
```

The body and secret are never logged or included in audit. Successful login creates the administrative session and appends its required audit event in the same Lua operation. Only after confirmed success does the API return `204 No Content` and set the administrative session cookie; an audit failure or uncertain operation result must not issue the cookie or confirm login. Missing and incorrect secrets receive the same `401 unauthenticated` response and retain best-effort audit. Direct non-browser clients may authenticate Admin API requests with the Admin Secret as a Bearer token instead of creating a session.

The session token is 256 random bits encoded as base64url without padding. Only its SHA-256 digest is stored in:

```text
hr1:admin_session:<session_digest>
```

with creation, last-seen, idle-expiry, absolute-expiry, Admin Secret `generation_id`, and the CSRF token stored in plaintext. The session digest also belongs to the `hr1:admin_sessions` expiry/capacity index; session creation, refresh, and logout maintain the Hash and index atomically. The session token itself is stored only as a SHA-256 digest; the CSRF token is never logged, included in audit, or returned anywhere except `GET /admin/v1/session` for that authenticated browser session. Sessions are not bound to IP address or User-Agent. The one-hour idle expiry is refreshed no more frequently than every five minutes; the twelve-hour absolute expiry never moves. An expired session or a session whose generation differs from the current `hr1:admin_auth` generation is invalid regardless of cleanup.

At startup, hookrelay derives an HMAC-SHA-256 generation tag from the configured Admin Secret, the persistent random generation salt, and the fixed context `hookrelay-admin-session-generation-v1`. A mismatch with `hr1:admin_auth` means the Admin Secret changed. One bounded Lua operation replaces the generation ID and tag, removes every indexed session and session Hash, and appends the mandatory rotation audit event before readiness. No plaintext secret or reusable secret value is stored. An uncertain or partially observed rotation prevents readiness pending operator reconciliation.

`GET /admin/v1/session` returns authentication state, idle and absolute expiry, and a stable random 128-bit CSRF token for the session. Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` requests require that token in `X-CSRF-Token` and require an exact allowed `Origin`. Bearer-authenticated requests do not require CSRF. Safe `GET` and `HEAD` requests never mutate application resources; successful authentication may perform the documented throttled update of session access metadata and its expiry index.

`DELETE /admin/v1/session` prioritizes revocation over audit persistence. After confirmed deletion or absence of the session, it clears the cookie and returns `204`; repeating logout also returns `204`. Audit of actual logout and session expiry is best effort and must not prevent revocation or keep an expired session valid. If Valkey is unavailable or revocation cannot be confirmed, logout returns `503` rather than claiming successful server-side revocation.

Login attempts use process-local per-source-IP and global token buckets. Initial limits are 5 attempts per minute with burst 5 per IP and 60 attempts per minute with burst 20 globally. A rejected rate-limited attempt returns `429 rate_limit_exceeded` with a bounded `Retry-After`. If the 100-session capacity is exhausted after expired-session cleanup, a valid new login returns `429 session_capacity_exceeded` without evicting an existing session. Outcomes are recorded in `hookrelay_admin_login_attempts_total{outcome}` with bounded success, failure, and rate-limited values and without IP labels. Failed CSRF or `Origin` validation returns `403 forbidden`.

## Runtime configuration inspection

```http
GET /debug/config
```

This read-only diagnostic route is exposed only on the administrative listener and accepts Admin Bearer authentication or an authenticated administrative browser session. It returns effective non-secret configuration with value sources (`default`, `env`, `flag`, or `file`), secret-configured flags instead of values, and a Valkey URL with redacted userinfo. It may include Webhook Endpoint counts but never credential values. It provides no runtime configuration mutation. See [process configuration](configuration.md#configuration-inspection).

## Command-line client

The main binary includes the server, administrative client, generators, version reporting, and container healthcheck:

```text
hookrelay serve
hookrelay admin ...
hookrelay generate ...
hookrelay version
hookrelay healthcheck ...
```

`hookrelay admin` is an HTTP client for the Admin API and never edits Valkey directly.

The Admin Secret is resolved in this order: `--admin-secret-file`, `HOOKRELAY_ADMIN_SECRET_FILE`, `HOOKRELAY_ADMIN_SECRET`, then a hidden interactive prompt when stdin is a TTY. The secret is never accepted as a command-line value. Admin URL resolution is `--admin-url`, `HOOKRELAY_ADMIN_URL`, then `http://127.0.0.1:8081`.

Webhook commands are:

```text
hookrelay admin webhook create
hookrelay admin webhook list
hookrelay admin webhook get
hookrelay admin webhook enable
hookrelay admin webhook disable
hookrelay admin webhook delete
hookrelay admin bot webhooks
```

Operational commands mirror the accepted Admin routes as their milestones arrive:

```text
hookrelay admin operations summary
hookrelay admin recipients list --status ...
hookrelay admin recipients inspect-block
hookrelay admin recipients clear-block
hookrelay admin message delivery-state
hookrelay admin dlq list
hookrelay admin dlq get
hookrelay admin dlq payload
hookrelay admin dlq replay
hookrelay admin dlq delete
hookrelay admin audit list
```

Create accepts credential input from `--credential-file` or `HOOKRELAY_WEBHOOK_CREDENTIAL`; configuring both is an error. A command-line credential value flag is not provided. Environment input is intended for automation but remains sensitive process configuration and must not be printed or logged.

Mutations of existing resources first read the resource and its `ETag`, then send `If-Match`. A stale precondition is reported and is not automatically retried. The `--yes` flag suppresses interactive confirmation but never bypasses entity versions or other safety checks.

For `hookrelay admin webhook create`, the CLI generates a `wh_<base64url-128-bit-random>` identifier before `POST` unless one was supplied, even though the raw HTTP API still permits omission and server-side generation. This gives the CLI a known identifier for recovery after a lost response. The CLI exposes that identifier on an uncertain outcome; it never silently regenerates it and blindly retries creation.

Neither the CLI nor the Admin API uses Admin operation IDs in the first version. After a transport failure or uncertain Lua outcome, the CLI does not blindly retry a mutation: it reads the endpoint by its known identity, compares safe observable state and, where applicable, the entity version, and reports successful observation of the desired state with an explicit warning when it matches. For delete it checks for absence; for enable/disable it checks the flag and ETag. If the state cannot be read or does not establish the desired result, it reports an uncertain outcome and requires operator reconciliation. Observing the desired state does **not** confirm that the original request performed the mutation, that the stored credential matches an unreadable create input, or that a partially executed Lua operation appended its mandatory audit event. The CLI must not claim confirmed mutation success on this basis; suspected partial execution requires checking persisted state **and** audit before another mutation. A raw API caller that omitted the identifier may search the list, but might be unable to identify the created endpoint unambiguously. Raw API clients follow the same no-blind-retry rule.

Output formats are `table` and `json`; table is the TTY default. Standard output contains only results, while progress and warnings use standard error. Secrets are never included.

Generators are:

```text
hookrelay generate consumer-secret
hookrelay generate admin-secret
hookrelay generate webhook-id
```

They write only the generated value and newline to standard output or support `--output-file` with mode `0600`. Existing files are not overwritten without explicit confirmation.

## Webhook Endpoint resource

A Webhook Endpoint contains one immutable Webhook Endpoint Identity, one immutable Bot Identity, one immutable verification credential, and a mutable enabled flag. Multiple Webhook Endpoints may belong to the same Bot Identity. Credential replacement creates a new endpoint for that Bot Identity, moves the external bot-platform webhook configuration to the new URL, and then disables and deletes the old endpoint.

Read responses never return credential values. They expose only credential kind and configured state.

Concurrent mutation uses strong HTTP entity versions:

```http
ETag: "<generation_id>:<config_version>"
If-Match: "<generation_id>:<config_version>"
```

`generation_id` is a UUIDv7 assigned on every creation of a Webhook Endpoint, including when an externally supplied Webhook Identifier is reused after deletion. `config_version` increments only within that generation. `POST`, `GET one`, and `PATCH` return `ETag`. `PATCH` and deletion of an existing resource require `If-Match`; wildcard and weak entity tags are unsupported. A missing precondition returns `428 Precondition Required`, and a stale or previous-generation value returns `412 Precondition Failed`. Webhook Type, Webhook Identifier, Bot Identifier, generation, and credential cannot be changed after creation.

## Create

```http
POST /admin/v1/webhooks
```

The body is limited to 16 KiB, maximum JSON nesting depth 40, and strictly validated as JSON:

```json
{
  "webhook_type": "telegram",
  "webhook_identifier": "wh_optional",
  "bot_id": "123456",
  "credential": {
    "kind": "secret_token",
    "value": "secret-value"
  },
  "enabled": true
}
```

`webhook_type`, `bot_id`, and credential are required. `webhook_identifier` is optional, and `enabled` defaults to true. Unknown fields are rejected. Duplicate JSON keys follow Go `encoding/json` last-value-wins behavior and are not rejected. Credential value is 1–8192 bytes and credential kind must belong to the bounded allowlist declared by the selected Webhook Type adapter; an unknown kind returns `400 unsupported_credential_kind`.

If the identifier is omitted, hookrelay generates:

```text
wh_<base64url-128-bit-random>
```

The operation checks the Bot Identity membership Set before its first write. If it already contains 100 endpoints, creation returns `409 bot_endpoint_limit_exceeded`. Otherwise the operation creates the endpoint Hash, adds membership to the Bot Identity Set, adds the endpoint to the global listing index, and appends the required audit event in the same Lua operation. Identifier conflicts return `409`.

Success returns `201 Created`, a `Location` header, an entity `ETag`, and a body:

```json
{
  "webhook_type": "telegram",
  "webhook_identifier": "wh_abc123",
  "bot_platform": "telegram",
  "bot_id": "123456",
  "enabled": true,
  "credential": {
    "kind": "secret_token",
    "configured": true
  },
  "generation_id": "0195c4d8-...",
  "config_version": 1,
  "created_ms": 1740000000000,
  "updated_ms": 1740000000000,
  "webhook_path": "/webhook/telegram/wh_abc123"
}
```

The credential value, Valkey keys, and deployment-specific public scheme and host are never returned.

## List

```http
GET /admin/v1/webhooks?limit=<n>&cursor=<opaque>
```

The endpoint uses cursor pagination over the global endpoint listing, ordered by creation time descending. `limit` defaults to 50 and is constrained to 1–200. The first version has no list filters.

The opaque cursor is base64url-encoded JSON carrying the last `created_ms` and Webhook Endpoint Identity so equal timestamps are stable. A malformed cursor returns `400 invalid_cursor`. Items contain safe endpoint metadata without credential values. The response contains an opaque `next_cursor` when more items remain.

## Get one

```http
GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}
```

The response contains safe endpoint metadata and an `ETag`. A missing endpoint returns `404`.

## Update enabled state

```http
PATCH /admin/v1/webhooks/{webhook_type}/{webhook_identifier}
If-Match: "<generation_id>:<config_version>"
```

The first version permits changing only the enabled flag:

```json
{
  "enabled": false
}
```

The same shape with `"enabled": true` re-enables a disabled endpoint. This `PATCH` route is the only public HTTP enable/disable mutation; the CLI's semantic `enable` and `disable` commands use it and do not introduce separate HTTP routes.

The mutation changes `enabled`, increments `config_version`, updates `updated_ms`, and appends the required audit event in the same Lua operation. Success returns `200`, a new `ETag`, and the complete current safe endpoint representation. Requesting the current value with a current `ETag` is a no-op: `200` with the unchanged `ETag` and representation, no version increment, and no audit event, so a reconciled retry cannot inflate the version. `If-Match` must be exactly one strong tag; weak tags, `*`, lists, and malformed values return `400 invalid_request`. The precondition is evaluated only for an existing endpoint, so a missing endpoint returns `404` with or without `If-Match`. Disabled endpoints retain their configuration and credential but externally behave like unknown endpoints and return the same `404`.

## Delete

```http
DELETE /admin/v1/webhooks/{webhook_type}/{webhook_identifier}
If-Match: "<generation_id>:<config_version>"
```

Deletion is permitted only for a disabled endpoint. It permanently removes the endpoint Hash, plaintext credential, Bot Identity Set membership, and global listing membership, and appends the required audit event in the same Lua operation. No Webhook Endpoint tombstone is created. An externally supplied Webhook Identifier may be created again later; random generated identifiers are not intentionally reused.

For an existing endpoint, missing `If-Match` returns `428`, a stale value returns `412`, and an enabled endpoint returns `409 endpoint_must_be_disabled`. A path that cannot name an endpoint (an unregistered Webhook Type or an invalid identifier shape) names an absent one and also returns `204`. Successful deletion returns `204 No Content`. Repeating deletion for an already absent endpoint also returns `204` and creates no additional audit event. This prevents a second deletion, but does not prove that an uncertain first execution appended its mandatory audit event; clients reconcile after a lost response rather than blindly retrying. The audit event for an actual deletion retains safe metadata without credential content.

## List Webhook Endpoints for a Bot Identity

```http
GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks
```

This endpoint reads the Bot Identity membership Set and returns safe metadata for every associated Webhook Endpoint. It supports the create-new-endpoint credential replacement flow. The first version does not paginate this normally small collection but enforces a hard maximum of 100 endpoints per Bot Identity.

## Operational and DLQ API surface

The accepted administrative route inventory is:

```http
GET    /admin/v1/operations/summary
GET    /admin/v1/recipient-states?status=<ready|leased|retry_wait|blocked>&limit=<n>&cursor=<opaque>
POST   /admin/v1/recipient-blocks/inspect
POST   /admin/v1/recipient-blocks/clear
GET    /admin/v1/messages/{message_id}/delivery-state
GET    /admin/v1/dead-letters?limit=<n>&cursor=<opaque>
GET    /admin/v1/dead-letters/{message_id}
POST   /admin/v1/dead-letters/{message_id}/payload
POST   /admin/v1/dead-letters/{message_id}/replay
DELETE /admin/v1/dead-letters/{message_id}
GET    /admin/v1/audit?limit=<n>&cursor=<opaque>
```

Recipient-state items carry `recipient`, `status`, and the index time under its own name (`ready_sequence`, `lease_expires_ms`, `retry_at_ms`, or `detected_ms` with the marker `reason_code`), ascending; the cursor is base64url JSON of the last score and member. Safe `GET` routes return metadata only and never payloads, credentials, Delivery Tokens, or unredacted secret-bearing diagnostics. Recipient-state results use structured Recipient fields rather than exposing internal `hr1:` keys. The `blocked` filter pages over the derived `hr1:blocked` index ordered by detection time. List routes use the common default limit 50 and range 1–200 with stable opaque cursors.

The block inspection request contains one structured Recipient:

```json
{
  "recipient": {
    "scope": "chat",
    "bot_platform": "telegram",
    "bot_id": "123456",
    "chat_id": "987654"
  }
}
```

It returns the marker, bounded queue-head and state metadata, presence of the referenced Canonical Message, memberships in the ready/lease/retry/blocked indexes, and a bounded list of violated invariants. It never returns payload or Delivery Token data. The clear request contains the same Recipient plus exact marker preconditions:

```json
{
  "recipient": {
    "scope": "chat",
    "bot_platform": "telegram",
    "bot_id": "123456",
    "chat_id": "987654"
  },
  "expected_detected_ms": 1740000000000,
  "expected_reason_code": "head_message_missing"
}
```

It succeeds with `204 No Content` only if those preconditions and all authoritative-state invariants match; it never repairs authoritative state. As for endpoint preconditions, missing `expected_*` values return `428 precondition_required` and a changed marker `412 precondition_failed`; a missing marker returns `404 recipient_block_not_found` and failed invariants `409 recipient_state_ambiguous` naming the first violated invariant. One Lua operation removes the marker, restores exactly the derived index implied by the verified state, and appends the mandatory audit event. See the [Recipient block recovery runbook](../runbooks/recipient-block-recovery.md).

`GET /admin/v1/messages/{message_id}/delivery-state` returns only `message_id`, `delivery_cycle`, the bounded state `queued`, `leased`, `retry_wait`, `dead_lettered`, or `acknowledged`, and safe queue-position classification. It exists in Milestone 2 so a replay with a lost response can be reconciled without exposing payloads or Delivery Tokens. `queue_position` is `head` or `behind_head` for a queued, leased, or retry-waiting message and absent otherwise; the Delivery Cycle of a message queued behind the head is its saved pending cycle (1 without replay history). `acknowledged` is reported only while the 24-hour compact success metadata is retained. A message with no retained state returns `404 message_not_found`; stored state that cannot be classified (for example attempt history without a saved pending pair) returns `409 recipient_state_ambiguous` naming the reason.

Payload inspection is deliberately `POST`, because it appends the mandatory access audit before returning the Canonical Message and therefore is not a safe read. Replay and permanent deletion use the mandatory state-change-plus-audit transitions. A missing Dead-letter Message returns `404 dead_letter_not_found`; an existing Recipient block returns `409 recipient_blocked`. Permanent deletion is idempotent for an absent message and returns `204` without another audit event, subject to the same uncertain-outcome warning as other administrative deletion.

Replay accepts:

```json
{
  "deduplication_conflict_resolution": "reject"
}
```

The field defaults to `reject`. If the original Deduplication Identity points to another message, `reject` returns `409 deduplication_conflict`; the explicit `keep_current` value permits replay while leaving the newer mapping unchanged. Replay never repoints that mapping to the older message. Success returns:

```json
{
  "status": "replayed",
  "message_id": "0195...",
  "delivery_cycle": 2,
  "queue_position": "head",
  "replayed_ms": 1740000000000,
  "deduplication_resolution": "not_conflicting"
}
```

`queue_position` is `head`, `after_active_head`, or `after_pending_replay` (behind earlier replays still waiting, which keep replay order); `deduplication_resolution` is `not_conflicting` or `kept_current`. The dead-letter list returns safe metadata (`message_id`, structured `recipient`, `dead_lettered_ms`, `dead_letter_reason`, `delivery_cycle`) newest first with an opaque cursor over the DLQ score and member; the single read adds the retained attempt history. A dead letter whose Canonical Message is missing, or whose stored structures have an unexpected type, refuses replay with `503 dependency_unavailable` (a definite refusal; reconciliation holds readiness for the missing message). Before replay, the CLI reads the current Delivery Cycle. After a lost response it never retries blindly: it reads the delivery-state route, which also reports a dead-lettered message with its cycle. A newer cycle in any state, including dead-lettered again, is reported as desired state observed (only replay advances the cycle) with audit confirmation still required; every other inconclusive combination is an uncertain outcome handled by the reconciliation runbook. The remaining operational views, payload command, permanent deletion, and audit UI contracts are completed with their later milestone rather than guessed by the storage adapter.

## Error envelope

Errors use:

```json
{
  "error": {
    "code": "endpoint_must_be_disabled",
    "message": "Disable the webhook endpoint before deleting it.",
    "request_id": "0195..."
  }
}
```

The initial bounded allowlist is:

```text
invalid_request
invalid_cursor
request_too_large
unsupported_media_type
unauthenticated
forbidden
rate_limit_exceeded
session_capacity_exceeded
bot_endpoint_limit_exceeded
webhook_endpoint_not_found
webhook_identifier_conflict
unsupported_webhook_type
unsupported_credential_kind
endpoint_must_be_disabled
dead_letter_not_found
message_not_found
deduplication_conflict
recipient_blocked
recipient_block_not_found
recipient_state_ambiguous
precondition_required
precondition_failed
dependency_unavailable
internal_error
```

## Removed credential-rotation API

The first version deliberately has no current/next credential model and no credential update or promotion endpoints. Credential replacement is performed by creating another Webhook Endpoint for the same Bot Identity and retiring the old endpoint.

# Administrative API

The administrative API is exposed only on the protected administrative listener and requires the shared Admin Secret or an authenticated administrative browser session. All field names use `snake_case`. State-changing requests are audited without credentials, payloads, Authorization headers, or complete request bodies. Administrative mutations also emit feature events under the [structured-log contract](platform.md#structured-log-contract); Webhook Endpoint events may include credential kind but not credential values, lengths, or fingerprints. Critical administrative mutations require a successful Valkey audit append in the same Lua operation as the state change: this covers Webhook Endpoint creation, enablement, disablement, and deletion, and administrative DLQ replay and permanent deletion. The API confirms success only after both state and audit are written. Standard-output logging remains best effort and cannot substitute for the Valkey audit record. Lua errors and lost responses can leave an uncertain outcome rather than an automatic rollback; see the [audit storage contract](storage.md#administrative-audit).

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

Login accepts a JSON body limited to 4 KiB:

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

with creation, last-seen, idle-expiry, absolute-expiry, and the CSRF token stored in plaintext. The session token itself is stored only as a SHA-256 digest; the CSRF token is never logged, included in audit, or returned anywhere except `GET /admin/v1/session` for that authenticated browser session. Sessions are not bound to IP address or User-Agent. The one-hour idle expiry is refreshed no more frequently than every five minutes; the twelve-hour absolute expiry never moves. An expired session is invalid regardless of whether cleanup or its best-effort audit event has completed.

`GET /admin/v1/session` returns authentication state, idle and absolute expiry, and a stable random 128-bit CSRF token for the session. Cookie-authenticated `POST`, `PUT`, `PATCH`, and `DELETE` requests require that token in `X-CSRF-Token` and require an exact allowed `Origin`. Bearer-authenticated requests do not require CSRF. Safe `GET` and `HEAD` requests never change state.

`DELETE /admin/v1/session` prioritizes revocation over audit persistence. After confirmed deletion or absence of the session, it clears the cookie and returns `204`; repeating logout also returns `204`. Audit of actual logout and session expiry is best effort and must not prevent revocation or keep an expired session valid. If Valkey is unavailable or revocation cannot be confirmed, logout returns `503` rather than claiming successful server-side revocation.

Login attempts use process-local per-source-IP and global token buckets. Initial limits are 5 attempts per minute with burst 5 per IP and 60 attempts per minute with burst 20 globally. Outcomes are recorded in `hookrelay_admin_login_attempts_total{outcome}` with bounded success, failure, and rate-limited values and without IP labels.

## Runtime configuration inspection

```http
GET /debug/config
```

This read-only diagnostic route is exposed only on the administrative listener and accepts Admin Bearer authentication or an authenticated administrative browser session. It returns effective non-secret configuration with value sources (`default`, `env`, `flag`, or `file`), secret-configured flags instead of values, and a Valkey URL with redacted userinfo. It may include Webhook Endpoint counts but never credential values. It provides no runtime configuration mutation. See [process configuration](configuration.md#configuration-inspection).

## Command-line client

The main binary includes the server, administrative client, and generators:

```text
hookrelay serve
hookrelay admin ...
hookrelay generate ...
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

The operation creates the endpoint Hash, adds membership to the Bot Identity Set, adds the endpoint to the global listing index, and appends the required audit event in the same Lua operation. Identifier conflicts return `409`.

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

The mutation changes `enabled`, increments `config_version`, updates `updated_ms`, and appends the required audit event in the same Lua operation. Success returns `200`, a new `ETag`, and the complete current safe endpoint representation. Disabled endpoints retain their configuration and credential but externally behave like unknown endpoints and return the same `404`.

## Delete

```http
DELETE /admin/v1/webhooks/{webhook_type}/{webhook_identifier}
If-Match: "<generation_id>:<config_version>"
```

Deletion is permitted only for a disabled endpoint. It permanently removes the endpoint Hash, plaintext credential, Bot Identity Set membership, and global listing membership, and appends the required audit event in the same Lua operation. No Webhook Endpoint tombstone is created. An externally supplied Webhook Identifier may be created again later; random generated identifiers are not intentionally reused.

For an existing endpoint, missing `If-Match` returns `428`, a stale value returns `412`, and an enabled endpoint returns `409 endpoint_must_be_disabled`. Successful deletion returns `204 No Content`. Repeating deletion for an already absent endpoint also returns `204` and creates no additional audit event. This prevents a second deletion, but does not prove that an uncertain first execution appended its mandatory audit event; clients reconcile after a lost response rather than blindly retrying. The audit event for an actual deletion retains safe metadata without credential content.

## List Webhook Endpoints for a Bot Identity

```http
GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks
```

This endpoint reads the Bot Identity membership Set and returns safe metadata for every associated Webhook Endpoint. It supports the create-new-endpoint credential replacement flow. The first version does not paginate this normally small collection but enforces a hard maximum of 100 endpoints per Bot Identity.

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
unauthenticated
forbidden
webhook_endpoint_not_found
webhook_identifier_conflict
unsupported_webhook_type
unsupported_credential_kind
endpoint_must_be_disabled
precondition_required
precondition_failed
dependency_unavailable
internal_error
```

## Removed credential-rotation API

The first version deliberately has no current/next credential model and no credential update or promotion endpoints. Credential replacement is performed by creating another Webhook Endpoint for the same Bot Identity and retiring the old endpoint.

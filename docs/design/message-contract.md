# Canonical message and ingestion contract

## JSON conventions

- JSON field names use `snake_case`.
- Timestamps are integer UTC Unix epoch milliseconds.
- Timestamp field names end in `_ms`.
- `received_ms` is mandatory.
- The first version does not include `schema_version`; it will be introduced if message-format evolution requires it.

## Webhook and bot identity

Webhook Type and Bot Platform are distinct concepts with a one-to-one mapping in the current implementation. A Webhook Endpoint Identity is:

```text
(Webhook Type, Webhook Identifier)
```

A Bot Identity is:

```text
(Bot Platform, Bot Identifier)
```

Each Webhook Endpoint Identity corresponds to exactly one Bot Identity, but one Bot Identity may have multiple Webhook Endpoint Identities. This permits credential replacement by creating a new endpoint for the same bot, switching the external bot-platform webhook configuration to the new URL and credential, and then disabling and deleting the old endpoint. Webhook Identifier and Bot Identifier remain different opaque values. Webhook Identifier is unique within its Webhook Type. Bot Identifier is assigned or defined by the Bot Platform.

A Recipient Identity is scope-specific:

```text
chat  = (Bot Platform, Bot Identifier, chat, Chat Identifier)
user  = (Bot Platform, Bot Identifier, user, User Identifier)
bot   = (Bot Platform, Bot Identifier, bot)
relay = (Bot Platform, Bot Identifier, relay)
```

Bot Identifier, Chat Identifier, and User Identifier are opaque, case-sensitive strings. Leading zeros and exact values are preserved unless the Bot Platform converter defines a canonical numeric representation. The common domain does not trim whitespace or change case; platform-specific canonicalization belongs to the Converter.

Identity-component validation is:

```text
bot_platform:
  1–32 ASCII characters
  pattern [a-z][a-z0-9_-]*

bot_id, chat_id, and user_id:
  1–128 UTF-8 bytes
  no colon
  no control characters
```

The serialized colon-delimited Recipient storage key is internal and is not included in the Canonical Message or Consumer API.

## Recipient scopes

`recipient.scope` has four values:

- `chat`: a known event for a platform-provided Chat Identifier;
- `user`: a known event without an applicable chat identity but with an explicitly selected end-user identity;
- `bot`: a known event that inherently applies to the Bot Identity rather than a chat or user;
- `relay`: a verified valid JSON payload that cannot be mapped to a known chat-, user-, or bot-scoped structure.

Chat scope includes only `chat_id`; user scope includes only `user_id`; those fields never appear together. Bot and relay scopes contain neither field because the scope itself distinguishes their per-Bot-Identity queues. Bot-, relay-, user-, and chat-scoped queues are independent and do not block one another.

Only verified payloads that are valid JSON may enter the relay scope. An unknown endpoint, failed verification, invalid JSON, or an oversized body does not create a Canonical Message.

## Canonical Message

One Webhook Request produces exactly one Canonical Message for exactly one Recipient. One request is never automatically split into a message batch.

The initial envelope is:

```json
{
  "message_id": "0195...",
  "received_ms": 1740000000123,
  "occurred_ms": 1740000000000,
  "recipient": {
    "scope": "chat",
    "bot_platform": "telegram",
    "bot_id": "123456",
    "chat_id": "987654"
  },
  "source_event_id": "123",
  "platform_event_type": "message",
  "payload": {}
}
```

Required fields:

- `message_id`;
- `received_ms`;
- `recipient.scope`;
- `recipient.bot_platform`;
- `recipient.bot_id`;
- the scope-specific Recipient Identifier: `recipient.chat_id` for chat or `recipient.user_id` for user; neither for bot or relay;
- `platform_event_type`;
- `payload`.

Optional fields:

- `occurred_ms`;
- `source_event_id`;
- `routing_issue`, for relay-scoped messages.

`message_id` is generated as UUIDv7 before the atomic acceptance operation. `occurred_ms`, when present, is the Bot Platform's event time and does not determine Delivery Queue order.

The Canonical Payload is the complete platform-specific JSON value. Unknown object fields are retained. Exact source bytes, whitespace, and key order are not retained as the payload representation. Duplicate object keys are not rejected and follow Go `encoding/json` last-value-wins semantics in the canonical representation. In the common contract, a valid unexpected top-level JSON value is preserved as one payload and sent to relay scope unless a Webhook Type adapter explicitly rejects that shape.

`platform_event_type` remains platform-specific. For an unknown structure, the Converter preserves the best safely extractable type or uses a reserved unknown value.

The initial bounded `routing_issue.code` allowlist is:

```text
unknown_event_structure
unknown_event_type
invalid_source_event_id
missing_chat_id
invalid_chat_id_type
invalid_chat_id_value
missing_user_id
invalid_user_id_type
invalid_user_id_value
unexpected_json_shape
```

An unrecognized internal reason is exposed as `unknown_event_structure`. New public codes require documentation and bounded-cardinality tests.

A relay-scoped message may contain:

```json
{
  "routing_issue": {
    "code": "missing_chat_id"
  }
}
```

The code is bounded and machine-readable. No exception text, stack trace, request body, or secret is included.

## Webhook route and request acceptance

The webhook ingestion route is:

```http
POST /webhook/{webhook_type}/{webhook_identifier}
```

Only `POST` is accepted by the common contract. Other methods receive `405 Method Not Allowed` with `Allow: POST`; a platform-specific verification challenge method requires an explicit adapter exception. CORS is not enabled for webhook routes.

`webhook_type` selects the Verifier and Converter and is 1–32 ASCII characters matching `[a-z][a-z0-9_-]*`. `webhook_identifier` resolves the Webhook Credential, configuration, and Bot Identity and is 1–128 ASCII characters matching `[A-Za-z0-9_-]+`. Slash, encoded slash, colon, whitespace, Unicode, dot segments, and empty values are rejected. Bot Identifier is not exposed in the route.

Unknown Webhook Type, unknown Webhook Identifier, and an identifier registered under another type all receive the same bounded `404` response so endpoint registration is not disclosed.

Every request receives a hookrelay-generated UUIDv7 `X-Request-Id` in success and error responses. It is distinct from `message_id` and is recorded in structured logs. Successful webhook responses do not expose `message_id`; accepted and duplicate requests use the same platform-compatible success response. Public webhook success bodies are empty. Error bodies are empty or the minimum platform-compatible response; detailed internal error envelopes are never exposed on webhook routes.

The HTTP request body hard limit is 256 KiB. A declared `Content-Length` above the limit is rejected before reading, but absent or acceptable length never replaces the actual streaming limit. Hookrelay reads at most 256 KiB plus one byte, computes SHA-256 during that read, verifies the exact bytes, and only after successful verification parses JSON and invokes the Converter. Exact request bytes are not retained after processing.

Each Webhook Type declares its accepted media types, with `application/json` as the default. Unsupported media types normally receive `415`. Regardless of media type, the verified body must produce a valid JSON Canonical Payload.

The first version accepts only an absent `Content-Encoding` or `Content-Encoding: identity`. Compressed encodings such as `gzip`, `br`, and `deflate` receive `415 Unsupported Media Type`. Compression support requires a later platform-specific decision defining signature bytes, compressed and decompressed limits, decompression safety, and fallback-deduplication input.

The public HTTP listener uses `MaxHeaderBytes = 32 KiB`, `ReadHeaderTimeout = 5 seconds`, and `IdleTimeout = 60 seconds`. A single short global `WriteTimeout` is not used because the same listener serves long polls. Route-specific request contexts enforce a 10-second webhook deadline, `wait_ms + 5 seconds` for claim, and 5 seconds for acknowledgement, negative acknowledgement, and extension. The webhook deadline includes body reading, so a client cannot hold a request open indefinitely by slowly sending its body.

A Verifier receives only the platform headers it explicitly needs. Sensitive headers are not logged, headers are not part of the Canonical Payload, cookies are unused, and arbitrary headers are not relayed to Queue Consumers.

Webhook ingress uses configurable process-local token buckets: one coarse global bucket and one bucket per Webhook Endpoint Identity. Both rate and burst are configurable; initial numeric defaults are deferred until expected traffic is known. The global bucket protects process and Valkey capacity, while the endpoint bucket isolates a noisy or attacked endpoint. Rate limiting occurs before expensive verification where possible. Rejected requests create neither Canonical Messages nor deduplication records and receive `429` with `Retry-After` or another retryable status selected by the platform adapter. This policy is per-process rather than a strict distributed quota if hookrelay later runs multiple replicas.

A process-local semaphore limits in-flight webhook requests to 100. It is acquired after route resolution and rate limiting but before body reading and verification. Exceeding the limit returns a platform-compatible retryable failure, normally `503` with `Retry-After: 1`. Telegram secret comparison does not receive a separate verification-concurrency semaphore; the general in-flight limit is sufficient until an adapter demonstrates more expensive verification.

Accepted, duplicate, and rejected requests behave as follows:

| Condition | Result |
|---|---|
| Unknown Webhook Identifier | reject, normally `404` |
| Verification fails | reject according to the platform adapter, normally `401` or `403` |
| Body exceeds 256 KiB | reject with `413` |
| Body is invalid JSON | reject with `400` |
| Verified known chat event | accept into `chat` scope |
| Verified known user-fallback event | accept into `user` scope |
| Verified known bot event | accept into `bot` scope |
| Verified valid JSON with unknown or incomplete structure | accept into `relay` scope |
| Valkey cannot atomically accept the message | retryable failure, normally `503` |

A successful platform response is returned only after the message has been atomically accepted or proven duplicate. The Webhook Type adapter maps internal accepted, duplicate, unknown-endpoint, verification-failure, invalid-JSON, oversized-body, rate-limited, blocked-Recipient, capacity-rejected, dependency-unavailable, and internal-error results to platform-compatible responses. The default mapping is respectively `200`, `200`, `404`, `401` or `403`, `400`, `413`, `429`, `503`, `503`, `503`, and `500`. An adapter may deliberately map a permanent platform error to success when required to stop futile redelivery. Accepted and duplicate results normally produce the same empty `200` response.

Concurrent requests for one Recipient are ordered by the sequence in which their atomic acceptance operations commit in Valkey, not by platform timestamps or TCP arrival order.

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

Webhook Endpoint Identity and Bot Identity correspond one-to-one, but Webhook Identifier and Bot Identifier remain different opaque values. Webhook Identifier is unique within its Webhook Type. Bot Identifier is assigned or defined by the Bot Platform.

A Recipient Identity is:

```text
(Bot Platform, Bot Identifier, Chat Identifier)
```

Bot Identifier and platform-provided Chat Identifier are opaque, case-sensitive strings. Leading zeros and exact values are preserved. Platform-specific normalization, when needed, belongs to the Converter.

## Recipient scopes

`recipient.scope` has three values:

- `chat`: a known event for a platform-provided Chat Identifier;
- `bot`: a known event that inherently applies to the Bot Identity rather than a chat;
- `router`: a verified valid JSON payload that cannot be mapped to a known chat-level or bot-level structure.

Bot and router scopes use distinct, shared, typed reserved Chat Identifier values. The implementation must prevent those values from colliding with platform-provided identifiers. There is one bot-scoped queue and one router-scoped fallback queue per Bot Identity. Those queues do not block any chat-scoped queue.

Only verified payloads that are valid JSON may enter the router scope. An unknown endpoint, failed verification, invalid JSON, or an oversized body does not create a Canonical Message.

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
- `recipient.chat_id`;
- `platform_event_type`;
- `payload`.

Optional fields:

- `occurred_ms`;
- `source_event_id`;
- `routing_issue`, for router-scoped messages.

`message_id` is generated as UUIDv7 before the atomic acceptance operation. `occurred_ms`, when present, is the Bot Platform's event time and does not determine Delivery Queue order.

The Canonical Payload is the complete platform-specific JSON value. Unknown object fields are retained. Exact source bytes, whitespace, and key order are not retained as the payload representation. A valid unexpected top-level JSON value is preserved as one payload and sent to router scope.

`platform_event_type` remains platform-specific. For an unknown structure, the Converter preserves the best safely extractable type or uses a reserved unknown value.

A router-scoped message may contain:

```json
{
  "routing_issue": {
    "code": "chat_id_missing"
  }
}
```

The code is bounded and machine-readable. No exception text, stack trace, request body, or secret is included.

## Request acceptance

The HTTP request body hard limit is 256 KiB. Accepted, duplicate, and rejected requests behave as follows:

| Condition | Result |
|---|---|
| Unknown Webhook Identifier | reject, normally `404` |
| Verification fails | reject according to the platform adapter, normally `401` or `403` |
| Body exceeds 256 KiB | reject with `413` |
| Body is invalid JSON | reject with `400` |
| Verified known chat event | accept into `chat` scope |
| Verified known bot event | accept into `bot` scope |
| Verified valid JSON with unknown or incomplete structure | accept into `router` scope |
| Valkey cannot atomically accept the message | retryable failure, normally `503` |

A successful platform response is returned only after the message has been atomically accepted or proven duplicate. The Webhook Type adapter maps internal accepted, duplicate, permanent-rejection, and transient-failure results to platform-compatible responses. Accepted and duplicate results normally produce the same empty `200` response.

Concurrent requests for one Recipient are ordered by the sequence in which their atomic acceptance operations commit in Valkey, not by platform timestamps or TCP arrival order.

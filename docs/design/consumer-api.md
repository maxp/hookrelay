# Consumer API

The Consumer API is exposed on the public listener and requires the shared Consumer Secret as an HTTP Bearer token. All request and response field names use `snake_case`.

## Common protocol

Consumer requests use:

```http
Authorization: Bearer <shared-consumer-secret>
Content-Type: application/json
Accept: application/json
```

An optional diagnostic instance identifier may be sent as:

```http
Consumer-Instance-Id: worker-7c9f6
```

It is not an authorization identity.

Consumer API request bodies are limited to 16 KiB, maximum JSON nesting depth 40, and strictly validated:

- the top-level JSON value is an object;
- unknown fields are rejected;
- duplicate JSON keys follow Go `encoding/json` last-value-wins behavior and are not rejected;
- trailing non-whitespace data is rejected;
- required strings are non-empty;
- UUIDv7 fields are validated syntactically;
- integers are not accepted as strings;
- fractional integer fields are rejected;
- non-JSON content types receive `415 Unsupported Media Type`.

Every response contains an internal UUIDv7 request identifier in:

```http
X-Request-Id: <request-id>
```

A caller-supplied correlation value may be accepted separately as bounded `X-Correlation-Id`; it never replaces the internal request identifier.

## Claim

```http
POST /v1/deliveries/claim
```

Request:

```json
{
  "operation_id": "0195c4d8-...",
  "wait_ms": 30000
}
```

`operation_id` is mandatory and unique across the shared Consumer API authorization scope. `wait_ms` is optional, defaults to 30,000, and is constrained to 0–30,000 milliseconds. Zero requests an immediate check.

Repeating an operation with the same arguments returns the recorded empty outcome or, while the claimed Delivery Attempt is still active, the recorded message response with the same Delivery Token. If that Delivery Attempt has already completed by acknowledgement, negative acknowledgement, or expiry, repeating the claim returns `409 claim_no_longer_active`; it never creates another lease and never returns a completed attempt's token. Reusing the identifier with different arguments returns `409 operation_conflict`.

A successful claim returns `200 OK`:

```json
{
  "delivery": {
    "delivery_token": "dlv_...",
    "delivery_cycle": 1,
    "attempt": 1,
    "claimed_ms": 1740000000000,
    "lease_expires_ms": 1740000060000
  },
  "message": {
    "message_id": "0195...",
    "received_ms": 1740000000000,
    "recipient": {
      "scope": "chat",
      "bot_platform": "telegram",
      "bot_id": "123456",
      "chat_id": "987654"
    },
    "platform_event_type": "message",
    "payload": {}
  }
}
```

Absent optional Canonical Message fields such as `occurred_ms`, `source_event_id`, and `routing_issue` are omitted rather than encoded as `null`.

A completed long poll with no work returns `204 No Content` without a body.

Other outcomes include:

- `400` invalid request;
- `401` missing or invalid Consumer Secret;
- `409` operation conflict or a completed claim replay (`claim_no_longer_active`);
- `429` active-lease or waiting-claim limit exceeded, with `Retry-After: 1`;
- `503` temporary dependency or Consumer API unavailability, optionally with `Retry-After`;
- `500` unexpected internal error.

## Acknowledgement

```http
POST /v1/deliveries/ack
```

Request:

```json
{
  "delivery_token": "dlv_..."
}
```

The Delivery Token is the idempotency key; no separate operation identifier is required.

Success returns `200 OK`:

```json
{
  "status": "acknowledged",
  "message_id": "0195...",
  "acknowledged_ms": 1740000030000
}
```

Repeating the same acknowledgement returns the recorded success. The Canonical Message is not returned.

## Negative acknowledgement

```http
POST /v1/deliveries/nack
```

Request:

```json
{
  "delivery_token": "dlv_...",
  "reason_code": "temporary_dependency_failure"
}
```

`reason_code` is optional and follows the bounded format documented by the delivery design.

A scheduled retry returns:

```json
{
  "status": "retry_scheduled",
  "message_id": "0195...",
  "attempt": 1,
  "retry_at_ms": 1740000030000
}
```

Exhausting the fourth attempt returns:

```json
{
  "status": "dead_lettered",
  "message_id": "0195...",
  "delivery_cycle": 1,
  "dead_lettered_ms": 1740000030000
}
```

Repeating the same negative acknowledgement returns the recorded result. The Canonical Message is not returned.

## Lease extension

```http
POST /v1/deliveries/extend
```

Request:

```json
{
  "delivery_token": "dlv_...",
  "operation_id": "0195c4d8-..."
}
```

The server chooses the extension duration. Repeating the same operation returns the recorded deadline. Reusing its identifier with another token returns `409 operation_conflict`.

Success returns:

```json
{
  "status": "extended",
  "message_id": "0195...",
  "lease_expires_ms": 1740000120000,
  "max_lease_expires_ms": 1740000300000
}
```

## Error envelope

All non-empty Consumer API errors use:

```json
{
  "error": {
    "code": "stale_delivery_token",
    "message": "The delivery attempt is no longer active.",
    "request_id": "0195..."
  }
}
```

The initial bounded error-code allowlist is:

```text
invalid_request
unauthenticated
operation_conflict
claim_no_longer_active
consumer_limit_exceeded
delivery_token_not_found
stale_delivery_token
delivery_already_acknowledged
delivery_already_nacked
maximum_lease_lifetime_reached
recipient_blocked
dependency_unavailable
internal_error
```

Delivery Token outcomes are:

| Situation | HTTP status | Code or result |
|---|---:|---|
| Unknown token or expired tombstone | `404` | `delivery_token_not_found` |
| Expired or superseded attempt | `409` | `stale_delivery_token` |
| Repeated identical acknowledgement | `200` | recorded acknowledgement |
| Repeated identical negative acknowledgement | `200` | recorded negative-acknowledgement result |
| Acknowledge after negative acknowledgement | `409` | `delivery_already_nacked` |
| Negative acknowledgement after acknowledgement | `409` | `delivery_already_acknowledged` |
| Recipient block marker exists | `409` | `recipient_blocked` |
| Extension after expiry | `409` | `stale_delivery_token` |
| Extension at maximum lifetime | `409` | `maximum_lease_lifetime_reached` |

Errors never contain secrets, Delivery Tokens, Valkey key names, stack traces, or payload data.

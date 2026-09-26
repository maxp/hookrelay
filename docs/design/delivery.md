# Ordered delivery design

## Ordering and consumer model

Each Recipient owns one Delivery Queue, and Recipient Identity is the unit of ordering. Different Recipient queues may progress concurrently. At most one message for a Recipient may have an active Message Lease.

Queue Consumers are equivalent clients competing for the next available Recipient queue from one shared pool. A consumer does not request a specific Recipient. A Consumer is a logical deployment with one credential; its ephemeral Consumer Instances share that credential and may process different Recipients concurrently. All Consumers have access to the same initial work pool.

Delivery is at least once. Hookrouter guarantees that message `N+1` is not issued for a Recipient until message `N` is acknowledged or moved to dead-letter. It cannot guarantee the order of external effects after a lease is lost, so consumers must be idempotent and must stop or fence work when they lose a lease.

Recipient selection is approximate round-robin with no priorities. A Recipient with remaining work returns to the end of the ready index. Chat-, bot-, and router-scoped queues participate equally. Retry backoff removes a Recipient from the ready index until its head becomes eligible again.

## Consumer API

The Consumer API is HTTP and uses long polling. Valkey is not exposed to consumers.

A claim request is conceptually:

```http
POST /v1/deliveries/claim
Authorization: Bearer <consumer-credential>
Content-Type: application/json

{
  "wait_ms": 30000,
  "operation_id": "0195..."
}
```

Rules:

- default and maximum wait are 30 seconds;
- an empty completed poll returns `204 No Content`;
- `operation_id` is a consumer-generated UUIDv7 unique within its credential;
- repeating the same operation returns the same message and Delivery Token, or the same empty outcome;
- claim results are retained for 10 minutes;
- cancellation before a lease is created abandons the wait;
- if lease creation races with disconnect, repeating the same operation recovers the lease;
- multiple claims per Consumer credential are allowed;
- a configurable maximum active lease count applies per Consumer;
- exceeding that limit returns `429 Too Many Requests`;
- a notification channel may wake polls, but Valkey's ready index is the source of truth and is always rechecked atomically.

A successful response is conceptually:

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
    "source_event_id": "123",
    "platform_event_type": "message",
    "payload": {}
  }
}
```

Optional absent fields are omitted rather than encoded as `null`.

## Lease and token semantics

Each Delivery Attempt receives a new opaque cryptographically random Delivery Token with at least 128 bits of entropy. It is bound to the Consumer credential that claimed it, not to a particular Consumer Instance. Replicas sharing that credential can recover and finish one another's work; another credential cannot use the token.

The token is sent in JSON request bodies, never in a URL. It is redacted from logs and is not derivable from `message_id`.

Operations are conceptually:

```http
POST /v1/deliveries/ack
POST /v1/deliveries/nack
POST /v1/deliveries/extend
```

`ack` and `nack` are idempotent by Delivery Token. Their terminal result is retained as a tombstone for one hour:

- repeating the same terminal operation returns the original successful result;
- attempting `ack` after a completed `nack`, or the inverse, returns conflict;
- an expired or superseded token is stale;
- a token belonging to another Consumer credential is forbidden;
- after tombstone expiry an unknown old token may return not found.

`extend` uses a consumer-generated UUIDv7 `operation_id`. Repeating the same extension operation returns the same deadline and does not extend twice. The server chooses the extension duration; the client does not submit an arbitrary deadline.

Lease time comparisons use the authoritative Valkey time inside atomic operations, never consumer clocks.

Accepted timing defaults are:

```text
initial_lease_duration_ms = 60_000
extension_duration_ms     = 60_000
max_lease_lifetime_ms     = 300_000
```

An extension adds to the current deadline but cannot exceed five minutes from the attempt start. `ack`, `nack`, and `extend` are rejected after the actual lease deadline even if maintenance has not processed expiry yet.

There is no cost-free lease release. A consumer that cannot finish sends `nack`; this increments the attempt count because an external effect may already have occurred.

## Retry policy

The first version has one global Retry Policy. One Delivery Cycle permits four attempts, including the initial attempt. Failure delays are:

```text
after attempt 1: nominal 1 second
after attempt 2: nominal 5 seconds
after attempt 3: nominal 30 seconds
after attempt 4: dead-letter
```

Each nominal delay receives jitter uniformly in the range 50% to 100% of the nominal delay. The server determines retry timing; consumers cannot override it. `nack` and lease expiry follow the same policy and both increment the attempt count. Their outcomes remain distinguishable in attempt history.

A consumer may include an optional bounded `reason_code` on `nack`. It is at most 64 ASCII characters, does not alter retry policy, and is not used as a metric label unless allowlisted. There is no free-form reason message and no consumer-controlled immediate dead-letter operation in the first version.

During retry delay, the failed message remains the queue head and later messages for its Recipient are blocked.

## Attempt history and dead-letter

Attempt history records safe metadata such as:

```json
{
  "delivery_cycle": 1,
  "attempt": 3,
  "claimed_ms": 1740000000000,
  "lease_expires_ms": 1740000060000,
  "completed_ms": 1740000025000,
  "outcome": "nack",
  "reason_code": "temporary_dependency_failure"
}
```

It does not store the Delivery Token, credential secret, payload copy, stack trace, or free-form exception text. A safe `consumer_id` may be retained.

After attempt four fails, the message moves atomically to the Recipient's dead-letter state and the next normal message becomes eligible. This is an explicit break in the original processing sequence.

Operator replay:

- returns the same Canonical Message to the head of its Recipient queue;
- preserves `message_id` and payload;
- starts a new Delivery Cycle with attempt one;
- issues a new Delivery Token;
- preserves previous attempt history;
- bypasses ingestion deduplication;
- requires explicit resolution if the original Deduplication Identity now points to another message.

The first operational UI supports safe metadata inspection, privileged payload inspection with audit, replay to the queue head, and confirmed permanent deletion.

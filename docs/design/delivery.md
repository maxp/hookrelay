# Ordered delivery design

## Ordering and consumer model

Each Recipient owns one Delivery Queue, and Recipient Identity is the unit of ordering. Different Recipient queues may progress concurrently. At most one message for a Recipient may have an active Message Lease.

Queue Consumers are equivalent clients competing for the next available Recipient queue from one shared pool. A consumer does not request a specific Recipient. All Consumer Instances use one shared secret supplied through deployment configuration, have identical authorization, and may process different Recipients concurrently. The first version does not identify or authorize separate logical Consumer deployments, and the Consumer API is always enabled.

The shared secret is a uniformly random opaque value with at least 64 bits of entropy. Production prefers a mounted secret file, while an environment variable is allowed for local development. Configuring both sources is a startup error, and absence of the secret never disables authentication. Only one secret is accepted at a time, so rotation requires a coordinated update or restart of hookrelay and all Consumer Instances.

The secret is sent as an HTTP Bearer token, compared in constant time, and never stored in Valkey, logs, metrics, UI output, URLs, or request bodies. Missing and incorrect values receive the same `401 Unauthorized` response. Because one secret authorizes the entire Consumer API, compromise of any Consumer Instance compromises the whole work pool.

Delivery is at least once. Hookrelay guarantees that message `N+1` is not issued for a Recipient until message `N` is acknowledged or moved to dead-letter. It cannot guarantee the order of external effects after a lease is lost, so consumers must be idempotent and must stop or fence work when they lose a lease.

Recipient selection is approximate round-robin with no priorities. A Recipient with remaining work returns to the end of the ready index. Chat-, user-, bot-, and relay-scoped queues participate equally. Retry backoff removes a Recipient from the ready index until its head becomes eligible again.

## Consumer API

The Consumer API is HTTP and uses long polling. Valkey is not exposed to consumers.

A claim request is conceptually:

```http
POST /v1/deliveries/claim
Authorization: Bearer <shared-consumer-secret>
Content-Type: application/json

{
  "wait_ms": 30000,
  "operation_id": "0195..."
}
```

Rules:

- default and maximum wait are 30 seconds;
- an empty completed poll returns `204 No Content`;
- `operation_id` is a consumer-generated UUIDv7 unique within the shared consumer authorization scope;
- repeating the same operation returns the same empty outcome, or the same message and Delivery Token only while the claimed Delivery Attempt remains active;
- after acknowledgement, negative acknowledgement, or expiry, repeating the claim operation returns `409 claim_no_longer_active` and never creates a new lease;
- claim operation markers are retained for 10 minutes;
- cancellation before a lease is created abandons the wait;
- if lease creation races with disconnect, repeating the same operation recovers the lease;
- multiple claims under the shared consumer secret are allowed;
- configurable global limits apply to active consumer leases and waiting claims;
- initial defaults are 100 active leases and 20 waiting claims;
- exceeding either limit returns `429 Too Many Requests` with `Retry-After: 1`;
- a notification channel may wake polls, but Valkey's ready index is the source of truth and is always rechecked atomically.

A Consumer Instance may send an optional bounded `Consumer-Instance-Id` header for diagnostics. It is caller-controlled, is not an authorization identity, is not used as a Prometheus label, and may be retained in attempt history.

Because all instances share one authorization scope, every idempotent operation uses a UUIDv7 `operation_id` unique across the entire Consumer API. Reusing an operation identifier with different arguments returns `409 Conflict` with `operation_conflict`; the server does not namespace it by the untrusted instance identifier.

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

Each Delivery Attempt receives a new opaque cryptographically random Delivery Token with at least 128 bits of entropy. Because every Consumer Instance uses the same configured secret, any authenticated Consumer Instance may acknowledge, negatively acknowledge, or extend any current token. The token is not bound to a particular instance.

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

## Maintenance of leases and retries

Valkey time is authoritative for `claimed_ms`, `lease_expires_ms`, `retry_at_ms`, and all deadline comparisons. Consumer and application-process clocks do not decide whether a lease or retry is due.

Two ordered Valkey indexes locate scheduled transitions:

```text
lease_deadlines: score = lease_expires_ms
retry_deadlines: score = retry_at_ms
```

An index entry is only a locator. Every maintenance transition atomically checks the current queue-head state, Delivery Token, attempt, and deadline before applying expiry, retry activation, or dead-letter movement. A stale index entry is ignored or removed without changing current state.

Maintenance is cooperative: every hookrelay replica may process due entries, and no maintenance leader is elected. Atomic idempotent transitions ensure that only one replica applies a state change. Initial configurable defaults are:

```text
maintenance_interval_ms        = 1_000
maintenance_interval_jitter_ms = 250
maintenance_batch_size         = 100
max_continuous_batches         = 5
```

Lease expiries and retry activations use separate batches of at most 100 entries. A full batch may trigger another immediate batch, but a replica yields after at most five continuous batches so maintenance does not monopolize Valkey or application capacity.

Maintenance metrics must expose applied, stale, and failed transitions, batch size and duration, and lag between Valkey time and the oldest due deadline.

Before a claim enters long polling with an empty ready index, it performs one inline maintenance pass over at most 10 due lease or retry entries, rechecks the ready index, and only then waits. This is a bounded self-healing path, not a replacement for background maintenance.

Canonical Messages, each Recipient's ordered message sequence, queue-head state, the current Delivery Attempt, Delivery Cycle history, and dead-letter entries are authoritative state. Ready, lease-deadline, retry-deadline, global dead-letter, and deduplication indexes are derived accelerators and must be rebuildable from authoritative state.

Startup reconciliation repairs only safely derivable differences such as missing or stale ready and deadline index entries. The first slice validates the queue, head state, message, and derived structures it implements before readiness; as lease expiry, retries, and dead-letter handling arrive, their startup checks and safe repairs join this same gate. An inconsistency that the current slice cannot handle safely does not pass readiness. Ambiguous authoritative Recipient state is never guessed or silently rewritten. Instead, hookrelay creates a minimal persistent block marker:

```text
hr1:q:<recipient_identity>
```

The marker contains only `detected_ms` and a bounded `reason_code`. While it exists, the Recipient is absent from ready and deadline indexes, cannot be claimed, and cannot accept new messages; affected webhook requests receive a retryable response. Other Recipients continue normally. Hookrelay emits a critical metric and structured log. The first version has no general repair engine or quarantine UI: an operator diagnoses the stored state, performs the documented manual recovery procedure, and removes the marker only after invariants have been verified.

Once the corresponding expiry and retry transitions are implemented, after a Valkey restore hookrelay starts not-ready, rebuilds safe derived indexes, processes overdue leases through the normal expiry transition, activates due retries, and creates block markers for ambiguous Recipient state. It becomes ready when general new operations are safe; individual blocked Recipients remain unavailable until operator recovery.

An active Delivery Queue record is removed when its final normal message is acknowledged or moved to dead-letter. Recipient dead-letter state and operational history may outlive an empty normal queue.

Queue capacity has both global and per-Recipient configured limits:

```text
max_queued_messages_global        = 100_000
max_queued_messages_per_recipient = 1_000
```

If either limit prevents atomic acceptance, hookrelay creates neither a deduplication record nor a partial message and returns a retryable platform response, normally `503`. It never deletes already accepted queue messages or redirects overflow into relay scope. Chat-, user-, bot-, and relay-scoped queues use the same per-Recipient limit. Production Valkey uses `noeviction`; capacity metrics and alerts must fire before hard rejection.

New-message acceptance stops at 90% of configured Valkey `maxmemory`, while acknowledgement, delivery, cleanup, and retention transitions continue so the system can recover. Crossing this hard threshold makes hookrelay not-ready for general ingestion. The limits are configurable and must be validated against production payload size and traffic rather than treated as a promise that 100,000 maximum-size payloads fit in memory.

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

It does not store the Delivery Token, shared consumer secret, payload copy, stack trace, or free-form exception text. A caller-provided non-authoritative instance identifier may be retained for diagnostics, but it is not a security identity.

After attempt four fails, the message moves atomically to the Recipient's dead-letter state and the next normal message becomes eligible. This is an explicit break in the original processing sequence.

Operator replay:

- returns the same Canonical Message before all not-yet-started messages for its Recipient;
- does not interrupt an already leased head or a head waiting for retry, and in that case inserts the replayed message immediately after the current head;
- preserves `message_id` and payload;
- starts a new Delivery Cycle with attempt one;
- issues a new Delivery Token;
- preserves previous attempt history;
- bypasses ingestion deduplication;
- requires explicit resolution if the original Deduplication Identity now points to another message.

Dead-letter messages are stored in one global DLQ ordered by `dead_lettered_ms`. Each entry retains its Recipient Identity and queue context so replay can atomically return it to the correct queue head. The global sequence is an operational listing order, not an ordering guarantee between Recipients.

After successful acknowledgement, the Canonical Payload and active queue state are deleted. Compact delivery metadata is retained for 24 hours, including `message_id`, recipient scope, Bot Platform, received and acknowledged times, delivery cycle, attempt count, and a safe Consumer identifier. It deliberately omits Bot, Chat, and User identifiers. Delivery Token tombstones and deduplication records retain their independently configured lifetimes.

The first operational UI supports safe metadata inspection, privileged payload inspection gated by a confirmed audit append, replay in the order described above, and confirmed permanent deletion. Payload inspection returns `503` without disclosing payload content if its audit cannot be confirmed. The audit event denotes authorized access beginning, not proof of client receipt; see [Admin API audit policies](admin-api.md#audit-failure-policies).

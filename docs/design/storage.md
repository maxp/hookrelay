# Valkey storage design

This document records the accepted internal storage shape for the standalone Valkey deployment. All keys use the compact versioned namespace `hr1:`. These names are internal implementation details, not public identifiers.

## Recipient key encoding

Recipient Identity is serialized as three colon-separated components:

```text
<bot_platform>:<bot_id>:<chat_id>
```

Every component is non-empty and must not contain `:`. Bot Platform is 1–32 lowercase ASCII characters matching `[a-z][a-z0-9_-]*`. Bot Identifier and Chat Identifier are 1–128 UTF-8 bytes and contain no control characters. Converters perform any platform-specific canonicalization before common validation; common code does not trim whitespace or change case.

The typed reserved Chat Identifier values are `$hr_bot` and `$hr_relay`. Only the common domain module creates them. A platform-provided identifier matching either value is invalid for chat scope and is relayed through `$hr_relay` with the bounded `reserved_chat_id` routing issue.

## Delivery Queue

Each Recipient's ordered Delivery Queue is a Valkey List of `message_id` values:

```text
hr1:r:<platform>:<bot>:<chat>:q
```

Operations are performed only inside the relevant atomic Lua transition:

- `RPUSH` appends an accepted message;
- `LINDEX 0` inspects the head;
- `LPOP` removes an acknowledged or dead-lettered head;
- `LLEN` obtains queue depth.

Valkey Streams are not used for Delivery Queues because hookrelay owns lease, retry, dead-letter, strict head-of-line blocking, and shared-pool semantics.

## Ready index and fairness

The global ready index is a Sorted Set:

```text
hr1:ready
```

Its member is the serialized Recipient Identity, and its score is a monotonically increasing fairness sequence allocated through:

```text
hr1:ready_seq
```

A claim takes the Recipient with the smallest score. If an acknowledgement exposes another message for that Recipient, the Recipient receives a new sequence and returns to the end of the ready index. A Recipient has at most one member in the index.

## Canonical Message

A Canonical Message is stored as one JSON blob:

```text
hr1:m:<message_id>
```

Mutable delivery state is not embedded in the blob. The blob is deleted after successful acknowledgement, retained while the message is queued or retrying, and retained with a Dead-letter Message until replay or deletion.

## Queue-head state

The current head state for a Recipient is stored separately, conceptually as a Valkey Hash:

```text
hr1:r:<platform>:<bot>:<chat>:s
```

It contains the current fields applicable to the state transition, including:

```text
status = ready | leased | retry_wait
head_message_id
delivery_cycle
attempt
delivery_token_digest
claimed_ms
lease_expires_ms
retry_at_ms
consumer_instance_id
```

The required invariant is:

```text
state.head_message_id == LINDEX(queue, 0)
```

Only the queue head has active delivery state. Every transition affecting the List, state, and derived indexes is atomic.

## Delivery Attempt history

Completed Delivery Attempts are appended as compact JSON entries to a Valkey List:

```text
hr1:a:<message_id>
```

After successful acknowledgement, full attempt history is removed after producing the accepted compact success metadata. For a Dead-letter Message, full history remains available until the message is replayed, deleted, or reaches its eventual DLQ retention policy.

## Delivery Token and idempotent operation records

The active Delivery Token is stored in plaintext in the Recipient head state for the lifetime of the Delivery Attempt. A repeated claim must return the same token after a lost HTTP response, so its idempotent operation record also temporarily contains the plaintext token:

```text
hr1:op:<operation_id>
```

The claim-operation record has a 10-minute TTL and stores the request-argument digest and completed response. The token itself is never written to logs, metrics, URLs, attempt history, audit records, or compact success metadata.

After acknowledgement, negative acknowledgement, or expiry, plaintext copies of the token are deleted. A one-hour terminal tombstone is keyed by SHA-256 digest:

```text
hr1:t:<delivery_token_digest>
```

It records the terminal outcome and enough safe metadata to make repeated `ack` or `nack` idempotent. Token encryption and a separate encryption key are deliberately not introduced in the first version. Valkey, its backups, and diagnostics are treated as secret-bearing infrastructure, and the Consumer API additionally requires the shared Consumer Secret.

## Deduplication records

Each deduplication record is a Valkey Hash:

```text
hr1:d:<dedup_identity_digest>
```

with:

```text
message_id
body_digest
accepted_ms
expires_ms
```

The record has a Valkey TTL. A global Sorted Set supports capacity-based removal of the oldest records:

```text
hr1:dedup_age
  score  = accepted_ms
  member = dedup_identity_digest
```

An index member whose record expired is stale and is removed by maintenance. A record missing from the index is restored by the consistency checker.

## Global dead-letter queue

The authoritative dead-letter record for a message is a Valkey Hash:

```text
hr1:dl:<message_id>
```

with:

```text
bot_platform
bot_id
chat_id
recipient_scope
dead_lettered_ms
dead_letter_reason
delivery_cycle
dedup_identity_digest
```

The Canonical Message and Delivery Attempt history remain stored separately as `hr1:m:<message_id>` and `hr1:a:<message_id>`.

A rebuildable global operational index is a Sorted Set:

```text
hr1:dlq
  score  = dead_lettered_ms
  member = message_id
```

The global order is only the order of entry into dead-letter; it does not create an ordering guarantee between Recipients.

Moving a message to dead-letter is one atomic transition that records the final attempt, removes the current queue head, creates the dead-letter Hash and index member, clears active head state, and exposes the next normal queue head if one exists.

Dead-letter replay removes the dead-letter record and index entry, starts a new Delivery Cycle, and returns the same `message_id` before every not-yet-started message for its Recipient. It never interrupts an already leased head or a head waiting for retry: in those cases the replayed message is inserted immediately after that current head. If there is no active or retrying head, it is inserted directly at the queue head. The transition preserves the queue/state head invariant and is atomic.

The dead-letter Hash is authoritative. Startup reconciliation removes stale global index members and restores missing index members. If the Canonical Message is missing or another ambiguity prevents safe repair, hookrelay creates the Recipient block marker described below.

Permanent deletion atomically removes the global index member, dead-letter Hash, Canonical Message, and attempt history, then emits an audit event without copying the payload.

## Ambiguous Recipient block marker

The first version uses a minimal persistent marker rather than a full quarantine subsystem:

```text
hr1:q:<platform>:<bot>:<chat>
```

The marker stores only:

```text
detected_ms
reason_code
```

Its presence blocks claim, acknowledgement, negative acknowledgement, extension, maintenance transitions, and new ingestion for that Recipient. The Recipient is absent from ready, lease-deadline, and retry-deadline indexes. Other Recipients continue normally.

Hookrelay does not automatically alter ambiguous authoritative state and does not provide a generic repair UI in the first version. Operator recovery follows a documented runbook, verifies the corrected invariants, and only then removes the marker. Detection and recovery are logged, metered, and audited.

## Compact success metadata

After successful acknowledgement, the Canonical Message and full attempt history are removed. Compact metadata remains for 24 hours in a Valkey Hash:

```text
hr1:success:<message_id>
```

with:

```text
recipient_scope
bot_platform
received_ms
acknowledged_ms
delivery_cycle
attempt_count
consumer_instance_id
```

It does not retain Bot Identifier, Chat Identifier, payload, Source Event Identifier, Delivery Token, reason code, or Deduplication Identity.

## Delivery Cycle history limit

Valkey retains at most the latest 10 Delivery Cycles for a message. Operator replay actions remain in the administrative audit log. When older cycles are removed, the message metadata retains aggregates:

```text
archived_cycles
archived_attempts
first_archived_ms
last_archived_ms
```

## Dead-letter retention

Dead-letter retention is configurable and defaults to 30 days. `hr1:dlq` already orders entries by `dead_lettered_ms`, so it also serves as the retention index; no separate expiry index is created.

When retention expires, one atomic maintenance transition removes the global DLQ member, dead-letter Hash, Canonical Message, and attempt history, then emits an audit event without copying the payload.

## Storage namespace versioning

The first version does not implement storage migrations or maintain a separate `hr1:schema` record. The `hr1` namespace itself identifies the storage format. A future incompatible format uses a new namespace such as `hr2` and requires an explicitly designed migration or cutover procedure rather than best-effort interpretation of old keys.

## Accepted key names

```text
hr1:m:<message_id>                         Canonical Message
hr1:r:<platform>:<bot>:<chat>:q            Delivery Queue
hr1:r:<platform>:<bot>:<chat>:s            queue-head state
hr1:ready                                  ready Recipient ZSET
hr1:ready_seq                              fairness sequence
hr1:leases                                 lease deadlines ZSET
hr1:retries                                retry deadlines ZSET
hr1:a:<message_id>                         attempt history
hr1:op:<operation_id>                      idempotent operation result
hr1:t:<delivery_token_digest>              terminal token result
hr1:d:<dedup_identity_digest>              dedup record
hr1:dedup_age                              dedup age ZSET
hr1:dlq                                    global DLQ ZSET
hr1:dl:<message_id>                        dead-letter metadata
hr1:q:<platform>:<bot>:<chat>              ambiguous Recipient block marker
hr1:success:<message_id>                   compact success metadata
hr1:audit                                  administrative audit Stream
hr1:stats:queued_messages                  derived global queued-message counter
```

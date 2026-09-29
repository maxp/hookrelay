# Ordered delivery design

## Ordering and consumer model

Each Recipient owns one Delivery Queue, and Recipient Identity is the unit of ordering. Different Recipient queues may progress concurrently. At most one message for a Recipient may have an active Message Lease.

Queue Consumers are equivalent clients competing for the next available Recipient queue from one shared pool. A consumer does not request a specific Recipient. All Consumer Instances use one shared secret supplied through deployment configuration, have identical authorization, and may process different Recipients concurrently. The first version does not identify or authorize separate logical Consumer deployments, and the Consumer API is always enabled.

The shared secret is a uniformly random opaque value with at least 64 bits of entropy and an accepted encoded length of 16–8192 bytes; length validation does not substitute for the entropy requirement. Production prefers a mounted secret file, while an environment variable is allowed for local development. Configuring both sources is a startup error, and absence of the secret never disables authentication. Only one secret is accepted at a time, so rotation requires a coordinated update or restart of hookrelay and all Consumer Instances.

The secret is sent as an HTTP Bearer token, compared in constant time, and never stored in Valkey, logs, metrics, UI output, URLs, or request bodies. Missing and incorrect values receive the same `401 Unauthorized` response. Because one secret authorizes the entire Consumer API, compromise of any Consumer Instance compromises the whole work pool.

Delivery is at least once. Hookrelay guarantees that message `N+1` is not issued for a Recipient until message `N` is acknowledged or moved to dead-letter. It cannot guarantee the order of external effects after a lease is lost, so consumers must be idempotent and must stop or fence work when they lose a lease.

Recipient selection is approximate round-robin with no priorities. A Recipient with remaining work returns to the end of the ready index. Chat-, user-, bot-, and relay-scoped queues participate equally. Retry backoff removes a Recipient from the ready index until its head becomes eligible again.

## Consumer API

The Consumer API is HTTP and uses long polling. Valkey is not exposed to consumers. Routes, request and response bodies, status codes, and error codes are defined only in the [Consumer API contract](consumer-api.md); this section records the delivery semantics behind that contract.

- A claim may wait up to 30 seconds for a ready Recipient.
- Repeating a claim `operation_id` returns the recorded empty outcome, or the same message and Delivery Token only while the claimed Delivery Attempt remains active. After acknowledgement, negative acknowledgement, or expiry, it never creates a new lease. Claim operation records are retained for 10 minutes.
- Cancellation before a lease is created abandons the wait. If lease creation races with disconnect, repeating the same operation recovers the lease.
- Multiple concurrent claims under the shared Consumer Secret are allowed.
- A configurable work-pool-wide limit applies to active leases, while a separate process-local limit applies to waiting HTTP claims. Initial defaults are 100 active leases work-pool-wide and 20 waiting claims per process.
- The initial long-poll implementation rechecks the ready index every 250 ms plus 0–50 ms of uniform jitter. A later notification channel may wake polls earlier, but Valkey's ready index remains the source of truth, periodic atomic rechecking remains the loss-recovery path, and the HTTP contract does not change.

A Consumer Instance may send an optional bounded `Consumer-Instance-Id` header for diagnostics. It is caller-controlled, is not an authorization identity, is not used as a Prometheus label, and may be retained in attempt history.

Because all instances share one authorization scope, every idempotent operation uses a UUIDv7 `operation_id` unique across the entire Consumer API. The server does not namespace it by the untrusted instance identifier.

## Lease and token semantics

Each Delivery Attempt receives a new opaque cryptographically random Delivery Token with at least 128 bits of entropy. Because every Consumer Instance uses one shared authorization scope, any Consumer Instance authenticated with the currently accepted secret may acknowledge, negatively acknowledge, or extend any current token. The token is not bound to a particular instance or to the literal secret value present when it was claimed: after coordinated secret rotation, the new secret authorizes existing live tokens and the old secret authorizes nothing.

The token is sent in JSON request bodies, never in a URL. It is redacted from logs and is not derivable from `message_id`.

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

Two ordered Valkey indexes locate scheduled transitions; their key layout is defined in the [storage design](storage.md#lease-and-retry-deadline-indexes):

```text
hr1:leases:  member = <recipient_identity>, score = lease_expires_ms
hr1:retries: member = <recipient_identity>, score = retry_at_ms
```

An index entry is only a locator. Every maintenance transition atomically checks the current queue-head state, Delivery Token, attempt, and deadline before applying expiry, retry activation, or dead-letter movement. A stale index entry is ignored or removed without changing current state.

The first version runs exactly one hookrelay process. Maintenance is nevertheless designed to be cooperative so that a later multi-process deployment needs no redesign: any process may process due entries, no maintenance leader is elected, and atomic idempotent transitions ensure that only one process applies a state change. Process-local limits, such as waiting claims and rate limits, would need their own multi-process decision before more than one process is supported. Initial configurable defaults are:

```text
maintenance_interval             = 1s
maintenance_interval_jitter      = 250ms
maintenance_batch_size           = 100
maintenance_max_continuous_batches = 5
```

Lease expiries and retry activations use separate batches of at most 100 entries. A full batch may trigger another immediate batch, but a process yields after at most five continuous batches so maintenance does not monopolize Valkey or application capacity.

Maintenance metrics must expose applied, stale, and failed transitions, batch size and duration, and lag between Valkey time and the oldest due deadline.

Before a claim enters long polling with an empty ready index, it performs one inline maintenance pass over at most 10 due lease or retry entries, rechecks the ready index, and only then waits. This is a bounded self-healing path, not a replacement for background maintenance.

Canonical Messages, each Recipient's ordered message sequence, queue-head state, the current Delivery Attempt, Delivery Cycle history, and dead-letter entries are authoritative state. Ready, lease-deadline, retry-deadline, blocked-Recipient, global dead-letter, and deduplication indexes are derived accelerators and must be rebuildable from authoritative state.

Startup reconciliation repairs only safely derivable differences such as missing or stale ready and deadline index entries. The first slice validates the queue, head state, message, and derived structures it implements before readiness; as lease expiry, retries, and dead-letter handling arrive, their startup checks and safe repairs join this same gate. An inconsistency that the current slice cannot handle safely does not pass readiness. Ambiguous authoritative Recipient state is never guessed or silently rewritten. Instead, hookrelay creates a minimal persistent block marker:

```text
hr1:q:<recipient_identity>
```

The marker contains only `detected_ms` and a bounded `reason_code`. The same atomic transition adds the Recipient to the derived `hr1:blocked` index used for listing and counting blocked Recipients; removing the marker removes the index member. While the marker exists, the Recipient is absent from ready and deadline indexes, cannot be claimed, and cannot accept new messages; affected webhook requests receive a retryable response. Other Recipients continue normally. Hookrelay emits a critical metric and structured log. The first version has no general repair engine or quarantine UI. An operator follows the [Recipient block recovery runbook](../runbooks/recipient-block-recovery.md), uses the narrow inspection operation to verify stored-state invariants, corrects authoritative state through an incident-specific reviewed procedure, and only then invokes the preconditioned audited clear operation. The clear operation never repairs authoritative state itself.

Once the corresponding expiry and retry transitions are implemented, after a Valkey restore hookrelay starts not-ready, rebuilds safe derived indexes, processes overdue leases through the normal expiry transition, activates due retries, and creates block markers for ambiguous Recipient state. It becomes ready when general new operations are safe; individual blocked Recipients remain unavailable until operator recovery.

An active Delivery Queue record is removed when its final normal message is acknowledged or moved to dead-letter. Recipient dead-letter state and operational history may outlive an empty normal queue.

Queue capacity has both global and per-Recipient configured limits:

```text
max_queued_messages_global        = 100_000
max_queued_messages_per_recipient = 1_000
```

If either limit prevents atomic acceptance, hookrelay creates neither a deduplication record nor a partial message and returns a retryable platform response, normally `503`. It never deletes already accepted queue messages or redirects overflow into relay scope. Chat-, user-, bot-, and relay-scoped queues use the same per-Recipient limit. Production Valkey uses `noeviction`; capacity metrics and alerts must fire before hard rejection.

New-message acceptance stops at 90% of configured Valkey `maxmemory`, while claims, acknowledgement, cleanup, and retention transitions continue so the system can recover. Crossing this hard threshold sets `hookrelay_accepting_webhooks` to `0` and makes `/health/accepting-webhooks` return `503`, but `/health/ready` remains successful while draining operations are safe so the Consumer API stays routable. The limits are configurable and must be validated against production payload size and traffic rather than treated as a promise that 100,000 maximum-size payloads fit in memory.

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
- starts a new Delivery Cycle with attempt one; a replay inserted behind an active or retrying head retains its new cycle while waiting, and a ready retry preempted by replay retains its existing attempt until it becomes head again;
- issues a new Delivery Token;
- preserves previous attempt history;
- bypasses ingestion duplicate suppression;
- rejects by default if the original Deduplication Identity now points to another message;
- may proceed only with the explicit `keep_current` resolution, which leaves the newer deduplication mapping unchanged; replay never repoints that mapping to the older message.

Dead-letter messages are stored in one global DLQ ordered by `dead_lettered_ms`. Each entry retains its Recipient Identity and queue context so replay can atomically return it to the correct queue head. The global sequence is an operational listing order, not an ordering guarantee between Recipients.

After successful acknowledgement, the Canonical Payload and active queue state are deleted. Compact delivery metadata is retained for 24 hours, including `message_id`, recipient scope, Bot Platform, received and acknowledged times, delivery cycle, attempt count, and a safe Consumer identifier. It deliberately omits Bot, Chat, and User identifiers. Delivery Token tombstones and deduplication records retain their independently configured lifetimes.

The first operational UI supports safe metadata inspection, privileged payload inspection gated by a confirmed audit append, replay in the order described above, and confirmed permanent deletion. Payload inspection returns `503` without disclosing payload content if its audit cannot be confirmed. The audit event denotes authorized access beginning, not proof of client receipt; see [Admin API audit policies](admin-api.md#audit-failure-policies).

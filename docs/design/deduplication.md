# Deduplication design

## Identity

A Deduplication Identity is:

```text
(Webhook Type, Bot Identity, Platform Deduplication Key)
```

It identifies one platform event or update delivery, not the longer-lived platform object affected by the event. `message_created` and `message_edited` for the same platform message are distinct events and require distinct deduplication keys.

Source Event Identifier and Platform Deduplication Key are separate concepts even when a platform happens to use the same value for both.

Each Webhook Type documents how its Converter derives the Platform Deduplication Key. A stable identifier provided by the Bot Platform is preferred. If none is available, the fallback is:

```text
body_sha256_v1:<base64url-without-padding>
```

The SHA-256 input is the exact limited HTTP request body. It is calculated during body reading rather than from reserialized JSON. SHA-256 is preferred initially because it is in the Go standard library, resists intentional collisions, and its cost for a body capped at 256 KiB is expected to be smaller than verification, parsing, Valkey, and network costs. A faster hash may be considered only after profiling.

The same request-body digest is recorded to detect a conflicting duplicate: the same Deduplication Identity with different bytes remains a duplicate, is not enqueued again, and increments a conflict metric with a safe structured log.

Chat-, bot-, and router-scoped messages all use the same deduplication process.

## Atomic acceptance

For a new message, one atomic Valkey operation must:

1. inspect the Deduplication Identity;
2. return the original `message_id` if it already exists;
3. create the deduplication record;
4. store the Canonical Message;
5. append its `message_id` to the Recipient's Delivery Queue;
6. add the Recipient to the ready index when the queue becomes available;
7. return an accepted result.

The operation may not leave a deduplication record without a queued message, a stored message without a queue reference, or a newly ready queue missing from its index.

The application generates the candidate UUIDv7 before this operation. If the request is duplicate, that unused candidate ID is discarded. A duplicate does not store another Canonical Message or modify the Delivery Queue and receives the normal platform success response.

A deduplication record contains at least:

- original `message_id`;
- acceptance time;
- digest of the original request body;
- expiry information.

When Valkey is unavailable, hookrouter cannot prove a duplicate and must return a retryable failure rather than use process-local state or accept the message only in memory.

## Retention and capacity

Deduplication uses:

- a configured global retention target;
- an optional retention override by Webhook Type;
- a configured minimum effective retention;
- a global maximum number of deduplication records;
- Valkey memory monitoring with `noeviction` rather than arbitrary eviction of queue data.

Under capacity pressure, hookrouter may delete the globally oldest deduplication records before their target retention, but never intentionally below the configured minimum window. If capacity cannot preserve the minimum window, new ingestion is rejected with a retryable response instead of silently reducing deduplication to zero.

The configured retention is not permanently changed under pressure. As traffic or stored volume falls, effective retention naturally returns toward the configured target.

Required capacity signals include:

- current and maximum deduplication record count;
- age of the oldest record;
- effective retention;
- early eviction count;
- capacity rejection count;
- payload-conflict count.

Labels must be bounded, such as Webhook Type and reason. Bot Identifier, Chat Identifier, Webhook Identifier, and deduplication keys are not metric labels.

After a deduplication record expires or is removed under capacity pressure, a repeated webhook is treated as a new event, receives a new `message_id`, and may be delivered again.

## Dead-letter replay distinction

Dead-letter replay is not ingestion. It preserves the original Canonical Message and `message_id`, starts a new Delivery Cycle, and does not check ingestion deduplication. If the original Deduplication Identity currently points to a different, newer message, replay requires explicit operator conflict resolution.

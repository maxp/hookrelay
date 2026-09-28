# Valkey storage design

This document records the accepted internal storage shape for the standalone Valkey deployment. All keys use the compact versioned namespace `hr1:`. These names are internal implementation details, not public identifiers.

## Record encoding policy

The first version uses mixed encodings: Valkey Hashes for mutable records, Lists and Sorted Sets for queues and indexes, one compact JSON blob for each Canonical Message, compact JSON objects as Delivery Attempt history List entries, and Stream fields for administrative audit. Hash fields are strings: IDs and enums use their canonical string form, integer UTC timestamps ending `_ms` and counters use base-10 decimal strings, and boolean fields use `"0"` or `"1"`. JSON blobs follow the public `snake_case` and integer-millisecond contract; these wire encodings do not change public API types. Audit fields carry safe metadata, never payloads or credentials.

Exact field sets, TTLs, positional Lua arguments and return tuples, and pre-write validations are fixed by the implementation spec **before coding each affected slice**, not left implementation-defined. Scripts return bounded-status RESP arrays parsed by typed Go code; see [script contract](code-structure.md#valkey-scripts-and-tests).

## Recipient key encoding

Recipient Identity is serialized with an explicit scope namespace:

```text
chat  → <bot_platform>:<bot_id>:chat:<chat_id>
user  → <bot_platform>:<bot_id>:user:<user_id>
bot   → <bot_platform>:<bot_id>:bot
relay → <bot_platform>:<bot_id>:relay
```

Every component is non-empty and must not contain `:`. Bot Platform is 1–32 lowercase ASCII characters matching `[a-z][a-z0-9_-]*`. Bot Identifier, Chat Identifier, and User Identifier are 1–128 UTF-8 bytes and contain no control characters. Converters perform any platform-specific canonicalization before common validation; common code does not trim whitespace or change case. Explicit scope prevents equal numeric chat and user values from colliding and removes the need for reserved synthetic Chat Identifier strings for bot or relay queues.

## Delivery Queue

Each Recipient's ordered Delivery Queue is a Valkey List of `message_id` values:

```text
hr1:r:<recipient_identity>:q
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
hr1:r:<recipient_identity>:s
```

It contains the current fields applicable to the state transition, including:

```text
status = ready | leased | retry_wait
head_message_id
delivery_cycle
attempt
delivery_token
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

The active Delivery Token is stored in plaintext as `delivery_token` in the Recipient head state for the lifetime of the Delivery Attempt. A repeated claim must return the same token after a lost HTTP response, so its idempotent operation record also temporarily contains the plaintext token:

```text
hr1:op:<operation_id>
```

The claim-operation record has a 10-minute TTL and stores the request-argument digest. For an empty completed poll, it stores the recorded empty outcome until expiry. For a successful claim, it temporarily stores the completed response, including the plaintext token and payload reference needed to replay a lost HTTP response while the Delivery Attempt remains active. After acknowledgement, negative acknowledgement, or expiry of that attempt, the plaintext token and replayable payload response are removed from the claim-operation record, leaving only a marker that causes repeat claim requests to return `409 claim_no_longer_active` until the original 10-minute TTL expires. The token itself is never written to logs, metrics, URLs, attempt history, audit records, or compact success metadata.

After acknowledgement, negative acknowledgement, or expiry, plaintext copies of the token are deleted from active state and operation caches. A one-hour terminal tombstone is keyed by SHA-256 digest:

```text
hr1:t:<delivery_token_digest>
```

It records the terminal outcome and enough safe metadata to make repeated `ack` or `nack` idempotent. Token encryption and a separate encryption key are deliberately not introduced in the first version. Valkey, its backups, and diagnostics are treated as secret-bearing infrastructure, and the Consumer API additionally requires the shared Consumer Secret.

## Administrative sessions

Administrative browser sessions are stored by session-token digest:

```text
hr1:admin_session:<session_digest>
```

The session token itself is never stored in plaintext. The record contains creation, last-seen, idle-expiry, absolute-expiry, and CSRF token fields. The CSRF token is stored in plaintext so it can be returned by `GET /admin/v1/session`; it is never logged or copied to audit. Successful login creates this record and appends the required audit event in the same Lua operation.

## Webhook Endpoint configuration

Webhook Endpoint configuration and verification credentials are stored in the dedicated Valkey instance behind an internal repository interface. The first version stores webhook credentials in plaintext rather than introducing an application encryption key or external secret manager.

Valkey, any future AOF/RDB backup artifacts, diagnostic exports, and operator access are therefore secret-bearing infrastructure. They require TLS and authentication in transit, restricted network and administrative access, and procedures that never expose credential fields in logs, metrics, audit events, UI responses, health output, or configuration listings. Backup storage, retention, and restore handling are deferred, but any future policy must treat backups as secret-bearing.

A Webhook Endpoint record is a Valkey Hash:

```text
hr1:wh:<webhook_type>:<webhook_identifier>
```

with:

```text
bot_id
enabled
credential_kind
credential_value
generation_id
created_ms
updated_ms
config_version
```

Webhook Type and Webhook Identifier are encoded in the key, and Bot Platform is derived from the one-to-one Webhook Type mapping. `generation_id` is a UUIDv7 created for this endpoint generation and changes when an identifier is deleted and later reused. `config_version` increments only within one generation. Admin API ETags combine both values so stale ETags from an earlier generation cannot mutate a recreated endpoint. Credential values are non-empty and limited to 8 KiB. Admin API read responses expose only credential kind and configured state, never values.

A Bot Identity index lists all endpoints configured for the bot:

```text
hr1:bot:<bot_platform>:<bot_id>:webhooks
  SET members = <webhook_type>:<webhook_identifier>
```

This permits multiple endpoints for one Bot Identity, including temporary overlap during credential replacement. Each endpoint still maps to exactly one Bot Identity. The first version allows at most 100 endpoints per Bot Identity.

A global Sorted Set lists endpoints for Admin API pagination:

```text
hr1:webhooks
  score  = created_ms
  member = <webhook_type>:<webhook_identifier>
```

Create, enable, disable, and delete transitions maintain the endpoint Hash, Bot Identity membership Set, global listing index, and required administrative audit append in the same Lua operation. Webhook ingestion reads the endpoint Hash directly from Valkey on every request. Valkey is both the source of truth and the immediate read path; no in-memory endpoint cache, refresh interval, revision notification, or cache staleness policy exists in the first version.

Webhook Identifier is optionally supplied on creation; otherwise hookrelay generates `wh_<base64url-128-bit-random>`. Webhook Identifier, Bot Identifier, generation ID, and credential are immutable. Webhook Identifier conflicts return `409`, and changing either identity requires a new endpoint.

An endpoint has exactly one credential. Credential replacement creates a new Webhook Endpoint for the same Bot Identity with the new credential, switches the external bot-platform webhook configuration to the new webhook URL, and then disables and deletes the old endpoint. No current/next credential state or credential-promotion operations exist.

Disabled endpoints externally behave like unknown endpoints and return the same `404`, while retaining configuration and credential for later enablement. Deletion is allowed only after disablement. It permanently removes the endpoint Hash, credential, Bot Identity membership, and listing membership without creating an endpoint tombstone. The same Webhook Identifier may be created again later if supplied externally; automatically generated identifiers use 128 random bits and are not intentionally reused. Disable and delete are audited without credential values.

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

An index member whose record expired is stale and is removed by maintenance. A record missing from the index is restored during startup reconciliation. Whether periodic consistency checking repeats this repair after startup remains open.

## Global dead-letter queue

The authoritative dead-letter record for a message is a Valkey Hash:

```text
hr1:dl:<message_id>
```

with:

```text
bot_platform
bot_id
recipient_scope
chat_id      # present only for chat scope
user_id      # present only for user scope
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

Dead-letter replay removes the dead-letter record and index entry, starts a new Delivery Cycle, and returns the same `message_id` before every not-yet-started message for its Recipient. It never interrupts an already leased head or a head waiting for retry: in those cases the replayed message is inserted immediately after that current head. If there is no active or retrying head, it is inserted directly at the queue head. The transition preserves the queue/state head invariant and includes the required administrative audit append in the same Lua operation.

The dead-letter Hash is authoritative. Startup reconciliation removes stale global index members and restores missing index members. If the Canonical Message is missing or another ambiguity prevents safe repair, hookrelay creates the Recipient block marker described below.

Administrative permanent deletion removes the global index member, dead-letter Hash, Canonical Message, and attempt history and appends the required audit event in the same Lua operation, without copying the payload into audit.

## Ambiguous Recipient block marker

The first version uses a minimal persistent marker rather than a full quarantine subsystem:

```text
hr1:q:<recipient_identity>
```

The marker stores only:

```text
detected_ms
reason_code
```

Its presence blocks claim, acknowledgement, negative acknowledgement, extension, maintenance transitions, and new ingestion for that Recipient. The Recipient is absent from ready, lease-deadline, and retry-deadline indexes. Other Recipients continue normally.

Hookrelay does not automatically alter ambiguous authoritative state and does not provide a generic repair UI in the first version. Operator recovery follows a documented runbook, verifies the corrected invariants, and only then removes the marker. Detection and recovery are logged, metered, and audited. Audit of ambiguity detection is best effort and must not prevent creation of the protective Recipient block marker. This policy does not authorize automatic repair of ambiguous authoritative state.

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

When retention expires, one Lua maintenance operation removes the global DLQ member, dead-letter Hash, Canonical Message, and attempt history and appends a required audit event without copying the payload. As with administrative permanent deletion, the transition is considered successfully completed only after both state changes and audit append succeed. Lua errors or lost responses are not evidence that no deletion occurred; the audit execution limits below still apply.

## Administrative audit

Administrative audit events are stored in the bounded `hr1:audit` Stream. Its accepted retention is at most 30 days or 1,000,000 events, whichever boundary is reached first. Event contents and redaction rules are defined in [platform design](platform.md#components).

For critical administrative mutations, one versioned Lua operation performs the state change and appends the required event with `XADD`. This applies to Webhook Endpoint creation, enablement, disablement, and deletion, and administrative DLQ replay and permanent deletion. The API reports success only after both state and audit writes succeed. A repeated deletion of an already absent endpoint performs no new mutation and creates no additional audit event.

Successful administrative login creates its session record and appends the required audit event in the same Lua operation. The server issues the session cookie only after confirmed success. Logout and session-expiry audit is best effort: an audit-write failure must not prevent revocation or keep an expired session valid. Logout returns `204` only after confirmed deletion or absence of the session, and `503` if revocation cannot be confirmed.

Background DLQ retention deletion also requires its state changes and audit append in the same Lua operation. Safe derived-index repairs and ambiguity detection use best-effort audit so a failed audit write does not itself stop reconciliation or protective Recipient blocking. Ambiguous authoritative state is still not repaired automatically.

Scripts validate arguments, key types, and applicable state/version preconditions before their first write. Lua execution prevents command interleaving but does not roll back writes after a runtime error. Unexpected script errors or lost responses can therefore leave a partially applied or completed operation; neither proves that nothing changed. Recovery must verify persisted state **and** audit rather than assume rollback. The CLI and raw API do not blindly retry uncertain administrative mutations; reads can establish that a desired state is observed but cannot, by themselves, prove that the original operation or its mandatory audit append completed. See [Admin CLI recovery](admin-api.md#command-line-client). Per-operation reconciliation details for later DLQ and session mutations are specified with their slices.

Privileged DLQ payload inspection requires a confirmed audit append before any payload content is returned to the administrator. An audit failure or uncertain append result produces `503` without disclosing the payload. The event contains only safe access metadata and denotes authorized access beginning, not proof that the client received the response.

Audit writes for rejected authentication and pprof access are best effort. Failed authentication stays rejected regardless of audit availability. Profiling may proceed with valid Admin Bearer authentication while Valkey or its audit Stream is unavailable; this exception preserves diagnostics during dependency incidents and does not apply to critical mutations or DLQ payload inspection.

Standard-output copies of audit events are best effort and are not a substitute wherever a Stream append is required. Audit persistence follows the existing standalone AOF `everysec` contract, including its accepted disaster-loss window; these requirements add no per-operation fsync confirmation.

## Storage namespace versioning

The first version does not implement storage migrations or maintain a separate `hr1:schema` record. The `hr1` namespace itself identifies the storage format. A future incompatible format uses a new namespace such as `hr2` and requires an explicitly designed migration or cutover procedure rather than best-effort interpretation of old keys.

## Milestone 1 storage contract boundary

Before implementing the first vertical slice, its spec fixes exact encodings and Lua contracts for the keys it touches: `hr1:wh:<webhook_type>:<webhook_identifier>`, `hr1:bot:<bot_platform>:<bot_id>:webhooks`, `hr1:webhooks`, `hr1:audit`, `hr1:d:<dedup_identity_digest>`, `hr1:dedup_age`, `hr1:m:<message_id>`, `hr1:r:<recipient_identity>:q`, `hr1:r:<recipient_identity>:s`, `hr1:ready`, `hr1:ready_seq`, `hr1:op:<operation_id>`, `hr1:t:<delivery_token_digest>`, `hr1:success:<message_id>`, `hr1:q:<recipient_identity>`, and `hr1:stats:queued_messages`. This covers endpoint creation and audit, atomic acceptance/deduplication, immediate claim, acknowledgement, terminal token result, claim replay state, and the first-slice Recipient ambiguity marker. The spec also fixes startup validation and safe repair for these structures and their derived indexes/counter; it must not reconstruct lost authoritative data or treat an unhandled inconsistency as ready. Later retry, DLQ, session, and maintenance field contracts and startup reconciliation checks are fixed before those slices rather than speculated about now.

## Accepted key names

```text
hr1:m:<message_id>                         Canonical Message
hr1:r:<recipient_identity>:q               Delivery Queue
hr1:r:<recipient_identity>:s               queue-head state
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
hr1:q:<recipient_identity>                 ambiguous Recipient block marker
hr1:success:<message_id>                   compact success metadata
hr1:admin_session:<session_digest>         administrative browser session
hr1:wh:<webhook_type>:<webhook_identifier> Webhook Endpoint Hash
hr1:bot:<bot_platform>:<bot_id>:webhooks   Bot Identity membership SET
hr1:webhooks                               global endpoint listing ZSET
hr1:audit                                  administrative audit Stream
hr1:stats:queued_messages                  derived global queued-message counter
```

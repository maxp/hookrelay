# hookrelay

Webhook relay that distributes incoming events to recipients through ordered delivery channels.

## Status

Milestone 1 (the tracer-bullet vertical slice) is implemented; its exit criterion is a green CI pipeline including the Compose smoke. Done: the application scaffold (listeners, configuration, observability, graceful shutdown) and the Admin endpoint API vertical — operators can create and read Telegram Webhook Endpoints through the Admin API, with each state change and its mandatory audit append committed as one atomic Lua operation in Valkey — the Admin CLI (`hookrelay admin webhook create|get`) on top of that API, and the webhook ingestion happy path: signed Telegram updates are verified, converted into Canonical Messages, deduplicated, and atomically queued per Recipient in Valkey. The ingestion rejection matrix, process protections, and the complete Telegram classification surface (every event-to-Recipient row, routing issues, fallback deduplication) are in place, and consumers can claim and acknowledge queued messages through the Consumer API (`POST /v1/deliveries/claim`, `POST /v1/deliveries/ack`). Claims long-poll for up to 30 seconds, startup reconciliation validates and safely repairs persisted state before readiness, and an automated Compose smoke test proves the whole slice.

Milestone 2 (the complete delivery failure path, the first deployable release candidate) is implemented; its exit criterion is a green CI pipeline including the extended Compose smoke. It adds negative acknowledgement, lease extension and expiry, bounded retries with jitter, the global DLQ after the fourth failed attempt, audited DLQ replay with deduplication-conflict protection, the delivery-state read, DLQ retention, cooperative background and inline maintenance, Recipient block listing, inspection, and preconditioned clearing, startup reconciliation that executes overdue expiries and retries before readiness, webhook rate limits, the Valkey memory acceptance stop, and deduplication early eviction. Work is tracked as Markdown issues under [`.scratch/milestone-1/`](.scratch/milestone-1/) and [`.scratch/milestone-2/`](.scratch/milestone-2/).

## Goals

- accept multiple webhook types selected by a route prefix;
- use the remainder of the route as the webhook identifier;
- verify each request according to its webhook type and identifier;
- convert verified requests into a canonical message;
- deduplicate canonical messages before routing;
- route new messages to ordered recipient queues;
- provide structured logging and core operational metrics;
- keep product behavior independent of the selected infrastructure.

## Processing model

```text
webhook route
  → resolve type and identifier
  → verify request
  → convert to canonical message
  → deduplicate
  → route to delivery queues
```

## Observability

The service must emit JSON logs to standard output with `snake_case` fields, integer `timestamp_ms`, bounded event names, and request/message correlation, without credentials or sensitive payloads. Feature events replace duplicate webhook and Consumer API access logs; empty long polls and health/metrics polling do not produce per-request logs. The [logging contract](docs/design/platform.md#structured-log-contract) defines fields, lifecycle events, and failure handling. Core metrics cover received requests, verification and conversion failures, duplicate messages, routed messages, processing latency, and delivery queue depth.

Log verbosity is selected at startup: development defaults to `debug`, production to `info`. The protected administrative listener provides authenticated, redacted configuration inspection at `GET /debug/config`. Critical administrative mutations require the state change and Valkey audit append to succeed in the same Lua operation before success is reported; standard-output logging remains best effort.

Privileged DLQ payload inspection requires a confirmed audit append before content is returned; failure returns `503` without disclosing the payload. Audit for rejected authentication and pprof access is best effort: audit failures never grant access, while explicitly enabled, Admin Bearer-authenticated profiling remains usable during Valkey outages. Session creation and retention-driven DLQ deletion also require audit; audit-write failures must not prevent logout, session expiry, safe derived-index repair, or protective Recipient blocking. See [audit failure policies](docs/design/admin-api.md#audit-failure-policies) and the [audit storage contract](docs/design/storage.md#administrative-audit).

## Applied technical decisions

- Go is the application runtime.
- Valkey is the current persistence and queue-coordination technology.
- Queue consumers use a long-polling HTTP Consumer API rather than direct Valkey access.
- Delivery is at least once and ordered independently for each recipient.
- Significant technical decisions and their rationale are recorded as ADRs under [`docs/adr/`](docs/adr/).

## Design documentation

- [`CONTEXT.md`](CONTEXT.md) defines the canonical domain vocabulary.
- [`docs/design/platform.md`](docs/design/platform.md) records the platform and component topology.
- [`docs/design/message-contract.md`](docs/design/message-contract.md) defines the initial webhook and Canonical Message contract.
- [`docs/design/deduplication.md`](docs/design/deduplication.md) defines deduplication and atomic acceptance.
- [`docs/design/delivery.md`](docs/design/delivery.md) defines the ordered-delivery state model.
- [`docs/design/consumer-api.md`](docs/design/consumer-api.md) defines the long-polling Consumer API.
- [`docs/design/admin-api.md`](docs/design/admin-api.md) defines administrative management of Webhook Endpoints.
- [`docs/design/configuration.md`](docs/design/configuration.md) defines flags, environment variables, defaults, and production validation.
- [`docs/design/code-structure.md`](docs/design/code-structure.md) defines the initial Go module map and seams.
- [`docs/design/deployment.md`](docs/design/deployment.md) defines container, Compose, and CI policy.
- [`docs/design/implementation-milestones.md`](docs/design/implementation-milestones.md) defines the tracer-bullet implementation sequence.
- [`docs/design/telegram-adapter.md`](docs/design/telegram-adapter.md) defines Telegram verification, update identity, and recipient extraction policy.
- [`docs/design/storage.md`](docs/design/storage.md) defines the accepted internal Valkey data structures and key namespace.
- [`docs/design/open-questions.md`](docs/design/open-questions.md) lists decisions that remain open.
- [`docs/runbooks/recipient-block-recovery.md`](docs/runbooks/recipient-block-recovery.md) defines safe diagnosis and clearing of an ambiguous Recipient block with `hookrelay admin recipients inspect-block`/`clear-block`.

## Repository layout

- `cmd/hookrelay` — the single executable entry point;
- `internal/app` — composition root, listeners, readiness gate, graceful shutdown;
- `internal/config` — flags, environment, defaults, and startup validation;
- `internal/observability` — structured logging and the private metrics registry;
- `internal/cli` — serve, admin, version, generate, and healthcheck commands;
- `internal/administration` — Admin API service and HTTP transport;
- `internal/model` — Recipient Identity and the Canonical Message codec;
- `internal/ingestion` — webhook pipeline and the Telegram adapter;
- `internal/delivery` — Consumer API transport and delivery use cases;
- `internal/jsonbody` — strict JSON request-body discipline shared by the Admin and Consumer APIs;
- `internal/valkey` — Valkey adapter: embedded versioned Lua scripts (`endpoint_create_v1`, `accept_v3`, `claim_v3`, `ack_v4`, `nack_v3`, `expire_lease_v3`, `activate_retry_v1`, `extend_v1`, `replay_dlq_v2`, `delivery_state_v1`, `expire_dlq_v1`, `evict_dedup_v1`, `inspect_block_v2`, `clear_block_v2`, `reconcile_recipient_v3`, `reconcile_dlq_v1`, `reconcile_dedup_v1`, `reconcile_counter_v1`), readiness gate, endpoint, Recipient, DLQ, and delivery-state stores, message acceptance;
- `internal/gen` — identifier and secret generation;
- `spike/` — the throwaway valkey-go client spike (see [ADR 0006](docs/adr/0006-valkey-go-client.md));
- `docs/design/` — accepted design documents; `docs/adr/` — architecture decision records.

## Development

Build and test with the pinned Go toolchain (see `go.mod`):

```sh
go build ./...
go test ./...
```

Storage tests run against a real pinned Valkey (a fake cannot prove script
semantics). Point `HOOKRELAY_TEST_VALKEY_URL` at an instance; without it the
integration tests skip locally. CI runs them against `valkey/valkey:9.1.2`:

```sh
docker run --rm -p 6379:6379 valkey/valkey:9.1.2
HOOKRELAY_TEST_VALKEY_URL=valkey://127.0.0.1:6379/0 go test ./...
```

Generate local shared secrets and run the Compose stack (pinned Valkey with
AOF `everysec` and `noeviction`):

```sh
go run ./cmd/hookrelay generate consumer-secret --output-file .secrets/consumer
go run ./cmd/hookrelay generate admin-secret --output-file .secrets/admin
chmod 600 .secrets/consumer .secrets/admin
docker compose up --build
```

The Compose smoke test proves the Milestone 1 slice and the Milestone 2
failure path against that stack in production mode (endpoint via the Admin
CLI → signed webhook and its duplicate → claim → ack and repeated ack →
empty queue → metrics and health → three nacks with observed retries →
fourth nack dead-letters → `admin dlq list|get|replay` → `admin message
delivery-state` → claim in `delivery_cycle=2` → ack → a lease left claimed
→ restart keeping the Valkey volume after the lease expired → the expiry is
processed by startup reconciliation before readiness → persisted endpoint
and continued deduplication). It shortens retry delays to 300 ms and the
initial lease to 5 s. It uses its own Compose project, generated secrets, and free
loopback ports, and cleans up after itself; CI runs it as the `smoke` job:

```sh
scripts/smoke.sh
```

Health endpoints live on the administrative listener:
`/health/live`, `/health/ready`, `/health/accepting-webhooks`, `/metrics`.
The public listener opens only after the readiness gate (Valkey availability,
script load with digest verification, known-structure validation) has
succeeded, and `/health/ready` reports ready only once that listener is open.
The gate re-runs every second; losing Valkey withdraws readiness until the
full gate passes again.

Before readiness (and again after Valkey recovers) hookrelay reconciles
persisted state in bounded `SCAN`/`ZSCAN` batches: it validates Webhook
Endpoints, Bot Identity sets, and global index types, holds readiness on
malformed live deduplication records, repairs derived ready, lease, retry,
blocked, DLQ, and deduplication indexes and (at startup) the queued-message
counter, and isolates any Recipient whose queue or head state is ambiguous
behind a persistent block marker (`hr1:q:<recipient>`) while every other
Recipient serves normally — including a queued message whose attempt
history has no valid saved replay cycle/attempt
(`queued_delivery_state_missing` / `queued_delivery_state_invalid`), which
is never silently reset to attempt 1. Authoritative state is never rewritten. Leases
already past their deadline are expired and due retries activated through
the normal transitions (with their events and metrics) before readiness,
followed by a verifying pass. State that cannot be isolated holds readiness
false (`"startup_reconciliation":"held"` in `/health/ready`), and so does a
dead-letter entry whose Canonical Message is missing
(`reason_code=dead_letter_message_missing`; each such entry is logged as
`dead_letter_message_missing` with its `message_id`) — never a Recipient
block, because clearing a block cannot certify DLQ integrity; recovery is a
reviewed incident correction followed by a restart. Progress is
exported as `hookrelay_reconciliation_in_progress` and
`hookrelay_consistency_issues_total{kind,resolution}`; each block, hold, and
repair is logged, and repairs and blocks are audited best effort.

A blocked Recipient stays blocked until an operator follows the
[Recipient block recovery runbook](docs/runbooks/recipient-block-recovery.md):
`hookrelay admin recipients list --status blocked`, `inspect-block`, and the
preconditioned, audited `clear-block`. Other Recipients keep serving. Do not
delete the marker or edit indexes by hand.

## Webhook ingestion

The public listener serves `POST /webhook/{webhook_type}/{webhook_identifier}`
(Telegram: `/webhook/telegram/<webhook_identifier>`, configured at Telegram
with the endpoint's `secret_token`). The pipeline resolves the route,
verifies the `X-Telegram-Bot-Api-Secret-Token` header (and the optional
`HOOKRELAY_TELEGRAM_SOURCE_CIDRS` allowlist), converts the update into a
Canonical Message for its chat, user, bot, or relay Recipient, and runs one
atomic `accept_v3` transition that deduplicates and queues it.

- Accepted and duplicate updates both receive an empty `200`, returned only
  after Valkey commits or proves the duplicate. A repeated `update_id` with
  different bytes is still a duplicate and increments
  `hookrelay_dedup_conflicts_total`.
- Unknown, disabled, or malformed routes: `404`; other methods: `405` with
  `Allow: POST`; verification failure: `403`; invalid JSON: `400`; body over
  256 KiB: `413`; non-JSON media type or compressed body: `415`; body not
  received within the 10-second request deadline: `408`; Valkey
  unavailable, blocked Recipient, capacity reached, or more than
  `HOOKRELAY_MAX_INFLIGHT_WEBHOOKS` (default 100) requests in flight: `503`
  with `Retry-After: 1`; corrupt storage state: `500`. Bodies are always
  empty; every response carries `X-Request-Id`.
- Rate limits: one global and one per-Webhook-Endpoint token bucket
  (`HOOKRELAY_WEBHOOK_GLOBAL_RATE`/`_BURST`,
  `HOOKRELAY_WEBHOOK_ENDPOINT_RATE`/`_BURST`; a zero rate disables a limit
  in development) are checked after the endpoint resolves and before the
  in-flight slot and the body read. An empty bucket answers `429` with
  `Retry-After` in whole seconds until a token (at least 1), counted as
  `outcome=rate_limited`. Endpoint buckets are process-local and bounded to
  the 10,000 most recently used endpoints.
- Memory stop: when Valkey `used_memory` reaches
  `HOOKRELAY_MEMORY_ACCEPTANCE_STOP_PERCENT` (default 90) of `maxmemory`
  (no stop without `maxmemory`), new messages are refused with `503` (no
  writes) while consumers keep draining; `/health/ready` is unaffected.
- Deduplication early eviction: when the live deduplication records reach
  `HOOKRELAY_MAX_DEDUP_RECORDS`, the oldest record is evicted early inside
  the acceptance itself (`accept_v3`), and maintenance evicts proactively
  (`evict_dedup_v1`) — but never a record younger than
  `HOOKRELAY_DEDUP_MIN_RETENTION`. Only then does acceptance refuse with
  `dedup_capacity`.
- `/health/accepting-webhooks` (and `hookrelay_accepting_webhooks`) reports
  `503`/`0` when Valkey is unavailable, the global queue
  (`HOOKRELAY_MAX_QUEUED_MESSAGES`) is full, the live deduplication records
  are at capacity with no record old enough to evict, or the memory stop
  applies; it is re-evaluated every second (and by each maintenance round).
  `/health/ready` stays `200` under capacity pressure so consumers can
  drain, and a single full Recipient only rejects its own messages.
- Feature events `webhook_accepted` and `webhook_duplicate`; metrics
  `hookrelay_webhook_requests_total{webhook_type,outcome}`,
  `hookrelay_webhook_request_duration_seconds`,
  `hookrelay_webhook_request_body_bytes`,
  `hookrelay_messages_accepted_total{bot_platform,recipient_scope}`,
  `hookrelay_messages_duplicate_total`, `hookrelay_dedup_conflicts_total`,
  `hookrelay_dedup_capacity_rejections_total`,
  `hookrelay_routing_issues_total{bot_platform,reason}`,
  `hookrelay_event_time_issues_total{bot_platform,reason}`,
  `hookrelay_webhook_inflight`, `hookrelay_dedup_records`,
  `hookrelay_dedup_record_capacity`, `hookrelay_dedup_early_evictions_total`,
  `hookrelay_dedup_oldest_record_age_seconds`,
  `hookrelay_dedup_effective_retention_seconds`,
  `hookrelay_valkey_memory_used_bytes`, `hookrelay_valkey_memory_max_bytes`,
  `hookrelay_valkey_aof_enabled`, `hookrelay_valkey_aof_delayed_fsync_total`
  (sampled by each maintenance round), and `hookrelay_accepting_webhooks`.

Storage keys and capacity limits are specified in
[`.scratch/milestone-1/spec.md`](.scratch/milestone-1/spec.md) (the v1
transition contracts) and
[`.scratch/milestone-2/spec.md`](.scratch/milestone-2/spec.md) (the v2
amendments and failure-path scripts: `accept_v2` and `ack_v2`/`ack_v3`
maintain the `hr1:mi:<message_id>` message metadata; `claim_v2` records
`attempt_started_ms` and `claim_v3` the token digest; `nack_v2` and
`expire_lease_v2` schedule retries or dead-letter the fourth failure, and
`activate_retry_v1` activates retries; `replay_dlq_v1` replays a dead
letter and `replay_dlq_v2` keeps waiting replays in replay order, and `ack_v4`, `nack_v3`, and `expire_lease_v3` restore the
cycle/attempt that replay saves as `pending_delivery_cycle` /
`pending_attempt` in `hr1:mi` when they expose the next head);
see also
[`docs/design/storage.md`](docs/design/storage.md).

## Consumer API

Queue consumers use the public listener with the shared Consumer Secret
(`Authorization: Bearer <consumer secret>`):

```sh
curl -X POST http://<public>/v1/deliveries/claim \
  -H 'Authorization: Bearer <consumer secret>' -H 'Content-Type: application/json' \
  -d '{"operation_id":"<uuidv7>","wait_ms":0}'
```

- `200` returns `{"delivery": {delivery_token, delivery_cycle, attempt,
  claimed_ms, lease_expires_ms}, "message": <Canonical Message>}`; `204` means
  no ready work. Each claim holds the Recipient's head under a 60-second
  lease (`HOOKRELAY_INITIAL_LEASE_DURATION`); later messages for that
  Recipient wait. Background maintenance expires a lease past its deadline
  as a failed attempt (recorded as `expired` in the attempt history) and
  schedules its retry exactly like `nack`; `ack`/`nack` with the expired
  token then return `409 stale_delivery_token` and a repeated claim
  `409 claim_no_longer_active`.
- Repeating an `operation_id` with the same arguments replays the recorded
  outcome (the same token while the attempt is active) for 10 minutes;
  `409 operation_conflict` for other arguments, `409 claim_no_longer_active`
  once the attempt ended; `429 consumer_limit_exceeded` with
  `Retry-After: 1` at `HOOKRELAY_MAX_ACTIVE_LEASES` unexpired leases;
  `503` means repeat the same `operation_id`.
- `POST /v1/deliveries/ack` with `{"delivery_token": "dlv_..."}` completes the
  attempt: `200 {"status":"acknowledged","message_id","acknowledged_ms"}`,
  and repeating it returns the same recorded result for one hour. The
  message leaves the queue and the Recipient's next message becomes ready.
  `404 delivery_token_not_found` for an unknown or expired token,
  `409 stale_delivery_token` after the lease deadline or when superseded,
  `409 recipient_blocked` while the Recipient is blocked,
  `409 delivery_already_nacked` after a negative acknowledgement. Compact
  success metadata (no identifiers or payload) is kept for 24 hours.
- `POST /v1/deliveries/nack` with `{"delivery_token": "dlv_...",
  "reason_code": "..."}` (`reason_code` optional, 1–64 characters of
  `[A-Za-z0-9_.:-]`) fails the attempt: `200 {"status":"retry_scheduled",
  "message_id","attempt","retry_at_ms"}`, and repeating it returns the same
  recorded result for one hour. The message stays the Recipient's head and
  waits for the server-chosen delay (`HOOKRELAY_RETRY_DELAYS`, default
  `1s,5s,30s`, × jitter `HOOKRELAY_RETRY_JITTER_MIN`–`_MAX`, default
  0.5–1.0); the failed attempt is recorded in the message's attempt history.
  `409 delivery_already_acknowledged` after an acknowledgement; otherwise the
  same `404`/`409`/`503` outcomes as `ack`. Background maintenance makes the
  message claimable again once its retry is due; the next claim returns it
  with a new Delivery Token and the next `attempt`. Failing the fourth
  attempt (`HOOKRELAY_MAX_DELIVERY_ATTEMPTS`) by `nack` returns
  `200 {"status":"dead_lettered","message_id","delivery_cycle",
  "dead_lettered_ms"}` (repeatable): the message moves to the global DLQ
  (blob and attempt history retained) and the Recipient's next message
  becomes claimable. A fourth expired lease dead-letters the same way.
- `POST /v1/deliveries/extend` with `{"delivery_token": "dlv_...",
  "operation_id": "<uuidv7>"}` pushes the lease deadline by the
  server-chosen `HOOKRELAY_LEASE_EXTENSION_DURATION` (60 s), never beyond
  `HOOKRELAY_MAX_LEASE_LIFETIME` (5 min) from the attempt start:
  `200 {"status":"extended","message_id","lease_expires_ms",
  "max_lease_expires_ms"}`. Repeating the same `operation_id` returns the
  recorded deadline for 10 minutes without extending again;
  `409 operation_conflict` for another token or a claim's operation id,
  `409 maximum_lease_lifetime_reached` at the cap, `409 stale_delivery_token`
  after the deadline, otherwise the same `404`/`409`/`503` outcomes as `ack`.
- Background maintenance starts once the process is ready and runs every
  `HOOKRELAY_MAINTENANCE_INTERVAL` (1 s) plus up to
  `HOOKRELAY_MAINTENANCE_INTERVAL_JITTER` (250 ms), in batches of
  `HOOKRELAY_MAINTENANCE_BATCH_SIZE` (100), at most
  `HOOKRELAY_MAINTENANCE_MAX_CONTINUOUS_BATCHES` (5) per kind and round. It
  expires due leases and activates due retries, each kind in its own
  batches. It is cooperative (every transition re-validates stored state)
  and stops taking
  batches at shutdown; a panic in the loop withdraws readiness and shuts the
  process down with a non-zero exit.
- A Recipient whose stored state is inconsistent is isolated behind a block
  marker during the claim scan and skipped; other Recipients continue.
- `wait_ms` (0–30000, default 30000) long-polls: the claim rechecks
  atomically every 250 ms plus 0–50 ms jitter until work appears (returned
  at once) or the deadline passes (`204`, recorded for replay). An
  in-process notifier also wakes the oldest waiting claim for an immediate
  recheck when this process makes work ready (acceptance, ack, dead-letter,
  retry activation, head replay, block clear);
  `HOOKRELAY_CLAIM_NOTIFICATIONS=false` leaves only the periodic rechecks
  (ADR 0008). A client that
  disconnects abandons the wait; repeating the same `operation_id` recovers
  a lease that raced with the disconnect. At most
  `HOOKRELAY_MAX_WAITING_CLAIMS` (default 20) claims wait per process;
  more receive `429 consumer_limit_exceeded` with `Retry-After: 1`.
  Graceful shutdown ends waiting claims with `503` and `Retry-After: 1`.
  Before a waiting claim waits on an empty ready index it runs one bounded
  maintenance pass (at most 10 due lease expiries and retry activations,
  counted as `kind=inline_lease_expiry`/`inline_retry_activation`) and
  rechecks at once, so a retry that just became due is claimed without
  waiting for background maintenance.
- Metrics: `hookrelay_delivery_claims_total{outcome}`,
  `hookrelay_delivery_attempts_total{recipient_scope,outcome}`,
  `hookrelay_delivery_attempt_duration_seconds{recipient_scope,outcome}`,
  `hookrelay_retries_waiting`, `hookrelay_dead_letter_messages`,
  `hookrelay_dead_letters_total{recipient_scope,reason}`,
  `hookrelay_maintenance_processed_total{kind,result}` (`kind` =
  `lease_expiry` | `retry_activation` | `dlq_retention` | `inline_lease_expiry` |
  `inline_retry_activation`; `result` = `applied` | `stale` |
  `blocked` | `failed`),
  `hookrelay_maintenance_due_lag_seconds{kind}`,
  `hookrelay_maintenance_batch_size{kind}`,
  `hookrelay_maintenance_duration_seconds{kind}`, `hookrelay_active_leases`, `hookrelay_waiting_claims`,
  `hookrelay_queue_messages`, `hookrelay_ready_recipients`,
  `hookrelay_blocked_recipients`, `hookrelay_oldest_ready_message_age_seconds`
  (the head of the Recipient ready longest, from its `received_ms`); feature event `delivery_claimed` (tokens
  are never logged), `delivery_acknowledged`, `delivery_nacked`,
  `delivery_lease_expired`, `delivery_dead_lettered`, `delivery_lease_extended`,
  `dead_letter_expired`.
- Dead letters are kept for `HOOKRELAY_DLQ_RETENTION` (default `720h`).
  Background maintenance then deletes each one — record, Canonical Message,
  metadata, and attempt history — and appends the `dead_letter_expired`
  audit event (actor `maintenance`, no payload) in the same atomic operation
  (`expire_dlq_v1`).

## Admin API

The administrative listener serves the authenticated Admin API. Requests
authenticate with `Authorization: Bearer <admin secret>`
(`HOOKRELAY_ADMIN_SECRET` or `HOOKRELAY_ADMIN_SECRET_FILE`); rejected attempts
are audited best effort. Health and metrics endpoints stay unauthenticated.

- `POST /admin/v1/webhooks` — create a Webhook Endpoint (`webhook_type`,
  `bot_id`, `credential`; optional `webhook_identifier`, `enabled`). The body
  must be `application/json` (`415` otherwise) and at most 16 KiB (`413`).
  Returns `201` with `Location`, an `ETag` of
  `"<generation_id>:<config_version>"`, and a safe body that never contains
  the credential value. Duplicate identifiers and the per-bot endpoint limit
  return `409`. A `503` whose message says the outcome is uncertain means the
  create may have been applied: read the endpoint and the audit before
  retrying.
- `GET /admin/v1/webhooks?limit&cursor` — every endpoint's safe metadata,
  newest first, 50 per page by default (1–200), with an opaque
  `next_cursor`; `400 invalid_cursor` for a malformed cursor.
- `GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}` — read the
  endpoint metadata with its `ETag`; a missing endpoint returns `404`.
- `PATCH /admin/v1/webhooks/{webhook_type}/{webhook_identifier}` with
  `{"enabled": true|false}` and `If-Match: "<generation_id>:<config_version>"`
  — enable or disable; `200` with the new `ETag` and the endpoint. A
  disabled endpoint answers webhooks exactly like an unknown one (`404`)
  and keeps its credential. Re-sending the current value changes nothing
  (same `ETag`, no audit). `428 precondition_required` without `If-Match`,
  `412 precondition_failed` for a stale or earlier-generation tag, `400`
  for a weak, wildcard, or malformed tag, `404` for a missing endpoint.
- `GET /admin/v1/recipient-states?status=ready|leased|retry_wait|blocked&limit&cursor`
  — Recipients in one state as structured Recipient fields with the state's
  time (`ready_sequence`, `lease_expires_ms`, `retry_at_ms`, or `detected_ms`
  plus `reason_code`), ascending, 50 per page by default (1–200), with an
  opaque `next_cursor` (`400 invalid_cursor` when malformed).
- `POST /admin/v1/recipient-blocks/inspect` with `{"recipient": {...}}` —
  read-only: marker, queue length, bounded head state (no token), head
  message presence, index memberships, and violated invariants.
- `POST /admin/v1/recipient-blocks/clear` with the Recipient,
  `expected_detected_ms`, and `expected_reason_code` — `204` after the
  invariants re-verify, removing the marker, restoring exactly the implied
  index, and appending the audit event atomically; `428 precondition_required`
  without the expected values, `412 precondition_failed` for a changed
  marker, `404 recipient_block_not_found`, `409 recipient_state_ambiguous`
  (with the violated invariant); a `503` may be an uncertain outcome.
- `GET /admin/v1/dead-letters?limit&cursor` — dead letters newest first by
  safe metadata (`message_id`, structured `recipient`, `dead_lettered_ms`,
  `dead_letter_reason`, `delivery_cycle`), 50 per page by default (1–200),
  with an opaque `next_cursor`. Payloads are never returned.
- `GET /admin/v1/dead-letters/{message_id}` — one dead letter with its
  retained attempt history (and any `archived_cycles_summary`);
  `404 dead_letter_not_found`.
- `POST /admin/v1/dead-letters/{message_id}/replay` with an optional
  `{"deduplication_conflict_resolution": "reject" | "keep_current"}`
  (default `reject`) — `200 {"status":"replayed","message_id",
  "delivery_cycle","queue_position","replayed_ms",
  "deduplication_resolution"}`. The message returns to its Recipient in a
  new Delivery Cycle (attempt 1) ahead of every not-yet-started message
  other than earlier replays still waiting, which keep replay order (first
  in, first out): as the head (`queue_position=head`), right behind a
  leased or retry-waiting head that is never interrupted
  (`after_active_head`), or behind the earlier waiting replays
  (`after_pending_replay`). Replaying several dead letters of one
  Recipient oldest first therefore restores their original order, whatever
  a Consumer claims in between. A preempted ready head keeps its cycle and
  attempt. Replay is not limited by the queue capacity limits. `404
  dead_letter_not_found`, `409 deduplication_conflict` when the original
  Deduplication Identity now maps to another message (`keep_current`
  replays and leaves that newer mapping unchanged), `409 recipient_blocked`;
  a `503` may be an uncertain outcome.
- `GET /admin/v1/messages/{message_id}/delivery-state` —
  `{"message_id","delivery_cycle","state","queue_position"?}` with `state`
  `queued` | `leased` | `retry_wait` | `dead_lettered` | `acknowledged`
  (while the 24-hour success metadata is retained) and `queue_position`
  `head` | `behind_head` for a message in its queue; never a payload or
  token. `404 message_not_found`, `409 recipient_state_ambiguous` for state
  that cannot be classified.

Every mutation commits the state change and the audit append as one atomic
Lua operation, logs a feature event such as `webhook_endpoint_created` or
`delivery_replayed`, and is counted in `hookrelay_audit_events_total`
(replays also in `hookrelay_dead_letter_replays_total{outcome}`); failed best-effort audit
appends are counted in `hookrelay_audit_write_failures_total`. The full contract lives in
[`docs/design/admin-api.md`](docs/design/admin-api.md).

## Admin CLI

`hookrelay admin` is an HTTP client for the Admin API; it never touches
Valkey directly.

```sh
export HOOKRELAY_ADMIN_SECRET_FILE=.secrets/admin
hookrelay admin webhook create --type telegram --bot-id 123456 --credential-file telegram-secret
hookrelay admin webhook get --type telegram --identifier wh_...
hookrelay admin webhook list [--limit 50] [--cursor <next_cursor>]
hookrelay admin webhook disable --type telegram --identifier wh_... --yes
hookrelay admin webhook enable --type telegram --identifier wh_... --yes
hookrelay admin recipients list --status blocked
hookrelay admin recipients inspect-block --bot-platform telegram --bot-id 123456 --scope chat --chat-id -100
hookrelay admin recipients clear-block --bot-platform telegram --bot-id 123456 --scope chat --chat-id -100 \
  --expected-detected-ms <ms> --expected-reason-code <code> --yes
hookrelay admin dlq list
hookrelay admin dlq get --message-id <message_id>
hookrelay admin dlq replay --message-id <message_id> [--deduplication-conflict-resolution keep_current] --yes
hookrelay admin message delivery-state --message-id <message_id>
```

- Admin URL: `--admin-url`, then `HOOKRELAY_ADMIN_URL`, then
  `http://127.0.0.1:8081`.
- Admin Secret: `--admin-secret-file`, then `HOOKRELAY_ADMIN_SECRET_FILE`,
  then `HOOKRELAY_ADMIN_SECRET`, then a hidden prompt on a terminal. It is
  never accepted as a command-line value.
- Webhook credential: `--credential-file` or `HOOKRELAY_WEBHOOK_CREDENTIAL`
  (not both). `--credential-kind` defaults to the only kind of the Webhook
  Type (`secret_token` for Telegram).
- Output: `--output table|json`; table on a terminal, JSON otherwise. Results
  go to standard output, warnings and errors to standard error; secrets are
  never printed.
- Exit codes: `0` success, `1` failure (including every unconfirmed create),
  `2` usage error.

`create` generates the `wh_…` identifier before sending unless `--identifier`
is given. If the response is lost or the server fails (transport error or
`5xx`), the CLI never retries: it reads the endpoint by that identifier and
prints `{"outcome": "desired_state_observed" | "uncertain", ...}` with a
warning. Observing the requested state does not confirm the create or its
audit event; check the audit before any further mutation, and reuse the same
`--identifier` if a retry is ever needed. `clear-block` follows the same
discipline: after a lost response or `5xx` it never retries, inspects the
Recipient once, and reports `desired_state_observed` (marker gone, audit not
confirmed) or `uncertain`, always exiting `1`. `dlq replay` reads the
current Delivery Cycle first; after a lost response or `5xx` it never
retries, reads the message's delivery state once, and reports
`desired_state_observed` when it is in a newer Delivery Cycle (queued,
active, acknowledged, or dead-lettered again; the audit is still
unconfirmed), otherwise `uncertain` — always exiting `1`.

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

Licensed under the [Eclipse Public License 2.0](LICENSE) (`EPL-2.0`).

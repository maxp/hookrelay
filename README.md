# hookrelay

Webhook relay that distributes incoming events to recipients through ordered delivery channels.

## Status

Milestone 1 (the tracer-bullet vertical slice) is implemented; its exit criterion is a green CI pipeline including the Compose smoke. Done: the application scaffold (listeners, configuration, observability, graceful shutdown) and the Admin endpoint API vertical — operators can create and read Telegram Webhook Endpoints through the Admin API, with each state change and its mandatory audit append committed as one atomic Lua operation in Valkey — the Admin CLI (`hookrelay admin webhook create|get`) on top of that API, and the webhook ingestion happy path: signed Telegram updates are verified, converted into Canonical Messages, deduplicated, and atomically queued per Recipient in Valkey. The ingestion rejection matrix, process protections, and the complete Telegram classification surface (every event-to-Recipient row, routing issues, fallback deduplication) are in place, and consumers can claim and acknowledge queued messages through the Consumer API (`POST /v1/deliveries/claim`, `POST /v1/deliveries/ack`). Claims long-poll for up to 30 seconds, startup reconciliation validates and safely repairs persisted state before readiness, and an automated Compose smoke test proves the whole slice. Work is tracked as Markdown issues under [`.scratch/milestone-1/`](.scratch/milestone-1/).

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
- [`docs/runbooks/recipient-block-recovery.md`](docs/runbooks/recipient-block-recovery.md) defines safe diagnosis and clearing of an ambiguous Recipient block (its `inspect-block`/`clear-block` operations arrive after Milestone 1).

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
- `internal/valkey` — Valkey adapter: embedded versioned Lua scripts (`endpoint_create_v1`, `accept_v2`, `claim_v2`, `ack_v3`, `nack_v1`, `reconcile_*_v1`), readiness gate, endpoint store, message acceptance;
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

The Compose smoke test proves the whole Milestone 1 slice against that stack
in production mode (endpoint via the Admin CLI → signed webhook and its
duplicate → claim → ack and repeated ack → empty queue → metrics and health →
restart keeping the Valkey volume → persisted endpoint and continued
deduplication). It uses its own Compose project, generated secrets, and free
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
malformed live deduplication records, repairs derived ready, lease, blocked,
and deduplication indexes and (at startup) the queued-message
counter, and isolates any Recipient whose queue or head state is ambiguous
behind a persistent block marker (`hr1:q:<recipient>`) while every other
Recipient serves normally. Authoritative state is never rewritten. A lease
already past its deadline, or state that cannot be isolated, holds readiness
false (`"startup_reconciliation":"held"` in `/health/ready`). Progress is
exported as `hookrelay_reconciliation_in_progress` and
`hookrelay_consistency_issues_total{kind,resolution}`; each block, hold, and
repair is logged, and repairs and blocks are audited best effort.

Milestone 1 has no clear or repair operation yet. Diagnosis is read-only:

- **Blocked Recipient:** find it with `ZRANGE hr1:blocked 0 -1 WITHSCORES`
  and read `HGETALL hr1:q:<recipient>` (`detected_ms`, `reason_code`). Other
  Recipients keep serving; the blocked one stays blocked until the audited
  clear operation of the
  [Recipient block recovery runbook](docs/runbooks/recipient-block-recovery.md)
  ships with the Admin block routes. Do not delete the marker by hand.
- **Readiness held by a due lease** (`reconciliation_hold` log,
  `reason_code=due_lease`): Milestone 1 deliberately invents no expiry. The
  hold ends when the Milestone 2 lease-expiry transition processes the
  lease; until then recovery requires a reviewed incident procedure, not ad
  hoc Valkey edits.

## Webhook ingestion

The public listener serves `POST /webhook/{webhook_type}/{webhook_identifier}`
(Telegram: `/webhook/telegram/<webhook_identifier>`, configured at Telegram
with the endpoint's `secret_token`). The pipeline resolves the route,
verifies the `X-Telegram-Bot-Api-Secret-Token` header (and the optional
`HOOKRELAY_TELEGRAM_SOURCE_CIDRS` allowlist), converts the update into a
Canonical Message for its chat, user, bot, or relay Recipient, and runs one
atomic `accept_v2` transition that deduplicates and queues it.

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
- `/health/accepting-webhooks` (and `hookrelay_accepting_webhooks`) reports
  `503`/`0` when Valkey is unavailable or the global queue
  (`HOOKRELAY_MAX_QUEUED_MESSAGES`) or live deduplication records
  (`HOOKRELAY_MAX_DEDUP_RECORDS`) are at capacity; it is re-evaluated every
  second. `/health/ready` stays `200` under capacity pressure so consumers
  can drain, and a single full Recipient only rejects its own messages.
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
  `hookrelay_dedup_record_capacity`, and `hookrelay_accepting_webhooks`.

Storage keys and capacity limits are specified in
[`.scratch/milestone-1/spec.md`](.scratch/milestone-1/spec.md) (the v1
transition contracts) and
[`.scratch/milestone-2/spec.md`](.scratch/milestone-2/spec.md) (the v2
amendments and failure-path scripts: `accept_v2` and `ack_v2`/`ack_v3`
maintain the `hr1:mi:<message_id>` message metadata; `claim_v2` records
`attempt_started_ms`; `nack_v1` schedules retries); see also
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
  Recipient wait.
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
  same `404`/`409`/`503` outcomes as `ack`. Retry activation and
  dead-lettering of the fourth failure are later Milestone 2 tickets; until
  then the fourth `nack` returns `500 internal_error` without changing state.
- A Recipient whose stored state is inconsistent is isolated behind a block
  marker during the claim scan and skipped; other Recipients continue.
- `wait_ms` (0–30000, default 30000) long-polls: the claim rechecks
  atomically every 250 ms plus 0–50 ms jitter until work appears (returned
  at once) or the deadline passes (`204`, recorded for replay). A client that
  disconnects abandons the wait; repeating the same `operation_id` recovers
  a lease that raced with the disconnect. At most
  `HOOKRELAY_MAX_WAITING_CLAIMS` (default 20) claims wait per process;
  more receive `429 consumer_limit_exceeded` with `Retry-After: 1`.
  Graceful shutdown ends waiting claims with `503` and `Retry-After: 1`.
- Metrics: `hookrelay_delivery_claims_total{outcome}`,
  `hookrelay_delivery_attempts_total{recipient_scope,outcome}`,
  `hookrelay_delivery_attempt_duration_seconds{recipient_scope,outcome}`,
  `hookrelay_retries_waiting`, `hookrelay_active_leases`, `hookrelay_waiting_claims`,
  `hookrelay_queue_messages`, `hookrelay_ready_recipients`,
  `hookrelay_blocked_recipients`; feature event `delivery_claimed` (tokens
  are never logged), `delivery_acknowledged`, `delivery_nacked`.

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
- `GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}` — read the
  endpoint metadata with its `ETag`; a missing endpoint returns `404`.

Every mutation commits the state change and the audit append as one atomic
Lua operation, logs a feature event such as `webhook_endpoint_created`, and
is counted in `hookrelay_audit_events_total`; failed best-effort audit
appends are counted in `hookrelay_audit_write_failures_total`. The full contract lives in
[`docs/design/admin-api.md`](docs/design/admin-api.md).

## Admin CLI

`hookrelay admin` is an HTTP client for the Admin API; it never touches
Valkey directly.

```sh
export HOOKRELAY_ADMIN_SECRET_FILE=.secrets/admin
hookrelay admin webhook create --type telegram --bot-id 123456 --credential-file telegram-secret
hookrelay admin webhook get --type telegram --identifier wh_...
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
`--identifier` if a retry is ever needed.

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

Licensed under the [Eclipse Public License 2.0](LICENSE) (`EPL-2.0`).

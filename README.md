# hookrelay

Webhook relay that distributes incoming events to recipients through ordered delivery channels.

## Status

Milestone 1 (the tracer-bullet vertical slice) is in progress. Done so far: the application scaffold (listeners, configuration, observability, graceful shutdown) and the Admin endpoint API vertical — operators can create and read Telegram Webhook Endpoints through the Admin API, with each state change and its mandatory audit append committed as one atomic Lua operation in Valkey. The ingestion, delivery, and Consumer API slices are pending. Work is tracked as Markdown issues under [`.scratch/milestone-1/`](.scratch/milestone-1/).

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
- [`docs/runbooks/recipient-block-recovery.md`](docs/runbooks/recipient-block-recovery.md) defines safe diagnosis and clearing of an ambiguous Recipient block.

## Repository layout

- `cmd/hookrelay` — the single executable entry point;
- `internal/app` — composition root, listeners, readiness gate, graceful shutdown;
- `internal/config` — flags, environment, defaults, and startup validation;
- `internal/observability` — structured logging and the private metrics registry;
- `internal/cli` — serve, version, generate, and healthcheck commands;
- `internal/administration` — Admin API service and HTTP transport;
- `internal/valkey` — Valkey adapter: embedded versioned Lua scripts, readiness gate, endpoint store;
- `internal/ingestion` — Webhook Type registry (verification and conversion seams);
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

Health endpoints live on the administrative listener:
`/health/live`, `/health/ready`, `/health/accepting-webhooks`, `/metrics`.
The public listener opens only after the readiness gate (Valkey availability,
script load with digest verification, known-structure validation) has
succeeded, and `/health/ready` reports ready only once that listener is open.
The gate re-runs every second; losing Valkey withdraws readiness until the
full gate passes again.

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

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

Licensed under the [Eclipse Public License 2.0](LICENSE) (`EPL-2.0`).

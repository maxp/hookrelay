# hookrelay

Webhook relay that distributes incoming events to recipients through ordered delivery channels.

## Status

The project is at the design stage. The runtime, ingestion contract, deduplication model, storage shape, ordered Consumer API, Admin API, Telegram adapter, deployment policy, and first implementation milestone have been selected; implementation has not started.

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

The source layout will be added together with the first implementation milestone.

## Development

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

Licensed under the [Eclipse Public License 2.0](LICENSE) (`EPL-2.0`).

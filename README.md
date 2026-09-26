# hookrouter

Webhook router that distributes incoming events to users through ordered delivery channels.

## Status

The project is at the design stage. The runtime, ingestion contract, deduplication model, and ordered Consumer API have been selected; storage schemas and the first implementation milestone remain to be specified.

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

The service must emit structured logs without credentials or sensitive payloads. Core metrics cover received requests, verification and conversion failures, duplicate messages, routed messages, processing latency, and delivery queue depth.

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
- [`docs/design/delivery.md`](docs/design/delivery.md) defines the long-polling Consumer API and ordered-delivery state model.
- [`docs/design/open-questions.md`](docs/design/open-questions.md) lists decisions that remain open.

## Repository layout

The source layout will be added together with the first implementation milestone.

## Development

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

Licensed under the [Eclipse Public License 2.0](LICENSE) (`EPL-2.0`).

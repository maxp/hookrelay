# hookrouter

Webhook router that distributes incoming events to users through Valkey-backed queues.

## Status

The project is at the initial setup stage. The application architecture, runtime, and public API will be documented as they are implemented.

## Goals

- accept and validate incoming webhooks;
- route events to the appropriate users;
- preserve per-user queue ordering;
- use Valkey for durable queue coordination;
- support retries, idempotency, and observability.

## Repository layout

The source layout will be added together with the first implementation milestone.

## Development

Development and contribution conventions are documented in [`AGENTS.md`](AGENTS.md).

## License

No license has been selected yet.

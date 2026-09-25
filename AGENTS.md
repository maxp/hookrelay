# AGENTS.md

Guidance for humans and coding agents working in this repository.

## Project purpose

`hookrouter` receives webhooks and routes them to users through Valkey-backed queues. The design must preserve ordering where promised, tolerate duplicate delivery, and make failures observable and recoverable.

## Engineering principles

- Prefer small, reviewable changes.
- Keep webhook ingestion fast; move non-essential work to background processing.
- Treat every incoming event and every queue delivery as potentially duplicated.
- Make idempotency explicit and test it.
- Do not log webhook secrets, authorization headers, or unredacted sensitive payloads.
- Use bounded retries with backoff and a dead-letter strategy.
- Document queue keys, payload schemas, ordering guarantees, and retention rules.
- Keep Valkey operations atomic where correctness depends on them.
- Add metrics and structured logs for ingestion, queue depth, retries, latency, and failures.

## Working agreement

1. Inspect the existing code and documentation before changing anything.
2. Update or add tests for behavior changes.
3. Run the relevant formatter, linter, and test suite before committing.
4. Update `README.md` when setup, architecture, or user-facing behavior changes.
5. Never commit credentials or local environment files. Provide `.env.example` entries instead.
6. Avoid adding dependencies without a clear need and document significant architectural decisions.

## Git conventions

- Use focused commits with imperative messages.
- Do not rewrite shared branch history without explicit approval.
- Primary remote: `origin` (`git@github.com:maxp/hookrouter.git`).
- Secondary remote: `craft` (`ssh://ssh.sourcecraft.dev/maxp/hookrouter.git`).
- Keep the primary branch synchronized across both remotes.

## Definition of done

A change is complete when it is documented as needed, covered by appropriate tests, formatted and linted, free of committed secrets, and ready to run in a clean checkout.

# AGENTS.md

Guidance for humans and coding agents working in this repository.

## Project purpose

`hookrelay` receives heterogeneous webhooks, resolves their type and identifier from the request route, verifies and converts them into canonical messages, deduplicates them, and routes new messages to ordered recipient queues. The design must preserve ordering where promised and make failures observable and recoverable.

Project goals describe required behavior without prescribing infrastructure or implementation technologies. Use the canonical domain vocabulary from `CONTEXT.md`.

## Applied technical decisions

- Valkey is the current persistence and queue-coordination technology.
- Record significant implementation choices and their rationale as ADRs under `docs/adr/`.
- Keep this section descriptive rather than normative: goals must remain independent of the selected technologies.

## Engineering principles

- Prefer small, reviewable changes.
- Keep webhook ingestion fast; move non-essential work to background processing.
- Treat every incoming event and every queue delivery as potentially duplicated.
- Make idempotency explicit and test it.
- Do not log webhook secrets, authorization headers, or unredacted sensitive payloads.
- Use bounded retries with backoff and a dead-letter strategy.
- Preserve the processing order: resolve route, verify, convert, deduplicate, then route.
- Document queue keys, payload schemas, ordering guarantees, and retention rules.
- Keep storage and queue operations atomic where correctness depends on them.
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
- Primary remote: `origin` (`git@github.com:maxp/hookrelay.git`).
- Secondary remote: `craft` (`ssh://ssh.sourcecraft.dev/maxp/hookrelay.git`).
- Keep the primary branch synchronized across both remotes.

## Definition of done

A change is complete when it is documented as needed, covered by appropriate tests, formatted and linted, free of committed secrets, and ready to run in a clean checkout.

## Agent skills

### Issue tracker

Issues and specs are tracked as local Markdown files under `.scratch/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Triage uses the standard Matt Pocock skill status vocabulary. See `docs/agents/triage-labels.md`.

### Domain docs

Domain documentation uses a single-context layout. See `docs/agents/domain.md`.

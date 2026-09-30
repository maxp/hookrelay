# Implementation milestones

The implementation is a multi-session build. After the design interview closes, the accepted design is converted into a specification and then blocker-aware tracer-bullet tickets. The first spec fixes the Milestone 1 Valkey field encodings, Lua `KEYS`/`ARGV`, bounded result tuples, pre-write checks, invariants, and startup reconciliation checks/repairs before those transitions are coded; later slices specify their own exact contracts. Markdown is the source of truth for the Milestone 1 HTTP APIs, with no generated clients initially.

A separate short `valkey-go`/pinned-Valkey spike ticket precedes the Go application scaffold. It verifies the client and script/failure contracts before committing the production adapter; an alternative client requires a concrete demonstrated blocker. At scaffold time, select and pin then-current stable Go and exact Valkey versions consistently across development, Compose, CI, and build images.

## First Bot Platform

Telegram is the first production Bot Platform adapter. Test-only adapters may support integration tests but are not product features. MaxBot is deferred until the common ingestion and delivery seams have been validated with Telegram.

The shared model and storage support `chat`, `user`, `bot`, and `relay` Recipient scopes from the first implementation. The first end-to-end fixture exercises chat scope; adapter tests cover all scope classifications that Telegram can produce.

## Milestone 1 — happy-path vertical slice

The first end-to-end slice proves:

```text
Admin API creates a Telegram Webhook Endpoint
→ signed Telegram webhook arrives
→ verify
→ convert Canonical Message
→ atomically deduplicate and enqueue
→ Consumer claims with wait_ms=0
→ Consumer acknowledges
→ metrics and structured logs describe the flow
```

It includes:

- Go module, process configuration, lifecycle, container, Compose, and CI scaffold;
- Admin Bearer authentication without browser sessions;
- Admin CLI create/get commands; create chooses its own identifier before sending the request, while the raw HTTP API still allows server-side generation;
- Telegram Verifier and Converter;
- all four Recipient scopes in the shared model;
- duplicate handling as part of atomic acceptance;
- the Lua transitions needed for endpoint creation, atomic acceptance, nonblocking claim, and acknowledgement; a minimal `hr1:audit` Stream and required audit append are implemented with endpoint creation from this first slice, never stubbed or replaced by stdout-only audit;
- claim with the complete `wait_ms=0–30,000` contract, initially implemented through bounded periodic ready-index rechecks; notification-based wake-up remains a later optimization;
- Delivery Token terminal tombstone and idempotent repeated acknowledgement;
- minimal startup reconciliation for the persisted structures implemented in this slice: validate authoritative state, repair only safely derivable indexes/counters, and apply the accepted Recipient block-marker policy to ambiguity it can isolate; never open the public listener or report ready before this check succeeds;
- automatic Compose smoke coverage.

It deliberately excludes:

- notification-based long-poll wake-up and its tuning; periodic waiting is already compliant;
- negative acknowledgement, retry, expiry, and DLQ;
- full Admin CRUD beyond create/get;
- browser sessions and UI;
- periodic consistency checking;
- startup recovery for retry, lease-expiry, and DLQ state, whose transitions belong to later slices. Encountering state that the current slice cannot safely reconcile does not permit a ready response.

The smoke test:

1. starts Compose;
2. creates a Telegram Webhook Endpoint;
3. sends a signed fixture webhook;
4. repeats the webhook and proves one stored message;
5. claims the message;
6. acknowledges it;
7. repeats acknowledgement and receives the recorded result;
8. proves the queue is empty;
9. checks metrics and health;
10. restarts the stack without removing its Valkey volume and verifies readiness, the persisted endpoint, and that a repeated webhook is still deduplicated.

Integration tests also cover safe repair of first-slice derived indexes/counters and refusal to become ready when a discovered inconsistency cannot be safely handled.

Milestone 1 is an internal tracer-bullet checkpoint, not a production-ready release. In particular, if an active lease reaches its deadline or survives a process restart, Milestone 1 does not invent temporary expiry or retry semantics: it detects the due `hr1:leases` entry, marks readiness false, and leaves state unchanged for diagnosis. The smoke path acknowledges its lease before restart. Milestone 2, which adds the accepted expiry and retry transitions, is the first deployable release candidate; its Compose smoke extends the Milestone 1 path with nacks and observed retries, dead-lettering, CLI DLQ listing, replay, and delivery-state reads, a claim in the new Delivery Cycle, and a lease left claimed across a restart that startup reconciliation expires before readiness.

## Milestone 2 — complete delivery failure path

Adds:

```text
claim
→ nack or lease expiry
→ bounded retry delay
→ claim with a new Delivery Token
→ four failed attempts
→ global DLQ
→ operator replay
```

This milestone completes the core ordering and recovery semantics and is the first deployable release candidate. It includes the cooperative background maintenance and bounded inline maintenance needed to execute lease expiry and retry activation; those mechanisms cannot be deferred after their transitions exist. It also includes the accepted Admin Bearer replay API, delivery-state reconciliation read, matching CLI command, and the narrow blocked-Recipient inspection and audited clear operations required by the recovery runbook. Replay rejects a conflicting Deduplication Identity by default and accepts only the explicit `keep_current` override, which never rewrites the newer mapping. Startup reconciliation expands from recognizing Milestone 1 lease state to executing the newly implemented lease-expiry, retry, and DLQ recovery transitions before readiness can be reported.

## Later milestones

3. Notification-based long-poll wake-up, waiting-claim tuning, and remaining Admin CRUD/CLI; the compliant periodic long-poll contract already exists from Milestone 1. *Implemented:* an in-process ready-work notifier ([ADR 0008](../adr/0008-in-process-ready-work-notification.md)) wakes the oldest waiting claim per ready-making transition while the 250 ms recheck stays unchanged; tuning is limited to wake-up and wait-duration metrics so any later recheck change is data-driven. The Admin API and CLI add the endpoint list, `PATCH` enable/disable, deletion, and the Bot Identity listing ([spec](../../.scratch/milestone-3/spec.md)).
4. Administrative browser sessions, operational UI, audit views, and the remaining DLQ inspection and permanent-deletion operations; replay already exists from Milestone 2.
5. Cross-slice reconciliation hardening and recovery tests for the complete first-version storage model; evaluate periodic consistency checking separately after implementation experience. Each earlier milestone extends startup checks as it introduces new persisted state.

The spec and tickets must include the client spike and first-slice script/storage contract before the scaffold and transition code, respectively. Operational backup and restore design remains deferred rather than becoming an implicit first-slice acceptance criterion.

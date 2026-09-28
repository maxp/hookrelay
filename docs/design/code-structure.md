# Go code structure

The Go module path is:

```text
github.com/maxp/hookrelay
```

The application builds one executable from:

```text
cmd/hookrelay/main.go
```

It provides the accepted `serve`, `admin`, and `generate` command families plus the `version` and `healthcheck` commands used by releases and containers. The entry point only delegates to the command and application composition modules; it does not contain HTTP, domain, Valkey, or observability logic.

## Initial module map

```text
cmd/hookrelay/

internal/
  app/
  config/
  model/
  ingestion/
  delivery/
  administration/
  valkey/
  observability/
```

Packages are created only when implementation requires them; empty package scaffolding is avoided.

### `internal/app`

The composition root and lifecycle owner. It wires immutable configuration, logging, metrics, Valkey adapters, feature modules, public and administrative HTTP servers, maintenance loops, readiness, and graceful shutdown. It is the only module expected to know all concrete implementations.

### `internal/config`

Parses command-line flags and environment variables, reads explicitly supported secret files, applies defaults, and validates contradictory or environment-specific settings. It returns immutable configuration and does not connect to Valkey or start runtime processes.

### `internal/model`

Contains only values and invariants genuinely shared between feature modules, such as Canonical Message, Bot Identity, Recipient Identity, Message Identifier, timestamps, and bounded result/error classifications. It has no HTTP, Valkey, metrics, environment, or process concerns and must not become a general-purpose type collection.

### `internal/ingestion`

Owns Webhook Endpoint lookup, Webhook Type registration, verification, conversion, response mapping, request acceptance, and Bot Platform adapters such as Telegram and MaxBot. The common pipeline uses a registry rather than switches spread across handlers. HTTP transport for `/webhook/...` remains local to this feature module.

### `internal/delivery`

Owns the ordered-delivery interface and state-machine use cases: claim, acknowledgement, negative acknowledgement, extension, leases, retries, maintenance, dead-letter transition, and replay. Consumer HTTP transport remains local to this module. Callers do not know Valkey keys or Lua scripts.

### `internal/administration`

Owns Admin Secret and browser-session authentication, CSRF, Webhook Endpoint management, DLQ operations, the operational UI, and administrative audit intent. Administrative HTTP and UI transport remain local to this module.

### `internal/valkey`

The concrete Valkey adapter and the only module that knows `hr1:` keys, Valkey commands, client behavior, and embedded Lua scripts. Scripts live as `.lua` files under this module and are embedded into the executable. It satisfies small interfaces declared by the feature modules that consume storage behavior; it does not export generic `Get` and `Set` repositories.

### `internal/observability`

Constructs structured logging, Prometheus registration, request identifiers, bounded labels, redaction rules, and shared transport/storage instrumentation. It does not absorb feature behavior into a generic facade.

## Webhook Type definitions

Webhook Types are registered as definitions composed from narrow interfaces:

```go
type Definition struct {
    Type            WebhookType
    Platform        BotPlatform
    CredentialKinds []CredentialKind
    Verifier        Verifier
    Converter       Converter
    ResponseMapper  ResponseMapper
}
```

Verification, conversion, and platform response mapping remain separate interfaces. A platform package may implement several with one concrete type, but callers learn only the capability they use.

Converters are pure transformations and never persist messages. Atomic acceptance is invoked by the ingestion module through a caller-owned interface such as `MessageAcceptor`, which returns bounded accepted, duplicate, Recipient-blocked, capacity, dependency, and internal-failure results while the Valkey implementation hides deduplication, queues, indexes, counters, and scripts.

## Valkey scripts and tests

Lua scripts are readable `.lua` files under `internal/valkey/scripts/` and are embedded into the binary with `//go:embed`. Initial scripts eventually cover acceptance, claim, acknowledgement, negative acknowledgement, extension, lease expiry, retry activation, dead-letter transition, replay, endpoint configuration, sessions, and administrative audit. Individual milestones implement only the scripts needed for their slice.

An embedded script registry associates each script name with a contract version, its embedded body, and the body's SHA-256 digest for local identity checks; Valkey's `EVALSHA` script SHA is obtained from `SCRIPT LOAD`. Startup loads the required scripts before readiness. Normal operations use `EVALSHA`; `NOSCRIPT` permits one `EVAL` of the same embedded body, never a non-atomic command sequence. A script load, compatibility, or execution failure fails the operation and readiness. An ambiguous transport error after dispatch is **not** `NOSCRIPT` and must not cause a blind retry. Versioned filenames such as `accept_v1.lua` make changed contracts explicit. The first implementation spec fixes the exact v1 scripts and their registry entries; future incompatible contract rollout on persisted `hr1:` data needs an explicit plan.

Each script returns a RESP array with a bounded status code in the first position and documented positional fields for that status. A typed Go parser rejects unknown statuses and invalid shapes; scripts do not serialize result JSON or depend on RESP maps. The affected slice's spec enumerates `KEYS`, `ARGV`, pre-write validation, status tuples, affected keys, and invariant tests for every transition.

Testing has three layers:

1. unit tests for pure Go values and feature logic;
2. integration tests for every Lua transition against a real pinned Valkey;
3. fault tests for process, connection, and ambiguous-response behavior.

Integration tests assert every status tuple variant and every affected List, Hash, index, counter, and idempotency record, including absence of mutation when preconditions fail. They exercise `SCRIPT FLUSH`/`NOSCRIPT` fallback and contract-parser rejection; fault tests cover uncertain responses without blind mutation retries. They use `HOOKRELAY_TEST_VALKEY_URL`; local runs explicitly skip when absent, while an obligatory CI job provides a pinned Valkey container. A fake Valkey does not prove script semantics, and Testcontainers is not added initially.

## API contract and client generation

For Milestone 1, the accepted Markdown Consumer and Admin API contracts are the source of truth; there are no generated clients. OpenAPI for both APIs is deferred until the HTTP DTOs stabilize. Its exact representation and any later generated-client policy are separate follow-up work, not prerequisites for the first slice.

## Seams and transport models

Storage interfaces are declared by the consuming feature modules, not by the Valkey implementation. Ingestion, delivery, and administration each expose only the operations they need; there is no generic key-value repository or one application-wide Store interface.

HTTP request, response, error, and administrative resource DTOs are separate from shared model values. Canonical Message is the deliberate exception: it has a stable JSON contract implemented through an explicit codec rather than incidental transport tags spreading through the model.

Time-dependent feature logic receives a small Clock interface. Application timestamps and tests use it, while leases, retries, and maintenance use authoritative Valkey time. Direct `time.Now()` calls are not spread across feature modules.

UUIDv7 and random-token generation are provided through injectable generators. Production randomness uses `crypto/rand`; failure aborts the operation and never falls back to non-cryptographic randomness. Tests use deterministic adapters.

## Initial dependency policy

- HTTP routing uses the standard `http.ServeMux` and `net/http` middleware.
- The initial Valkey client is `github.com/valkey-io/valkey-go`, subject to a separate, short blocker-aware spike **before** the implementation scaffold. The spike checks script load, `EVALSHA`/`NOSCRIPT` fallback, `TIME`, authentication, TLS, reconnect, timeout, OOM, script errors, and the configured connection/multiplexing limits against a pinned Valkey. `go-redis` remains a fallback only if the spike identifies a concrete blocker.
- UUIDv7 generation uses `github.com/google/uuid` behind the local generator interface when the selected version provides `NewV7`.
- JSON request decoding and validation use `encoding/json` plus explicit field validation. Hookrelay does not reject duplicate object keys: standard last-value-wins decoder behavior applies to Consumer, Admin, and webhook JSON. This behavior must be documented in public contracts and tests so it is not mistaken for strict duplicate-key rejection.
- Prometheus exposition uses `github.com/prometheus/client_golang` with private registries and explicit registration rather than global defaults.
- The Admin CLI's hidden Admin Secret prompt and terminal detection use `golang.org/x/term`.
- Structured logging uses standard `log/slog` with JSON output and centralized attribute conventions and redaction.
- HTTP tests use `testing`, `net/http/httptest`, and focused golden JSON only for stable public contracts.
- Tests initially use standard `testing`, small helpers with `t.Helper`, and handwritten fakes for caller-owned interfaces. No assertion or mocking framework is added.
- At scaffold time, check the then-current stable Go release and pin the Go version, toolchain directive, CI image, and container build image consistently. Pin the exact Valkey container version used by Compose and integration tests at the same point; neither toolchain nor image pin is selected during design.
- New dependencies require a concrete need, are pinned through `go.mod`, and are followed by `go mod tidy`. CI runs `govulncheck`; floating branches and unexplained pseudo-versions are avoided.

## Module design rules

- Feature modules define the smallest storage interfaces they need; the Valkey adapter implements them.
- HTTP DTOs are distinct from shared model values unless the representation is deliberately the stable Canonical Message contract.
- Transport maps requests to feature commands and feature results to responses; it does not perform state transitions directly.
- Complexity belongs behind deep module interfaces. A module is not introduced only to pass arguments through unchanged.
- Unit and integration tests exercise the same interfaces as callers.
- New Bot Platforms are localized to ingestion adapters and registry wiring rather than cross-cutting type switches.

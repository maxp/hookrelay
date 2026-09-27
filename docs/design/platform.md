# Platform design

This document records the platform choices agreed for the first implementation. Domain terms are defined in [`CONTEXT.md`](../../CONTEXT.md); architectural rationale is recorded in [`docs/adr/`](../adr/).

## Naming conventions

The project, service, executable, container service, and default image name are `hookrelay`. Implementation-owned identifiers use explicit project prefixes:

- environment variables: `HOOKRELAY_...`;
- Prometheus metrics: `hookrelay_...`;
- Valkey keys: `hr1:...`, where `hr` identifies hookrelay and `1` is the storage-key namespace version;
- structured-log service name: `hookrelay`.

The compact `hr1` prefix is reserved for internal Valkey keys only. Public names, configuration, metrics, logs, executables, and images continue to use the full `hookrelay` name. The former `hookrouter` name is not used in new persistent keys or public contracts.

## Runtime and process topology

- The application runtime is Go.
- The HTTP layer starts with the standard `net/http` package rather than an application framework.
- The first version is one Go codebase, one binary, and one main application process.
- Background maintenance is separated behind internal interfaces so it can become another entry point or process later.
- Public and administrative HTTP listeners use separate ports.
- An edge proxy, TLS termination, and public-network policy belong to the deployment environment rather than the domain application.
- OCI containers are the deployment artifact. Docker Compose is the initial local environment; Kubernetes is not required.

## Components

The initial runtime consists of:

- `hookrelay`, providing webhook ingestion, the Consumer API, maintenance tasks, the administrative API, and the operational web UI;
- Valkey, providing persistence and queue coordination;
- optional Prometheus and Grafana services enabled through a Docker Compose observability profile.

The operational web UI is served by the Go application on the administrative listener. It is an operational panel rather than a full control plane. Configuration is stored in Valkey behind an internal repository interface. Administrative access is restricted to a trusted network and a separate shared Admin Secret; the Consumer Secret is never accepted for administrative access.

The Admin Secret is an opaque uniformly random value with at least 64 bits of entropy. Production prefers a mounted secret file, while an environment variable is allowed for local development. Configuring both sources is a startup error. Only one Admin Secret is accepted at a time, it is compared in constant time, and it is never stored in Valkey, logs, metrics, UI output, or URLs. Rotation is a coordinated single-secret update rather than a zero-downtime dual-secret transition.

The administrative listener hosts the Admin API, operational UI, liveness, readiness, metrics, and optional profiling endpoints. It uses a separate port and binds only to a trusted interface or network policy; it is not published directly to the public internet.

For browser access, an administrator enters the Admin Secret on a login page. The server exchanges it for a random administrative session token in an `HttpOnly`, `Secure`, `SameSite=Strict` cookie. Administrative sessions are stored in Valkey by token digest, have a one-hour idle timeout and a twelve-hour absolute timeout, support immediate logout, and are all invalidated when the Admin Secret changes. Cookie-authenticated state-changing requests require `Origin` validation and CSRF tokens, and no state is changed through `GET` requests.

The initial UI polls JSON endpoints every 5–10 seconds and supports operational inspection of health, Valkey, queues, leases, retry backlog, the global DLQ, quarantined Recipients, recent compact delivery metadata, and Grafana links. Its actions are privileged DLQ payload inspection, replay, confirmed deletion, and safe quarantine recovery. Webhook, Bot, credential, routing, and runtime-limit configuration are outside the first UI and remain Admin API or CLI responsibilities.

Administrative audit events are written both to a bounded Valkey Stream for recent UI access and to structured standard output for external retention. Audit events contain UUIDv7 identity, integer-millisecond time, bounded actor, operation, target, request, outcome, and optional reason codes; they never contain secrets, Delivery Tokens, full payloads, Authorization headers, stack traces, or complete request bodies. Login outcomes, logout and expiry, privileged payload views, replay, deletion, quarantine actions, configuration and secret changes, and reconciliation repairs or unresolved ambiguity are audited. Normal claim, acknowledgement, negative acknowledgement, and extension activity remains in Delivery Attempt history rather than the administrative audit log.

The browser UI polls its JSON endpoints every 5–10 seconds. Administrative sessions are stored in Valkey by token digest with a one-hour idle timeout and a twelve-hour absolute timeout. UI state-changing requests use CSRF protection and `Origin` validation.

## Observability

- Application logs are structured JSON written to standard output.
- Secrets, authorization headers, delivery tokens, and unredacted payloads are never logged.
- The administrative listener exposes Prometheus-compatible metrics.
- Prometheus and Grafana are optional external components rather than runtime dependencies of hookrelay.
- OpenTelemetry may be added later; the initial metrics path is direct Prometheus exposition.
- Administrative health, readiness, metrics, and optional profiling endpoints are not exposed on the public listener.

## Valkey operational contract

The first version uses one standalone Valkey instance in both local and production deployments. It does not provide Valkey replication, Sentinel failover, Cluster sharding, or automatic storage-node failover. Loss or unavailability of that instance makes ingestion and queue operations unavailable until it recovers or is restored.

AOF is mandatory in production with:

```text
appendonly yes
appendfsync everysec
maxmemory-policy noeviction
```

After the atomic acceptance script succeeds, the message becomes visible to consumers and hookrelay may return the platform success response. The first version does not use `WAIT`, `WAITAOF`, a pending-durability state, or a second activation phase. Consequently, a host, process, power, filesystem, or storage failure can lose approximately the latest second of writes even though hookrelay already returned `200`. This accepted loss window must be stated in production operating documentation and monitored; the runtime choice deliberately favors the simpler, lower-latency persistence path over per-message fsync confirmation.

Correctness-sensitive transitions use small versioned Lua scripts through `EVALSHA`, with `EVAL` only as the safe `NOSCRIPT` reload path. Hookrelay never falls back to a non-atomic sequence of commands. Scripting failure or incompatibility makes the operation fail and affects readiness.

Periodic RDB snapshots may supplement AOF for backup, but do not replace it. Backup and restore procedures must be tested, and every restore enters the documented reconciliation flow before readiness.

Production deployment must also define and test:

- supported Valkey version;
- backup schedule, storage, and restore procedure;
- TLS, authentication, and network access;
- memory and disk limits;
- latency and capacity targets;
- AOF rewrite monitoring and disk-exhaustion response.

Readiness is false when there is no connection to the writable standalone instance, atomic scripts cannot execute, storage schema is incompatible, startup reconciliation is incomplete, production AOF or `noeviction` settings are missing, or the configured persistence contract cannot be verified. Grafana or Prometheus scraper availability, ordinary dead-letter messages, and bounded maintenance lag do not by themselves make hookrelay unready.

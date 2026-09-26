# Platform design

This document records the platform choices agreed for the first implementation. Domain terms are defined in [`CONTEXT.md`](../../CONTEXT.md); architectural rationale is recorded in [`docs/adr/`](../adr/).

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

- `hookrouter`, providing webhook ingestion, the Consumer API, maintenance tasks, the administrative API, and the operational web UI;
- Valkey, providing persistence and queue coordination;
- optional Prometheus and Grafana services enabled through a Docker Compose observability profile.

The operational web UI is served by the Go application on the administrative listener. It is an operational panel rather than a full control plane. Configuration is stored in Valkey behind an internal repository interface. Administrative access is restricted to a trusted network and an administrator token.

## Observability

- Application logs are structured JSON written to standard output.
- Secrets, authorization headers, delivery tokens, and unredacted payloads are never logged.
- The administrative listener exposes Prometheus-compatible metrics.
- Prometheus and Grafana are optional external components rather than runtime dependencies of hookrouter.
- OpenTelemetry may be added later; the initial metrics path is direct Prometheus exposition.
- Administrative health, readiness, metrics, and optional profiling endpoints are not exposed on the public listener.

## Valkey operational contract

Local development may use one Valkey instance. Production deployment must separately define and test:

- supported Valkey version and topology;
- AOF and `appendfsync` policy;
- replication and failover behavior;
- backup and restore procedures;
- TLS and authentication;
- memory limits and `noeviction` behavior;
- latency and capacity targets;
- support for the atomic scripting or function mechanism selected by the implementation.

A successful Valkey operation does not by itself promise absolute zero loss under every infrastructure failure. The documented durability envelope depends on this production configuration.

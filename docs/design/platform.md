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

The initial UI polls JSON endpoints every 5–10 seconds and supports operational inspection of health, Valkey, queues, leases, retry backlog, the global DLQ, blocked Recipients, recent compact delivery metadata, and Grafana links. Its actions are privileged DLQ payload inspection, replay, and confirmed deletion. Repair of an ambiguously blocked Recipient is an operator runbook task rather than a general first-version UI action. Webhook, Bot, credential, routing, and runtime-limit configuration are outside the first UI and remain Admin API or CLI responsibilities.

Administrative audit events are written both to a bounded Valkey Stream for recent UI access and to structured standard output for external retention. The Valkey stream retains at most 30 days and 1,000,000 events, trimming whichever boundary is exceeded first. Audit events contain UUIDv7 identity, integer-millisecond time, bounded actor, operation, target, request, outcome, and optional reason codes; they never contain secrets, Delivery Tokens, full payloads, Authorization headers, stack traces, or complete request bodies. Login outcomes, logout and expiry, privileged payload views, replay, deletion, blocked-Recipient recovery, configuration and secret changes, and reconciliation repairs or unresolved ambiguity are audited. Normal claim, acknowledgement, negative acknowledgement, and extension activity remains in Delivery Attempt history rather than the administrative audit log.

The browser UI polls its JSON endpoints every 5–10 seconds. Administrative sessions are stored in Valkey by token digest with a one-hour idle timeout and a twelve-hour absolute timeout. At most 100 sessions may be active; capacity does not evict existing sessions, and new login attempts fail with a bounded capacity error until expired sessions are cleaned up. Changing the Admin Secret invalidates all sessions. UI state-changing requests use CSRF protection and `Origin` validation.

## Observability

- Application logs are structured JSON written to standard output.
- Secrets, authorization headers, delivery tokens, and unredacted payloads are never logged.
- Bot Identifier and Chat Identifier are logged in clear text in every structured event where they are known. Log access and retention must therefore treat them as potentially sensitive identifiers.
- Prometheus labels may use bounded Bot Platform, Webhook Type, Recipient Scope, outcome, reason code, operation kind, and HTTP status class. They must not use Bot Identifier, Chat Identifier, Message Identifier, Webhook Identifier, Source Event Identifier, Delivery Token, operation identifier, or Consumer Instance Identifier.
- The administrative listener exposes Prometheus-compatible metrics.
- Prometheus and Grafana are optional external components rather than runtime dependencies of hookrelay.
- OpenTelemetry may be added later; the initial metrics path is direct Prometheus exposition.
- Application logs are retained by the external logging platform, with a recommended maximum retention of 30 days. Deployments may use a shorter period; a longer period requires an explicit operational decision. Local container logs must be rotated. Hookrelay does not store application logs in Valkey.
- Administrative health, readiness, metrics, and optional profiling endpoints are not exposed on the public listener.

### Required ingestion metrics

```text
hookrelay_webhook_requests_total{webhook_type,outcome}
hookrelay_webhook_request_duration_seconds{webhook_type}
hookrelay_webhook_request_body_bytes{webhook_type}
hookrelay_messages_accepted_total{bot_platform,recipient_scope}
hookrelay_messages_duplicate_total{webhook_type}
hookrelay_dedup_conflicts_total{webhook_type}
hookrelay_routing_issues_total{bot_platform,reason}
```

Webhook outcomes are bounded to accepted, duplicate, unknown endpoint, verification failure, invalid JSON, oversized body, capacity rejection, dependency unavailable, and internal error.

### Required delivery metrics

```text
hookrelay_delivery_claims_total{outcome}
hookrelay_delivery_attempts_total{recipient_scope,outcome}
hookrelay_delivery_attempt_duration_seconds{recipient_scope,outcome}
hookrelay_active_leases
hookrelay_waiting_claims
hookrelay_queue_messages
hookrelay_ready_recipients
hookrelay_oldest_ready_message_age_seconds
hookrelay_retries_waiting
hookrelay_dead_letter_messages
hookrelay_dead_letters_total{recipient_scope,reason}
hookrelay_dead_letter_replays_total{outcome}
```

Delivery attempt outcomes are bounded to acknowledgement, negative acknowledgement, expiry, and dead-letter.

`hookrelay_queue_messages` is backed by a derived global counter stored as `hr1:stats:queued_messages`. Atomic Lua transitions update it with queue changes. It is not authoritative state, and the consistency checker compares it with bounded scans.

### Required Valkey and maintenance metrics

```text
hookrelay_valkey_operation_duration_seconds{operation}
hookrelay_valkey_operations_total{operation,outcome}
hookrelay_valkey_script_errors_total{script}
hookrelay_valkey_connected
hookrelay_valkey_aof_enabled
hookrelay_valkey_aof_delayed_fsync_total
hookrelay_valkey_memory_used_bytes
hookrelay_valkey_memory_max_bytes

hookrelay_maintenance_processed_total{kind,result}
hookrelay_maintenance_due_lag_seconds{kind}
hookrelay_maintenance_batch_size{kind}
hookrelay_maintenance_duration_seconds{kind}
hookrelay_blocked_recipients
hookrelay_consistency_issues_total{kind,resolution}
hookrelay_reconciliation_in_progress
```

Valkey operation labels are bounded to accepted operation names such as accept, claim, acknowledgement, negative acknowledgement, extension, lease expiry, retry activation, dead-letter replay, maintenance, and reconciliation.

### Health endpoints

`GET /health/live` performs no Valkey or external dependency checks. It returns `200` while the process and diagnostic HTTP server can respond.

`GET /health/ready` reports whether hookrelay can safely handle new work. It is not ready when Valkey is unavailable or read-only, production AOF or `noeviction` is missing, Lua scripts cannot execute, startup reconciliation is incomplete, the hard global queue limit is reached, minimum deduplication retention cannot be preserved, or required secret configuration is invalid. A full individual Recipient does not make the whole process unready; requests for that Recipient receive their own retryable capacity response.

Both endpoints return bounded safe JSON such as `{"status":"ready"}` or a not-ready result with named check outcomes. They never expose Valkey URLs, credentials, internal key names, stack traces, Recipient identifiers, or detailed memory values. They require no Admin Secret because they are available only on the protected administrative listener.

## Service objectives and alerts

The initial ingestion availability SLO is 99.9% over a rolling 30-day window. Unknown endpoints, invalid credentials, invalid JSON, and oversized bodies are caller outcomes and do not consume the availability budget. Valkey unavailability, capacity rejection, internal error, and script failure do consume it. This target reflects the accepted single-instance Valkey topology.

Initial ingestion latency objectives for accepted and duplicate requests with bodies up to 256 KiB under normal supported load are:

```text
p95 < 100 ms
p99 < 300 ms
```

The ready-to-claim latency objective, measured from a message becoming ready until creation of a successful claim response while a long poll is waiting and limits are not exhausted, is:

```text
p95 < 1 second
p99 < 3 seconds
```

These are initial objectives to validate with the implementation prototype and production-like load tests.

Maintenance due-lag alerts are evaluated separately for lease expiry and retry activation:

```text
warning:  lag > 3 seconds for 5 minutes
critical: lag > 10 seconds for 2 minutes
```

Dead-letter alerts are:

- warning on any newly created Dead-letter Message;
- critical when the DLQ grows continuously or exceeds 100 messages;
- critical when the oldest Dead-letter Message exceeds 25 days with the default 30-day retention.

Queue backlog alerts use age and capacity:

```text
warning:  oldest ready message > 1 minute for 5 minutes
critical: oldest ready message > 5 minutes for 5 minutes
warning:  global queue usage > 70%
critical: global queue usage > 90%
```

Deduplication alerts are:

- warning when effective retention falls below configured target;
- critical when effective retention is at or below 120% of minimum retention;
- critical on any capacity rejection;
- warning when early eviction occurs during a five-minute window;
- warning on any deduplication payload conflict.

Valkey persistence and capacity alerts are:

- critical when AOF is disabled, eviction is not `noeviction`, Valkey is unavailable, an AOF rewrite fails, or Lua script execution fails;
- warning when delayed fsync increases or an AOF rewrite takes unusually long;
- warning above 70% memory or disk use;
- critical above 90% memory or disk use.

Hookrelay directly stops new-message acceptance at 90% of configured Valkey memory. Filesystem usage is normally collected by the infrastructure or Valkey exporter. A trustworthy externally supplied critical disk signal may make readiness false; otherwise disk exhaustion is handled through alerts and the resulting write/AOF errors rather than guessed by the application.

Consumer-health alerts use expired Delivery Attempts:

```text
warning:  expired attempts > 1% of attempts over 15 minutes
critical: expired attempts > 10% of attempts over 15 minutes
```

Per-instance investigation uses structured logs and `consumer_instance_id`, not a high-cardinality Prometheus label.

Hookrelay measures end-to-end acknowledgement and dead-letter outcomes, but no formal delivery-success SLO is declared yet because success also depends on external Queue Consumers. A joint hookrelay-and-consumer product SLO may be defined later.

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

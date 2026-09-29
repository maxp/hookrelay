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

The operational web UI is served by the Go application on the administrative listener. It is an operational panel rather than a full control plane. Webhook Endpoint configuration, Bot Identity mapping, enabled state, and verification credentials are managed through the authenticated Admin API and stored in the dedicated Valkey instance behind an internal repository interface. Webhook ingestion reads the current endpoint record directly from Valkey for every request; the first version has no in-memory configuration cache or cache-invalidation protocol. Administrative access is restricted to a trusted network and a separate shared Admin Secret; the Consumer Secret is never accepted for administrative access.

The Admin Secret is an opaque uniformly random value with at least 64 bits of entropy and an accepted encoded length of 16–8192 bytes; length validation does not substitute for the entropy requirement. Production prefers a mounted secret file, while an environment variable is allowed for local development. Configuring both sources is a startup error. Only one Admin Secret is accepted at a time, it is compared in constant time, and it is never stored in Valkey, logs, metrics, UI output, or URLs. Rotation is a coordinated single-secret update rather than a zero-downtime dual-secret transition.

The administrative listener hosts the Admin API, operational UI, liveness, readiness, metrics, redacted configuration inspection, and optional profiling endpoints. It uses a separate port and binds only to a trusted interface or network policy; it is not published directly to the public internet. `GET /debug/config` requires Admin authentication and exposes effective non-secret settings, their sources, and secret-configured flags, never secret values; Valkey URL userinfo is redacted.

Go pprof is compiled in but disabled by default. `HOOKRELAY_PPROF_ENABLED=true` explicitly registers selected handlers by hand on the administrative mux; hookrelay never uses a side-effect import that registers them on the default mux. Profiling requires the Admin Secret as a Bearer token; browser sessions are not accepted. At most one profile request runs concurrently, CPU profiles are limited to 30 seconds, traces to 5 seconds, and responses use `Cache-Control: no-store`. Profiling access is logged to standard output and audited in Valkey when available, both best effort. A Valkey outage or audit-write failure does not itself block an otherwise authorized profile request, so profiling remains usable during dependency incidents; Admin Bearer authentication and profiling limits remain mandatory. Heap and goroutine profiles are treated as secret-bearing artifacts because process memory may contain plaintext webhook credentials, shared secrets, Delivery Tokens, payloads, and Recipient identifiers. Block and mutex sampling remain disabled by default.

For browser access, an administrator enters the Admin Secret on a login page. The server creates a session and its required audit event in the same Lua operation, then issues the random administrative session token in an `HttpOnly`, `SameSite=Strict`, `Path=/` cookie with no `Domain` only after confirmed success. Production also requires `Secure`; explicitly configured loopback-only local HTTP development may omit it as defined by the Admin API and configuration contracts. Logout and session-expiry audit is best effort and must not prevent revocation or keep an expired session valid. Logout confirms success only after the session is deleted or known absent; if revocation cannot be confirmed, it returns `503`. Administrative sessions are stored in Valkey by token digest, have a one-hour idle timeout and a twelve-hour absolute timeout, and support immediate logout. At most 100 sessions may be active; capacity does not evict existing sessions, and a valid new login returns `429 session_capacity_exceeded` after expired-session cleanup while the limit remains reached. Every session becomes invalid when the Admin Secret changes. A persistent random salt and HMAC-SHA-256 generation tag detect the change at startup; one bounded audited transition replaces the UUIDv7 generation and removes all indexed sessions before readiness. Each session carries that generation ID, and plaintext Admin Secret storage remains forbidden. Cookie-authenticated state-changing requests require `Origin` validation and CSRF tokens. `GET` requests never mutate application resources, although successful session authentication may perform the documented throttled update of session access metadata and its expiry index.

The initial UI polls JSON endpoints every 5–10 seconds and supports operational inspection of health, Valkey, queues, leases, retry backlog, the global DLQ, blocked Recipients, recent compact delivery metadata, and Grafana links. Its actions are privileged DLQ payload inspection, replay, and confirmed deletion. Repair of an ambiguously blocked Recipient is an operator runbook task rather than a general first-version UI action. Webhook, Bot, credential, routing, and runtime-limit configuration are outside the first UI and remain Admin API or CLI responsibilities.

Administrative audit events are written both to a bounded Valkey Stream for recent UI access and to structured standard output for external retention. The Valkey stream retains at most 30 days and 1,000,000 events, trimming whichever boundary is exceeded first. Audit events contain UUIDv7 identity, integer-millisecond time, bounded actor, operation, target, request, outcome, and optional reason codes; they never contain secrets, Delivery Tokens, full payloads, Authorization headers, stack traces, or complete request bodies. Login outcomes, logout and expiry, privileged payload views, replay, deletion, blocked-Recipient recovery, configuration changes, detected secret-generation changes, and reconciliation repairs or unresolved ambiguity are audited. Normal claim, acknowledgement, negative acknowledgement, and extension activity remains in Delivery Attempt history rather than the administrative audit log.

Critical administrative mutations require audit persistence in Valkey. Webhook Endpoint creation, enablement, disablement, and deletion, administrative DLQ replay and permanent deletion, Admin Secret generation changes, and preconditioned Recipient-block clearing change state and append their audit event in the same Lua operation. The API confirms success only after both succeed; a standard-output audit copy remains best effort. Lua preconditions are checked before writes, but runtime errors do not roll back completed writes, and a lost response does not prove that no change occurred. The [storage contract](storage.md#administrative-audit) records these limits.

Background DLQ deletion at retention expiry has the same mandatory state-change-plus-audit requirement. Safe derived-index repair and detection of ambiguous Recipient state use best-effort audit: an audit-write failure must not itself block reconciliation or creation of a protective Recipient block marker. This does not permit automatic repair of ambiguous authoritative state.

Failed authentication remains a refusal even when its audit write fails. Audit for rejected authentication is best effort; it never changes the refusal into successful authentication or replaces the normal rejection response with an audit error.

Privileged DLQ payload inspection requires a confirmed Valkey audit append before any payload content is returned. If audit cannot be confirmed, the request returns `503` without disclosing payload content. The audit event records authorized access beginning, not proof that the client received the payload. This mandatory policy is distinct from the best-effort profiling exception above.

## Observability

- Application logs use standard `log/slog` and are structured JSON written to standard output.
- Secrets, authorization headers, delivery tokens, and unredacted payloads are never logged.
- Bot Identifier, Chat Identifier, User Identifier, and source IP are logged in clear text wherever they are known and applicable. Log access and retention must therefore treat these fields as potentially sensitive identifiers.
- Prometheus labels may use bounded Bot Platform, Webhook Type, Recipient Scope, outcome, reason code, operation kind, HTTP status class, and a per-adapter allowlist of known Platform Event Types in which every unrecognized value is aggregated as `unknown`. They must not use Bot Identifier, Chat Identifier, Message Identifier, Webhook Identifier, Source Event Identifier, Delivery Token, operation identifier, or Consumer Instance Identifier.
- The administrative listener exposes Prometheus-compatible metrics.
- Prometheus and Grafana are optional external components rather than runtime dependencies of hookrelay.
- OpenTelemetry may be added later; the initial metrics path is direct Prometheus exposition.
- Application logs are retained by the external logging platform, with a recommended maximum retention of 30 days. Deployments may use a shorter period; a longer period requires an explicit operational decision. Local container logs must be rotated. Hookrelay does not store application logs in Valkey.
- Administrative health, readiness, metrics, and optional profiling endpoints are not exposed on the public listener.

### Log verbosity

`HOOKRELAY_LOG_LEVEL` selects `debug`, `info`, `warn`, or `error` at startup. Defaults are `debug` in development and `info` in production. The first version has no runtime API or signal to change verbosity and no `trace` log level. Every level obeys the same payload and secret redaction rules.

### Structured-log contract

Every application log event has these required fields:

```text
timestamp_ms
level
service
version
event
message
```

Field names use `snake_case`. `timestamp_ms` is an integer UTC Unix timestamp in milliseconds, `service` is `hookrelay`, and `version` identifies the running build. `event` is a bounded machine-readable event name; `message` is a short human-readable description. Centralized `slog` attribute handling emits `timestamp_ms` and `message` rather than the default `time` and `msg` keys.

`request_id` is required within HTTP request flows, but not for background events without a request. Additional context is included when known and applicable:

```text
webhook_type
webhook_identifier
bot_platform
bot_id
recipient_scope
chat_id
user_id
message_id
operation_id
consumer_instance_id
source_ip
```

Recipient fields follow the Canonical Message identity: `chat_id` is present only for chat scope, `user_id` only for user scope, and neither for bot or relay scope. The initial version does not introduce `trace_id` or `span_id` without OpenTelemetry. HTTP requests are correlated by `request_id`, message lifecycles by `message_id`, and idempotent Consumer operations by `operation_id`.

### Feature events and log volume

Accepted and duplicate webhook outcomes use `webhook_accepted` and `webhook_duplicate`. They include the available request, endpoint, Bot Identity, Recipient, and message context, `platform_event_type`, HTTP `status`, and `duration_ms`. Byte counts may be logged as `body_size` and `canonical_payload_size` without logging either body or payload contents.

Delivery lifecycle events are:

```text
delivery_claimed
delivery_acknowledged
delivery_nacked
delivery_lease_extended
delivery_lease_expired
delivery_dead_lettered
delivery_replayed
```

They include `message_id`, Recipient identity fields, `delivery_cycle`, `attempt`, and, when applicable, `request_id`, `consumer_instance_id`, `duration_ms`, and a bounded `reason_code`. Delivery Tokens are never logged.

Webhook Endpoint lifecycle events are:

```text
webhook_endpoint_created
webhook_endpoint_enabled
webhook_endpoint_disabled
webhook_endpoint_deleted
```

These events may include `credential_kind`, but never credential values, lengths, fingerprints, or the Admin Secret.

For webhook and Consumer API requests, feature events contain HTTP status, duration, and relevant identifiers instead of a second generic access log. Empty long-poll outcomes (`204`) are counted in metrics only, without a per-request log entry; successful claims and errors are logged. Health and metrics polling also produce no per-request logs. Administrative mutations produce feature events and administrative audit events. Unexpected route or method failures may use a separate bounded event.

### Errors and log-write failures

Errors use a bounded `error_code` and a safe human-readable `error`, for example:

```json
{
  "error_code": "dependency_unavailable",
  "error": "valkey script failed"
}
```

These are additional fields on the common log envelope, not a separate envelope. Error descriptions must not contain payloads, credentials, or sensitive headers. Wrapped library errors are sanitized before logging; hookrelay does not automatically serialize complete error chains that might contain credential-bearing URLs or request data. Redacted stack traces are reserved for panics and internal bugs and are never included in administrative audit events.

A standard-output log-write failure must not panic or turn an otherwise successful request into a failure. Hookrelay increments an internal failure metric when possible without recursively logging through the failed sink. This best-effort application logging policy does not weaken the separate persistence contract for administrative audit: critical mutations require the Valkey state change and audit append to succeed in the same Lua operation before the API confirms success.

### Required ingestion metrics

```text
hookrelay_webhook_requests_total{webhook_type,outcome}
hookrelay_webhook_request_duration_seconds{webhook_type}
hookrelay_webhook_request_body_bytes{webhook_type}
hookrelay_messages_accepted_total{bot_platform,recipient_scope}
hookrelay_messages_duplicate_total{webhook_type}
hookrelay_dedup_conflicts_total{webhook_type}
hookrelay_dedup_records
hookrelay_dedup_record_capacity
hookrelay_dedup_oldest_record_age_seconds
hookrelay_dedup_effective_retention_seconds
hookrelay_dedup_early_evictions_total
hookrelay_dedup_capacity_rejections_total
hookrelay_webhook_inflight
hookrelay_accepting_webhooks
hookrelay_routing_issues_total{bot_platform,reason}
hookrelay_event_time_issues_total{bot_platform,reason}
```

`hookrelay_accepting_webhooks` is `1` only while new-message acceptance is enabled; it becomes `0` under the accepted queue, memory, or deduplication stop conditions without implying that Consumer draining is unavailable. Deduplication gauges and counters provide the capacity signals required by the deduplication contract. Webhook outcomes are bounded to accepted, duplicate, unknown endpoint, method not allowed, verification failure, invalid JSON, oversized body, unsupported media type, request timeout, body read failed, rate limited, overloaded (in-flight limit), Recipient blocked, capacity rejection, dependency unavailable, and internal error. Requests whose route does not name a registered Webhook Type use `webhook_type="unknown"`. `hookrelay_event_time_issues_total` counts events whose documented timestamp was `missing`, of `invalid_type`, or an `invalid_value` (non-integer or out of range), so `occurred_ms` was omitted.

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

Delivery attempt outcomes are bounded to acknowledgement, negative acknowledgement, expiry, and dead-letter. Active leases are derived from the unexpired range of `hr1:leases`; waiting claims are counted in process memory.

`hookrelay_queue_messages` is backed by a derived global counter stored as `hr1:stats:queued_messages`. Atomic Lua transitions update it with queue changes. It is not authoritative state. Startup reconciliation compares it with bounded scans and repairs safe differences; whether a periodic consistency checker repeats that work after startup remains open.

### Required administrative metrics

```text
hookrelay_admin_login_attempts_total{outcome}
hookrelay_admin_sessions
hookrelay_webhook_endpoints
hookrelay_audit_events_total{operation,outcome}
hookrelay_audit_write_failures_total{operation}
hookrelay_dlq_payload_inspections_total{outcome}
```

Administrative labels use only bounded operation and outcome allowlists. They never contain session, endpoint, message, Recipient, or actor identifiers.

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

Valkey operation labels are bounded to accepted operation names such as accept, claim, acknowledgement, negative acknowledgement, extension, lease expiry, retry activation, dead-letter replay, maintenance, and reconciliation. Latency-sensitive ingestion uses a direct atomic script call without automatic pipelining. Maintenance and other bulk work may use explicitly bounded batching or pipelining. Automatic pipelining is enabled only after representative benchmarks demonstrate a benefit without unacceptable tail latency or head-of-line effects.

### Health endpoints

`GET /health/live` performs no Valkey or external dependency checks. It returns `200` while the process and diagnostic HTTP server can respond.

`GET /health/ready` reports whether hookrelay can safely serve its required APIs and allow stored state to drain. It is not ready when Valkey is unavailable or read-only, production AOF or `noeviction` is missing, Lua scripts cannot execute, startup reconciliation is incomplete, or required configuration is invalid. Runtime global queue, memory, or deduplication pressure alone does not make this endpoint fail: it keeps returning `200` while claims, acknowledgements, cleanup, and retention remain safe, and its bounded body includes `"accepting_webhooks": false`. A full individual Recipient likewise does not make the whole process unready; requests for that Recipient receive their own retryable capacity response.

`GET /health/accepting-webhooks` is the separate ingestion-acceptance signal. It returns `200` while new webhook messages may be accepted and `503` while a global queue, memory, or deduplication stop condition rejects them. Webhook requests receive their documented retryable response independently of health polling. Deployment readiness probes use `/health/ready`; an ingestion-specific upstream or alert may use `/health/accepting-webhooks`, but must not use that endpoint to remove the Consumer API from routing.

All three health endpoints return bounded safe JSON such as `{"status":"ready","accepting_webhooks":true}` or a result with named bounded check outcomes. They never expose Valkey URLs, credentials, internal key names, stack traces, Recipient identifiers, or detailed memory values. They require no Admin Secret because they are available only on the protected administrative listener.

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

Backup and restore procedures are explicitly deferred at this design stage. They must eventually account for AOF, optional RDB snapshots or storage snapshots, secret-bearing Valkey data, and the documented reconciliation flow before readiness, but no schedule, storage medium, restore drill cadence, RPO, RTO, selective restore policy, or disk-exhaustion runbook is selected yet.

Production deployment must also define and test:

- supported Valkey version;
- TLS, authentication, and network access;
- memory and disk limits;
- latency and capacity targets;
- AOF rewrite monitoring.

The canonical readiness rules are defined under [health endpoints](#health-endpoints) and are not repeated with a second independent list here. Startup reconciliation checks compatibility of known `hr1:` structures by inspecting expected key types, required fields, value encodings, and script contracts; a separate `hr1:schema` record is not required. An incompatible known structure or an unisolatable ambiguity prevents readiness, while an ambiguity safely isolated to one Recipient follows the accepted block-marker policy. Grafana or Prometheus scraper availability, ordinary dead-letter messages, and bounded maintenance lag do not by themselves make hookrelay unready.

## Process lifecycle and shutdown

The controlled shutdown deadline is 30 seconds. Shutdown first marks readiness false and stops accepting new webhooks and claims, then stops new HTTP connections, cancels outstanding long polls with `503 Service Unavailable` and `Retry-After: 1`, drains short in-flight requests and atomic operations, stops maintenance from taking new batches, closes telemetry, and exits. Work exceeding the deadline is forcefully terminated.

A lease already created for a claim is never rolled back during shutdown. While the Delivery Attempt remains active, repeating the claim by `operation_id` can recover the same lease result; after the attempt terminates, the claim operation follows the accepted `claim_no_longer_active` behavior. Hookrelay never negatively acknowledges external consumers' active leases merely because the server is shutting down.

An in-flight webhook that has not begun atomic acceptance is cancelled and receives a retryable failure where possible. A script already executing is allowed to complete within the shutdown deadline; a lost response is recovered by platform retry and deduplication.

The first SIGTERM or SIGINT starts graceful shutdown. A second signal forces termination. Controlled clean shutdown exits with code 0, runtime/internal failure with 1, and configuration or CLI usage failure with 2.

The application supervisor recovers panics only at deliberate seams. A panic in a critical maintenance loop records a redacted stack trace, marks readiness false, and initiates shutdown. Optional tasks may restart with bounded backoff. HTTP panic middleware returns `500` and increments a metric without exposing payload or secrets.

The administrative listener starts before reconciliation so liveness, readiness, and metrics expose startup progress. The public listener starts only after basic validation and reconciliation succeed. Reconciliation is introduced incrementally: Milestone 1 checks and safely repairs the structures it implements, while later milestones extend the same startup gate whenever they add persisted lease, retry, DLQ, session, or other state. No milestone may report ready by skipping a detected inconsistency that it cannot safely isolate or reconcile. Ambiguous Recipient state follows the persistent `hr1:q:<recipient_identity>` block-marker policy rather than guessed repair.

If Valkey is lost after startup, liveness remains healthy while readiness becomes false and mutations return `503`. Hookrelay reconnects with exponential backoff starting at 100 milliseconds, factor 2, capped at 5 seconds, and jitter between 50% and 100%. The backoff resets after 30 seconds of stable connectivity. Recovery performs lightweight reconciliation and verifies AOF, `noeviction`, and scripting before readiness returns. Administrative sessions cannot be validated while Valkey is unavailable.

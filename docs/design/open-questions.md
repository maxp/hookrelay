# Open design questions

The following questions remain intentionally unresolved. The Milestone 1 choices for Admin CLI recovery, PATCH-only HTTP enable/disable, Markdown API contracts, Lua result encoding and registry, mixed Valkey records, first-slice audit, incremental startup reconciliation, version pinning at scaffold, and a pre-scaffold client spike are accepted in their respective design documents. Exact first-slice fields, script tuples, and startup validation/repair procedures are specification work before coding, not invitations to choose different policies.

## Queue maintenance and recovery

- Whether a periodic consistency checker is needed after experience with startup reconciliation.

## API and authorization

- Exact OpenAPI representation for the accepted Consumer and Admin API contracts after DTOs stabilize, and whether later versions need generated clients; neither blocks Milestone 1.
- Operational procedures for coordinated single-secret rotation of Consumer and Admin secrets.
- Encoded-character constraints or larger login-body handling needed to guarantee that every accepted 8192-byte Admin Secret can be represented in JSON without exceeding the current 16 KiB login limit; the same question applies to future credential kinds that permit the full 8192-byte value.
- Per-operation reconciliation details for later administrative DLQ and session mutations after uncertain Lua execution; the accepted policy already forbids blind retries and false success claims.

## Valkey schema and durability

- Exact fields, `KEYS`/`ARGV`, status tuple shapes, and script preconditions for **later** storage slices, to be fixed before each is coded; Milestone 1 requires these in its specification.
- Rollout and rollback of later incompatible script contracts against existing persisted `hr1:` state; the embedded registry, startup load, `EVALSHA`/`NOSCRIPT`, and v1 testing policy are already selected.
- Future `hr2` cutover and backup compatibility if an incompatible storage format is ever introduced.
- Backup and restore are explicitly deferred at this stage: schedule, retention, backup medium, RPO/RTO, restore drills, selective restore, and disk-exhaustion runbook remain undefined.

## Operations

- Exact metric additions needed to make every stated latency objective and promised internal-failure signal directly measurable, including accepted/duplicate webhook latency, ready-to-claim latency, log-write failures, and recovered panics.
- Validation and possible recalibration of the accepted initial SLOs and alert thresholds using production-like load tests.
- Access-control policy for logs containing clear-text Bot Identifier, Chat Identifier, User Identifier, and source IP fields.
- Operational handling, secure transfer, and retention of secret-bearing pprof artifacts; access policy is selected.

## Resolved during Milestone 1

- **Routing-issue precedence (Telegram):** a Canonical Message carries one `routing_issue`, and the first detected issue wins. An invalid or missing `update_id` is detected first, so `invalid_source_event_id` takes precedence over an event-structure or identifier issue in the same Update; the message is relay-scoped either way and keeps its `platform_event_type`.
- **Timestamp extraction failures:** measured by `hookrelay_event_time_issues_total{bot_platform,reason}` (`missing`, `invalid_type`, `invalid_value`) with the `event_time_invalid` structured warning.

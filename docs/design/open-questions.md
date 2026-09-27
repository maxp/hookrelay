# Open design questions

The following questions remain intentionally unresolved. They should be answered before or during specification of the affected implementation slice.

## Queue maintenance and recovery

- Exact operational quarantine representation and operator recovery workflow for ambiguous Recipient state.
- Whether the consistency checker belongs in the first production milestone or the immediately following milestone.

## Queue capacity and retention

- Numeric global and per-Recipient queued-message limits.
- Retention of dead-letter messages and full attempt history.
- Exact configured deduplication target and minimum retention durations.
- Numeric `max_dedup_records` and Valkey memory safety thresholds.

## Recipient and index lifecycle

- Exact stable serialized values used for reserved bot- and relay-scope Chat Identifiers.
- Exact bounded `routing_issue.code` allowlist.

## API and authorization

- Concrete endpoint paths and complete request/response/error schemas.
- Exact `HOOKRELAY_...` configuration keys and CLI command used to generate the shared Consumer secret.
- Operational procedure for coordinated single-secret rotation.
- Exact `HOOKRELAY_...` configuration keys and coordinated rotation procedure for the single Admin Secret.
- Administrative session and audit-stream retention/capacity limits beyond their accepted timeout semantics.
- Detailed admin API and CLI contract.

## Valkey schema and durability

- Validation rules for colon-delimited Recipient storage keys under the `hr1` namespace, including reserved bot/relay Chat Identifier values.
- Remaining Valkey data-structure choices and exact `hr1:...` key names for queues, indexes, messages, attempts, tokens, deduplication, and DLQ.
- Exact version identifiers, loading, testing, and rollout procedure for Lua scripts.
- Schema/version migration and backup compatibility.
- Backup schedule and retention, restore objectives, and disk-exhaustion runbook.

## Operations

- Exact metrics, label allowlists, SLOs, and alerts.
- Whether Bot Identifier and Chat Identifier are stored or logged directly, hashed, or redacted in each operational surface.
- Production container base image, supported architectures, and profiling access policy.

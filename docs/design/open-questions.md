# Open design questions

The following questions remain intentionally unresolved. They should be answered before or during specification of the affected implementation slice.

## Queue maintenance and recovery

- Whether Valkey time is used for all lease and retry deadlines.
- Exact lease-deadline and retry-deadline index design.
- Cooperative maintenance versus a maintenance leader.
- Maintenance sweep interval and batch sizes.
- Whether claim requests perform a small inline overdue-retry sweep before waiting.
- Source-of-truth records versus rebuildable indexes.
- Consistency checking and automatic index repair.
- Startup reconciliation after Valkey restore.

## Queue capacity and retention

- Global queued-message capacity.
- Per-Recipient queue capacity.
- Capacity behavior is expected to reject new webhook acceptance rather than delete acknowledgedly accepted messages, but exact thresholds remain open.
- Retention of compact successful-delivery metadata.
- Retention of dead-letter messages and attempt history.
- Exact configured deduplication target and minimum retention durations.
- Numeric `max_dedup_records` and Valkey memory safety thresholds.

## Recipient and index lifecycle

- Whether empty Recipient queue state is removed immediately or retained briefly.
- Exact organization of Recipient dead-letter state and its global operational index.
- Exact stable serialized values used for reserved bot- and router-scope Chat Identifiers.
- Exact bounded `routing_issue.code` allowlist.

## API and authorization

- Concrete endpoint paths and complete request/response/error schemas.
- Consumer credential format, storage hashing, rotation, and revocation.
- Numeric maximum active leases and long polls per Consumer.
- Administrator token format and rotation.
- Audit event schema and retention.
- Detailed admin API and CLI contract.

## Valkey schema and durability

- Key naming and hash-slot strategy.
- Valkey standalone, Sentinel, or Cluster topology.
- Lua scripts versus Valkey Functions.
- AOF, `appendfsync`, replication acknowledgement, failover, and accepted loss window.
- Schema/version migration and backup compatibility.

## Operations

- Exact metrics, label allowlists, SLOs, and alerts.
- Whether Bot Identifier and Chat Identifier are stored or logged directly, hashed, or redacted in each operational surface.
- UI update mechanism and exact read/action permissions.
- Production container base image, supported architectures, and profiling access policy.

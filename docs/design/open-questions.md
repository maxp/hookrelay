# Open design questions

The following questions remain intentionally unresolved. They should be answered before or during specification of the affected implementation slice.

## Queue maintenance and recovery

- Operator runbook for diagnosing and clearing an `hr1:q:...` ambiguous Recipient block marker.
- Whether a periodic consistency checker is needed after experience with startup reconciliation.

## API and authorization

- Final OpenAPI representation and generated-client policy for the accepted Consumer API contract.
- Exact `HOOKRELAY_...` configuration keys and CLI command used to generate the shared Consumer secret.
- Operational procedure for coordinated single-secret rotation.
- Exact `HOOKRELAY_...` configuration keys and coordinated rotation procedure for the single Admin Secret.
- Detailed admin API and CLI contract.

## Valkey schema and durability

- Exact field encodings and Lua argument/return contracts for the accepted `hr1:...` structures.
- Exact version identifiers, loading, testing, and rollout procedure for Lua scripts.
- Future `hr2` cutover and backup compatibility if an incompatible storage format is ever introduced.
- Backup schedule and retention, restore objectives, and disk-exhaustion runbook.

## Operations

- Validation and possible recalibration of the accepted initial SLOs and alert thresholds using production-like load tests.
- Access-control policy for logs containing clear-text Bot Identifier and Chat Identifier fields.
- Production container base image, supported architectures, and profiling access policy.

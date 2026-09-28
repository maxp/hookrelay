# Open design questions

The following questions remain intentionally unresolved. The Milestone 1 choices for Admin CLI recovery, PATCH-only HTTP enable/disable, Markdown API contracts, Lua result encoding and registry, mixed Valkey records, first-slice audit, incremental startup reconciliation, version pinning at scaffold, and a pre-scaffold client spike are accepted in their respective design documents. Exact first-slice fields, script tuples, and startup validation/repair procedures are specification work before coding, not invitations to choose different policies.

## Queue maintenance and recovery

- Operator runbook for diagnosing and clearing an `hr1:q:...` ambiguous Recipient block marker.
- Whether a periodic consistency checker is needed after experience with startup reconciliation.

## API and authorization

- Exact OpenAPI representation for the accepted Consumer and Admin API contracts after DTOs stabilize, and whether later versions need generated clients; neither blocks Milestone 1.
- Operational procedures for coordinated single-secret rotation of Consumer and Admin secrets.
- Exact non-plaintext mechanism by which an Admin Secret change invalidates every existing browser session and can be detected/audited; this must be selected before the browser-session slice.
- Per-operation reconciliation details for later administrative DLQ and session mutations after uncertain Lua execution; the accepted policy already forbids blind retries and false success claims.
- Milestone 1 interim behavior for a valid claim with `wait_ms > 0`, because the final Consumer API accepts 0–30,000 while long-poll waiting is scheduled for Milestone 3. The milestone must either implement compliant waiting earlier or be explicitly treated as a non-deployable API subset without weakening the final contract.
- Milestone 1 handling of an active lease that reaches its deadline or survives a process restart before lease-expiry/retry transitions arrive in Milestone 2. This intermediate state must not be represented as production-ready or silently repaired with different attempt semantics.

## Valkey schema and durability

- Exact fields, `KEYS`/`ARGV`, status tuple shapes, and script preconditions for **later** storage slices, to be fixed before each is coded; Milestone 1 requires these in its specification.
- Rollout and rollback of later incompatible script contracts against existing persisted `hr1:` state; the embedded registry, startup load, `EVALSHA`/`NOSCRIPT`, and v1 testing policy are already selected.
- Future `hr2` cutover and backup compatibility if an incompatible storage format is ever introduced.
- Backup and restore are explicitly deferred at this stage: schedule, retention, backup medium, RPO/RTO, restore drills, selective restore, and disk-exhaustion runbook remain undefined.

## Operations

- Validation and possible recalibration of the accepted initial SLOs and alert thresholds using production-like load tests.
- Access-control policy for logs containing clear-text Bot Identifier, Chat Identifier, User Identifier, and source IP fields.
- Operational handling, secure transfer, and retention of secret-bearing pprof artifacts; access policy is selected.

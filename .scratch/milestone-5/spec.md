# Milestone 5 — complete-model reconciliation hardening and recovery proof

Status: done

Specification source of truth: `CONTEXT.md`, `docs/design/*`, `docs/adr/*`, and the implemented contracts in `.scratch/milestone-1/spec.md` through `.scratch/milestone-4/spec.md`. This milestone hardens the shared startup/recovery gate for the complete first-version `hr1:` storage model. Domain vocabulary follows `CONTEXT.md`.

## Problem Statement

Each earlier milestone extended startup reconciliation for the state it introduced, but the result is still slice-oriented. Webhook Endpoint Hashes are validated while their two rebuildable indexes only cause readiness failure when they drift; transient delivery records and message-lifecycle records have not yet received one explicit cross-slice recovery review; and the recovery tests mostly prove one structure at a time. A Valkey restart, interrupted Lua outcome, or operator write can therefore expose interactions that no complete-model test currently pins.

The design also leaves periodic consistency checking intentionally unresolved. A full scan while serving has different race, latency, and operational consequences from startup/recovery reconciliation and must not be enabled merely because the startup code exists.

## Solution

1. Make Webhook Endpoint index reconciliation symmetric: the endpoint Hash remains authoritative; missing or incorrectly scored global-listing members and missing Bot Identity memberships are restored; orphan or mismatched derived members are removed; malformed authoritative endpoint state still holds readiness.
2. Audit every accepted `hr1:` key family and every transition boundary against the complete message lifecycle, then specify and implement only evidence-backed missing validations or safe repairs.
3. Add fault and recovery tests that compose acceptance, claim, nack/expiry, replay, acknowledgement, endpoint administration, Admin Secret rotation, sessions, DLQ operations, and Valkey loss/restart through the real application seams.
4. Decide periodic consistency checking separately from startup hardening, recording measured evidence and the selected operational policy before any background full scan is introduced.

## User Stories

1. As an operator, I want a missing Webhook Endpoint listing or Bot Identity membership rebuilt automatically from the endpoint Hash, so that harmless derived-index loss does not keep the service down.
2. As an operator, I want stale endpoint index members removed automatically, so that list APIs and endpoint-capacity checks do not retain deleted configuration.
3. As an operator, I want a malformed endpoint Hash or an irreconcilable endpoint-capacity violation to hold readiness without guessing or exposing credentials.
4. As an on-call engineer, I want one documented matrix covering every first-version key family, its authority, safe repairs, holds, and tests, so that recovery behavior is reviewable rather than accidental.
5. As an on-call engineer, I want restart and dependency-recovery tests over mixed queue, DLQ, endpoint, and session state, so that readiness only returns after the whole model is safe.
6. As a maintainer, I want partial or uncertain transition outcomes injected at storage seams and reconciled through supported behavior, so that Lua's no-rollback failure model is exercised.
7. As an operator, I want periodic consistency checking enabled only if measured evidence justifies its serving-time cost and races, so that hardening does not create an unbounded background scan.

## Implementation Decisions

### Scope and sequence

Ticket 01 implements the already-deferred endpoint-index reconciliation. Ticket 02 publishes the complete key-family and transition-boundary gap matrix before any additional hardening scripts are coded. Tickets 03 and 04 implement the evidence-backed delivery-record and message-lifecycle amendments fixed by that matrix and update this spec with their exact contracts first. Ticket 05 composes the recovery fault matrix. Ticket 06 evaluates periodic checking and exposed the recovery quiescence gap. Added ticket 08 specifies and implements that barrier before ticket 07 extends the smoke and closes the milestone.

This sequencing is deliberate: Milestone 5 is hardening, not permission to invent repairs. Every repair must be derived from authoritative state; every ambiguity that cannot be isolated or reconstructed safely continues to hold readiness or create the accepted Recipient block marker.

### Endpoint index authority

The authoritative record remains:

```text
hr1:wh:<webhook_type>:<webhook_identifier>  Hash
```

The rebuildable indexes remain:

```text
hr1:bot:<bot_platform>:<bot_id>:webhooks    Set
hr1:webhooks                                Sorted Set, score = created_ms
```

The endpoint Hash determines its Bot Identity membership and listing score. Reconciliation never changes endpoint fields or credential values.

### `reconcile_endpoint_v1`

One embedded Lua script reconciles one member at a time. It is registered as `reconcile_endpoint_v1` version 1.

- KEYS: `hr1:webhooks`.
- ARGV: `mode` (`bot_member` | `listing_member` | `endpoint`), `member` (`<webhook_type>:<webhook_identifier>`), `scanned_bot_platform` (only for `bot_member`), `scanned_bot_id` (only for `bot_member`), key prefix (`hr1`).
- The first version accepts the implemented Telegram mapping: Webhook Type and Bot Platform are both `telegram`; identifier, Bot Identifier, credential, UUIDv7, timestamp, and version encodings are the same as endpoint creation and startup validation.
- Before any write, the script validates its arguments and the types of every key it may touch. Caller-contract violations are error replies. Persisted incompatibilities use bounded statuses.

Modes:

1. `bot_member`: the caller found `member` in the scanned Bot Identity Set. A malformed member or absent endpoint Hash is removed from that Set. If the endpoint is valid but names another Bot Identity, the member is removed from the scanned Set. A matching membership is unchanged. A wrong-typed or malformed authoritative endpoint is not removed and returns `invalid`/`wrong_type`.
2. `listing_member`: the caller found `member` in `hr1:webhooks`. A malformed member or absent endpoint Hash is removed from the listing. A valid endpoint with a missing or wrong score is set to `created_ms`. A wrong-typed or malformed authoritative endpoint is unchanged and returns `invalid`/`wrong_type`.
3. `endpoint`: the caller found the endpoint Hash. A valid endpoint restores its correct Bot Identity Set member and its listing member/score. After reverse cleanup, `SCARD > 100` is `invalid` with reason `bot_endpoint_limit` even when the membership already exists. Before adding a missing Bot membership, `SCARD` must be below 100; 100 existing members is also `invalid`. An invalid endpoint is unchanged.

Returns:

```text
{"consistent"}
{"repaired", bot_membership_repaired, listing_repair}
  bot_membership_repaired = 0 | 1
  listing_repair = 0 | 1 | 2   # none | member restored | score repaired
{"orphan_removed", reason}
{"invalid", reason}
{"wrong_type", reason}
{"absent"}
```

Reasons are bounded: `bot_member_malformed`, `bot_member_orphan`, `bot_member_mismatch`, `listing_member_malformed`, `listing_member_orphan`, `endpoint_key_invalid`, `endpoint_record_invalid`, `bot_endpoint_limit`, `endpoint_type`, `bot_index_type`, `listing_type`.

### Endpoint scan order and reporting

The Go reconciler uses bounded `SCAN`, `SSCAN`, and `ZSCAN`, never `KEYS`, in this order:

1. scan Bot Identity Sets and remove malformed, orphan, or mismatched members;
2. scan the global listing and remove malformed/orphan members or repair scores;
3. scan endpoint Hashes and restore both indexes.

Reverse cleanup runs first so stale members do not cause a false 100-endpoint capacity hold. Invalid Bot Identity key names and malformed authoritative endpoint key names are unhandled inconsistencies. Safe repairs/removals append best-effort reconciliation audit events without credentials. Findings feed `hookrelay_consistency_issues_total` with bounded kinds for endpoint index members removed, restored, or score-repaired. Invalid authoritative state increments an `unhandled` finding and holds readiness.

### Complete-model gap matrix

Ticket 02 records, for every accepted key family in `storage.md`:

- whether it is authoritative, derived, disposable, idempotency-only, or audit-only;
- the transitions that create, mutate, and delete it;
- existing startup and recovery validation;
- safe repair, safe removal, Recipient isolation, or readiness-hold policy;
- an existing or required real-Valkey test;
- whether a lost/partial Lua outcome can leave a detectable intermediate state.

The completed matrix is [`storage-reconciliation-matrix.md`](storage-reconciliation-matrix.md). It selects the following two additions; no other automatic repair is accepted by this milestone.

### Active Delivery Attempt reconciliation (`reconcile_attempt_v1`)

Ticket 03 adds one embedded Lua script, `reconcile_attempt_v1`, for a Recipient whose authoritative head state says `leased`. It strengthens `reconcile_recipient_v3`; it does not replace its queue, blob, marker, or derived-index checks.

- KEYS: `hr1:ready`, `hr1:leases`, `hr1:retries`, `hr1:blocked`.
- ARGV: `mode` (`verify` | `block`), `recipient_identity`, expected token digest (`64` lowercase hex for `verify`, empty for `block`), detailed reason (`token_digest_mismatch` for `block`, empty for `verify`), claim operation TTL in milliseconds (`600000` in v1, used only to validate active-record TTL bounds), key prefix (`hr1`).
- Resolved keys: `hr1:r:<recipient_identity>:q`, `hr1:r:<recipient_identity>:s`, `hr1:q:<recipient_identity>`, `hr1:t:<delivery_token_digest>`, and `hr1:op:<operation_id>`.
- Argument/key validation errors are script errors. Unexpected persisted types/fields return bounded results before writes.

Order and results:

1. Global index types are validated before writes. Marker present → `{"already_blocked"}` after atomically removing any stale ready/lease/retry members and restoring the blocked member from the marker; active-attempt state is not changed.
2. Queue/state absent or non-List/non-Hash → `{"not_leased"}` when no leased state exists, otherwise create the `active_attempt_inconsistent` marker and return `{"blocked", reason}`.
3. State not `leased` → `{"not_leased"}`.
4. A leased state must have a queue head equal to `head_message_id`; positive `delivery_cycle`, `attempt`, `claimed_ms`, `attempt_started_ms` (or the accepted pre-v2 fallback to `claimed_ms`), `lease_expires_ms`; plaintext `delivery_token`; and a 64-lowercase-hex `delivery_token_digest`. A pre-v3 leased state without the digest returns `{"legacy"}` and keeps the accepted TTL-only recovery behavior. Any other malformed field creates the block and returns `blocked`.
5. `SHA-256(delivery_token)` cannot be computed in Lua. Before calling the script, Go reads the leased state's plaintext token and digest without logging either, computes SHA-256, and refuses to call the script if they differ; it creates the same block through a dedicated `mode=block` call carrying no token. For `mode=verify`, ARGV additionally carries the expected token digest; the script compares it with the current state digest to fence a concurrent transition. A changed state returns `{"changed"}` and Go rereads once.
6. The digest selects a Hash token record whose `state=active`, `recipient_identity`, `message_id`, `operation_id`, `claimed_ms`, and `lease_expires_ms` equal the leased head. Absence, wrong type, or mismatch creates the marker and returns `blocked`.
7. The claim operation Hash must have `kind=claim`, `state=active`, the same plaintext token, message, Recipient, cycle, attempt, and claimed time, and a nonempty `args_digest`. Its recorded original `lease_expires_ms` must be a positive integer strictly after `claimed_ms` and no later than the current head/token deadline. Lease extension changes head/token deadlines but preserves the recorded claim response, as required by Milestone 2. Absence, wrong type, or mismatch creates the marker and returns `blocked`.
8. Token and operation records must both have positive TTLs. The operation TTL must not exceed the configured claim-operation TTL; the token TTL must cover the remaining lease and no more than that remainder plus the claim-operation TTL. A missing/nonpositive/out-of-bound TTL creates the marker and returns `blocked`.
9. The lease index is derived: a missing/wrong score is repaired to `lease_expires_ms` and returns `{"repaired"}`; otherwise `{"consistent"}`.

Bounded statuses:

```text
{"consistent"}
{"repaired"}
{"not_leased"}
{"already_blocked"}
{"blocked", reason}
{"legacy"}
{"changed"}
{"wrong_type", reason}
```

Reasons: `active_attempt_structure`, `active_attempt_state`, `token_missing`, `token_type`, `token_mismatch`, `claim_operation_missing`, `claim_operation_type`, `claim_operation_mismatch`, `token_ttl`, `claim_operation_ttl`, `lease_index_type`.

The script's `mode` ARGV is `verify|block`; `verify` also requires the expected digest, while `block` requires an empty expected digest and atomically creates/aligns the marker and indexes after Go detects a plaintext-token/digest mismatch. The Go reconciler invokes it after `reconcile_recipient_v3` reports a consistent/repaired non-due leased head. `blocked` uses marker reason `active_attempt_inconsistent`; the detailed reason is returned/logged/metriced but the marker stays bounded. The script never rewrites or deletes token/op/head records. `wrong_type` holds readiness only for a wrong-typed global index; a wrong-typed per-Recipient/token/op key is safely isolated by the block. `legacy` is observed but not repaired. Terminal token records and completed extend operations are TTL-bound idempotency records and are deliberately not globally scanned.

### Message lifecycle reconciliation (`reconcile_message_v1`)

Ticket 04 adds `reconcile_message_v1`, applied to every message ID discovered from a queue, `hr1:dl:*`, `hr1:success:*`, `hr1:m:*`, `hr1:mi:*`, or `hr1:a:*`. Go deduplicates IDs across bounded scans and supplies the configured Deduplication Retention only for TTL validation; the script never reads payload content back into Go.

- KEYS: `hr1:ready`, `hr1:leases`, `hr1:retries`, `hr1:blocked` (standalone Valkey; message/Recipient keys resolve from the prefix).
- ARGV: `mode` (`inspect` | `delete_orphans`), `message_id`, `recipient_identity` (`""` when the caller has no queue locator), `queue_position` (`head` | `behind_head` | `none`), dedup retention milliseconds, key prefix (`hr1`).
- Resolved keys: message blob, metadata, history, dead-letter record, compact success record, optional Recipient queue/head/marker, and the metadata's dedup record.

Validation and classification:

1. Global index and marker types and the queue locator are checked before writes. Wrong-typed message-lifecycle records with a valid queue locator are isolated like other queue-local corruption; unlocated type errors hold readiness. The script returns no payload, credential, CSRF value, or Delivery Token.
2. A String blob must decode as a Canonical Message object with matching `message_id`, positive integer `received_ms`, a valid structured Recipient whose serialization equals `recipient_identity` when supplied, nonempty `platform_event_type`, and a present JSON `payload` value (including `null` or `false`; nested keys do not affect envelope-member presence). Invalid JSON/shape/identity → `message_invalid`.
3. Live lifecycle states are mutually exclusive: queued, dead-lettered, and acknowledged compact success may not overlap. A blob not queued and not dead-lettered is `message_orphan`, except a valid blob whose Recipient, authoritative head ID, and valid block marker agree while its queue is absent/wrong-typed remains the already isolated queue incident; unrelated blobs, including other IDs for that blocked Recipient, still hold readiness; a success record plus blob/metadata/history is `success_overlap`; a DLQ record plus queue position is `dead_letter_overlap`.
4. Queued messages require a valid blob. The head is governed by Recipient reconciliation. A non-head message with history requires a complete positive pending cycle/attempt pair; partial/malformed metadata is invalid. Metadata may be absent only for an accepted pre-M2 message with no history/pending state.
5. Dead-lettered messages require a valid blob; the Dead-letter Hash and blob Recipient/message identity must agree; metadata, if present, must be a Hash; history must be a valid List.
6. Attempt history entries must decode as the accepted `attempt` or leading `archived_cycles_summary` shapes. Attempt cycles/attempts/times are positive integers, `completed_ms >= claimed_ms`, `lease_expires_ms >= claimed_ms`, outcome is `nack|expired`, optional reason/consumer fields satisfy their bounds, entries are ordered by nondecreasing `(delivery_cycle, attempt)`, and at most the latest 10 explicit cycles remain after an optional summary. Reconciliation never changes history.
7. Metadata fields are allowlisted: optional nonempty 64-hex `dedup_identity_digest`, and either both positive pending fields or neither. When a digest and live dedup record both exist, the record must be a Hash; if it points to this message its `accepted_ms`/`expires_ms`/TTL must be valid, while a mapping to another message is permitted after replay. An absent dedup record is permitted and never restored.
8. A success Hash must contain the accepted compact fields and a positive remaining TTL no greater than 24 hours; it must have no blob, metadata, history, queue, or DLQ record. Invalid success evidence holds readiness; it is never reconstructed.
9. Every discovered Recipient block marker is validated before isolated-Recipient exclusions, even when no queue/message remains. It must be a Hash containing positive integer `detected_ms` and one accepted bounded reason. `reconcile_recipient_v3` returns `unhandled` with `marker_invalid` for malformed fields before any index alignment; message validation also reports `marker_invalid`. Reconciliation does not overwrite malformed markers.
10. For a queued inconsistency with a valid Recipient locator, `inspect` atomically creates/keeps marker reason `message_lifecycle_inconsistent`, removes ready/lease/retry membership, restores the blocked member, and returns `blocked` with the detailed reason. Unlocatable, DLQ, success, and lone-blob inconsistencies return `inconsistent` and do not mutate.
11. Provably orphaned metadata/history (no blob, queue, DLQ, or success state) returns `orphan_records`; `mode=delete_orphans` rechecks the same absence atomically, deletes only metadata/history, and returns `removed_orphans`. A lone blob is not deleted automatically because it may be evidence of a partial acceptance.

Returns:

```text
{"consistent", lifecycle}
{"legacy", lifecycle}
{"already_blocked", lifecycle}
{"blocked", reason}
{"orphan_records", metadata_present, history_present}
{"removed_orphans", removed_count}
{"inconsistent", reason}
{"wrong_type", reason}
```

Lifecycle: `queued`, `dead_lettered`, `acknowledged`, `none`. Reasons: `message_invalid`, `message_orphan`, `message_missing`, `metadata_invalid`, `history_invalid`, `pending_state_missing`, `pending_state_invalid`, `dead_letter_invalid`, `dead_letter_overlap`, `success_invalid`, `success_overlap`, `dedup_record_invalid`, `marker_invalid`, `lifecycle_ambiguous`.

Go policy:

- queued inconsistency with a valid Recipient locator is already atomically isolated by the script (`message_lifecycle_inconsistent`), so the pass continues serving other Recipients;
- dead-letter, success, lone-blob, or unlocatable inconsistency → hold readiness with `message_lifecycle_inconsistent`;
- `orphan_records` → rerun `reconcile_message_v1` in delete mode; audit the safe removal best effort;
- no Canonical Message, dedup record, history entry, pending pair, success record, token, or audit event is invented.

### Recovery fault matrix

The composed tests must cover at least:

- endpoint Hash with each derived index missing, stale, mismatched, and wrong-typed;
- ready, leased, retry-wait, blocked, dead-lettered, acknowledged, and replay-behind-head messages in one store;
- overdue lease and retry processing before readiness;
- changed Admin Secret plus valid, orphaned, expired, and stale-generation sessions;
- `SCRIPT FLUSH`, Valkey loss after readiness, and lightweight recovery before readiness returns;
- an interrupted or uncertain representative critical transition whose persisted state is reconciled without a blind mutation retry;
- unchanged payload/credential/token redaction in logs and audit.

### Recovery quiescence barrier (ticket 08)

The complete-model pass requires quiescent message/queue discovery. Withdrawing readiness alone is not a barrier: after first startup the listeners remain open and maintenance has already started. Before every startup/recovery scan, an in-process exclusive work barrier closes admission to storage-backed `/admin/v1/` requests, public webhook/Consumer requests, and background maintenance rounds; it drains admitted work before invoking reconciliation. Public requests and new maintenance rounds are also refused/skipped whenever readiness is false. Registered due transitions run inside the exclusive phase, not as admitted maintenance rounds. Existing requests finish with their ordinary bounded deadlines; the five-minute reconciliation deadline includes drain time. A cancelled drain does not start a scan. The barrier reopens after the pass, including failed/held passes; public work and maintenance still require readiness, while administrative diagnosis remains available between passes. Health, metrics, static UI, and authenticated profiling remain available during scanning. No new API is introduced; refusals are retryable `503`, with `Retry-After: 1`, safe request correlation, and no secret/payload content. This protects this single process only; it is not multi-process coordination.

### Periodic consistency checking

Periodic checking is not part of tickets 01–05. Ticket 06 measures a production-like complete pass, documents races with live transitions, and decides one of:

- startup/recovery only (retain the current policy);
- a bounded incremental checker with a separately specified cursor, interval, work budget, metrics, and readiness/error policy;
- an operator-triggered check with a separately specified API/CLI contract.

Accepted decision: retain startup/recovery-only complete-model checking; no periodic checker or online operator-triggered check API. [`docs/design/consistency-checking.md`](../../docs/design/consistency-checking.md) records four production-like synthetic profiles, three measured passes per profile, official Valkey source constraints, serving-time races, detection limits, budgets, readiness/alert policy, and prerequisites for reconsideration. Ticket 08 makes recovery discovery quiescent. No new hard-to-reverse checker mechanism is selected, so no ADR is added.

## Testing Decisions

- Storage-seam tests pin every `reconcile_endpoint_v1` tuple, every affected key, snapshot-equal invalid/wrong-type refusals, idempotent second passes, batched scans, and `SCRIPT FLUSH` reload.
- Reconciliation tests distinguish safe derived repair from malformed authoritative state and assert bounded consistency metrics/audit reasons.
- Composed tests use the real pinned Valkey; no fake Valkey is introduced.
- Recovery tests assert readiness ordering and externally observable API behavior, not only raw keys.
- The final smoke keeps the persisted Valkey volume across restart and proves repaired endpoint listing/Bot Identity membership alongside the existing delivery/session path.

## Out of Scope

- Backup/restore policy, `hr2` migration, multi-process coordination, and Valkey Cluster key declaration.
- Automatic repair of authoritative queue, endpoint, dead-letter, audit, or message data.
- A generic operator repair API or UI.
- Periodic or online operator-triggered consistency checking (ticket 06 selected startup/recovery-only).
- New product API features unrelated to recovery.

## Completion evidence

All tickets 01–08 are done. Final validation on 2026-09-30: gofmt; `go test ./...` and `go test -race ./...` with real Valkey 9.1.2; `go vet ./...`; `git diff --check`; `bash -n scripts/smoke.sh`; and two consecutive green full production-mode Compose smoke runs with persisted-volume endpoint-index fault recovery beside DLQ/session/delivery state. The synthetic measurement harness is opt-in; ordinary tests validate its small fixture and skip the large profiles.

Review follow-up: all eight confirmed findings are covered by `internal/valkey/reconcile_regression_test.go`. The regressions were red before the fixes and green afterward; shared delivery fixtures now use production-shaped 64-hex dedup digests. Validation after these fixes: `go test -count=1 ./...`, `go test -race -count=1 ./...` against a dedicated Valkey 9.1.2, `go vet ./...`, gofmt, `git diff --check`, `bash -n scripts/smoke.sh`, and the full production-mode Compose smoke all passed.

## Further Notes

A derived index may be repaired only from an authoritative record whose identity and encoding are valid. Removing an orphan index member is safe because no authoritative record names it. Credentials, payloads, Authorization headers, CSRF tokens, session tokens, and Delivery Tokens never enter reconciliation findings, logs, metrics, or audit.

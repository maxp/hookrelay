# Milestone 3 — notification wake-up and Webhook Endpoint administration

Status: ready-for-agent

Specification source of truth: `CONTEXT.md`, `docs/design/*`, `docs/adr/*`, and the implemented contracts in `.scratch/milestone-1/spec.md` and `.scratch/milestone-2/spec.md`. This spec fixes the Milestone 3 notification seam, the waiting-claim behavior around it, and the storage/script contracts for the remaining Webhook Endpoint administration before any of it is coded (`docs/design/implementation-milestones.md`). Domain vocabulary follows `CONTEXT.md`.

## Problem Statement

A waiting claim notices new work only on its next periodic recheck, so ready-to-claim latency is up to ~300 ms even when the process itself just made the work ready, and every idle waiter polls Valkey four times a second. Operators can create and read a Webhook Endpoint, but cannot list endpoints, disable or re-enable one, delete one, or list the endpoints of a Bot Identity — so the documented credential-replacement flow (create new → switch platform → disable and delete old) cannot be completed through hookrelay.

## Solution

1. An in-process **ready-work notifier** (ADR 0008): every transition in this process that may expose claimable work signals it, and the signal wakes at most one waiting claim, which immediately performs its normal atomic recheck. The periodic 250 ms + 0–50 ms recheck is unchanged and remains the loss-recovery path; the Consumer API contract does not change.
2. The remaining Webhook Endpoint administration exactly per `docs/design/admin-api.md`: list, `PATCH` enable/disable with strong `If-Match`, delete of a disabled endpoint, and the Bot Identity endpoint listing, with the matching `hookrelay admin webhook list|enable|disable|delete` and `hookrelay admin bot webhooks` commands.

## User Stories

1. As a Consumer, I want a waiting claim to return as soon as this process makes a message ready, so that delivery latency is not bounded below by the recheck interval.
2. As a Consumer, I want the claim contract (`wait_ms`, `204`, replay, `429`) unchanged, so that existing clients need no change.
3. As an operator, I want a lost or spurious notification never to lose or duplicate work, so that the optimization cannot weaken correctness.
4. As an operator, I want to turn notifications off by configuration, so that I can fall back to pure periodic rechecks if the optimization misbehaves.
5. As an operator, I want metrics that separate notification wake-ups from periodic rechecks and measure how long claims wait, so that later recheck tuning is data-driven.
6. As an operator, I want to page through all Webhook Endpoints newest first, so that I can audit configuration.
7. As an operator, I want to disable an endpoint so that it behaves like an unknown endpoint, and re-enable it later with its credential intact.
8. As an operator, I want enable/disable and delete to require the current `ETag`, so that I never act on stale state or on a recreated endpoint.
9. As an operator, I want to delete only a disabled endpoint, and a repeated delete to be harmless, so that deletion is deliberate and retry-safe.
10. As an operator, I want every enable, disable, and delete audited atomically with the change, so that confirmed success proves both happened.
11. As an operator, I want to list every endpoint of one Bot Identity, so that I can complete credential replacement.
12. As an operator, I want the CLI to confirm destructive mutations and never blindly retry after a lost response, so that uncertain outcomes are reported honestly.

## Implementation Decisions

### Scope and sequence

Two independent tracks: notifier (tickets 01–02) and endpoint administration (tickets 03–06); ticket 07 extends the smoke and closes the milestone. No new persisted key types; no change to delivery Lua scripts.

### Ready-work notifier

- Package `delivery` owns a `ReadyNotifier` with `Signal(source)` and a waiter registration used only by the claim handler. Waiters are kept FIFO; `Signal` wakes the **oldest** registered waiter with a non-blocking send on its 1-buffered channel and removes it. With no registered waiter the signal is dropped (a new waiter always performs its own first check, so no credit is stored).
- A woken waiter performs one normal atomic claim check. Work found → claimed response. Empty (the work went to another claim, the signal was a false positive, or the Recipient became blocked) → it re-registers and keeps waiting within its original deadline, with the next periodic recheck paced from that check.
- The periodic recheck (250 ms + 0–50 ms uniform jitter) runs regardless of notifications. Cancellation, shutdown, and the `wait_ms` deadline behave exactly as today; a deregistered waiter never receives a stale wake that affects a later request.
- Signals are hints derived from existing script results; no script changes. Sources (bounded `source` label):
  - `accept` — `accept_v3` `accepted` (the Recipient may have become ready);
  - `ack` — `ack_v4` `acknowledged` (a next head may be exposed);
  - `dead_letter` — `nack_v3` or `expire_lease_v3` `dead_lettered` (a next head may be exposed);
  - `retry_activation` — `activate_retry_v1` `activated`, background and inline;
  - `replay` — `replay_dlq_v2` `replayed` with `queue_position=head`;
  - `block_clear` — `clear_block_v2` `cleared` with restored index `ready`.
  A `retry_scheduled` nack/expiry does not signal (the head waits). A false positive costs one extra recheck by one waiter.
- The claim that ran the inline maintenance pass does not need its own signals; signals from the inline pass may wake another waiter, which is correct.
- Configuration: `HOOKRELAY_CLAIM_NOTIFICATIONS` (bool, default `true`). When `false`, `Signal` is a no-op and claims rely only on periodic rechecks.
- Multi-process deployment would need a cross-process channel (e.g. Valkey Pub/Sub) behind the same `Signal`/wait seam; out of scope (ADR 0008).

### Observability (M3 additions)

- `hookrelay_ready_signals_total{source,result}` — `result` is `delivered` (a waiter was woken) or `no_waiter`.
- `hookrelay_claim_wakeups_total{trigger,outcome}` — `trigger` is `notification` or `periodic`; `outcome` is `claimed` or `empty`. Counts rechecks after the first check of a waiting claim.
- `hookrelay_claim_wait_duration_seconds{outcome}` — time from the start of a waiting claim (`wait_ms>0`) to its completion; `outcome` is `claimed`, `empty`, `cancelled`, or `unavailable` (shutdown/dependency). Buckets from 5 ms to 30 s.
- Feature events: `webhook_endpoint_enabled`, `webhook_endpoint_disabled`, `webhook_endpoint_deleted` (safe metadata only: type, identifier, bot platform, bot identifier, credential kind, generation, config version).

### Webhook Endpoint administration — storage

Keys unchanged: `hr1:wh:<type>:<identifier>` Hash, `hr1:bot:<platform>:<bot_id>:webhooks` Set, `hr1:webhooks` ZSET, `hr1:audit` Stream. The bot Set key is resolved inside the script from the Hash's `bot_id` (standalone Valkey; same precedent as `ack_v1` resolving keys from the token record), and verified to be a Set.

- **`endpoint_set_enabled_v1`** — KEYS: endpoint Hash, audit Stream. ARGV: `webhook_type`, `webhook_identifier`, `enabled` (`0`|`1`), `expected_generation_id` (empty = no `If-Match`), `expected_config_version` (empty with the previous), `event_id`, `request_id`. Order: argument/key validation (error reply); key types (`wrong_type`); Hash absent → `not_found`; no expected tag → `precondition_required`; generation or version mismatch → `precondition_failed` + current `generation_id`, `config_version`; `enabled` already equal → `unchanged` + full safe fields (no write, no audit); else HSET `enabled`, `config_version+1`, `updated_ms` from `TIME`, XADD audit (`webhook_endpoint_enabled`|`webhook_endpoint_disabled`, `actor=admin_bearer`, target `<type>:<identifier>`, `outcome=success`) → `updated` + safe fields (`bot_id`, `enabled`, `credential_kind`, `generation_id`, `created_ms`, `updated_ms`, `config_version`). Never returns the credential value.
- **`endpoint_delete_v1`** — KEYS: endpoint Hash, global listing ZSET, audit Stream. ARGV: `webhook_type`, `webhook_identifier`, `bot_platform`, `expected_generation_id`, `expected_config_version`, `event_id`, `request_id`. Order: validation; key types; Hash absent → `absent` (no write, no audit; `If-Match` not required); no expected tag → `precondition_required`; mismatch → `precondition_failed`; `enabled=1` → `must_be_disabled`; else DEL Hash, SREM bot Set (DEL when empty), ZREM listing, XADD audit (`webhook_endpoint_deleted`) → `deleted` + `bot_id`, `credential_kind`, `generation_id`, `config_version`, `deleted_ms`.
- Lists are read-only and need no script: the global list pages `ZRANGE hr1:webhooks … BYSCORE REV LIMIT` and reads each Hash; the bot list reads `SMEMBERS` and each Hash, sorted by `created_ms` descending then member descending. A listing member whose Hash is missing (or wrong type) is skipped and logged once per request as `webhook_index_orphan` with the member and index name; endpoint index reconciliation remains Milestone 5 hardening.

### Webhook Endpoint administration — HTTP (per `admin-api.md`)

- `GET /admin/v1/webhooks?limit&cursor`: `limit` 1–200 (default 50), newest first; cursor base64url JSON `{"created_ms":…,"id":"<type>:<identifier>"}`, strictly after that position (ties ordered by member descending); malformed cursor `400 invalid_cursor`. Body `{"items":[<safe endpoint>…],"next_cursor":"…"}` (`next_cursor` omitted on the last page). Items use the create/get representation.
- `PATCH /admin/v1/webhooks/{type}/{identifier}`: strict JSON `{"enabled": bool}` (required; unknown fields `400`); `If-Match` must be one strong tag `"<generation_id>:<config_version>"`; weak, `*`, lists, or malformed values → `400 invalid_request`; missing → `428 precondition_required` (after the endpoint is found); stale → `412 precondition_failed`; missing endpoint `404 webhook_endpoint_not_found`. Success `200` + new `ETag` + full safe representation; `unchanged` → `200` with the current (unchanged) `ETag` and no audit.
- `DELETE /admin/v1/webhooks/{type}/{identifier}`: same `If-Match` rules; `409 endpoint_must_be_disabled`; `204` on deletion and on an absent endpoint (no audit).
- `GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks`: `bot_platform` must be a known platform and `bot_id` valid for it, else `400 invalid_request`; `200 {"items":[…]}` (empty list when none; no pagination, at most 100).
- All routes Admin Bearer only; mutations audited through the scripts; `wrong_type` → `503 dependency_unavailable` with an error log; uncertain script outcomes are reported as `503` and never retried by the server.

### CLI

- `hookrelay admin webhook list [--limit N] [--cursor C]` — one page, `next_cursor` printed like `dlq list`; table or JSON.
- `hookrelay admin webhook enable|disable|delete --type T --identifier I --yes` — `--yes` is required (as for `dlq replay`); the command GETs the endpoint and its `ETag`, then sends the mutation with `If-Match`. `412` is reported and not retried. `delete` of an enabled endpoint reports the `409` and suggests `disable` first.
- Lost response (transport error or `5xx`): re-read the endpoint. Enable/disable: same `generation_id`, desired `enabled`, and `config_version` greater than the read one → "desired state observed" warning that mutation and audit are unconfirmed; otherwise uncertain outcome. Delete: `404` → "absence observed" with the same caveat; otherwise uncertain. Exit codes follow the existing CLI convention.
- `hookrelay admin bot webhooks --platform P --bot-id B`.

### Reconciliation

Unchanged. No new persisted state; the notifier holds only process memory.

## Testing Decisions

Same three layers as Milestones 1–2.

- Notifier unit tests: FIFO single wake, drop without waiters, deregistration on deadline/cancel, no leaked wake into a later waiter, disabled mode, concurrency under `-race`.
- Claim handler tests with fakes: a signal wakes a waiter well before the periodic interval (use a long configured interval to prove it); a false-positive signal re-registers and the claim still honors its deadline; periodic recheck still finds work with notifications disabled.
- Composed integration over real Valkey: a claim waiting with a 5 s recheck interval returns promptly after a webhook is accepted, after an ack exposes the next head, after a replay, and after retry activation.
- Storage-seam tests for both endpoint scripts: every tuple, every key, snapshot-equal refusals, argument rejection, `SCRIPT FLUSH` reload, recreated-generation `412`.
- HTTP-seam tests for every route and error code; CLI tests for confirmations, `If-Match` flow, and uncertain-outcome reporting.
- Compose smoke: credential replacement flow — create a second endpoint for the bot, list the bot's endpoints, disable and delete the first, prove the old path returns `404` and the new one accepts; a waiting claim returns promptly after a webhook.

## Out of Scope

- Cross-process notification (Valkey Pub/Sub or keyspace events) and multi-process deployment.
- Changing the periodic recheck interval or making it configurable.
- `operations/summary`, DLQ payload inspection and permanent deletion, audit listing, browser sessions, UI (Milestone 4).
- Endpoint index reconciliation and periodic consistency checking (Milestone 5).

## Further Notes

The PATCH `unchanged` result is a deliberate idempotency choice: re-sending the same enabled state with a current `ETag` changes nothing and writes no audit, so a CLI retry after reconciliation cannot inflate `config_version`.

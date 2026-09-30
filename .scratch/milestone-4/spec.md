# Milestone 4 — browser sessions, operational UI, audit views, and the remaining DLQ operations

Status: ready-for-agent

Specification source of truth: `CONTEXT.md`, `docs/design/*`, `docs/adr/*`, and the implemented contracts in `.scratch/milestone-1/spec.md`, `.scratch/milestone-2/spec.md`, and `.scratch/milestone-3/spec.md`. This spec fixes the Milestone 4 storage encodings, Lua script contracts, session and CSRF behavior, and the M4 subsets of the Admin API and CLI before any of it is coded (`docs/design/implementation-milestones.md`). Domain vocabulary follows `CONTEXT.md`.

## Problem Statement

Operators can list, read, and replay Dead-letter Messages, but cannot see a dead letter's payload or permanently delete one, cannot read the administrative audit Stream except through Valkey, and have no single view of process and queue health. Everything goes through the CLI or raw Bearer requests: there is no browser login and no operational panel, although the design promises both (`platform.md`, `admin-api.md#browser-session-api`).

## Solution

1. The remaining DLQ operations: privileged payload inspection gated by a confirmed audit append, and permanent deletion under a strong `If-Match`, each one Lua operation with its mandatory audit (`admin-api.md#operational-and-dlq-api-surface`).
2. Read-only operational views: `GET /admin/v1/operations/summary` and `GET /admin/v1/audit`.
3. Administrative browser sessions exactly per `admin-api.md#browser-session-api` and `storage.md#administrative-sessions`: Admin Secret generation tracking at startup, login/session/logout, cookie authentication with CSRF and `Origin` validation, bounded capacity, expiry, and startup reconciliation of session state.
4. An embedded operational web UI on the administrative listener (ADR 0009) that polls the JSON API and offers exactly the accepted actions: payload inspection, replay, and confirmed deletion.
5. The matching CLI commands `hookrelay admin operations summary`, `dlq payload`, `dlq delete`, and `audit list`.

## User Stories

1. As an operator, I want to see a dead letter's Canonical Message only after my access is durably audited, so that payload access is always accountable.
2. As an operator, I want to permanently delete a dead letter I have reviewed, and only the version I reviewed, so that a message that was replayed and dead-lettered again is never deleted by a stale decision.
3. As an operator, I want a repeated deletion to be harmless, so that deletion is retry-safe.
4. As an operator, I want to page through the administrative audit newest first, so that I can review who did what without Valkey access.
5. As an operator, I want one summary of readiness, Valkey memory, queue depths, blocked Recipients, and the DLQ, so that I can judge health at a glance.
6. As an operator, I want to log in to a browser panel with the Admin Secret and log out, so that I do not paste the secret into every request.
7. As an operator, I want every session invalidated when the Admin Secret changes, so that rotation revokes browser access.
8. As an operator, I want browser sessions to expire after an hour idle and twelve hours total, and at most 100 to exist, so that forgotten sessions do not accumulate.
9. As an operator, I want cookie-authenticated mutations to require CSRF and `Origin` checks, so that another site cannot act through my browser.
10. As an operator, I want the panel to show health, queues, leases, retries, blocked Recipients, the DLQ, message delivery state, audit, and a Grafana link, refreshing itself.
11. As an operator, I want the panel's replay and delete actions confirmed and never retried blindly, so that an uncertain outcome is reported honestly.
12. As a security reviewer, I want webhook payloads rendered in the panel as inert text, so that a hostile payload cannot run script in an administrator's browser.

## Implementation Decisions

### Scope and sequence

Three tracks. DLQ and read views (tickets 01–04) work under Admin Bearer and are independent. Sessions (05–08) build the authentication seam. The UI (09–10) needs both. Ticket 11 extends the smoke and closes the milestone.

New persisted keys (all already in the accepted key list): `hr1:admin_auth`, `hr1:admin_session:<session_digest>`, `hr1:admin_sessions`. New scripts: `dlq_payload_v1`, `dlq_delete_v1`, `operations_summary_v1`, `admin_auth_v1`, `session_create_v1`, `session_authenticate_v1`, `session_delete_v1`, `expire_sessions_v1`, `reconcile_session_v1`, `replay_dlq_v3`. No change to delivery, acceptance, or endpoint scripts.

### Audit actor

Audit events keep the existing fields (`event_id`, `timestamp_ms`, `actor`, `operation`, `target`, `request_id`, `outcome`, optional `reason`). `actor` gains `admin_session` besides `admin_bearer`, `maintenance`, and `startup`. New admin-facing scripts take the actor as an argument restricted to `admin_bearer`|`admin_session`; `replay_dlq_v3` is `replay_dlq_v2` with that one extra argument (ARGV appended; behavior otherwise identical, v2 stays registered until the handler switches). The handler takes the actor from the authenticated request context.

### Route authentication classes

- **Bearer only:** every Webhook Endpoint and Bot Identity route, `POST /admin/v1/recipient-blocks/inspect|clear`. Webhook configuration is outside the first UI (`platform.md`), and block recovery is a runbook task; keeping these Bearer-only also keeps the endpoint and block scripts unchanged. A session cookie on these routes is ignored (`401` without `Authorization`).
- **Bearer or session:** `operations/summary`, `recipient-states`, `messages/{id}/delivery-state`, `dead-letters` (list, get, payload, replay, delete), `audit`.
- **Session routes:** `/admin/v1/session` (below).
- Health, readiness, and metrics are unchanged (unauthenticated on the admin listener).

If an `Authorization` header is present, only Bearer is evaluated (no cookie fallback). All `/admin/v1/` responses carry `Cache-Control: no-store`.

### DLQ payload inspection

**`dlq_payload_v1`** — KEYS: audit Stream. ARGV: `message_id`, `actor`, `event_id`, `request_id`, key prefix. Keys `hr1:dl:<id>` and `hr1:m:<id>` are resolved from the prefix (standalone precedent). Order: argument validation (error reply); audit Stream type (`wrong_type`); `hr1:dl` absent → `not_found` (no audit); `hr1:dl` not a Hash or missing `delivery_cycle`/`dead_lettered_ms` → `wrong_type`; `hr1:m` absent → `message_missing` (no audit, no disclosure); `hr1:m` not a String → `wrong_type`; XADD audit (`operation=dead_letter_payload_viewed`, `target=<message_id>`, `outcome=success`) → `{"disclosed", blob, delivery_cycle, dead_lettered_ms}`. Because the XADD precedes the return in one script, the blob is only ever returned after the append.

`POST /admin/v1/dead-letters/{message_id}/payload`: body empty or exactly `{}` (else `400 invalid_request`); invalid `message_id` shape → `404 dead_letter_not_found`. `200`:

```json
{
  "message_id": "0195...",
  "delivery_cycle": 1,
  "dead_lettered_ms": 1740000000000,
  "message": { "...": "the stored Canonical Message, the Consumer API representation" }
}
```

`not_found` → `404 dead_letter_not_found`; `message_missing` and `wrong_type` → `503 dependency_unavailable` with an error log; any script error or lost reply → `503` and no payload. Feature event `dead_letter_payload_viewed` (message ID, actor, Recipient scope — never payload). Metric `hookrelay_dead_letter_payload_views_total{outcome}` (`disclosed`, `not_found`, `unavailable`). Payload inspection does not consult Recipient blocks.

### DLQ permanent deletion

The dead-letter single read gains `ETag: "<delivery_cycle>:<dead_lettered_ms>"`. Every dead-lettering writes a new `dead_lettered_ms` and every replay a new cycle, so the tag names one dead-letter entry.

**`dlq_delete_v1`** — KEYS: DLQ index, audit Stream. ARGV: `message_id`, `expected_delivery_cycle`, `expected_dead_lettered_ms` (both empty = no `If-Match`), `actor`, `event_id`, `request_id`, prefix. The block marker key is resolved from the record's `recipient_identity`. Order: validation; index/audit types (`wrong_type`); `hr1:dl` absent → ZREM a stale index member (a safe derived repair) and return `absent` (no audit); malformed record → `wrong_type`; no expected tag → `precondition_required`; mismatch → `precondition_failed` + current cycle and `dead_lettered_ms`; marker `hr1:q:<recipient_identity>` present → `recipient_blocked`; else DEL `hr1:dl`, `hr1:m`, `hr1:mi`, `hr1:a`, ZREM index, XADD audit (`dead_letter_deleted`, `reason=<dead_letter_reason>`) → `{"deleted", deleted_ms, recipient_identity, dead_letter_reason}`. It never touches deduplication records (same as retention expiry).

`DELETE /admin/v1/dead-letters/{message_id}`: `If-Match` must be one strong tag `"<cycle>:<ms>"`; weak, `*`, list, or malformed → `400 invalid_request` for an existing entry; missing → `428 precondition_required`; stale → `412 precondition_failed`; `409 recipient_blocked`; `204` on deletion and on absence (including an invalid ID shape or a malformed `If-Match` for an absent entry), absence writing no audit. Feature event `dead_letter_deleted`; metric `hookrelay_dead_letter_deletions_total{outcome}` (`deleted`, `absent`, `refused`, `unavailable`). The existing `hookrelay_dead_letter_messages` gauge follows from the index as today.

### Audit listing

`GET /admin/v1/audit?limit&cursor`: `limit` 1–200 (default 50); newest first via `XREVRANGE hr1:audit`; cursor is base64url JSON `{"id":"<stream id>"}` continuing strictly after that entry (exclusive `(` range); malformed cursor → `400 invalid_cursor`. Body `{"items":[…],"next_cursor":"…"}` (omitted on the last page). Items contain `stream_id` plus only the allowlisted fields above that are present; any other stored field is dropped. Missing Stream → empty list; wrong type → `503`. No script (single read). Reading the audit is not itself audited.

### Operations summary

**`operations_summary_v1`** — read-only script over KEYS `hr1:stats:queued_messages`, `hr1:ready`, `hr1:leases`, `hr1:retries`, `hr1:blocked`, `hr1:dlq`, `hr1:webhooks`, `hr1:admin_sessions`, `hr1:audit`, returning one consistent snapshot: counts, earliest lease deadline, earliest retry deadline, oldest and newest `dead_lettered_ms`, and the audit Stream length. Any key of the wrong type → `wrong_type` → `503`. Memory comes from the existing `INFO memory` reader.

`GET /admin/v1/operations/summary` → `200`:

```json
{
  "generated_ms": 1740000000000,
  "readiness": { "ready": true, "accepting_webhooks": true, "startup_reconciliation": "complete" },
  "valkey": { "used_memory_bytes": 1048576, "maxmemory_bytes": 268435456 },
  "queues": {
    "queued_messages": 12,
    "ready_recipients": 3,
    "leased_recipients": 2,
    "retry_wait_recipients": 1,
    "blocked_recipients": 0,
    "earliest_lease_expires_ms": 1740000030000,
    "earliest_retry_at_ms": 1740000005000
  },
  "dead_letters": { "count": 4, "oldest_dead_lettered_ms": 1739000000000, "newest_dead_lettered_ms": 1739900000000 },
  "webhook_endpoints": { "count": 5 },
  "admin_sessions": { "indexed": 1 },
  "audit": { "length": 250 },
  "links": { "grafana_url": "https://grafana.example/d/hookrelay" }
}
```

Absent optional values (no leases, no DLQ entries, no configured Grafana URL) are omitted. The summary is served even when the process is not ready (it then describes why), but a Valkey failure → `503 dependency_unavailable`.

Configuration: `HOOKRELAY_UI_GRAFANA_URL` / `--ui-grafana-url`, optional absolute `http`/`https` URL, default empty; shown in `/admin/v1/operations/summary` only.

### Admin Secret generation (`hr1:admin_auth`)

Hash fields: `generation_salt` (32 random bytes, base64url without padding), `generation_tag` (lowercase hex HMAC-SHA-256 keyed by the Admin Secret over the decoded salt bytes followed by `hookrelay-admin-session-generation-v1`), `generation_id` (UUIDv7), `updated_ms`.

**`admin_auth_v1`** — KEYS: `hr1:admin_auth`, `hr1:admin_sessions`, audit Stream. ARGV: `mode` (`initialize`|`rotate`), `expected_generation_id` (rotate only), `new_generation_id`, `generation_salt`, `generation_tag`, `event_id`, prefix. Order: validation; types (`wrong_type`).
- `initialize`: record present → `exists` (no write); else HSET all fields with `TIME`, XADD (`actor=startup`, `operation=admin_auth_initialized`, `target=admin_auth`) → `initialized`.
- `rotate`: absent → `absent`; missing/malformed fields → `wrong_type`; `generation_id` or `generation_salt` differs from the arguments → `changed`; stored tag equals the new one → `current` (no write); index with more than 100 members → `too_many_sessions` (no write); else DEL every indexed session Hash, DEL the index, HSET `generation_tag`, `generation_id`, `updated_ms`, XADD (`actor=startup`, `operation=admin_secret_generation_changed`, `target=admin_auth`, `reason=sessions_revoked_<n>` bucketed as `0`, `1_10`, `11_100`) → `{"rotated", revoked_count}`. The salt is kept.

Startup runs this as the first step of every reconciliation pass: `HGETALL`; missing → `initialize` (one reread on `exists`); present and well-formed → compute the tag with the stored salt, equal → nothing; unequal → `rotate` (one reread on `changed`). A malformed record, `wrong_type`, `too_many_sessions`, a second `exists`/`changed`, or any script error or lost reply fails the pass (readiness withheld, `admin_auth_inconsistent` error log naming the reason). The salt, tag, and secret never leave the process in logs, metrics, or audit.

### Session storage and scripts

Session token: 32 random bytes, base64url without padding (43 characters). `session_digest` = lowercase hex SHA-256 of the token string. CSRF token: 16 random bytes, base64url (22 characters). Session Hash `hr1:admin_session:<digest>` fields: `created_ms`, `last_seen_ms`, `idle_expires_ms`, `absolute_expires_ms`, `generation_id`, `csrf_token`. Index `hr1:admin_sessions` score = `min(idle_expires_ms, absolute_expires_ms)`. Constants: idle 1 h, absolute 12 h, refresh throttle 5 min, capacity 100. All times from Valkey `TIME`.

- **`expire_sessions_v1`** — KEYS: index, audit. ARGV: `limit` (≤ 1000), prefix. Removes index members with score ≤ now (and their Hashes), up to `limit`, appending one `admin_session_expired` event (`actor=maintenance`) per removed session → `{"expired", n}`. Called by the maintenance loop each cycle; the audit append is in-script but can never undo or block the removal (types are prechecked; the design's "best effort" means expiry never waits on audit).
- **`session_create_v1`** — KEYS: `hr1:admin_auth`, index, audit. ARGV: `session_digest`, `csrf_token`, `idle_ms`, `absolute_ms`, `capacity`, `event_id`, `request_id`, prefix. Order: validation; types; `hr1:admin_auth` absent or without `generation_id` → `auth_uninitialized`; session key already present → `collision`; remove expired sessions as `expire_sessions_v1` does (limit 1000); ZCARD ≥ capacity → `capacity_exceeded`; else HSET the session with the current `generation_id`, ZADD, XADD (`actor=admin_session`, `operation=admin_login`, `target=session`, `outcome=success`) → `{"created", created_ms, idle_expires_ms, absolute_expires_ms}`.
- **`session_authenticate_v1`** — KEYS: `hr1:admin_auth`, index, audit. ARGV: `session_digest`, `idle_ms`, `refresh_ms`, `event_id`, prefix. Order: validation; types; Hash absent → ZREM the digest, `{"invalid","absent"}`; Hash malformed → DEL + ZREM, `{"invalid","malformed"}`; now ≥ either expiry → DEL + ZREM + XADD `admin_session_expired` (`actor=maintenance`), `{"invalid","expired"}`; `generation_id` differs from `hr1:admin_auth` (or that record is absent) → DEL + ZREM, `{"invalid","generation_changed"}`; if `now - last_seen_ms ≥ refresh_ms`: `last_seen_ms=now`, `idle_expires_ms=min(now+idle_ms, absolute_expires_ms)`, ZADD the new score. → `{"valid", csrf_token, idle_expires_ms, absolute_expires_ms}`. Deleting an already invalid session is cleanup, not a mutation of an application resource.
- **`session_delete_v1`** — KEYS: index, audit. ARGV: `session_digest`, `event_id`, `request_id`, prefix. Types; DEL + ZREM; XADD `admin_logout` only if the Hash existed → `deleted` | `absent`.
- **`reconcile_session_v1`** — per digest found either in the index or by `SCAN MATCH hr1:admin_session:*` through the existing paged reconciliation scan helper: Hash absent → ZREM; Hash malformed, expired, or of a stale generation → DEL + ZREM; valid and unindexed or with a wrong score → ZADD the correct score. Repairs append a best-effort `session_index_repaired` audit (`actor=startup`). Sessions are disposable, so deleting a suspicious one is always safe (it only forces a new login). Index of the wrong type fails the pass.

### Session HTTP (per `admin-api.md`)

- `POST /admin/v1/session`: `Origin` must equal the configured `HOOKRELAY_ADMIN_ORIGIN` serialization exactly (else `403 forbidden`); then the per-source-IP (`RemoteAddr` host; forwarded headers are never trusted; LRU of 10,000 addresses) and global token buckets (5/min burst 5; 60/min burst 20) → `429 rate_limit_exceeded` with `Retry-After`; `Content-Type: application/json`, body ≤ 16 KiB (`413 request_too_large`), strict `{"admin_secret": string}` (`400`); constant-time secret comparison → `401 unauthenticated` with best-effort `admin_login` failure audit; `session_create_v1`: `created` → `204` + `Set-Cookie: hookrelay_admin=<token>; Path=/; Max-Age=43200; HttpOnly; SameSite=Strict` plus `Secure` when `HOOKRELAY_ADMIN_COOKIE_SECURE`; `capacity_exceeded` → `429 session_capacity_exceeded`; anything else → `503`, no cookie. Metric `hookrelay_admin_login_attempts_total{outcome}`: `success`, `failure`, `rate_limited`, `capacity_exceeded`, `unavailable`.
- `GET /admin/v1/session`: cookie only; valid → `200 {"authenticated": true, "idle_expires_ms", "absolute_expires_ms", "csrf_token"}`; otherwise `401` and a clearing cookie (`Max-Age=0`).
- `DELETE /admin/v1/session`: no cookie or an invalid session → `204` + clearing cookie; a valid session requires `Origin` and `X-CSRF-Token` (`403` otherwise), then `session_delete_v1` → `204` + clearing cookie (also for `absent`); Valkey failure → `503` without claiming revocation.

### Cookie authentication middleware

For Bearer-or-session routes without `Authorization`: a `hookrelay_admin` cookie with a 43-character base64url value is authenticated through `session_authenticate_v1` (any other value → `401`). `invalid` → `401` + clearing cookie + best-effort `admin_auth_rejected` audit (`reason=session_<reason>`). Valkey failure → `503`. For `POST`, `PUT`, `PATCH`, `DELETE`: `Origin` must match exactly and `X-CSRF-Token` must equal the session's token (constant time), else `403 forbidden` and `hookrelay_admin_csrf_rejections_total{reason}` (`origin`, `token`). The request context records `actor=admin_session`. Pprof, when it lands, stays Bearer-only.

### Operational UI (ADR 0009)

- Plain HTML, CSS, and JavaScript ES modules under `internal/administration/ui/`, embedded with `//go:embed`; no build step, no Node toolchain, no frontend dependency. Served on the admin listener at `GET /ui/…`; `GET /` redirects to `/ui/`. Assets need no authentication (they contain no data); all data comes from the JSON API with the session cookie.
- Headers on UI responses: `Content-Security-Policy: default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'`, `X-Content-Type-Options: nosniff`, `Referrer-Policy: no-referrer`, `Cache-Control: no-cache`. No inline script, style, or event-handler attributes.
- Data is rendered only through DOM APIs and `textContent`; never `innerHTML` or string-built markup with data. Payload JSON is shown pretty-printed in a `<pre>` via `textContent`.
- Views: login; overview (summary and readiness, polled every 5 s, paused while the tab is hidden); Recipients by status with paging; dead letters (list, detail with attempt history, **Show payload** after an "access is audited" confirmation, **Replay** with confirmation and an explicit `keep_current` choice only after a `409 deduplication_conflict`, **Delete** with a confirmation naming the message and Recipient and sending the detail's `ETag`); message delivery-state lookup; audit list; header with the Grafana link when configured and logout.
- A `401` returns to the login view. After a network error or `5xx` on replay or delete, the UI never retries; it re-reads the delivery state or dead letter and shows "desired state observed (unconfirmed)" or "outcome uncertain", mirroring the CLI.
- Recent compact delivery metadata is reachable by message ID through the delivery-state lookup; a listing of recent successes needs an index and is out of scope.

### CLI

- `hookrelay admin operations summary` — table or JSON.
- `hookrelay admin audit list [--limit N] [--cursor C]` — one page, `next_cursor` printed like `dlq list`.
- `hookrelay admin dlq payload --message-id ID` — always prints the JSON response; stderr notes that the access was audited. A lost response is reported as an error; the operator may simply repeat (another audited view).
- `hookrelay admin dlq delete --message-id ID --yes` — GET for the `ETag` (a missing entry is an error so a typo is not reported as success), DELETE with `If-Match`; `412` reported, not retried; `409` explained. Lost response: re-read; `404` → "absence observed" warning (deletion and audit unconfirmed), otherwise uncertain. Never retries.

### Reconciliation

Startup gains two steps before the existing ones: the Admin Secret generation check, then session reconciliation. Neither touches delivery state. The maintenance loop gains `expire_sessions_v1` (limit 100 per cycle) and updates `hookrelay_admin_sessions` (gauge of the index size).

## Testing Decisions

Same three layers as Milestones 1–3.

- Storage-seam tests for every new script: every tuple, every key, snapshot-equal refusals, argument rejection, `SCRIPT FLUSH` reload; payload never returned when the audit Stream has the wrong type; delete of a replayed-and-re-dead-lettered entry refuses the old tag; admin-auth rotation revokes exactly the indexed sessions and keeps the salt; session refresh throttling and absolute cap; generation mismatch invalidates.
- HTTP-seam tests for every route and error code, route authentication classes (cookie ignored on Bearer-only routes), CSRF/Origin matrix, cookie attributes in production and loopback-insecure modes, rate limits and capacity, `Cache-Control: no-store`.
- UI tests in Go: assets served with the headers above; every HTML file has no inline script/style or `on*` attributes and references only existing embedded files; no `innerHTML` in any JavaScript file. A manual browser walkthrough is recorded in ticket 10.
- Composed integration over real Valkey: restart with a changed Admin Secret revokes sessions before readiness; session reconciliation repairs a dropped index member and deletes an orphan.
- Compose smoke: login via curl with a cookie jar, `GET /admin/v1/session`, a cookie-authenticated replay needing CSRF (and refused without it), payload inspection writing an audit entry visible in `audit list`, delete with `If-Match`, summary counts, logout, and the UI shell served with its CSP.

## Out of Scope

- Session access to Webhook Endpoint, Bot Identity, and block inspect/clear routes; webhook configuration in the UI.
- A listing of recent acknowledged messages (needs an index); `/debug/config` and pprof handlers; audit filtering.
- Cross-slice reconciliation hardening and periodic consistency checking (Milestone 5).
- Multi-process session rate limiting (buckets stay process-local).

## Further Notes

`If-Match` on DLQ deletion follows the CLI rule that mutations of existing resources read the resource and its `ETag` first (`admin-api.md#command-line-client`). Replay keeps its Milestone 2 contract without `If-Match`; its conflict protection is the Deduplication Identity check and the cycle read before replay.

# 04: Admin CLI

**What to build:** An operator can create and read Webhook Endpoints from the terminal, with a known identifier and honest uncertain-outcome reporting after a lost response — never a blind retry of a critical mutation.

**Blocked by:** 03 (admin endpoint API).

**Status:** done

- [x] `hookrelay admin webhook create|get` per the CLI contract, speaking only HTTP to the Admin API (never direct Valkey)
- [x] Secret resolution: `--admin-secret-file` → `HOOKRELAY_ADMIN_SECRET_FILE` → `HOOKRELAY_ADMIN_SECRET` → hidden interactive prompt on TTY; never a command-line value; admin URL: flag → env → loopback default
- [x] Create generates the `wh_<base64url-128-bit>` identifier client-side unless supplied, exposes it on an uncertain outcome, and never silently regenerates or blindly retries
- [x] After transport failure or uncertain result: re-read the endpoint by known identity, report observed-desired-state with an explicit warning that mutation and audit confirmation remain unproven; unreadable/absent state reported as uncertain requiring operator reconciliation
- [x] Output formats `table` (TTY default) and `json`; stdout carries results only, progress and warnings go to stderr; secrets never printed
- [x] CLI failures surface the bounded API error codes; network-level failures produce the uncertain-outcome path

## Comments

- 2026-09-28: Implemented in `internal/cli/admin.go`. `hookrelay admin webhook create|get` speak only HTTP. Precedence: `--admin-url` → `HOOKRELAY_ADMIN_URL` → `http://127.0.0.1:8081`; `--admin-secret-file` → `HOOKRELAY_ADMIN_SECRET_FILE` → `HOOKRELAY_ADMIN_SECRET` → hidden prompt (only when stdin is a terminal). Credential from `--credential-file` or `HOOKRELAY_WEBHOOK_CREDENTIAL` (both is a usage error); `--credential-kind` defaults to the Webhook Type's sole kind from the built-in registry. Create fixes the `wh_` identifier before the POST. A 4xx is a definite refusal (bounded code + request_id on stderr, exit 1). A transport error, any 5xx, or an unreadable 201 takes the reconciliation path: no second POST, one GET by the known identity, stdout `{"outcome": "desired_state_observed"|"uncertain", ...}`, and a stderr warning that mutation and audit remain unconfirmed; exit 1 in every unconfirmed case (the 0/1/2 exit contract has no separate code). Safe-state comparison covers type, identifier, bot_id, enabled, credential kind, and configured flag; credential value and generation are not observable. Transport errors are printed without the request URL.
- Decision: `golang.org/x/term` v0.46.0 added for the hidden prompt and terminal detection (Go-team module, depends only on x/sys, which moved v0.47.0 → v0.48.0); recorded in the code-structure dependency policy.
- Tests: fake-API unit tests for every acceptance item (precedence tables, lost response via hijacked connection, 5xx with absent/differing/unreadable state, output defaults, no secrets in output); real-Valkey integration test (DB 2) drives the CLI against the composed Admin API, including a real create whose response is dropped: exactly one POST, `desired_state_observed`, exactly one audit event. Live-verified against `hookrelay serve`.

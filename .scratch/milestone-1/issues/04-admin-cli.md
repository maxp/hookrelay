# 04: Admin CLI

**What to build:** An operator can create and read Webhook Endpoints from the terminal, with a known identifier and honest uncertain-outcome reporting after a lost response — never a blind retry of a critical mutation.

**Blocked by:** 03 (admin endpoint API).

**Status:** ready-for-agent

- [ ] `hookrelay admin webhook create|get` per the CLI contract, speaking only HTTP to the Admin API (never direct Valkey)
- [ ] Secret resolution: `--admin-secret-file` → `HOOKRELAY_ADMIN_SECRET_FILE` → `HOOKRELAY_ADMIN_SECRET` → hidden interactive prompt on TTY; never a command-line value; admin URL: flag → env → loopback default
- [ ] Create generates the `wh_<base64url-128-bit>` identifier client-side unless supplied, exposes it on an uncertain outcome, and never silently regenerates or blindly retries
- [ ] After transport failure or uncertain result: re-read the endpoint by known identity, report observed-desired-state with an explicit warning that mutation and audit confirmation remain unproven; unreadable/absent state reported as uncertain requiring operator reconciliation
- [ ] Output formats `table` (TTY default) and `json`; stdout carries results only, progress and warnings go to stderr; secrets never printed
- [ ] CLI failures surface the bounded API error codes; network-level failures produce the uncertain-outcome path

# 07: Milestone 3 smoke and documentation

**What to build:** The Compose smoke proves credential replacement and notification wake-up; docs describe the Milestone 3 behavior.

**Blocked by:** 02, 03, 05, 06.

**Status:** done

- [x] Smoke: create a second endpoint for the bot, `admin bot webhooks` lists both, disable and delete the first, old path `404`, new path accepts; a waiting claim returns promptly after a webhook; `hookrelay_ready_signals_total` present
- [x] README, `admin-api.md`, `consumer-api.md`, `delivery.md`, `implementation-milestones.md`, `configuration.md` updated; spec status `done`

## Comments

- 2026-09-30: Implemented. `scripts/smoke.sh` appends two steps after the restart checks (so the Milestone 1–2 path still uses `wh_smoke`): it drains the stalled message's retry, parks a `wait_ms=10000` claim, sends a new webhook, and asserts the claim returns that message and `hookrelay_ready_signals_total{result="delivered",source="accept"} >= 1` (deterministic: the waiter is registered throughout its wait); then credential replacement — create `wh_smoke2` with a new credential, `admin bot webhooks` lists both, `admin webhook list --limit 1` pages, disable (`config_version` 2) → old path `404` → delete (`deleted`) → bot list shows only the new endpoint → the new path accepts, and the disable/delete audit counters are 1. Passed locally against the pinned stack. README (status, routes, CLI, smoke, spec pointer), `admin-api.md`, `consumer-api.md`, `delivery.md`, `platform.md`, `configuration.md`, `deployment.md`, `implementation-milestones.md`, and the CI comment updated; spec status `done`.
- 2026-09-30: Review follow-up (standards and spec review since `ba97f67`). Fixed: a claim no longer wakes itself with its own inline-pass signal (it deregisters for the pass); the wait-duration histogram times every waiting-claim outcome (`refused` added); hand-ons are counted as `source="handoff"` and unlisted sources as `unknown`; a malformed `If-Match` on a missing endpoint now yields `404` (PATCH) / `204` (DELETE); the bot listing is truncated to 100 with a warning; real-Valkey test for replay waking a waiting claim. Refactors: shared list limit/cursor helpers across the three paginated routes, `getBool` config helper, `EndpointRef` and `DeletedEndpoint` admin types, shared confirmed/unconfirmed mutation helpers, `endpointKey` and `stringFields` in the store, one claim-outcome→label mapping, named ready-signal source constants per module, `OrphanUnknownType`, unused `delivery.ReadySignaler` removed, the discarded precondition-failed version no longer returned. Spec and design docs record the previously undocumented choices (unnameable DELETE path → `204`, unknown-type orphans, the CLI's "already desired" observation rule, body-first PATCH validation).

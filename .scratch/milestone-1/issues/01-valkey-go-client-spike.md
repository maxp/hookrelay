# 01: valkey-go client spike

**What to build:** Verified confidence that the selected Valkey client satisfies hookrelay's script and failure contracts, before any production adapter code exists. A short throwaway spike program exercises the client against a pinned Valkey; the deliverable is the verdict and the pinned versions, not reusable code.

**Blocked by:** None (can start immediately).

**Status:** done

- [x] Spike program exercises: script load, `EVALSHA`, `NOSCRIPT` → single `EVAL` reload of the same body, `TIME` callable inside a script (effect replication), authentication, TLS, reconnect behavior, command timeouts, OOM response under `noeviction`, script runtime errors, and the configured connection/multiplexing limits
- [x] Ambiguous transport error after dispatch is distinguishable from `NOSCRIPT` (no-blind-retry precondition for later tickets)
- [x] Findings recorded as a short note in the repository: accepted client or a concrete demonstrated blocker
- [x] Go version (toolchain directive, CI image, build image) and exact Valkey version (Compose, integration tests) selected and recorded — Go `1.27.1`, Valkey `valkey/valkey:9.1.2`, valkey-go `v1.0.78`
- [x] Client resource limits (max connections, min idle) validated against the client's connection model

## Comments

- 2026-09-28: Spike verdict — `valkey-go v1.0.78` accepted, no blocker for `go-redis` fallback. Ten contract tests in `spike/valkey-client/` (own Go module, throwaway; kept as evidence until the production adapter lands) against `valkey/valkey:9.1.2` containers: plain, TLS+auth, and OOM/`noeviction` instances. Key findings: auth/TLS/dial failures surface eagerly at client construction; `CLIENT LIST` replies are RESP3 verbatim strings needing a `txt:` prefix strip; writes are not auto-retried after connection kills (read-only only) — pins the no-blind-retry rule; context timeout during an in-flight write is ambiguous (command was applied). Config mapping recorded: `PipelineMultiplex` (2^N cap, default 4 conns) + `BlockingPoolSize` bound the connection model. Full report: `docs/adr/0006-valkey-go-client.md`.
- 2026-09-28: Two-axis code review (Standards: BLOCK on one finding; Spec: OK). Fixed: removed the committed container-password literal (P0 — credentials only via env now), aligned the ADR env list, fixed stale comments, made the wrong-CA sub-check honestly skip-when-absent, and refined two findings into their final form: (1) canceled writes are dropped before flush or applied — the outcome is unknowable from the error alone (test pins both parts deterministically); (2) `BlockingPoolMinSize` retention is not observable through `Do`-level blocking commands — recorded as an adapter-ticket finding for the long-poll slice. Spike env keys were regenerated with a random password and torn down after the run.

# 01: valkey-go client spike

**What to build:** Verified confidence that the selected Valkey client satisfies hookrelay's script and failure contracts, before any production adapter code exists. A short throwaway spike program exercises the client against a pinned Valkey; the deliverable is the verdict and the pinned versions, not reusable code.

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] Spike program exercises: script load, `EVALSHA`, `NOSCRIPT` → single `EVAL` reload of the same body, `TIME` callable inside a script (effect replication), authentication, TLS, reconnect behavior, command timeouts, OOM response under `noeviction`, script runtime errors, and the configured connection/multiplexing limits
- [ ] Ambiguous transport error after dispatch is distinguishable from `NOSCRIPT` (no-blind-retry precondition for later tickets)
- [ ] Findings recorded as a short note in the repository: accepted client or a concrete demonstrated blocker (fallback is go-redis only on such a blocker)
- [ ] Go version selected (toolchain directive, CI image, build image) and exact Valkey version selected (Compose, integration tests) — recorded and consistent
- [ ] Client resource limits (max connections, min idle) validated against the client's connection model

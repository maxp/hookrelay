# Use valkey-go as the Valkey client

The Go application uses `github.com/valkey-io/valkey-go` as the Valkey client. The Milestone 1 client spike (ticket `01-valkey-go-client-spike`) verified the client and script/failure contracts against a pinned Valkey before the production adapter; no concrete blocker for an alternative client was demonstrated, so `go-redis` is not adopted.

## Pinned versions

Selected at spike time and materialized consistently by the scaffold ticket:

- Go: `1.27.1` (toolchain directive, CI image, build image);
- Valkey: `valkey/valkey:9.1.2` (Compose, integration tests, CI) — one documented minor line;
- Client: `valkey-go v1.0.78`.

## Verified contracts

The spike tests run against a real pinned Valkey (`HR_SPIKE_ADDR`, `HR_SPIKE_SECURE_ADDR`, `HR_SPIKE_OOM_ADDR`; tests skip locally without them, matching the repo's real-Valkey integration convention):

- Script flow: `SCRIPT LOAD`, `EVALSHA` by digest, `NOSCRIPT` after `SCRIPT FLUSH`, and one `EVAL` reload of the same body; the library's Lua helper performs the EVALSHA-with-fallback internally and survives `SCRIPT FLUSH`.
- `TIME` is callable inside scripts (effect replication) and its result is usable and storable — the authoritative-time mechanism the transition scripts require.
- Script runtime errors are `*valkey.ValkeyError` and leave earlier writes applied (no rollback), confirming the design rule that scripts must validate preconditions before their first write.
- Error classes: server rejections (`NOSCRIPT`, OOM under `noeviction`) surface as `*valkey.ValkeyError`; dial, TLS-handshake, and context failures are ordinary Go errors — callers can distinguish server errors from ambiguous transport errors without string matching.
- Ambiguity: a context deadline during an in-flight write does not prove non-execution (the command was found applied after the server unpaused) and a write can fail with a transport-class error (`EOF`) on a killed connection while a read on another pooled connection transparently retried. This pins the no-blind-retry rule for script transitions.
- Auth/TLS: authentication is verified eagerly at client construction — wrong password (`WRONGPASS`), missing credentials (`NOAUTH`), unreachable address, and unrelated CA all fail construction, so dependency unavailability is detectable before readiness. Correct credentials over TLS with a CA pool succeed.
- Recovery: after server-side `CLIENT KILL`, read-only commands retry transparently (`DisableRetry=false` default retries read-only only) and the client recovers without recreation.
- Connection model: connections are created lazily; `PipelineMultiplex` caps pipeline connections at 2^N under concurrent load (default 2 → 4); `BlockingPoolSize` caps the pool shared by blocking commands — the future long-poll path; `BlockingPoolMinSize` does not prewarm connections eagerly. `ClientOption.ClientName` is applied to every connection (HELLO SETNAME), enabling per-client accounting via `CLIENT LIST`.

## Configuration mapping

`HOOKRELAY_VALKEY_MAX_CONNECTIONS=20` and `HOOKRELAY_VALKEY_MIN_IDLE=2` map to the verified knobs: `PipelineMultiplex` bounds the pipeline ring (2^N) and `BlockingPoolSize`/`BlockingPoolMinSize` bound the blocking pool. The adapter ticket selects concrete values within these bounds (default multiplex 2 with blocking pool sized for the waiting-claim limit is sufficient for Milestone 1; a single multiplexed client serves all operations).

## Spike caveats carried into the adapter ticket

- `CLIENT LIST` (and similar) replies are RESP3 verbatim strings: `ToString()` returns a `txt:`-prefixed payload that callers must strip before line parsing.
- `NewClient` dials eagerly; construction failure is the first dependency check, not the first command.
- The library uses SHA-1 for its internal script identity; hookrelay's registry keeps its own body SHA-256 digest for contract identity checks, which is compatible.

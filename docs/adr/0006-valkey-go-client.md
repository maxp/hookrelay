# Use valkey-go as the Valkey client

The Go application uses `github.com/valkey-io/valkey-go` as the Valkey client. The Milestone 1 client spike (ticket `01-valkey-go-client-spike`) verified the client and script/failure contracts against a pinned Valkey before the production adapter; no concrete blocker for an alternative client was demonstrated, so `go-redis` is not adopted.

## Pinned versions

Selected at spike time and materialized consistently by the scaffold ticket:

- Go: `1.27.1` (toolchain directive, CI image, build image);
- Valkey: `valkey/valkey:9.1.2` (Compose, integration tests, CI) — one documented minor line;
- Client: `valkey-go v1.0.78`.

## Verified contracts

The spike tests run against a real pinned Valkey. Local containers provide three instances — plain, TLS+auth (`--requirepass`, server cert signed by a local CA), and OOM (`maxmemory 8mb`, `noeviction`) — exposed through the environment variables `HR_SPIKE_ADDR`, `HR_SPIKE_SECURE_ADDR`, `HR_SPIKE_SECURE_PASSWORD`, `HR_SPIKE_TLS_CA`, `HR_SPIKE_OOM_ADDR` (plus an unrelated CA file at `/tmp/hr-spike-tls/wrong-ca.crt` for the unrelated-CA sub-check, which skips when absent). Tests skip locally without this environment, matching the repo's real-Valkey integration convention; the container and certificate provisioning itself is local-only and materializes as Compose/CI at the scaffold ticket, together with the recorded version pins.

- Script flow: `SCRIPT LOAD`, `EVALSHA` by digest, `NOSCRIPT` after `SCRIPT FLUSH`, and one `EVAL` reload of the same body; the library's Lua helper performs the EVALSHA-with-fallback internally and survives `SCRIPT FLUSH`.
- `TIME` is callable inside scripts (effect replication) and its result is usable and storable — the authoritative-time mechanism the transition scripts require.
- Script runtime errors are `*valkey.ValkeyError` and leave earlier writes applied (no rollback), confirming the design rule that scripts must validate preconditions before their first write.
- Error classes: server rejections (`NOSCRIPT`, OOM under `noeviction`) surface as `*valkey.ValkeyError`; dial, TLS-handshake, and context failures are ordinary Go errors — callers can distinguish server errors from ambiguous transport errors without string matching.
- Ambiguity: a server-side client pause holds a sent command and applies it when the pause lifts — a late response still means applied. A context-canceled wait returns a context error while the client actively drops not-yet-flushed commands on cancellation: the canceled command may have been flushed (applied) or dropped (never sent), and the error alone never resolves which. A write can also fail with a transport-class error (`EOF`) on a killed connection while a read on another pooled connection transparently retried. Together these pin the no-blind-retry rule for script transitions: a timeout or cancellation never proves non-execution, and it never proves execution either.
- Auth/TLS: authentication is verified eagerly at client construction — wrong password (`WRONGPASS`), missing credentials (`NOAUTH`), unreachable address, and unrelated CA all fail construction, so dependency unavailability is detectable before readiness. Correct credentials over TLS with a CA pool succeed.
- Recovery: after server-side `CLIENT KILL`, read-only commands retry transparently (`DisableRetry=false` default retries read-only only) and the client recovers without recreation.
- Connection model: connections are created lazily; `PipelineMultiplex` caps pipeline connections at 2^N under concurrent load (default 2 → 4); `BlockingPoolSize` caps the pool shared by blocking commands — the future long-poll path; `BlockingPoolMinSize` does not prewarm connections eagerly, and min-idle retention is not observable through `Do`-level blocking commands (BLPOP), whose pooled connections are released after use — the long-poll ticket must verify retention behavior with `Dedicated()`/`B()` connections when it lands. `ClientOption.ClientName` is applied to every connection (HELLO SETNAME), enabling per-client accounting via `CLIENT LIST`.

## Configuration mapping

`HOOKRELAY_VALKEY_MAX_CONNECTIONS=20` and `HOOKRELAY_VALKEY_MIN_IDLE=2` map to the verified knobs: `PipelineMultiplex` bounds the pipeline ring (2^N) and `BlockingPoolSize`/`BlockingPoolMinSize` bound the blocking pool. The adapter ticket selects concrete values within these bounds (default multiplex 2 with blocking pool sized for the waiting-claim limit is sufficient for Milestone 1; a single multiplexed client serves all operations).

## Spike caveats carried into the adapter ticket

- `CLIENT LIST` (and similar) replies are RESP3 verbatim strings: `ToString()` returns a `txt:`-prefixed payload that callers must strip before line parsing.
- `NewClient` dials eagerly; construction failure is the first dependency check, not the first command.
- The library uses SHA-1 for its internal script identity; hookrelay's registry keeps its own body SHA-256 digest for contract identity checks, which is compatible.

## Update (2026-09-29, Milestone 1 complete)

The long-poll ticket implemented the waiting contract as periodic atomic `claim_v1` rechecks (250 ms + 0–50 ms jitter) on the pipelined connection ring, as the delivery design specifies; hookrelay issues no blocking commands, so the blocking-pool retention question above did not arise. `HOOKRELAY_VALKEY_MAX_CONNECTIONS` (minimum 2) now bounds the pipelined ring plus the blocking pool: the ring uses 2^N connections with N ≤ 2 and 2^N ≤ max − 1, and the (unused) blocking pool receives the remainder with `HOOKRELAY_VALKEY_MIN_IDLE` as its idle floor (`valkey.ApplyConnectionLimits`). A later notification-based wake-up that uses blocking commands must revisit this split.

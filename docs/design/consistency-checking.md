# Consistency checking policy

Milestone 5 selects **startup/recovery-only complete-model reconciliation** for the first version. No periodic checker, online full-scan reuse, or operator-triggered check API is enabled. Normal Lua transitions and bounded maintenance continue to validate the state they touch; they are not complete-model sweeps.

## Evidence and detection objective

The [complete-model matrix](../../.scratch/milestone-5/storage-reconciliation-matrix.md) specifies authority, safe repairs, isolation, and readiness holds. [Ticket 06](../../.scratch/milestone-5/issues/06-periodic-consistency-decision.md) separates checking while serving from the already implemented startup gate. Measurements below show that a complete pass is not suitable for the one-second maintenance cadence. More importantly, the current queue/message discovery is not concurrency-safe; merely pacing it would not fix that.

The objective is to validate persisted, continuously present first-version state **before acquiring readiness at startup and after dependency loss**. No maximum detection delay is promised for untouched corruption introduced while serving: detection is on a relevant transition or the next startup/recovery gate. Direct production storage edits are unsupported. A suspected whole-model incident requires a coordinated process restart and the existing reconciliation gate, not an ad hoc online scan. A successful pass proves the implemented invariants, not reconstruction of missing authoritative data, mandatory audit completion for an uncertain mutation, or durability beyond AOF's accepted loss window.

### Reproducible cost experiment

`internal/valkey/reconcile_cost_test.go` contains an opt-in experiment and a small regular fixture-validation test. Run **only against a dedicated disposable integration database**; the test flushes that database. For each profile, for example:

```sh
HOOKRELAY_TEST_VALKEY_URL=valkey://127.0.0.1:<dedicated-port>/0 \
HOOKRELAY_RECONCILIATION_COST_PROFILE=capacity \
  go test ./internal/valkey -run '^TestReconciliationCost$' -count=1 -v -timeout 20m
```

Measured locally on 2026-09-30: Linux x86_64, Intel Core Ultra 7 155H (22 logical CPUs), Go 1.27.1, Valkey 9.1.2 in Docker, standalone loopback TCP, AOF `everysec`, `noeviction`, reconciliation `Full=true`, `BatchSize=100`, queue check bound 1000. Each profile runs three full passes; setup is excluded. Endpoints, queued messages, and 100 sessions use real transitions. Retained dedup records for drained messages and DLQ records use schema-shaped fixtures; DLQ history, live leases, retries, replay cycles, and consumer load are not included. Payload sizes below are the text field size, not total JSON bytes. The dedup total includes queued-message records.

| Profile | Endpoints | Recipients | Queued | Dedup | DLQ / sessions | Payload | Pass range | Allocated per pass |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| medium | 1000 | 1000 | 10,000 | 100,000 | 100 / 100 | 1 KiB | 3.43–3.54 s | 56.6 MB |
| capacity | 1000 | 10,000 | 100,000 | 1,000,000 | 100 / 100 | 1 KiB | 32.49–33.78 s | 548.7 MB |
| deep | 1000 | 100 | 100,000 | 100,000 | 100 / 100 | 1 KiB | 34.74–36.30 s | 137.3 MB |
| large-payload | 1000 | 1000 | 10,000 | 100,000 | 100 / 100 | 64 KiB | 7.08–7.59 s | 56.6 MB |

A separate connection sampled PING every 10 ms during each pass. Per-pass p99 ranges were 0.57–0.76 ms (medium), 0.62–0.74 ms (capacity), 0.79–0.86 ms (deep), and 0.78–0.86 ms (large-payload). Worst sampled PING was 21.60 ms. Post-pass Go heap was 11.5–36.6 MB; that is **not peak RSS or peak heap**. These are synthetic quiescent recovery costs, not ingestion/claim tail-latency results or production SLO certification. Host scheduling, filesystem, payload maximum, retained history, storage size beyond these counts, and due-work verification passes can change the cost. Re-run on the deployment's hardware and data distribution before setting recovery objectives. The default capacity profile checks the configured 100,000 queued / 1,000,000 dedup limits, not every possible worst-case combination.

## Serving-time races and safe scope

Valkey's [SCAN contract](https://valkey.io/commands/scan/) is not a snapshot: continuous presence over a full iteration is guaranteed, duplicates are possible, and changing elements may or may not appear. `COUNT` is a work hint, not a reply-size or latency cap; `MATCH` filters after scanning. Several namespace scans therefore traverse the whole keyspace repeatedly. Client maps retain Recipients and message locators over the pass. Small command batches do not bound total duration or memory.

[Lua execution](https://valkey.io/topics/programmability/) blocks other server activities while each script runs. Isolation is not rollback after error; hookrelay's [audit failure contract](storage.md#administrative-audit) remains unchanged. The message validator decodes JSON/history and searches the entire Recipient queue inside a script for each queued message; deep queues repeat that search. A batch-size setting cannot bound that script's work.

Concrete healthy-transition races in the existing discovery contract:

- Discover B behind A; ack A before validating B. B is now head, but the saved `behind_head` locator makes the validator isolate a healthy Recipient.
- Finish queue discovery; acceptance atomically creates C and its queue position; discover C later through the blob scan without a queue locator. The validator can report a false `message_orphan` global hold.
- Offset-based queue pages can skip or repeat messages when acknowledgement removes heads or replay inserts positions.
- Counter compare-and-set does not fence an entire scan: scan A=1, ack A, accept into B, scan B=2; a counter initially/finally equal to 2 can be incorrectly repaired to the moving sum 3. Consequently `Full=true` remains startup-only.
- A Go-observed token/digest mismatch followed by an unconditional block-mode script is not live fencing. Verification's expected digest protects its successful path, not all isolation paths.

Endpoint membership/score, dedup-age, DLQ-index, and disposable session-index repairs are **candidates** for future concurrent checking only when they reread current authority and apply the repair atomically. A scanned key disappearing must be ordinary change, not corruption. This is not certification of a full concurrent pass. Orphan removal, message positions, active-attempt blocking, and counter repair require separately reviewed fencing/coordination.

## Alternatives considered

| Choice | Benefit | Cost / reason not selected now |
|---|---|---|
| Startup/recovery only (selected) | Existing complete-model contract; quiescent discovery; no added serving scan load | Latent corruption may wait until a touched transition or restart; recovery pays the pass cost |
| Bounded incremental background checker | Can eventually cover continuously present records without one monolithic call | Needs resumable phase/cursor state, strict command/time/memory budgets, stale-observation `changed` results, safe block fencing, and sweep-age metrics; pacing the current reconciler alone is unsafe |
| Operator-triggered online checker | Explicit operator-controlled cost and timing | Same races online; current reconciler mutates. Needs authorization, inspection/mutation modes, single-flight, cancellation/progress, audit policy, and an explicit quiescence contract. Restart already supplies the narrow supported check path |

No new hard-to-reverse operational mechanism is selected, so no ADR is added. This design decision can be revisited if incidents show that next-gate detection is insufficient. Before implementing a checker, amend the spec and create blocker-aware implementation tickets; do not silently repurpose `Maintenance.RunRound` or the one-second readiness probe.

## Work, readiness, and observability policy

- One reconciliation at a time in the application's gate, with the existing **five-minute** deadline, including admitted-work drain. Default scan hint/script collection batch is 100; this is not a strict server work cap. Due transitions and a verifying pass may add another sweep. Deadline/error or unisolatable ambiguity keeps readiness false; there is no fail-open timeout.
- An in-process recovery admission barrier drains API requests and background rounds before discovery. Public storage requests are retryably refused while unready. During the drain/scan, `/admin/v1/` storage requests also receive `503`; health, metrics, static UI, and authenticated profiling stay reachable. Between failed/held passes administrative diagnosis reopens; it can temporarily receive `503` as the next pass drains. A long poll can extend drain up to its ordinary bounded deadline. Due transitions run within the exclusive recovery phase. The barrier is not multi-process coordination.
- Runtime maintenance remains bounded lease expiry, retry activation, retention, session cleanup, capacity work, and sampling. It does not repeat reconciliation or repair the queued counter. Recovery `Full=false` still checks all lifecycle families but does not recompute that counter; a suspected counter drift requires a coordinated restart.
- Existing `hookrelay_reconciliation_in_progress`, `hookrelay_consistency_issues_total{kind,resolution}`, Valkey operation duration/error metrics, safe `reconciliation_started/completed/hold/failed` events, and readiness are the signals. Findings contain only bounded kinds and safe identifiers; no new checker/cursor labels, secrets, tokens, or payloads.
- Alert critical on a held finding (`increase(hookrelay_consistency_issues_total{resolution="held"}[5m]) > 0`), warning on a new block, and warning on any derived repair/removal/restoration for incident review. Existing-block `kept` counts may recur across passes; they are not new incidents. Warn if reconciliation remains in progress for **60 s**, critical after **240 s**; alert on readiness not returning, and on script/dependency errors. These are operational thresholds, not measured production guarantees.
- Reconsider an incremental checker only with measured accepted/duplicate and claim p95/p99 under representative serving load, a specified maximum sweep/detection age, a bounded per-tick command/time/retained-memory budget, stale-observation tests, and a readiness/isolation policy that cannot mistake healthy transitions for corruption. The current PING experiment does not supply those acceptance measurements.

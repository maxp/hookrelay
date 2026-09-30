# 04: Message lifecycle record hardening

**What to build:** Implement `reconcile_message_v1` and the complete queued/dead-lettered/acknowledged message-lifecycle scan fixed by ticket 02.

**Blocked by:** 02.

**Status:** done

- [x] Implement/register `reconcile_message_v1` exactly per the Milestone 5 spec, returning no payload or secret-bearing fields
- [x] Discover/deduplicate message IDs from queues, blobs, metadata, histories, dead letters, and compact success records in bounded scans
- [x] Semantically validate Canonical Message identity/Recipient, metadata/pending state, attempt history, DLQ agreement, success TTL/fields, block markers, and lifecycle exclusivity
- [x] Block queue-local ambiguity; hold readiness for DLQ/success/lone-blob/unlocatable ambiguity
- [x] Remove only atomically rechecked orphan metadata/history; never recreate dedup, payload, history, pending state, success, token, or audit data
- [x] Keep accepted pre-M2 metadata compatibility only where no history/pending state requires the key
- [x] Tests: every status/reason, snapshot-equal refusals, orphan cleanup, all lifecycle states, cross-state overlaps, batches, second pass, and `SCRIPT_FLUSH`

## Comments

- 2026-09-30: Implemented and registered `reconcile_message_v1`, plus bounded queue/key-family discovery in `Adapter.Reconcile`. The pass validates Canonical Message and Recipient identity, metadata and pending replay pairs, attempt history, dead-letter agreement, success records/TTL, marker fields, dedup cross-links, and lifecycle exclusivity without returning payloads. Queue-local ambiguity is isolated as `message_lifecycle_inconsistent`; global ambiguity holds readiness; only atomically rechecked orphan metadata/history is removed. Real-Valkey tests cover queued/dead-lettered/acknowledged/legacy states, bounded reasons, overlaps, TTL and marker validation, orphan cleanup, batches, idempotent second pass, and `SCRIPT FLUSH`. `go test ./...`, `go test -race ./...`, `go vet ./...`, and `git diff --check` passed against pinned Valkey 9.1.2.

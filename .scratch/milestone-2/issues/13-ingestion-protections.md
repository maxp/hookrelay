# 13: Ingestion protections

**What to build:** Token-bucket rate limiting, the memory acceptance stop, and deduplication early eviction with their metrics.

**Blocked by:** 03 (maintenance sampling).

**Status:** done

- [x] Global and per-endpoint token buckets (bounded LRU) before the in-flight semaphore; `429` + `Retry-After`; `rate_limited` outcome
- [x] Memory stop from `INFO memory` sampling; `valkey_memory_used_bytes`, `valkey_memory_max_bytes`, `valkey_aof_enabled`, `valkey_aof_delayed_fsync_total`
- [x] `evict_dedup_v1` for proactive maintenance and `accept_v3` for atomic, bounded acceptance-time eviction at full dedup capacity; preserve duplicate and other capacity-precondition semantics, count both paths in `dedup_early_evictions_total`; `dedup_oldest_record_age_seconds`, `dedup_effective_retention_seconds`; tests for eligible oldest, too-young oldest, corrupt index/record, concurrent acceptance, and no writes on refusal
- [x] `oldest_ready_message_age_seconds`

## Comments

- 2026-09-30: Implemented. Global and per-endpoint token buckets (bounded LRU of 10,000) after endpoint resolution and before the in-flight slot: `429` + `Retry-After` seconds, `outcome=rate_limited`. The memory stop from `INFO memory` (acceptance probe and maintenance) refuses new messages with `503` while draining continues. `accept_v3` evicts the oldest eligible dedup record atomically at the cap (`early_evicted` in the accepted tuple); `evict_dedup_v1` runs proactively each maintenance round; both count in `hookrelay_dedup_early_evictions_total`. New gauges: dedup oldest age and effective retention, Valkey memory used/max, AOF enabled and delayed fsyncs, oldest ready message age. Storage-seam tests cover eligible/too-young/corrupt candidates with no writes on refusal and concurrent acceptance at the cap. Details in the spec's ticket-13 note.

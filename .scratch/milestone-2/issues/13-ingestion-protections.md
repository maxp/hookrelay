# 13: Ingestion protections

**What to build:** Token-bucket rate limiting, the memory acceptance stop, and deduplication early eviction with their metrics.

**Blocked by:** 03 (maintenance sampling).

**Status:** ready-for-agent

- [ ] Global and per-endpoint token buckets (bounded LRU) before the in-flight semaphore; `429` + `Retry-After`; `rate_limited` outcome
- [ ] Memory stop from `INFO memory` sampling; `valkey_memory_used_bytes`, `valkey_memory_max_bytes`, `valkey_aof_enabled`, `valkey_aof_delayed_fsync_total`
- [ ] `evict_dedup_v1` for proactive maintenance and `accept_v3` for atomic, bounded acceptance-time eviction at full dedup capacity; preserve duplicate and other capacity-precondition semantics, count both paths in `dedup_early_evictions_total`; `dedup_oldest_record_age_seconds`, `dedup_effective_retention_seconds`; tests for eligible oldest, too-young oldest, corrupt index/record, concurrent acceptance, and no writes on refusal
- [ ] `oldest_ready_message_age_seconds`

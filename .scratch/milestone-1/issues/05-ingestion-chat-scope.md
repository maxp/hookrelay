# 05: Webhook ingestion happy path — chat scope

**What to build:** A signed Telegram update for a known chat flows end to end: route resolution, verification, chat-scope conversion, and atomic acceptance into the durable delivery structures — deduplicated on repeat. Accepted and duplicate requests return the same empty platform success only after Valkey commits.

**Blocked by:** 03 (admin endpoint API — endpoints and the adapter core must exist).

**Status:** ready-for-agent

- [ ] `POST /webhook/{webhook_type}/{webhook_identifier}`: type/identifier pattern validation (encoded slash, colon, whitespace, Unicode, dot segments, empty rejected); POST-only with `405` + `Allow: POST`; identical bounded `404` for unknown type, unknown identifier, identifier under another type, and disabled endpoints; UUIDv7 `X-Request-Id` on every response
- [ ] Body handling: 256 KiB hard limit with declared `Content-Length` precheck and streaming enforcement (read at most limit + one byte); SHA-256 computed during the read; exact bytes not retained; `application/json` with optional UTF-8 charset parsed as media type, others `415`
- [ ] Telegram Verifier: exactly one `X-Telegram-Bot-Api-Secret-Token` header compared in constant time against the stored credential; bounded failure reasons `credential_missing`, `credential_repeated`, `credential_mismatch`, `source_ip_denied`; optional operator-configured Telegram source CIDR allowlist as defense-in-depth; `403` on failure
- [ ] Converter chat-scope rows of the event-to-Recipient table (message, edited_message, channel_post, edited_channel_post, business and guest message variants, deleted_business_messages, my_chat_member, chat_member, chat_join_request, chat_boost, removed_chat_boost, message_reaction, message_reaction_count, callback_query with message, stopped_message_generation); integer-aware decoding (`UseNumber`, no float64, int64 bounds, negative chat ids, canonical decimal without leading zeros); compact payload re-encoding preserving unknown fields
- [ ] `accept_v1` atomic acceptance: dedup record + TTL + age index, message blob, queue RPUSH, head-state creation on first message, ready-index membership with fresh fairness sequence, stats counter — all or nothing; the candidate `message_id` is discarded on duplicate
- [ ] Duplicate with identical bytes → same empty `200`, no second blob or queue entry; same Deduplication Identity with different bytes → still a duplicate, `dedup_conflicts_total` incremented with a safe structured log
- [ ] Platform responses: empty `200` for accepted and duplicate; `503` + `Retry-After: 1` when Valkey cannot atomically accept; success returned only after acceptance or proven duplicate
- [ ] Feature events `webhook_accepted` / `webhook_duplicate` with contract fields (`platform_event_type`, status, `duration_ms`, body/payload sizes) and ingestion metrics with bounded labels
- [ ] Integration tests: fixture → stored message/queue/head-state/dedup/ready asserted at the storage seam; duplicate → no new state; Valkey unavailable → `503` with no partial state

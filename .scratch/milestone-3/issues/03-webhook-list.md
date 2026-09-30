# 03: Webhook Endpoint list

**What to build:** `GET /admin/v1/webhooks` with cursor pagination newest first, and `hookrelay admin webhook list`.

**Blocked by:** none.

**Status:** done

- [x] Adapter read over `hr1:webhooks` (score + member descending, strict-after cursor) and each Hash; orphan members skipped and logged as `webhook_index_orphan`
- [x] Handler: `limit` 1–200 default 50, `400 invalid_cursor`, `next_cursor` omitted on the last page, safe representation only
- [x] CLI `list [--limit] [--cursor]`, table/JSON
- [x] Tests: storage (ties on equal `created_ms`, orphan skip), HTTP seam, CLI, integration paging across pages

## Comments

- 2026-09-30: Implemented. `EndpointRepository.ListEndpoints` pages `hr1:webhooks` like the DLQ list (descending score then member, strict-after cursor across ties) and reads the page's Hashes in one pipelined `DoMulti`; a member whose Hash is missing, of the wrong type, or malformed comes back with an orphan reason instead of failing the page (`parseEndpoint` extracted from `GetEndpoint`, malformed records wrap `errMalformedEndpoint`). Reads count under the existing `endpoint_read` operation label. The service skips orphans and records of an unregistered Webhook Type, logging one `webhook_index_orphan` warning per request (count, first member, reason, bounded index name — no `hr1:` key). Cursor JSON `{"created_ms","id"}`. CLI `webhook list [--limit] [--cursor]` mirrors `dlq list` (no `--all`; spec updated). Tests: storage seam over real Valkey (ties, all orphan kinds), HTTP seam, CLI, composed integration paging five endpoints two per page.

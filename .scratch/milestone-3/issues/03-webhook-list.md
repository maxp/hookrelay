# 03: Webhook Endpoint list

**What to build:** `GET /admin/v1/webhooks` with cursor pagination newest first, and `hookrelay admin webhook list`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] Adapter read over `hr1:webhooks` (score + member descending, strict-after cursor) and each Hash; orphan members skipped and logged as `webhook_index_orphan`
- [ ] Handler: `limit` 1–200 default 50, `400 invalid_cursor`, `next_cursor` omitted on the last page, safe representation only
- [ ] CLI `list [--limit] [--cursor] [--all]`, table/JSON
- [ ] Tests: storage (ties on equal `created_ms`, orphan skip), HTTP seam, CLI, integration paging across pages

## Comments

# 03: Audit listing

**What to build:** `GET /admin/v1/audit` and `hookrelay admin audit list`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] Newest first via `XREVRANGE`, `limit` 1–200 (default 50), exclusive stream-ID cursor, `400 invalid_cursor`, allowlisted fields plus `stream_id`, missing Stream → empty, wrong type → `503`
- [ ] CLI `admin audit list [--limit] [--cursor]`, table and JSON
- [ ] Tests: storage seam (paging across equal-millisecond IDs, unknown fields dropped), HTTP seam, CLI

## Comments

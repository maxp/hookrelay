# 03: Audit listing

**What to build:** `GET /admin/v1/audit` and `hookrelay admin audit list`.

**Blocked by:** none.

**Status:** done

- [x] Newest first via `XREVRANGE`, `limit` 1–200 (default 50), exclusive stream-ID cursor, `400 invalid_cursor`, allowlisted fields plus `stream_id`, missing Stream → empty, wrong type → `503`
- [x] CLI `admin audit list [--limit] [--cursor]`, table and JSON
- [x] Tests: storage seam (paging across equal-millisecond IDs, unknown fields dropped), HTTP seam, CLI

## Comments

- 2026-09-30: Implemented. `administration.AuditLog` port (`ListAudit(limit, before)`), `valkey.NewAuditLog` reading `XREVRANGE hr1:audit (<id> - COUNT n` and copying only the allowlisted fields; `streamEntry` decoder shared with the `AuditEntries` test seam. `GET /admin/v1/audit` registered only when the reader is wired (the CLI server wiring does). Cursor `{"id":"<ms>-<seq>"}` validated as a complete stream ID. CLI `admin audit list`. Tests: storage seam (equal-millisecond paging, exclusive bound, unknown fields dropped, missing and wrong-type Stream), HTTP seam, CLI.

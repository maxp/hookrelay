# 06: Bot Identity endpoint listing

**What to build:** `GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks` and `hookrelay admin bot webhooks`.

**Blocked by:** none.

**Status:** ready-for-agent

- [ ] Adapter read over the bot Set and each Hash, sorted by `created_ms` then member descending; orphans skipped and logged
- [ ] Handler: `400 invalid_request` for unknown platform or invalid `bot_id`, `200 {"items":[…]}` (empty when none)
- [ ] CLI `bot webhooks --platform --bot-id`
- [ ] Tests: storage, HTTP seam, CLI, integration

## Comments

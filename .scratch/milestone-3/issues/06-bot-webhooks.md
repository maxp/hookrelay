# 06: Bot Identity endpoint listing

**What to build:** `GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks` and `hookrelay admin bot webhooks`.

**Blocked by:** none.

**Status:** done

- [x] Adapter read over the bot Set and each Hash, sorted by `created_ms` then member descending; orphans skipped and logged
- [x] Handler: `400 invalid_request` for unknown platform or invalid `bot_id`, `200 {"items":[…]}` (empty when none)
- [x] CLI `bot webhooks --platform --bot-id`
- [x] Tests: storage, HTTP seam, CLI, integration

## Comments

- 2026-09-30: Implemented. `TypeCatalog.KnownPlatform` (backed by `ingestion.Registry.HasPlatform`) validates the platform; `bot_id` uses the create-time canonical decimal pattern. `EndpointRepository.ListBotEndpoints` reads `SMEMBERS` then the shared pipelined Hash read, sorted by `created_ms` then member descending (orphans have no `created_ms` and sort last); a wrong-typed Set maps to `ErrStoredWrongType` → `503`. Response `{"items":[…]}` with the shared orphan handling (`index":"bot_webhooks"`). CLI `bot webhooks --platform --bot-id` reuses the list table/JSON printer. Tests: storage (order, orphan, absent, wrong type), HTTP seam (validation never reaches storage), CLI, composed integration (only the bot's endpoints, disabled included, newest first).

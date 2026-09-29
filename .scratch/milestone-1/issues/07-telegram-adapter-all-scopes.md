# 07: Telegram adapter — all scopes and routing issues

**What to build:** The complete Telegram classification surface: user, bot, and relay scopes alongside chat; every bounded routing issue; fallback deduplication for unusable `update_id`; event-time extraction rules; unknown-structure preservation. Every row of the event-to-Recipient table is proven by a fixture through the HTTP seam.

**Blocked by:** 05 (ingestion happy path — chat scope and acceptance must exist).

**Status:** done

- [x] User-fallback rows: `inline_query`, `chosen_inline_result`, `shipping_query`, `pre_checkout_query`, `purchased_paid_media`, `business_connection`, `subscription`, `managed_bot`, inline `callback_query` (from.id), `poll_answer` with `voter_chat` vs user variants — chat priority rules preserved
- [x] Bot scope: known `poll` update; unknown structures never auto-classify as bot scope
- [x] Relay scope: verified valid JSON unmappable to chat/user/bot; known chat/user event with missing or invalid identifier → relay + the matching bounded code (`missing_chat_id`, `invalid_chat_id_type`, `invalid_chat_id_value`, `missing_user_id`, `invalid_user_id_type`, `invalid_user_id_value`)
- [x] Update identity: missing/invalid `update_id` → relay scope, `routing_issue.code = invalid_source_event_id`, no `source_event_id`, fallback platform deduplication key `body_sha256_v1:<base64url>` of the exact request body
- [x] Event-field selection: exactly one known field → its type; exactly one unknown field → preserved name + `unknown_event_type`; no non-null field → `$unknown` + `unknown_event_type`; multiple non-null fields → `$unknown` + `unknown_event_structure`; null-valued known fields do not count
- [x] `occurred_ms`: root `date` for creation events, `edit_date` for edits, documented dates for membership/reaction/boost events; seconds × 1000; omitted (with bounded metric + structured warning) when absent/wrong type/out of range; nested reply/forward dates ignored
- [x] `callback_query.chat_instance`, `inline_message_id`, `poll.id` never become Recipient identifiers
- [x] `hookrelay_routing_issues_total{bot_platform,reason}` with bounded reasons; unknown event-field names appear only in structured logs, metrics aggregate them under `unknown`
- [x] Adapter tests cover every table row and every rule above via signed HTTP fixtures, asserting the resulting Recipient queue and Canonical Message at the storage seam

## Comments

- 2026-09-28: Ticket 05 landed the complete converter table and routing rules in `internal/ingestion/telegram.go` (see the 05 comments). Remaining here: signed HTTP fixtures for every table row asserted at the storage seam, `platform_event_type="unknown"` metric aggregation, and the bounded metric for unusable event timestamps (the `event_time_invalid` warning already exists).
- 2026-09-29: Completed. `telegram_fixtures_test.go` drives every event-to-Recipient row (17 chat rows plus `poll_answer` with `voter_chat`, 10 user-fallback rows including inline `callback_query` and `poll_answer` without `voter_chat`, `poll` bot scope), every relay rule (unknown field, no field, only-null fields, multiple fields, null beside a known field, the six identifier issues, non-object known event), five unusable `update_id` shapes with the `body_sha256_v1` fallback key and exact-byte deduplication, and the event-time rules through the signed HTTP route over real Valkey, asserting the Recipient queue membership and the stored Canonical Message (scope, identifiers, event type, routing issue, source event, occurred_ms, preserved payload). `chat_instance`, `inline_message_id`, and `poll.id` fixtures prove they never become identifiers; nested `reply_to_message.date` is ignored.
- Event time: a documented timestamp that is missing (now also counted, per the adapter design "If an optional event timestamp is missing, has the wrong type, or is out of range"), of the wrong type, or an invalid value omits `occurred_ms`, keeps routing, emits the `event_time_invalid` warning with a bounded `reason`, and increments the new `hookrelay_event_time_issues_total{bot_platform,reason}` (`missing` | `invalid_type` | `invalid_value`; documented in platform.md). Events whose row defines no timestamp (e.g. `inline_query`, `callback_query`) are not counted.
- Metrics: `hookrelay_routing_issues_total` counts asserted per reason; the test scans every exported series to prove an unknown event-field name never becomes a label value (no current metric carries `platform_event_type`, so aggregation under `unknown` has no series to apply to).


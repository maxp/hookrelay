# 07: Telegram adapter — all scopes and routing issues

**What to build:** The complete Telegram classification surface: user, bot, and relay scopes alongside chat; every bounded routing issue; fallback deduplication for unusable `update_id`; event-time extraction rules; unknown-structure preservation. Every row of the event-to-Recipient table is proven by a fixture through the HTTP seam.

**Blocked by:** 05 (ingestion happy path — chat scope and acceptance must exist).

**Status:** ready-for-agent

- [ ] User-fallback rows: `inline_query`, `chosen_inline_result`, `shipping_query`, `pre_checkout_query`, `purchased_paid_media`, `business_connection`, `subscription`, `managed_bot`, inline `callback_query` (from.id), `poll_answer` with `voter_chat` vs user variants — chat priority rules preserved
- [ ] Bot scope: known `poll` update; unknown structures never auto-classify as bot scope
- [ ] Relay scope: verified valid JSON unmappable to chat/user/bot; known chat/user event with missing or invalid identifier → relay + the matching bounded code (`missing_chat_id`, `invalid_chat_id_type`, `invalid_chat_id_value`, `missing_user_id`, `invalid_user_id_type`, `invalid_user_id_value`)
- [ ] Update identity: missing/invalid `update_id` → relay scope, `routing_issue.code = invalid_source_event_id`, no `source_event_id`, fallback platform deduplication key `body_sha256_v1:<base64url>` of the exact request body
- [ ] Event-field selection: exactly one known field → its type; exactly one unknown field → preserved name + `unknown_event_type`; no non-null field → `$unknown` + `unknown_event_type`; multiple non-null fields → `$unknown` + `unknown_event_structure`; null-valued known fields do not count
- [ ] `occurred_ms`: root `date` for creation events, `edit_date` for edits, documented dates for membership/reaction/boost events; seconds × 1000; omitted (with bounded metric + structured warning) when absent/wrong type/out of range; nested reply/forward dates ignored
- [ ] `callback_query.chat_instance`, `inline_message_id`, `poll.id` never become Recipient identifiers
- [ ] `hookrelay_routing_issues_total{bot_platform,reason}` with bounded reasons; unknown event-field names appear only in structured logs, metrics aggregate them under `unknown`
- [ ] Adapter tests cover every table row and every rule above via signed HTTP fixtures, asserting the resulting Recipient queue and Canonical Message at the storage seam

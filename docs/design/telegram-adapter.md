# Telegram adapter

Telegram is the first production Bot Platform adapter.

## Credential and verification

The supported credential kind is:

```text
secret_token
```

It corresponds to Telegram `setWebhook.secret_token` and is validated as 1–256 characters from `A-Z`, `a-z`, `0-9`, `_`, and `-`. The operator supplies this value when creating the Webhook Endpoint and uses the same value when configuring Telegram. Hookrelay does not generate Telegram secret tokens and never returns a stored value.

The Verifier requires exactly one `X-Telegram-Bot-Api-Secret-Token` header and compares it with the stored credential in constant time. Missing, repeated, or incorrect values fail verification and normally receive `403 Forbidden`. Header and credential values are not trimmed or normalized. An optional operator-configured Telegram source CIDR allowlist is checked as defense-in-depth but never replaces the secret token.

Besides the [common log envelope and HTTP outcome fields](platform.md#structured-log-contract), verification failures log only request ID, Webhook Type and Identifier, Bot Identity, source IP, body size, and one bounded reason: `credential_missing`, `credential_repeated`, `credential_mismatch`, or `source_ip_denied`. Credentials, request bodies, computed signatures, and sensitive headers are never logged. Hookrelay adds no artificial response delay for failed verification; constant-time comparison and rate limiting provide the intended protection without consuming connections through sleeps.

## Bot Identifier

Telegram Bot Identifier is the bot's positive Telegram user ID supplied by the administrator. It is stored as a canonical decimal string with ASCII digits, no sign, no leading zeros, and at most 20 characters. Hookrelay does not derive it from the secret token and does not require the Telegram Bot API token.

## Update identity

For a structurally known Telegram `Update`:

```text
source_event_id             = decimal(update_id)
platform_deduplication_key  = decimal(update_id)
```

The full Deduplication Identity also includes Webhook Type and Bot Identity. If a verified valid JSON payload lacks a usable integer `update_id`, the message is relay-scoped and uses SHA-256 of the exact request body as the fallback platform deduplication key.

## Platform Event Type

Telegram `Update` contains `update_id` and at most one event payload field. The known nonempty event-field name becomes `platform_event_type`. No known event field produces `$unknown` and relay scope with `unknown_event_type`. Multiple event fields produce relay scope with `unknown_event_structure`. The complete `Update` remains the Canonical Payload.

## Recipient extraction

Recipient extraction is chat-first:

1. use the event's applicable Telegram `Chat.id` when present;
2. otherwise use the event's explicitly defined primary actor or end-user ID;
3. otherwise use bot scope for a known bot-scoped event;
4. otherwise use relay scope.

The Recipient representation distinguishes chat and user namespaces:

```text
chat  = (telegram, bot_id, chat, chat_id)
user  = (telegram, bot_id, user, user_id)
bot   = (telegram, bot_id, bot)
relay = (telegram, bot_id, relay)
```

The adapter never takes an arbitrary nested `User.id`; every event type has an explicit extraction rule.

Initial user fallback rules, used only when no applicable `chat.id` exists, are:

| Telegram event | User or chat fallback |
|---|---|
| `inline_query` | `inline_query.from.id` as user |
| `chosen_inline_result` | `chosen_inline_result.from.id` as user |
| inline `callback_query` | `callback_query.from.id` as user |
| `shipping_query` | `shipping_query.from.id` as user |
| `pre_checkout_query` | `pre_checkout_query.from.id` as user |
| `purchased_paid_media` | `purchased_paid_media.from.id` as user |
| `business_connection` | `business_connection.user.id` as user |
| `subscription` | `subscription.user.id` as user |
| `managed_bot` | `managed_bot.user.id` as user |
| `poll_answer` | `poll_answer.voter_chat.id` as chat when present; otherwise `poll_answer.user.id` as user |

For `callback_query`, `callback_query.message.chat.id` has priority when available; `from.id` is only the inline/no-chat fallback. The complete extraction table is expanded and tested with the adapter implementation.

## Bot and relay classification

The known `poll` update is bot-scoped because it has no applicable chat or end-user recipient. Other known bot-scoped events must be explicitly added to the adapter table; unknown structures do not become bot-scoped automatically and instead use relay scope.

If a known chat- or user-scoped event lacks its required identifier or has an invalid identifier type/value, it is preserved as relay scope and uses the matching bounded routing issue:

```text
missing_chat_id
invalid_chat_id_type
invalid_chat_id_value
missing_user_id
invalid_user_id_type
invalid_user_id_value
```

Telegram Chat and User identifiers are parsed without `float64` and stored as canonical decimal strings. Chat identifiers may be negative; User identifiers are positive. Values must fit `int64`. Numeric parsing removes noncanonical leading zeros.

## Event time

`received_ms` is always recorded by hookrelay. `occurred_ms` uses the timestamp that represents the occurrence of the specific Telegram event:

- ordinary creation events use their documented `date` field when available;
- message-edit events use `edit_date`, because the edit itself is the event being relayed;
- Telegram seconds are multiplied by 1000;
- when the selected event structure has no applicable timestamp, `occurred_ms` is omitted.

The adapter does not substitute the original message creation time for an edit event when `edit_date` is available.

## Media type and responses

Telegram accepts `application/json` with an optional UTF-8 charset parameter, parsed as a media type rather than compared as a raw header string. Other media types receive `415`.

Accepted, duplicate, and relay-scoped accepted updates receive an empty `200`. Unknown endpoint is `404`, verification failure is `403`, invalid JSON is `400`, oversized body is `413`, temporary rate/capacity/Valkey failure is `503`, and an internal error is `500`.

Hookrelay does not call Telegram `setWebhook` in the first version and does not store or use the Telegram Bot API token. Operators configure the webhook URL, `secret_token`, and `allowed_updates` directly at Telegram. Unknown new update types are preserved through relay scope.

## Parsing and update validation

Telegram request JSON must be a top-level object with maximum nesting depth 40. A valid non-object JSON value is rejected with `400` rather than routed to relay scope because it cannot represent a Telegram `Update`.

The adapter parses numbers without `float64`, using integer-aware decoding. `update_id` must be an unquoted, nonnegative integer that fits `int64`. A missing or invalid `update_id` does not discard an otherwise valid Update object: the message uses relay scope, omits `source_event_id`, records `routing_issue.code = "invalid_source_event_id"`, and uses SHA-256 of the exact request body as the fallback deduplication key.

To identify the event field, the adapter excludes `update_id` and examines non-null top-level fields. Exactly one known field selects its documented event type and extraction rule. Exactly one unknown field preserves that field name as `platform_event_type`, uses relay scope, and records `unknown_event_type`. No non-null event field yields `$unknown` and `unknown_event_type`. Multiple non-null event fields yield `$unknown` and `unknown_event_structure`. A known event field whose value is `null` does not count as a valid event payload.

The complete payload is decoded with `json.Decoder.UseNumber()` and re-encoded as compact JSON for the Canonical Message. Unknown fields and integer precision are preserved semantically; duplicate keys use last-value-wins; whitespace and object-key order are not preserved. The fallback body digest continues to use the exact original request bytes.

Event timestamps are extracted only from explicit event-specific paths. Creation events use their documented root `date`; edit events use `edit_date`; membership, reaction, and boost events use their documented event date. Nested reply, forward, and unrelated dates are ignored. If an optional event timestamp is missing, has the wrong type, or is out of range, routing remains unchanged, `occurred_ms` is omitted, and hookrelay emits a bounded metric and structured warning.

## Event-to-Recipient table

Chat-first extraction uses:

| Telegram event | Recipient |
|---|---|
| `message` | `message.chat.id` as chat |
| `edited_message` | `edited_message.chat.id` as chat |
| `channel_post` | `channel_post.chat.id` as chat |
| `edited_channel_post` | `edited_channel_post.chat.id` as chat |
| `business_message` | `business_message.chat.id` as chat |
| `edited_business_message` | `edited_business_message.chat.id` as chat |
| `deleted_business_messages` | `deleted_business_messages.chat.id` as chat |
| `guest_message` | `guest_message.chat.id` as chat |
| `callback_query` with message | `callback_query.message.chat.id` as chat |
| inline `callback_query` | `callback_query.from.id` as user |
| `message_reaction` | `message_reaction.chat.id` as chat |
| `message_reaction_count` | `message_reaction_count.chat.id` as chat |
| `my_chat_member` | `my_chat_member.chat.id` as chat |
| `chat_member` | `chat_member.chat.id` as chat |
| `chat_join_request` | `chat_join_request.chat.id` as chat |
| `chat_boost` | `chat_boost.chat.id` as chat |
| `removed_chat_boost` | `removed_chat_boost.chat.id` as chat |
| `inline_query` | `inline_query.from.id` as user |
| `chosen_inline_result` | `chosen_inline_result.from.id` as user |
| `shipping_query` | `shipping_query.from.id` as user |
| `pre_checkout_query` | `pre_checkout_query.from.id` as user |
| `purchased_paid_media` | `purchased_paid_media.from.id` as user |
| `business_connection` | `business_connection.user.id` as user |
| `subscription` | `subscription.user.id` as user |
| `managed_bot` | `managed_bot.user.id` as user |
| `poll_answer` with `voter_chat` | `poll_answer.voter_chat.id` as chat |
| other `poll_answer` | `poll_answer.user.id` as user |
| `poll` | bot scope |
| `stopped_message_generation` | `stopped_message_generation.chat.id` as chat |

For message-like, reaction, membership, and boost events, a present applicable Chat Identifier has priority over actor or subject User identifiers. `callback_query.chat_instance`, `inline_message_id`, and `poll.id` are not Recipient identifiers.

## Endpoint replacement and duplicate updates

Multiple enabled Telegram Webhook Endpoints may belong to the same Bot Identity and use different secret tokens. This supports replacement by creating a new endpoint, switching Telegram to the new URL, and retiring the old endpoint while delayed retries may still arrive. Webhook Identifier is excluded from Deduplication Identity, so the same `update_id` received through old and new endpoints is accepted once; a differing body for the same identity is recorded as a deduplication conflict.

Hookrelay permits disabling and deleting the last endpoint of a Bot Identity. Existing Delivery Queues and DLQ entries continue independently. The Admin CLI warns before the operation but does not forbid it.

Hookrelay does not verify the configured Telegram Bot Identifier through `getMe`; doing so would require storing a Bot API token. The administrator is responsible for matching Bot Identifier, Webhook Endpoint, and Telegram bot configuration.

A valid `update_id` with no non-null event field is relay-scoped with `platform_event_type = "$unknown"` and `unknown_event_type`; the update ID remains the source event and deduplication key. Multiple non-null event fields are relay-scoped with `$unknown` and `unknown_event_structure`, again retaining `update_id` as source event and deduplication key.

Metrics may use known Telegram event types as bounded labels. All unknown event-field names are aggregated under `platform_event_type="unknown"`; the actual unknown field name may appear only in structured logs.

## Business and guest chat contexts

The first version deliberately ignores `business_connection_id` and `guest_query_id` when constructing Recipient Identity. Business and guest message updates use their ordinary `chat.id` with chat scope, even though Telegram documents those contexts as potentially separate from other bot chats that share the same numeric Chat Identifier.

Consequently, events with the same Bot Identity and Chat Identifier are ordered in one Delivery Queue even when they belong to different Telegram business connections or guest-query contexts. This is an accepted first-version simplification. The full payload retains the context identifiers so consumers can distinguish them. If real traffic demonstrates a harmful collision, Recipient Identity may later gain an explicit platform context dimension through a new storage namespace rather than changing `hr1` interpretation in place.

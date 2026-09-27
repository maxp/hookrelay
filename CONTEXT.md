# Webhook Relay

This context receives heterogeneous webhooks, converts them into a common message envelope, removes duplicates, and relays messages to ordered recipient queues.

## Language

### Ingress

**Webhook Request**:
An inbound HTTP request carrying a provider-specific event and addressed to a webhook route.
_Avoid_: Event, message

**Webhook Route**:
The request path composed of a type prefix followed by a webhook identifier.
_Avoid_: Queue route, delivery route

**Webhook Type**:
A class of webhook requests selected by the type prefix. Each webhook type owns its verification rules and converter. Webhook type and bot platform are distinct concepts, but this context defines a one-to-one correspondence between them: each supported bot platform determines exactly one webhook type, and each webhook type corresponds to exactly one bot platform.
_Avoid_: Bot platform, provider, format

**Type Prefix**:
The leading part of a webhook route that selects the webhook type.
_Avoid_: Type, provider name

**Webhook Identifier**:
The remainder of the webhook route after the type prefix. It is unique within one webhook type and identifies the endpoint's verification credentials, routing configuration, and bot identity. It is distinct from the bot identifier, with a one-to-one correspondence between webhook endpoint identity and bot identity in this context.
_Avoid_: Bot identifier, user ID, route ID

**Webhook Endpoint Identity**:
The combination of webhook type and webhook identifier that uniquely identifies an inbound webhook endpoint. Each webhook endpoint identity corresponds to exactly one bot identity, and each bot identity corresponds to exactly one webhook endpoint identity.
_Avoid_: Bot identity, webhook identifier alone

### Verification and normalization

**Webhook Credential**:
A secret associated with a webhook identifier and used by that webhook type's verifier.
_Avoid_: Global password, API key

**Verifier**:
The webhook-type-specific policy that authenticates a webhook request by checking its presented credential against the credential associated with the webhook identifier.
_Avoid_: Validator, parser

**Converter**:
The webhook-type-specific transformation from one verified webhook request into exactly one canonical message for exactly one recipient. The converter forms the recipient identity from the verified endpoint's bot identity and message-specific data. It classifies the recipient scope as chat-level for an identified chat, bot-level for a known event that inherently applies to the bot, or relay-level when a verified valid JSON payload cannot be mapped to a known chat-level or bot-level structure. Bot-level and relay-level messages use separate shared reserved chat identifiers. The converter preserves the full platform-specific payload for the queue consumer.
_Avoid_: Verifier, queue router, payload formatter

**Canonical Message**:
The common envelope produced before deduplication and routing. It contains hookrelay metadata, exactly one recipient identity, a platform event type, and the full bot-platform-specific payload needed by the queue consumer. Each verified webhook request produces exactly one canonical message.
_Avoid_: Raw webhook request, normalized cross-platform payload, message batch

**Canonical Payload**:
The complete bot-platform-specific JSON value from the webhook request, preserved without discarding unknown fields. Its semantic JSON content is part of the message; exact source bytes, whitespace, and object-key order are not. A verified valid JSON value of an unexpected top-level shape is preserved as one payload and routed to the relay-level recipient.
_Avoid_: Normalized payload, selected fields, exact HTTP body, automatically split batch

**Platform Event Type**:
The bot-platform-specific classification of the source event. It is preserved without cross-platform normalization. When a verified valid JSON payload has an unknown structure, hookrelay preserves the best safely extractable platform event type or uses a documented reserved unknown value and delivers the message to the relay-level recipient.
_Avoid_: Webhook type, bot platform, canonical event type

**Routing Issue**:
A bounded machine-readable code explaining why a canonical message was sent to the relay-level recipient, such as an unknown event structure, missing chat identifier, invalid chat identifier type, or unexpected JSON shape. It contains no free-form error text or payload data. The canonical JSON field is `routing_issue.code`.
_Avoid_: Exception message, stack trace, conversion failure

**Message Identifier**:
A hookrelay-generated UUIDv7 that uniquely identifies one canonical message and remains unchanged across all of its delivery attempts.
_Avoid_: Source event identifier, deduplication key, delivery token

**Source Event Identifier**:
A bot-platform-provided identifier for the source event, retained for consumer use and diagnostics. It may differ from the platform deduplication key.
_Avoid_: Message identifier, delivery token

**Platform Deduplication Key**:
The bot-platform-specific opaque value produced by the converter to identify repeated delivery of the same source event. Its derivation is defined separately for each webhook type. A platform-provided stable identifier is preferred; when none exists, that webhook type may explicitly define a digest of the exact request body as its fallback.
_Avoid_: Message identifier, source event identifier, arbitrary reserialized payload hash

**Received Time**:
The UTC Unix epoch time, in integer milliseconds, at which hookrelay began receiving the webhook request. It is recorded in the canonical message as `received_ms` and does not determine delivery-queue order.
_Avoid_: Platform event time, acceptance order

**Occurred Time**:
The optional UTC Unix epoch time, in integer milliseconds, at which the bot platform reports that the source event occurred. It is recorded as `occurred_ms` when available and does not determine delivery-queue order.
_Avoid_: Received time, acceptance order

**Deduplication Identity**:
The combination of webhook type, bot identity, and platform deduplication key used to classify a canonical message as new or duplicate. It identifies one platform event or update delivery, not the longer-lived platform object affected by that event.
_Avoid_: Message identifier, recipient identity, source object identifier

### Processing and delivery

**Duplicate Message**:
A canonical message that represents an event already accepted by the system according to its deduplication identity.
_Avoid_: Retry, repeated request

**Deduplication**:
The decision that classifies a canonical message as new or duplicate before it can be routed to delivery queues.
_Avoid_: Verification, idempotency check

**Bot Platform**:
The messaging platform in which a bot operates, such as Telegram or MaxBot. It forms part of bot and recipient identity. Bot platform is distinct from webhook type, although this context maps each supported bot platform to exactly one webhook type and vice versa.
_Avoid_: Recipient type, webhook type, subscription type, provider

**Bot Identifier**:
An external identifier assigned or defined by the bot platform and stored as an opaque string. It identifies a bot only within one bot platform and is distinct from the webhook identifier.
_Avoid_: Webhook identifier, recipient identifier, bot credential

**Bot Identity**:
The combination of bot platform and bot identifier that uniquely identifies a bot for routing purposes.
_Avoid_: Webhook identity, subscription identity

**Chat Identifier**:
An opaque identifier for a chat within the context of one bot identity. A chat may represent a direct conversation, group, channel, or another platform-specific conversation destination. The bot-platform-specific converter produces its canonical string form. Bot-level and relay-level messages use distinct shared documented reserved values so they still have recipients and independent ordered delivery queues.
_Avoid_: User identifier, globally unique chat ID, internal recipient ID

**Recipient Scope**:
How hookrelay determined the destination represented by a recipient. A `chat` recipient uses a platform-provided chat identifier. A `bot` recipient represents a known event that inherently applies to the bot and uses the shared reserved bot-level chat identifier. A `relay` recipient represents a verified valid JSON payload whose structure cannot be mapped to a known chat-level or bot-level event and uses the shared reserved relay-level chat identifier. The canonical JSON field is `recipient.scope`.
_Avoid_: Bot platform, event type, queue type, conversion status

**Recipient**:
The logical destination for canonical messages and the unit of independent ordering. A recipient is uniquely identified by the combination of bot platform, bot identifier, and chat identifier.
_Avoid_: User, subscription, queue consumer

**Recipient Identity**:
The combination of one bot identity and one chat identifier that uniquely identifies a recipient: bot platform, bot identifier, and chat identifier. Recipient scope explains whether the chat identifier came from the platform or is the reserved bot-level value, but it does not add another identity component.
_Avoid_: Recipient type, subscription identity, user identifier

**Delivery Queue**:
The ordered logical sequence of canonical messages routed for one recipient. Different recipient queues may progress concurrently. The term does not prescribe a queue technology.
_Avoid_: Webhook route, storage-specific queue

**Queue Routing**:
The deterministic placement of a new canonical message into the delivery queue identified by the message's single recipient identity after deduplication.
_Avoid_: Recipient selection, fan-out, type resolution, conversion

### Queue consumption

**Queue Consumer**:
One of the equivalent clients that competes for work from a shared pool of recipient delivery queues through the consumer API. A queue consumer is not permanently assigned to a recipient.
_Avoid_: Recipient, delivery queue, worker

**Consumer API**:
The interface through which a queue consumer waits for the next available message among its authorized delivery queues and reports the outcome of processing. It does not expose the queue storage technology.
_Avoid_: Valkey API, delivery worker

**Message Lease**:
A time-bounded exclusive claim for the head message of one recipient's delivery queue. Each delivery attempt has a distinct opaque cryptographically random token bound to the consumer credential that claimed it and used to acknowledge, negatively acknowledge, or extend the lease. The lease may be extended only up to its maximum lifetime. After expiry its token is stale, and the message follows the retry policy.
_Avoid_: Lock, acknowledgement, message identifier, consumer instance identity

**Acknowledgement**:
A queue consumer's report that the leased message was processed successfully and may be removed from the delivery queue. Repeating an acknowledgement with the same delivery token returns the previously recorded successful result.
_Avoid_: HTTP response, lease

**Negative Acknowledgement**:
A queue consumer's report that the leased message was not processed successfully and should follow the retry policy. It ends the delivery attempt, increments its attempt count, and either schedules retry for the same queue head or moves the message to dead-letter. Repeating it with the same delivery token returns the previously recorded result.
_Avoid_: Verification failure, duplicate message, cost-free lease release

**Delivery Attempt**:
One message lease and its resulting acknowledgement, negative acknowledgement, or expiry. Each attempt has a new delivery token; extending a live lease does not create another attempt.
_Avoid_: Webhook retry, HTTP request, delivery cycle

**Delivery Token**:
The opaque secret identifying one delivery attempt. It is bound to the consumer credential that claimed the message, is transmitted only in request bodies, and cannot be derived from the message identifier. After acknowledgement, negative acknowledgement, or expiry, its recorded outcome supports idempotent retries for a bounded period.
_Avoid_: Message identifier, consumer credential, URL identifier

**Lease Extension**:
An idempotent request to extend a live message lease by the server-defined interval, identified by a unique operation identifier. It retains the delivery token and cannot extend a lease beyond the configured maximum lifetime.
_Avoid_: New delivery attempt, client-selected deadline

**Retry Policy**:
The bounded rules that determine when a message receives another delivery attempt after a negative acknowledgement or expired lease.
_Avoid_: Deduplication, webhook retry

**Delivery Cycle**:
One bounded sequence of delivery attempts for a canonical message. The first cycle begins when the message is accepted. An operator replay from dead-letter starts a new cycle and resets its attempt number without erasing earlier attempt history.
_Avoid_: Delivery attempt, webhook retry, new message

**Dead-letter Message**:
A canonical message removed from normal delivery after exhausting its retry policy while retaining enough failure information for inspection and recovery. Moving a message to dead-letter unblocks its recipient's delivery queue and records an explicit break in the original processing sequence.
_Avoid_: Duplicate message, rejected webhook

**Dead-letter Replay**:
An operator-requested return of a dead-letter message to the head of its recipient's delivery queue. Replay preserves the canonical message and message identifier, starts a new delivery cycle, creates new delivery attempts and tokens, and does not pass through ingestion deduplication. If the original deduplication identity now points to another message, replay requires explicit conflict resolution.
_Avoid_: Webhook retry, new canonical message, duplicate acceptance

# Provide ordered at-least-once delivery per recipient

A Recipient, identified by Bot Platform, Bot Identifier, and Chat Identifier, owns one ordered Delivery Queue. At most one message for a Recipient may have an active lease; later messages remain blocked through retry backoff until the head is acknowledged or moved to dead-letter. Delivery is at least once, so consumers must be idempotent. Moving a message to dead-letter explicitly breaks the original processing sequence and unblocks later messages.

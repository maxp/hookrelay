# 02: Remaining ready-signal sources

**What to build:** Every other transition that may expose claimable work signals the notifier, so waiting claims wake after acks, dead-lettering, retry activation, replay, and block clear.

**Blocked by:** 01.

**Status:** ready-for-agent

- [ ] `ack` on `acknowledged`; `dead_letter` on `dead_lettered` from nack and lease expiry (background and inline)
- [ ] `retry_activation` on `activated` (background and inline); `retry_scheduled` does not signal
- [ ] `replay` on `replayed` with `queue_position=head`; `block_clear` on `cleared` with restored index `ready`
- [ ] Tests: real-Valkey integration with a long recheck interval for ack-exposes-next-head, retry activation, and replay; unit tests for the no-signal cases

## Comments

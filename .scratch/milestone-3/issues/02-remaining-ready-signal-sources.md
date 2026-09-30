# 02: Remaining ready-signal sources

**What to build:** Every other transition that may expose claimable work signals the notifier, so waiting claims wake after acks, dead-lettering, retry activation, replay, and block clear.

**Blocked by:** 01.

**Status:** done

- [x] `ack` on `acknowledged`; `dead_letter` on `dead_lettered` from nack and lease expiry (background and inline)
- [x] `retry_activation` on `activated` (background and inline); `retry_scheduled` does not signal
- [x] `replay` on `replayed` with `queue_position=head`; `block_clear` on `cleared` with restored index `ready`
- [x] Tests: real-Valkey integration with a long recheck interval for ack-exposes-next-head, retry activation, and replay; unit tests for the no-signal cases

## Comments

- 2026-09-30: Implemented alongside 01 (the integration harness needed maintenance wiring). Signals: Consumer API ack (`acknowledged` only) and dead-lettering nack; maintenance retry activation and dead-lettering expiry (background, inline, and reconciliation's `ProcessDue`, where nobody waits yet, so the signal is dropped); administration replay with `queue_position=head` and block clear restoring `ready`, through a local `administration.ReadySignal` interface. Tests: unit tests per source including every no-signal case; real-Valkey test covers accept, ack-exposes-next-head, and background retry activation. Replay and clear are covered at the service seam with a recording signal.

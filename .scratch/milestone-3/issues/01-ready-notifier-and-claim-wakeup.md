# 01: Ready-work notifier and claim wake-up

**What to build:** The in-process `ReadyNotifier` and its use by waiting claims, proven end to end with the acceptance source: a claim waiting on an empty index returns promptly after a webhook is accepted, long before its periodic recheck.

**Blocked by:** none (Milestone 2 complete).

**Status:** done

- [x] `delivery.ReadyNotifier`: FIFO waiters, `Signal(source)` wakes the oldest waiter or drops, deregistration on deadline/cancel/shutdown without leaking a wake into a later request
- [x] Claim wait loop selects on notification, periodic timer, cancellation, shutdown, and deadline; an empty wake re-registers without resetting the timer
- [x] `accept` source wired from ingestion on `accepted`
- [x] `HOOKRELAY_CLAIM_NOTIFICATIONS` (default `true`) in config, `configuration.md`, `.env.example`
- [x] Metrics `hookrelay_ready_signals_total{source,result}`, `hookrelay_claim_wakeups_total{trigger,outcome}`, `hookrelay_claim_wait_duration_seconds{outcome}`
- [x] Tests: notifier unit (under `-race`), handler with fakes and a long recheck interval, real-Valkey integration (accept wakes a waiting claim)

## Comments

- 2026-09-30: Implemented. `delivery.ReadyNotifier` (FIFO `container/list`, 1-buffered per-registration channel; `deregister` hands an unconsumed wake to the next waiter so a claim leaving at its deadline never loses a signal; nil/disabled is a no-op). The claim loop registers before every waiting check (so a signal during the check is not missed), selects on the wake alongside the periodic timer, and re-registers after an empty wake. `HOOKRELAY_CLAIM_NOTIFICATIONS` (default `true`). Metrics `hookrelay_ready_signals_total`, `hookrelay_claim_wakeups_total` (rechecks after the first check; the inline-pass recheck and the final deadline check are not counted), `hookrelay_claim_wait_duration_seconds`. `/debug/config` does not exist yet, so the flag is documented in `configuration.md` and `.env.example` only. Tests: notifier unit under `-race`, handler with fakes and a one-hour recheck interval, real-Valkey wake-up test with a 20 s recheck interval and no inline pass.

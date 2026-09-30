# 01: Ready-work notifier and claim wake-up

**What to build:** The in-process `ReadyNotifier` and its use by waiting claims, proven end to end with the acceptance source: a claim waiting on an empty index returns promptly after a webhook is accepted, long before its periodic recheck.

**Blocked by:** none (Milestone 2 complete).

**Status:** ready-for-agent

- [ ] `delivery.ReadyNotifier`: FIFO waiters, `Signal(source)` wakes the oldest waiter or drops, deregistration on deadline/cancel/shutdown without leaking a wake into a later request
- [ ] Claim wait loop selects on notification, periodic timer, cancellation, shutdown, and deadline; an empty wake re-registers without resetting the timer
- [ ] `accept` source wired from ingestion on `accepted`
- [ ] `HOOKRELAY_CLAIM_NOTIFICATIONS` (default `true`) in config, validation, `/debug/config`, `configuration.md`, `.env.example`
- [ ] Metrics `hookrelay_ready_signals_total{source,result}`, `hookrelay_claim_wakeups_total{trigger,outcome}`, `hookrelay_claim_wait_duration_seconds{outcome}`
- [ ] Tests: notifier unit (under `-race`), handler with fakes and a long recheck interval, real-Valkey integration (accept wakes a waiting claim)

## Comments

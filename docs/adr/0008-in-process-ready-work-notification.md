# Wake waiting claims with an in-process ready-work notifier

Milestone 3 wakes waiting claims early through a process-local notifier: every transition in this process that may expose claimable work (acceptance, acknowledgement, dead-lettering, retry activation, head replay, block clear) signals it, and each signal wakes at most one waiting claim, oldest first, which then performs the normal atomic recheck. The first version runs exactly one hookrelay process, and every ready-making transition executes in it, so a local signal sees all new work without a new Valkey connection, subscriber reconnect handling, or new script versions.

Signals are hints. Valkey's ready index remains the source of truth, a woken claim that finds nothing simply waits again, and the unchanged 250 ms periodic recheck recovers any lost or dropped signal. Waking one waiter per signal avoids herd rechecks; waking none when nobody waits is safe because every new claim checks first.

Multi-process deployment would need a cross-process channel, most likely Valkey Pub/Sub published from the ready-making scripts, behind the same signal/wait seam. That is deferred with the multi-process decision itself. `HOOKRELAY_CLAIM_NOTIFICATIONS=false` disables the notifier as a rollback path.

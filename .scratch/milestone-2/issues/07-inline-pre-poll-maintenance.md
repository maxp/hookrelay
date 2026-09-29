# 07: Inline pre-poll maintenance

**What to build:** Before a claim waits on an empty ready index, it processes at most 10 due lease expiries and retry activations and rechecks once — the bounded self-healing path.

**Blocked by:** 04.

**Status:** done

- [x] Inline pass bounded to 10 entries, then one recheck, then the normal waiting loop
- [x] Metrics attribute inline work to `maintenance_processed_total{kind=inline_*}`
- [x] Tests: a due retry is claimed by the very claim that found the index empty

## Comments

- 2026-09-29: Implemented. `delivery.Maintenance.InlinePass` (at most 10 entries across both kinds, leases first, inline_* kinds, same transitions/events/attempt metrics); the claim handler runs it once when a waiting claim's first check is empty and rechecks immediately without recording empty, then waits normally; `wait_ms=0` claims skip it (user decision). Tests: InlinePass bound with fakes, handler recheck ordering and once-only, real-Valkey claim that activates a due retry itself with no background maintenance.
- Review follow-up: the pass is detached from the request and bounded (2 s budget) so a started transition completes; after it the claim returns without rechecking when the client left (cancelled) or shutdown began (503); a failed read is logged and leaves the other kind its budget; failures are logged under the inline kinds; the design docs list the inline kinds.

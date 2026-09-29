# 07: Inline pre-poll maintenance

**What to build:** Before a claim waits on an empty ready index, it processes at most 10 due lease expiries and retry activations and rechecks once — the bounded self-healing path.

**Blocked by:** 04.

**Status:** ready-for-agent

- [ ] Inline pass bounded to 10 entries, then one recheck, then the normal waiting loop
- [ ] Metrics attribute inline work to `maintenance_processed_total{kind=inline_*}`
- [ ] Tests: a due retry is claimed by the very claim that found the index empty

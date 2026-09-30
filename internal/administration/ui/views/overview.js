// Overview: readiness, Valkey memory, queues, the DLQ, and counts from the
// operations summary, refreshed by the shell's poller.

import { get } from "../api.js";
import { age, bytes, card, el, notice, replace, time } from "../dom.js";

export const title = "Overview";
export const pollMs = 5000;

export async function render(main, ctx) {
  const { body: s } = await get("/admin/v1/operations/summary");
  ctx.setGrafana(s.links && s.links.grafana_url);
  const r = s.readiness;
  const memory = s.valkey.maxmemory_bytes
    ? `${bytes(s.valkey.used_memory_bytes)} of ${bytes(s.valkey.maxmemory_bytes)}`
    : `${bytes(s.valkey.used_memory_bytes)} (no limit)`;
  const q = s.queues;
  const dl = s.dead_letters;
  replace(
    main,
    el("h1", {}, title),
    r
      ? r.ready
        ? notice(r.accepting_webhooks ? "Ready and accepting webhooks." : "Ready; webhook acceptance is stopped.", r.accepting_webhooks ? "" : "warn")
        : notice(`Not ready: startup reconciliation is ${r.startup_reconciliation}.`, "bad")
      : null,
    el("h2", {}, "Queues"),
    el(
      "div",
      { class: "cards" },
      card("Queued messages", q.queued_messages),
      card("Ready Recipients", q.ready_recipients),
      card("Leased Recipients", q.leased_recipients),
      card("Waiting for retry", q.retry_wait_recipients),
      card("Blocked Recipients", q.blocked_recipients, q.blocked_recipients > 0 ? "bad" : ""),
      card("Earliest lease expiry", q.earliest_lease_expires_ms ? age(q.earliest_lease_expires_ms) : "—"),
      card("Earliest retry", q.earliest_retry_at_ms ? age(q.earliest_retry_at_ms) : "—"),
    ),
    el("h2", {}, "Dead letters"),
    el(
      "div",
      { class: "cards" },
      card("Dead-letter messages", dl.count, dl.count > 0 ? "warn" : ""),
      card("Oldest", dl.oldest_dead_lettered_ms ? time(dl.oldest_dead_lettered_ms) : "—"),
      card("Newest", dl.newest_dead_lettered_ms ? time(dl.newest_dead_lettered_ms) : "—"),
    ),
    el("h2", {}, "Platform"),
    el(
      "div",
      { class: "cards" },
      card("Valkey memory", memory),
      card("Webhook Endpoints", s.webhook_endpoints.count),
      card("Admin sessions", s.admin_sessions.indexed),
      card("Audit events", s.audit.length),
    ),
    el("p", { class: "muted" }, `Updated ${time(s.generated_ms)}.`),
  );
}

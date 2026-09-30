// Recipients by delivery state over the derived indexes. Blocked
// Recipients are shown for inspection only: block recovery follows the
// runbook through the CLI (docs/runbooks/recipient-block-recovery.md).

import { get } from "../api.js";
import { age, el, notice, recipientText, replace, time } from "../dom.js";
import { Pager } from "../pager.js";

export const title = "Recipients";
export const pollMs = 10000;

const statuses = [
  ["ready", "Ready"],
  ["leased", "Leased"],
  ["retry_wait", "Waiting for retry"],
  ["blocked", "Blocked"],
];
const pager = new Pager();
let shownStatus = "";

export async function render(main, ctx, status = "ready") {
  if (!statuses.some(([s]) => s === status)) status = "ready";
  if (status !== shownStatus) {
    pager.reset();
    shownStatus = status;
  }
  const { body } = await get(`/admin/v1/recipient-states${pager.query({ status, limit: 50 })}`);
  const rows = body.items.map((it) =>
    el(
      "tr",
      {},
      el("td", {}, recipientText(it.recipient)),
      el("td", {}, detail(it)),
      el("td", { class: "muted" }, it.marker_missing ? "index member without marker (reconciliation finding)" : ""),
    ),
  );
  replace(
    main,
    el("h1", {}, title),
    el(
      "div",
      { class: "toolbar" },
      statuses.map(([s, label]) =>
        el("button", { type: "button", class: s === status ? "primary" : "", onclick: () => ctx.navigate(`#/recipients/${s}`) }, label),
      ),
    ),
    status === "blocked" && body.items.length
      ? notice("Blocked Recipients are recovered with the runbook and the CLI (recipients inspect-block / clear-block), not from this panel.", "warn")
      : null,
    rows.length
      ? el("table", {}, el("thead", {}, el("tr", {}, el("th", {}, "Recipient"), el("th", {}, "State"), el("th", {}, ""))), el("tbody", {}, rows))
      : el("p", { class: "muted" }, "No Recipients in this state."),
    pager.controls(body.next_cursor, () => render(main, ctx, status)),
  );
}

function detail(it) {
  if (it.lease_expires_ms) return `lease expires ${time(it.lease_expires_ms)} (${age(it.lease_expires_ms)})`;
  if (it.retry_at_ms) return `retry at ${time(it.retry_at_ms)} (${age(it.retry_at_ms)})`;
  if (it.detected_ms) return `blocked ${time(it.detected_ms)}: ${it.reason_code || "unknown reason"}`;
  if (it.ready_sequence !== undefined) return `ready (sequence ${it.ready_sequence})`;
  return it.status;
}

// Administrative audit, newest first.

import { get } from "../api.js";
import { el, replace, time } from "../dom.js";
import { Pager } from "../pager.js";

export const title = "Audit";

const pager = new Pager();

export async function render(main, ctx) {
  const { body } = await get(`/admin/v1/audit${pager.query({ limit: 50 })}`);
  const rows = body.items.map((e) =>
    el(
      "tr",
      {},
      el("td", {}, time(e.timestamp_ms)),
      el("td", {}, e.actor || ""),
      el("td", {}, e.operation || ""),
      el("td", { class: "mono" }, e.target || ""),
      el("td", { class: e.outcome === "failure" ? "bad" : "" }, e.outcome || ""),
      el("td", {}, e.reason || ""),
      el("td", { class: "mono" }, e.request_id || ""),
    ),
  );
  replace(
    main,
    el("h1", {}, title),
    rows.length
      ? el(
          "table",
          {},
          el("thead", {}, el("tr", {}, ["Time", "Actor", "Operation", "Target", "Outcome", "Reason", "Request"].map((h) => el("th", {}, h)))),
          el("tbody", {}, rows),
        )
      : el("p", { class: "muted" }, "No audit events are retained."),
    pager.controls(body.next_cursor, () => render(main, ctx)),
  );
}

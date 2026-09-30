// Dead letters: the global DLQ newest first, and one entry's detail with
// the accepted actions — audited payload view, replay, and confirmed
// permanent deletion. A mutation whose outcome is unknown is never
// retried: the view re-reads state and reports what it observed.

import { ApiError, get, request } from "../api.js";
import { el, notice, prettyJSON, recipientText, replace, time } from "../dom.js";
import { Pager } from "../pager.js";

export const title = "Dead letters";

const pager = new Pager();
const messageIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export async function render(main, ctx, messageId) {
  if (messageId) return renderDetail(main, ctx, messageId);
  const { body } = await get(`/admin/v1/dead-letters${pager.query({ limit: 50 })}`);
  const rows = body.items.map((d) =>
    el(
      "tr",
      {},
      el("td", { class: "mono" }, el("a", { href: `#/dead-letters/${encodeURIComponent(d.message_id)}` }, d.message_id)),
      el("td", {}, recipientText(d.recipient)),
      el("td", {}, time(d.dead_lettered_ms)),
      el("td", {}, d.dead_letter_reason),
      el("td", {}, d.delivery_cycle),
    ),
  );
  replace(
    main,
    el("h1", {}, title),
    rows.length
      ? el(
          "table",
          {},
          el("thead", {}, el("tr", {}, ["Message", "Recipient", "Dead-lettered", "Reason", "Cycle"].map((h) => el("th", {}, h)))),
          el("tbody", {}, rows),
        )
      : el("p", { class: "muted" }, "The dead-letter queue is empty."),
    pager.controls(body.next_cursor, () => render(main, ctx)),
  );
}

async function renderDetail(main, ctx, messageId) {
  const back = el("p", {}, el("a", { href: "#/dead-letters" }, "← Dead letters"));
  if (!messageIdPattern.test(messageId)) {
    replace(main, back, notice("Not a Message Identifier.", "bad"));
    return;
  }
  const path = `/admin/v1/dead-letters/${encodeURIComponent(messageId)}`;
  let d;
  let etag;
  try {
    const res = await get(path);
    d = res.body;
    etag = res.headers.get("ETag");
  } catch (err) {
    if (err instanceof ApiError && err.code === "dead_letter_not_found") {
      replace(main, back, el("h1", { class: "mono" }, messageId), notice("This message is not dead-lettered (it may have been replayed, deleted, or expired)."));
      return;
    }
    throw err;
  }
  const status = el("div", {});
  const payloadBox = el("div", {});
  const actions = el(
    "div",
    { class: "toolbar" },
    el("button", { type: "button", onclick: () => viewPayload(messageId, payloadBox, status) }, "Show payload"),
    el("button", { type: "button", class: "primary", onclick: () => replay(messageId, d, status, "reject") }, "Replay"),
    el("button", { type: "button", class: "danger", onclick: () => remove(messageId, d, etag, status, ctx) }, "Delete permanently"),
  );
  const history = (d.attempts || []).map((a) =>
    el(
      "tr",
      {},
      el("td", {}, a.delivery_cycle),
      el("td", {}, a.attempt),
      el("td", {}, a.outcome),
      el("td", {}, a.reason_code || ""),
      el("td", {}, time(a.claimed_ms)),
      el("td", {}, time(a.completed_ms)),
      el("td", { class: "mono" }, a.consumer_instance_id || ""),
    ),
  );
  const archived = d.archived_cycles_summary;
  replace(
    main,
    back,
    el("h1", { class: "mono" }, messageId),
    el(
      "dl",
      { class: "fields" },
      el("dt", {}, "Recipient"),
      el("dd", {}, recipientText(d.recipient)),
      el("dt", {}, "Dead-lettered"),
      el("dd", {}, time(d.dead_lettered_ms)),
      el("dt", {}, "Reason"),
      el("dd", {}, d.dead_letter_reason),
      el("dt", {}, "Delivery Cycle"),
      el("dd", {}, d.delivery_cycle),
    ),
    actions,
    status,
    el("h2", {}, "Attempt history"),
    archived
      ? el("p", { class: "muted" }, `${archived.archived_cycles} earlier cycles (${archived.archived_attempts} attempts) archived, ${time(archived.first_archived_ms)} – ${time(archived.last_archived_ms)}.`)
      : null,
    history.length
      ? el(
          "table",
          {},
          el("thead", {}, el("tr", {}, ["Cycle", "Attempt", "Outcome", "Reason", "Claimed", "Completed", "Consumer"].map((h) => el("th", {}, h)))),
          el("tbody", {}, history),
        )
      : el("p", { class: "muted" }, "No retained attempts."),
    payloadBox,
  );
}

async function viewPayload(messageId, box, status) {
  if (!window.confirm("Viewing the payload is recorded in the administrative audit. Continue?")) return;
  replace(status);
  try {
    const { text } = await request("POST", `/admin/v1/dead-letters/${encodeURIComponent(messageId)}/payload`);
    replace(
      box,
      el("h2", {}, "Canonical Message"),
      el("p", { class: "muted" }, "This access was recorded in the administrative audit. Shown exactly as stored."),
      el("pre", { class: "payload" }, prettyJSON(text)),
    );
  } catch (err) {
    replace(status, notice(`The payload was not disclosed: ${err.message}`, "bad"));
  }
}

async function replay(messageId, d, status, resolution) {
  const prompt =
    resolution === "keep_current"
      ? "Replay keeping the newer deduplication mapping unchanged?"
      : `Replay ${messageId} to ${recipientText(d.recipient)} as a new Delivery Cycle, ahead of its not-yet-started messages?`;
  if (!window.confirm(prompt)) return;
  replace(status, notice("Replaying…"));
  try {
    const { body } = await request("POST", `/admin/v1/dead-letters/${encodeURIComponent(messageId)}/replay`, {
      deduplication_conflict_resolution: resolution,
    });
    replace(
      status,
      notice(`Replayed in Delivery Cycle ${body.delivery_cycle}, queue position ${body.queue_position.replaceAll("_", " ")}.`),
      el("p", {}, el("a", { href: `#/message/${encodeURIComponent(messageId)}` }, "Follow its delivery state")),
    );
  } catch (err) {
    if (err instanceof ApiError && err.code === "deduplication_conflict") {
      replace(
        status,
        notice("The original Deduplication Identity now maps to a newer message. Replay only if the newer mapping should stay unchanged.", "warn"),
        el("button", { type: "button", onclick: () => replay(messageId, d, status, "keep_current") }, "Replay keeping the current mapping"),
      );
      return;
    }
    if (err instanceof ApiError && err.uncertain) {
      await reconcileReplay(messageId, d.delivery_cycle, status, err);
      return;
    }
    replace(status, notice(`Replay refused: ${err.message}`, "bad"));
  }
}

// reconcileReplay reads the delivery state once after an unknown outcome.
async function reconcileReplay(messageId, previousCycle, status, cause) {
  let observed;
  try {
    const { body } = await get(`/admin/v1/messages/${encodeURIComponent(messageId)}/delivery-state`);
    observed = body;
  } catch {
    observed = null;
  }
  if (observed && observed.delivery_cycle > previousCycle) {
    replace(
      status,
      notice(
        `Desired state observed (unconfirmed): the message is ${observed.state} in Delivery Cycle ${observed.delivery_cycle}, so a replay ran, ` +
          "but this does not confirm that this request performed it or that its audit event was written. Check the audit before any further action.",
        "warn",
      ),
    );
    return;
  }
  replace(
    status,
    notice(`Outcome uncertain (${cause.message}). Not retrying: check the delivery state and the audit, and follow the reconciliation runbook.`, "bad"),
  );
}

async function remove(messageId, d, etag, status, ctx) {
  const prompt = `Permanently delete ${messageId} (${recipientText(d.recipient)}, dead-lettered ${time(d.dead_lettered_ms)})? Its Canonical Message and attempt history are removed.`;
  if (!window.confirm(prompt)) return;
  replace(status, notice("Deleting…"));
  try {
    await request("DELETE", `/admin/v1/dead-letters/${encodeURIComponent(messageId)}`, undefined, { "If-Match": etag });
    replace(status, notice("Deleted."));
    setTimeout(() => ctx.navigate("#/dead-letters"), 1200);
  } catch (err) {
    if (err instanceof ApiError && err.code === "precondition_failed") {
      replace(status, notice("The dead letter changed since it was loaded (replayed or dead-lettered again). Reload before deciding.", "warn"));
      return;
    }
    if (err instanceof ApiError && err.uncertain) {
      let absent = false;
      try {
        await get(`/admin/v1/dead-letters/${encodeURIComponent(messageId)}`);
      } catch (readErr) {
        absent = readErr instanceof ApiError && readErr.code === "dead_letter_not_found";
      }
      replace(
        status,
        absent
          ? notice("Absence observed (unconfirmed): the message is no longer dead-lettered, but this does not confirm that this request deleted it or that its audit event was written. Check the audit.", "warn")
          : notice(`Outcome uncertain (${err.message}). Not retrying: check the dead letter and the audit.`, "bad"),
      );
      return;
    }
    replace(status, notice(`Deletion refused: ${err.message}`, "bad"));
  }
}

// Delivery-state lookup by Message Identifier. It also reaches the
// retained compact success metadata: "acknowledged" is reported while it
// is retained.

import { ApiError, get } from "../api.js";
import { el, notice, replace } from "../dom.js";

export const title = "Message lookup";

const messageIdPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export async function render(main, ctx, messageId = "") {
  const input = el("input", { type: "text", size: 40, placeholder: "Message Identifier", value: messageId, class: "mono", "aria-label": "Message Identifier" });
  const result = el("div", {});
  const form = el(
    "form",
    {
      class: "toolbar",
      onsubmit: (event) => {
        event.preventDefault();
        ctx.navigate(`#/message/${encodeURIComponent(input.value.trim().toLowerCase())}`);
      },
    },
    input,
    el("button", { type: "submit", class: "primary" }, "Look up"),
  );
  replace(main, el("h1", {}, title), form, result);
  if (!messageId) return;
  if (!messageIdPattern.test(messageId)) {
    replace(result, notice("Not a Message Identifier.", "bad"));
    return;
  }
  try {
    const { body } = await get(`/admin/v1/messages/${encodeURIComponent(messageId)}/delivery-state`);
    replace(
      result,
      el(
        "dl",
        { class: "fields" },
        el("dt", {}, "State"),
        el("dd", {}, body.state.replaceAll("_", " ")),
        el("dt", {}, "Delivery Cycle"),
        el("dd", {}, body.delivery_cycle),
        body.queue_position ? [el("dt", {}, "Queue position"), el("dd", {}, body.queue_position.replaceAll("_", " "))] : null,
      ),
      body.state === "dead_lettered" ? el("p", {}, el("a", { href: `#/dead-letters/${encodeURIComponent(messageId)}` }, "Open the dead letter")) : null,
    );
  } catch (err) {
    if (err instanceof ApiError && err.code === "message_not_found") {
      replace(result, notice("No delivery state is retained for this message (unknown, or acknowledged longer ago than the success retention)."));
      return;
    }
    if (err instanceof ApiError && err.code === "recipient_state_ambiguous") {
      replace(result, notice(`The stored state cannot be classified: ${err.message}`, "bad"));
      return;
    }
    throw err;
  }
}

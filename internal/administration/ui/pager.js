// Cursor pager for the Admin API list routes: the API hands out opaque
// next_cursor values, so going back keeps the cursors already seen.

import { el } from "./dom.js";

export class Pager {
  constructor() {
    this.reset();
  }

  reset() {
    this.cursors = [""];
    this.index = 0;
    this.next = "";
  }

  get cursor() {
    return this.cursors[this.index];
  }

  // query builds the list query for the current page.
  query(params = {}) {
    const q = new URLSearchParams(params);
    if (this.cursor) q.set("cursor", this.cursor);
    const s = q.toString();
    return s ? `?${s}` : "";
  }

  // controls renders previous/next buttons; rerender is called after a move.
  controls(nextCursor, rerender) {
    this.next = nextCursor || "";
    return el(
      "div",
      { class: "toolbar" },
      el("button", {
        type: "button",
        disabled: this.index === 0,
        onclick: () => {
          this.index--;
          rerender();
        },
      }, "Previous"),
      el("span", { class: "muted" }, `Page ${this.index + 1}`),
      el("button", {
        type: "button",
        disabled: !this.next,
        onclick: () => {
          this.cursors = this.cursors.slice(0, this.index + 1);
          this.cursors.push(this.next);
          this.index++;
          rerender();
        },
      }, "Next"),
    );
  }
}

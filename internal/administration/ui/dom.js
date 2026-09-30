// DOM helpers. Every piece of data is inserted as text: webhook payloads
// and identifiers are attacker-influenced, so markup is never built from
// strings (ADR 0009).

// el creates an element with attributes and children. Strings and numbers
// become text nodes; null and undefined children are skipped. Event
// handlers are passed as "on<event>" functions, never as markup.
export function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2), value);
    } else if (key === "class") {
      node.className = value;
    } else if (value === true) {
      node.setAttribute(key, "");
    } else {
      node.setAttribute(key, String(value));
    }
  }
  append(node, children);
  return node;
}

function append(node, children) {
  for (const child of children) {
    if (child === undefined || child === null || child === false) continue;
    if (Array.isArray(child)) {
      append(node, child);
    } else if (child instanceof Node) {
      node.appendChild(child);
    } else {
      node.appendChild(document.createTextNode(String(child)));
    }
  }
}

// replace swaps a container's children.
export function replace(container, ...children) {
  container.replaceChildren();
  append(container, children);
}

// time formats integer milliseconds as a local timestamp.
export function time(ms) {
  if (!ms) return "—";
  return new Date(ms).toLocaleString(undefined, { hour12: false });
}

// age formats the distance from now as a short relative duration.
export function age(ms) {
  if (!ms) return "";
  const seconds = Math.round((ms - Date.now()) / 1000);
  const abs = Math.abs(seconds);
  const text = abs < 90 ? `${abs}s` : abs < 5400 ? `${Math.round(abs / 60)}m` : abs < 129600 ? `${Math.round(abs / 3600)}h` : `${Math.round(abs / 86400)}d`;
  return seconds >= 0 ? `in ${text}` : `${text} ago`;
}

// bytes formats a byte count.
export function bytes(n) {
  if (!n) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i++;
  }
  return `${n.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

// recipientText renders a structured Recipient as one line.
export function recipientText(r) {
  if (!r) return "—";
  const parts = [`${r.bot_platform} bot ${r.bot_id}`];
  if (r.chat_id) parts.push(`chat ${r.chat_id}`);
  else if (r.user_id) parts.push(`user ${r.user_id}`);
  else parts.push(`${r.scope} scope`);
  return parts.join(" · ");
}

// prettyJSON indents JSON text without parsing it, so numbers beyond the
// JavaScript safe-integer range are shown exactly as stored.
export function prettyJSON(raw) {
  let out = "";
  let depth = 0;
  let inString = false;
  const newline = () => "\n" + "  ".repeat(depth);
  for (let i = 0; i < raw.length; i++) {
    const c = raw[i];
    if (inString) {
      out += c;
      if (c === "\\") out += raw[++i] ?? "";
      else if (c === '"') inString = false;
      continue;
    }
    if (c === '"') {
      inString = true;
      out += c;
    } else if (c === "{" || c === "[") {
      const close = c === "{" ? "}" : "]";
      if (raw[i + 1] === close) {
        out += c + close;
        i++;
      } else {
        depth++;
        out += c + newline();
      }
    } else if (c === "}" || c === "]") {
      depth--;
      out += newline() + c;
    } else if (c === ",") {
      out += c + newline();
    } else if (c === ":") {
      out += ": ";
    } else if (!/\s/.test(c)) {
      out += c;
    }
  }
  return out;
}

// notice renders a status line; kind is "", "warn", or "bad".
export function notice(text, kind = "") {
  return el("div", { class: `notice ${kind}`.trim(), role: kind === "bad" ? "alert" : "status" }, text);
}

// card renders one labelled figure.
export function card(label, value, kind = "") {
  return el("div", { class: "card" }, el("div", { class: "label" }, label), el("div", { class: `value ${kind}`.trim() }, value));
}

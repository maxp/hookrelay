// Operational panel shell: session bootstrap, login and logout, hash
// routing, and polling. Views live in views/*.js and render into <main>.

import { ApiError, get, request, setCSRFToken, setUnauthenticatedHandler } from "./api.js";
import { el, notice, replace, time } from "./dom.js";
import * as audit from "./views/audit.js";
import * as deadLetters from "./views/deadletters.js";
import * as message from "./views/message.js";
import * as overview from "./views/overview.js";
import * as recipients from "./views/recipients.js";

// routes maps the first hash segment to its view module. A view exports
// title, render(main, ctx, ...args), and optionally pollMs.
const routes = {
  overview,
  recipients,
  "dead-letters": deadLetters,
  message,
  audit,
};
const navOrder = ["overview", "recipients", "dead-letters", "message", "audit"];

const main = document.getElementById("main");
const top = document.getElementById("top");
const nav = document.getElementById("nav");
const grafana = document.getElementById("grafana");
const expiry = document.getElementById("expiry");

let pollTimer = 0;
let renderSeq = 0;
let signedIn = false;

const ctx = {
  setGrafana(url) {
    if (url && /^https?:\/\//.test(url)) {
      grafana.href = url;
      grafana.hidden = false;
    } else {
      grafana.hidden = true;
    }
  },
  navigate(hash) {
    location.hash = hash;
  },
};

function stopPolling() {
  clearTimeout(pollTimer);
  pollTimer = 0;
}

function currentRoute() {
  const parts = location.hash.replace(/^#\/?/, "").split("/").filter(Boolean).map(decodeURIComponent);
  const name = routes[parts[0]] ? parts[0] : "overview";
  return { name, args: routes[parts[0]] ? parts.slice(1) : [] };
}

function renderNav(active) {
  replace(
    nav,
    navOrder.map((name) => el("a", { href: `#/${name}`, class: name === active ? "current" : "" }, routes[name].title)),
  );
}

// show renders the current route and schedules its next poll. A newer
// navigation supersedes an older render still in flight.
async function show() {
  stopPolling();
  if (!signedIn) return;
  const seq = ++renderSeq;
  const { name, args } = currentRoute();
  const view = routes[name];
  renderNav(name);
  document.title = `${view.title} · hookrelay`;
  try {
    await view.render(main, ctx, ...args);
  } catch (err) {
    if (seq !== renderSeq || !signedIn) return;
    if (err instanceof ApiError && err.status === 401) return;
    replace(main, el("h1", {}, view.title), notice(`Could not load: ${err.message}`, "bad"));
  }
  if (seq === renderSeq && view.pollMs && signedIn) {
    pollTimer = setTimeout(() => {
      if (!document.hidden) show();
    }, view.pollMs);
  }
}

async function bootstrap() {
  let session;
  try {
    ({ body: session } = await get("/admin/v1/session"));
  } catch (err) {
    if (err instanceof ApiError && err.status === 401) {
      showLogin("");
      return;
    }
    showLogin(`The session could not be checked: ${err.message}`);
    return;
  }
  setCSRFToken(session.csrf_token);
  signedIn = true;
  top.hidden = false;
  expiry.textContent = `Session until ${time(Math.min(session.idle_expires_ms, session.absolute_expires_ms))}`;
  show();
}

function showLogin(message) {
  signedIn = false;
  stopPolling();
  setCSRFToken("");
  top.hidden = true;
  const secret = el("input", { type: "password", id: "admin-secret", name: "admin_secret", autocomplete: "current-password", required: true });
  const status = el("div", {});
  const submit = el("button", { type: "submit", class: "primary" }, "Log in");
  const form = el(
    "form",
    {
      class: "login",
      onsubmit: async (event) => {
        event.preventDefault();
        submit.disabled = true;
        const value = secret.value;
        secret.value = "";
        try {
          await request("POST", "/admin/v1/session", { admin_secret: value });
          await bootstrap();
        } catch (err) {
          replace(status, notice(loginMessage(err), "bad"));
        } finally {
          submit.disabled = false;
        }
      },
    },
    el("h1", {}, "hookrelay operations"),
    el("label", { for: "admin-secret" }, "Admin Secret"),
    secret,
    submit,
    status,
  );
  replace(main, form);
  if (message) replace(status, notice(message, "warn"));
  secret.focus();
}

function loginMessage(err) {
  if (!(err instanceof ApiError)) return err.message;
  switch (err.code) {
    case "unauthenticated":
      return "The Admin Secret is not correct.";
    case "rate_limit_exceeded":
      return "Too many login attempts; wait and try again.";
    case "session_capacity_exceeded":
      return "Too many active sessions; log out elsewhere or wait for one to expire.";
    case "forbidden":
      return "This page's origin is not the configured administrative origin (HOOKRELAY_ADMIN_ORIGIN).";
    default:
      return `Login failed: ${err.message}`;
  }
}

document.getElementById("logout").addEventListener("click", async () => {
  try {
    await request("DELETE", "/admin/v1/session");
    showLogin("Logged out.");
  } catch (err) {
    replace(main, notice(`Logout could not be confirmed: ${err.message}. The session may still be valid.`, "bad"));
  }
});

setUnauthenticatedHandler(() => {
  if (signedIn) showLogin("The session ended; log in again.");
});
window.addEventListener("hashchange", show);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden && signedIn && pollTimer) show();
});

bootstrap();

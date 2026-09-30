// JSON Admin API client for the browser session. The session cookie is
// HttpOnly and sent by the browser; state-changing requests carry the
// session's CSRF token, and the browser adds the Origin header.

let csrfToken = "";
let onUnauthenticated = () => {};

export function setCSRFToken(token) {
  csrfToken = token || "";
}

// setUnauthenticatedHandler registers what happens on a 401: the session
// ended, so the panel returns to the login view.
export function setUnauthenticatedHandler(fn) {
  onUnauthenticated = fn;
}

// ApiError carries the HTTP status and the bounded error code. status 0
// is a transport failure: the request may or may not have reached the
// server, so a mutation's outcome is unknown.
export class ApiError extends Error {
  constructor(status, code, message, requestId) {
    super(message || code || `HTTP ${status}`);
    this.status = status;
    this.code = code || "";
    this.requestId = requestId || "";
  }

  // uncertain reports whether a mutation may have happened.
  get uncertain() {
    return this.status === 0 || this.status >= 500;
  }
}

// request sends one API request and returns { status, body, headers }.
// Non-2xx responses throw ApiError; a 401 also triggers the login view.
export async function request(method, path, body, headers = {}) {
  const init = { method, credentials: "same-origin", cache: "no-store", headers: { Accept: "application/json", ...headers } };
  if (body !== undefined) {
    init.headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  if (method !== "GET" && method !== "HEAD" && csrfToken) {
    init.headers["X-CSRF-Token"] = csrfToken;
  }
  let response;
  try {
    response = await fetch(path, init);
  } catch (err) {
    throw new ApiError(0, "network_error", "The request did not complete; its outcome is unknown.");
  }
  let data = null;
  const text = await response.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }
  }
  if (!response.ok) {
    const e = (data && data.error) || {};
    if (response.status === 401 && path !== "/admin/v1/session") {
      onUnauthenticated();
    }
    throw new ApiError(response.status, e.code, e.message, e.request_id);
  }
  return { status: response.status, body: data, headers: response.headers };
}

export const get = (path) => request("GET", path);

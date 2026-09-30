# 10: Operational UI views and actions

**What to build:** Recipient-state, dead-letter, delivery-state, and audit views with payload inspection, replay, and delete.

**Blocked by:** 03, 09.

**Status:** done

- [x] Recipients by status with paging; delivery-state lookup by message ID; audit list with paging
- [x] Dead letters: list, detail with attempt history; payload after an audited-access confirmation, rendered via `textContent`
- [x] Replay with confirmation; `keep_current` offered only after `409 deduplication_conflict`; delete with confirmation and the detail `ETag`
- [x] Lost responses re-read state and report observed/uncertain, never retrying
- [x] Manual browser walkthrough recorded in this ticket

## Comments

- 2026-09-30: Implemented. Views `recipients.js` (status tabs, paging, 10 s poll; blocked Recipients point to the runbook/CLI), `deadletters.js` (list with paging; detail with attempt history and archived-cycle summary; payload after a native confirm; replay with confirm and `keep_current` offered only after `409 deduplication_conflict`; delete with a confirm naming message, Recipient, and time and sending the detail `ETag` as `If-Match`; `412` asks to reload; a transport error or `5xx` re-reads once and reports "desired state observed (unconfirmed)" or "outcome uncertain", never retrying), `message.js` (delivery-state lookup, link to the dead letter), `audit.js` (paging), shared `pager.js`. Payloads are pretty-printed from the raw response text by `prettyJSON` without `JSON.parse`, so integers beyond 2^53 are shown exactly as stored (found during the walkthrough).
- 2026-09-30: Browser walkthrough (headless Chromium via a throwaway `playwright-core` in /tmp, not a repo dependency; local `hookrelay serve` on loopback with an insecure cookie, Valkey test instance DB 3). Seeded an endpoint, a queued message, and two dead letters (one with the text `<img src=x onerror=alert(1)> hostile payload`). Observed: `/` → `/ui/`; wrong secret refused with the field cleared; login cookie `hookrelay_admin` HttpOnly, SameSite=Strict; overview cards and Grafana link; recipients by status; DLQ list; the hostile payload rendered as text with zero `<img>` elements; replay → "Replayed in Delivery Cycle 2, queue position head"; delete → "Deleted." and back to the list; lookup of the replayed message → queued, cycle 2, head; lookup of the deleted one → not retained; audit newest first with `dead_letter_deleted`, `dead_letter_replayed`, `dead_letter_payload_viewed`, `admin_login` success and failure, all `actor=admin_session`; logout → login view and `GET /admin/v1/session` = 401. Console showed only the expected 401/404 network responses, no CSP violations. Fixed afterwards: duplicated "chat" in the Recipient label, tiny message-ID heading, exact large-integer display.

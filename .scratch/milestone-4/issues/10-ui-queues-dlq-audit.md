# 10: Operational UI views and actions

**What to build:** Recipient-state, dead-letter, delivery-state, and audit views with payload inspection, replay, and delete.

**Blocked by:** 03, 09.

**Status:** ready-for-agent

- [ ] Recipients by status with paging; delivery-state lookup by message ID; audit list with paging
- [ ] Dead letters: list, detail with attempt history; payload after an audited-access confirmation, rendered via `textContent`
- [ ] Replay with confirmation; `keep_current` offered only after `409 deduplication_conflict`; delete with confirmation and the detail `ETag`
- [ ] Lost responses re-read state and report observed/uncertain, never retrying
- [ ] Manual browser walkthrough recorded in this ticket

## Comments

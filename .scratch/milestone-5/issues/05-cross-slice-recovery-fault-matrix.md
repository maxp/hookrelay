# 05: Cross-slice recovery fault matrix

**What to build:** Prove the complete first-version startup and Valkey-recovery gate across mixed endpoint, delivery, DLQ, and administrative-session state.

**Blocked by:** 01, 03, 04.

**Status:** done

- [x] Build mixed real-Valkey fixtures containing ready, leased, retry-wait, blocked, replayed, dead-lettered, and acknowledged messages plus endpoints and browser sessions
- [x] Exercise overdue lease/retry processing before readiness and verify an idempotent second pass
- [x] Exercise changed Admin Secret with valid, orphaned, expired, and stale-generation sessions
- [x] Exercise endpoint index loss/drift beside delivery repairs in the same pass
- [x] Exercise `SCRIPT FLUSH`, Valkey loss after readiness, and a lightweight recovery pass before readiness returns
- [x] Inject representative uncertain/partial critical-transition residue selected by tickets 02–04 and reconcile it without blindly retrying the mutation
- [x] Assert bounded metrics/logs/audit and absence of credentials, payloads, session/CSRF tokens, and Delivery Tokens

## Comments

- 2026-09-30: Added the composed real-Valkey recovery matrix. One mixed store now covers ready, leased, retry-wait, blocked, replayed, dead-lettered, acknowledged, due-lease, and due-retry state beside a Webhook Endpoint and browser session; the same startup pass reloads flushed scripts, repairs endpoint/session/delivery derivations, processes due work, and proves a clean idempotent second pass. Separate cases cover Admin Secret rotation across indexed, orphaned, expired, and stale-generation sessions; gate reload followed by a lightweight recovery pass; and partial claim residue that is isolated without a blind claim retry or token reconstruction. Logs, audit, and bounded consistency issues are asserted to exclude credentials, payloads, CSRF values, and Delivery Tokens.

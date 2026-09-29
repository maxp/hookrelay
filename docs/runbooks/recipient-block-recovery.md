# Recipient block recovery

> **Availability:** the `hookrelay admin recipients inspect-block`/`clear-block` commands and their `POST /admin/v1/recipient-blocks/…` routes are not part of Milestone 1. Until they ship, blocked Recipients stay blocked and diagnosis is read-only (see the README's reconciliation section); the safety rules below already apply.

Use this runbook when hookrelay creates `hr1:q:<recipient_identity>` after detecting ambiguous authoritative Recipient state. The marker already prevents new ingestion, claims, token operations, and maintenance transitions for that Recipient; other Recipients continue normally.

This procedure clears a protective block only after the authoritative queue, head state, Canonical Message, and attempt state are known to be consistent. It is not a generic repair mechanism and does not authorize guessing or deleting ambiguous data.

## Safety rules

- Use the Admin API through `hookrelay admin`; do not delete the marker or edit derived indexes with `valkey-cli`.
- If authoritative state itself needs correction, stop this runbook. That correction requires a separately reviewed incident-specific tool or versioned repair procedure; ad hoc Valkey commands are not authorized here. Resume at inspection only after that procedure completes.
- Treat Valkey diagnostics as secret-bearing. Do not copy payloads, credentials, Delivery Tokens, or raw keys into tickets, logs, or chat.
- Do not clear a marker merely to restore readiness or reduce an alert.
- Have a second operator review any incident-specific mutation of authoritative state.
- Backup and restore policy remains deferred; this runbook does not imply that an ad hoc data export is an approved backup.

## 1. Identify the blocked Recipient

List blocked Recipients:

```text
hookrelay admin recipients list --status blocked
```

Record the structured Recipient fields, `detected_ms`, `reason_code`, request ID, and incident reference. Do not use the internal serialized Recipient Identity as an operator-facing identifier.

## 2. Inspect without mutation

Run:

```text
hookrelay admin recipients inspect-block \
  --bot-platform <platform> \
  --bot-id <bot-id> \
  --scope <chat|user|bot|relay> \
  [--chat-id <chat-id> | --user-id <user-id>]
```

The command calls `POST /admin/v1/recipient-blocks/inspect`. It reports only safe metadata:

- marker `detected_ms` and `reason_code`;
- queue length and bounded head-state fields;
- whether the head Canonical Message exists;
- ready, lease, retry, and blocked index memberships;
- bounded invariant violations.

It never returns a payload or Delivery Token and performs no writes.

## 3. Classify the incident

One of the following must be true before proceeding:

1. authoritative state is already consistent and only derived index membership requires restoration; or
2. an incident-specific repair of authoritative state has been designed, reviewed, executed, and independently verified.

The general invariants include:

- an existing head state references `LINDEX(queue, 0)`;
- every queued `message_id` has its required Canonical Message;
- only the queue head has active delivery state;
- status and deadline fields agree;
- no active Delivery Token is invented, copied, or replaced;
- a Dead-letter Message is not simultaneously present as a normal queued position except during the defined atomic replay transition;
- the Recipient belongs to no ready, lease, or retry index while blocked.

If the correct authoritative state is not unambiguous, stop. Keep the marker and escalate the incident. The clear operation cannot repair or choose authoritative state.

## 4. Reinspect after any repair

Run `inspect-block` again. Confirm that it reports no authoritative-state ambiguity and that `detected_ms` and `reason_code` still match the values recorded in step 1. A changed marker means the preconditions are stale; restart the procedure from step 1.

## 5. Clear with exact preconditions

Run:

```text
hookrelay admin recipients clear-block \
  --bot-platform <platform> \
  --bot-id <bot-id> \
  --scope <chat|user|bot|relay> \
  [--chat-id <chat-id> | --user-id <user-id>] \
  --expected-detected-ms <detected-ms> \
  --expected-reason-code <reason-code> \
  --yes
```

The command calls `POST /admin/v1/recipient-blocks/clear`. The versioned Lua transition:

1. verifies the exact marker preconditions;
2. revalidates queue, head-state, message, and applicable deadline invariants;
3. refuses any ambiguity without mutation;
4. removes the marker and `hr1:blocked` membership;
5. restores exactly the ready, lease, retry, or no-index membership implied by verified state;
6. appends the mandatory administrative audit event in the same operation.

A missing marker returns `recipient_block_not_found`. Failed invariants return `recipient_state_ambiguous`. A transport failure or unexpected script error is an uncertain outcome and must not be blindly retried.

## 6. Reconcile an uncertain clear

After an uncertain result:

1. run `recipients list --status blocked` and `inspect-block` again;
2. inspect the administrative audit through the supported Admin API when available;
3. if either state or mandatory audit cannot be established, report an uncertain outcome and escalate rather than retrying the mutation.

Observing that the marker disappeared does not by itself prove that the mandatory audit append completed.

## 7. Verify recovery

Confirm:

- the Recipient no longer appears in the blocked list;
- exactly the expected derived index membership is reported;
- `hookrelay_blocked_recipients` decreased when appropriate;
- no new consistency issue is emitted for the Recipient;
- delivery or ingestion resumes according to the verified state;
- the audit event and request ID are attached to the incident record.

Do not send a synthetic production webhook solely to test recovery unless the Bot Platform and consumer side effects make that explicitly safe.

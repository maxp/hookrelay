package valkey

import (
	"context"
	"strconv"
	"testing"

	"github.com/maxp/hookrelay/internal/delivery"
)

func TestReconcileReviewEndpointCapacity(t *testing.T) {
	a, _ := claimSetup(t)
	e := endpointFixture()
	ctx := context.Background()
	if _, _, result := a.CreateEndpoint(ctx, e, "event", "webhook_endpoint_created", "req"); result != CreateOK {
		t.Fatal(result)
	}
	for i := 0; i < 100; i++ {
		member := "telegram:wh_capacity" + itoa(i)
		a.testDo(t, "COPY", "hr1:wh:telegram:"+e.Identifier, "hr1:wh:"+member)
		a.testDo(t, "SADD", "hr1:bot:telegram:"+e.BotID+":webhooks", member)
		a.testDo(t, "ZADD", "hr1:webhooks", hget(t, a, "hr1:wh:telegram:"+e.Identifier, "created_ms"), member)
	}
	before := snapshot(t, a)
	rep := reconcile(t, a, false)
	if rep.Hold() != "webhook_endpoint_inconsistent" {
		t.Fatalf("101 endpoints allowed readiness: %+v", rep)
	}
	assertUnchanged(t, a, before, "endpoint capacity hold")
}

func TestReconcileReviewClaimDeadlineBounds(t *testing.T) {
	for _, deadline := range []string{"0", "bad", "claimed", "after_current"} {
		t.Run(deadline, func(t *testing.T) {
			a, _, digest := claimedAttempt(t)
			value := deadline
			if deadline == "claimed" {
				value = hget(t, a, "hr1:r:"+ridA+":s", "claimed_ms")
			} else if deadline == "after_current" {
				current, err := strconv.ParseInt(hget(t, a, "hr1:r:"+ridA+":s", "lease_expires_ms"), 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				value = strconv.FormatInt(current+1, 10)
			}
			a.testDo(t, "HSET", "hr1:op:op-1", "lease_expires_ms", value)
			res := runAttempt(t, a, "verify", digest, "")
			if res.Status != "blocked" || field(t, res, 0) != "claim_operation_mismatch" {
				t.Fatalf("invalid original claim deadline accepted: %s", res.Status)
			}
		})
	}
}

func TestReconcileReviewExtendedLease(t *testing.T) {
	a, s := extendSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	ctx := context.Background()
	claim := s.Claim(ctx, claimReq("op-claim", "args", "dlv_token1"))
	if claim.Delivery.MessageID != "m1" {
		t.Fatal(claim)
	}
	ext := s.Extend(ctx, extendReq("dlv_token1", "ext-1", "ext-args"))
	if ext.Outcome != delivery.ExtendExtended || ext.LeaseExpiresMs <= claim.Delivery.LeaseExpiresMs {
		t.Fatal(ext)
	}
	before := snapshot(t, a)
	rep := reconcile(t, a, false)
	if rep.Hold() != "" || rep.Findings["blocked"] != 0 {
		t.Fatalf("healthy extended lease rejected: %+v", rep)
	}
	assertUnchanged(t, a, before, "extended lease reconciliation")
	if got := s.Ack(ctx, ackReq("dlv_token1")); got.Outcome != delivery.AckAcknowledged {
		t.Fatalf("extended lease no longer acknowledgeable: %+v", got)
	}
}

func TestReconcileReviewCanonicalPayload(t *testing.T) {
	for _, payload := range []string{`{"payload":null}`, `null`, `false`, `[]`, `{"escaped":"\"payload\":null"}`} {
		t.Run(payload, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			a.testDo(t, "SET", "hr1:m:m1", `{"message_id":"m1","received_ms":1,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-1"},"platform_event_type":"test","payload":`+payload+`}`)
			before := snapshot(t, a)
			res := runMessage(t, a, "inspect", "m1", ridA, "head")
			if res.Status != "consistent" {
				t.Fatalf("valid payload rejected: %s", res.Status)
			}
			assertUnchanged(t, a, before, "valid payload")
		})
	}
}

func TestReconcileReviewUnrelatedBlockDoesNotHideGlobalCorruption(t *testing.T) {
	for _, family := range []string{"blob", "success", "same_recipient_blob"} {
		t.Run(family, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueJSON(t, a, "blocked", ridA)
			a.testDo(t, "DEL", "hr1:r:"+ridA+":s")
			if family == "blob" {
				enqueueJSON(t, a, "orphan", ridB)
				a.testDo(t, "DEL", "hr1:r:"+ridB+":q", "hr1:r:"+ridB+":s")
			} else if family == "same_recipient_blob" {
				// A blocked Recipient alone cannot certify ownership of another
				// blob; only its exact authoritative head may lose its locator.
				a.testDo(t, "COPY", "hr1:m:blocked", "hr1:m:orphan")
				a.testDo(t, "SET", "hr1:m:orphan", `{"message_id":"orphan","received_ms":1,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-1"},"platform_event_type":"test","payload":{}}`)
			} else {
				a.testDo(t, "HSET", "hr1:success:orphan", "recipient_scope", "invalid")
			}
			rep := reconcile(t, a, false)
			if rep.Hold() != "message_lifecycle_inconsistent" || rep.Findings["blocked"] != 1 {
				t.Fatalf("unrelated block hides global corruption: %+v", rep)
			}
			if again := reconcile(t, a, false); again.Hold() != rep.Hold() {
				t.Fatalf("hold changed on repeated pass: %+v", again)
			}
		})
	}
}

func TestReconcileReviewMalformedMarkers(t *testing.T) {
	for _, queued := range []bool{false, true} {
		for _, fields := range [][2]string{{"1", "not-an-accepted-reason"}, {"bad", "queue_head_mismatch"}, {"0", "head_state_missing"}, {"1.5", "queue_head_mismatch"}} {
			t.Run(fields[0]+"/"+fields[1]+"/"+strconv.FormatBool(queued), func(t *testing.T) {
				a, _ := claimSetup(t)
				if queued {
					enqueueJSON(t, a, "m1", ridA)
				}
				a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", fields[0], "reason_code", fields[1])
				before := snapshot(t, a)
				rep := reconcile(t, a, false)
				if rep.Hold() == "" {
					t.Fatalf("malformed marker accepted: %+v", rep)
				}
				assertUnchanged(t, a, before, "malformed marker")
			})
		}
	}
}

func TestReconcileReviewWrongTypeQueuedMetadataIsIsolated(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	enqueueJSON(t, a, "m2", ridB)
	a.testDo(t, "DEL", "hr1:mi:m1")
	a.testDo(t, "SET", "hr1:mi:m1", "bad")
	rep := reconcile(t, a, false)
	if rep.Hold() != "" || rep.BlockReasons["message_lifecycle_inconsistent"] != 1 {
		t.Fatalf("local corruption globally held: %+v", rep)
	}
	if claim := s.Claim(context.Background(), claimReq("op-other", "args", "dlv_other")); claim.Delivery.MessageID != "m2" {
		t.Fatalf("unrelated Recipient unavailable: %+v", claim)
	}
}

func TestReconcileReviewMessageTypes(t *testing.T) {
	for _, family := range []string{"m", "mi", "a", "dl", "success"} {
		t.Run(family, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			key := "hr1:" + family + ":m1"
			a.testDo(t, "DEL", key)
			if family == "m" {
				a.testDo(t, "HSET", key, "bad", "type")
			} else {
				a.testDo(t, "SET", key, "bad")
			}
			res := runMessage(t, a, "inspect", "m1", ridA, "head")
			if res.Status != "blocked" {
				t.Fatalf("queued wrong type not isolated: %s", res.Status)
			}
			a.testDo(t, "DEL", "hr1:r:"+ridA+":q", "hr1:r:"+ridA+":s", "hr1:q:"+ridA)
			before := snapshot(t, a)
			res = runMessage(t, a, "inspect", "m1", "", "none")
			if res.Status != "wrong_type" {
				t.Fatalf("unlocated wrong type not held: %s", res.Status)
			}
			assertUnchanged(t, a, before, "unlocated wrong type")
		})
	}
}

func TestReconcileReviewMissingEnvelopePayload(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	a.testDo(t, "SET", "hr1:m:m1", `{"message_id":"m1","received_ms":1,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-1"},"platform_event_type":"test","other":{"payload":{}}}`)
	res := runMessage(t, a, "inspect", "m1", ridA, "head")
	if res.Status != "blocked" || field(t, res, 0) != "message_invalid" {
		t.Fatalf("missing envelope payload accepted: %s", res.Status)
	}
}

func TestReconcileReviewMalformedDigest(t *testing.T) {
	a, _ := claimSetup(t)
	enqueueJSON(t, a, "m1", ridA)
	a.testDo(t, "HSET", "hr1:mi:m1", "dedup_identity_digest", "garbage")
	res := runMessage(t, a, "inspect", "m1", ridA, "head")
	if res.Status != "blocked" || field(t, res, 0) != "metadata_invalid" {
		t.Fatalf("invalid digest accepted: %s", res.Status)
	}
}

func TestReconcileReviewFractionalHistory(t *testing.T) {
	for _, entry := range []string{
		`{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1.5,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}`,
		`{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2.5,"completed_ms":2,"outcome":"nack"}`,
		`{"kind":"attempt","delivery_cycle":1,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2.5,"outcome":"nack"}`,
		`{"kind":"archived_cycles_summary","archived_cycles":1,"archived_attempts":1,"first_archived_ms":1.5,"last_archived_ms":2}`,
		`{"kind":"archived_cycles_summary","archived_cycles":1,"archived_attempts":1,"first_archived_ms":1,"last_archived_ms":2.5}`,
	} {
		t.Run(entry, func(t *testing.T) {
			a, _ := claimSetup(t)
			enqueueJSON(t, a, "m1", ridA)
			a.testDo(t, "RPUSH", "hr1:a:m1", entry)
			res := runMessage(t, a, "inspect", "m1", ridA, "head")
			if res.Status != "blocked" || field(t, res, 0) != "history_invalid" {
				t.Fatalf("fractional history accepted: %s", res.Status)
			}
		})
	}
}

package valkey

import (
	"context"
	"testing"
)

// This schedule documents why complete-model discovery must be quiescent:
// a locator correct before acknowledgement becomes stale after head advance.
// The recovery admission barrier prevents this schedule in the application;
// this is not a contract allowing the reconciler to run while serving.
func TestMessageLocatorRequiresQuiescentDiscovery(t *testing.T) {
	a, s := claimSetup(t)
	enqueueJSON(t, a, "first", ridA)
	enqueueJSON(t, a, "second", ridA)
	s.Claim(context.Background(), claimReq("op-first", "args", "dlv_first"))
	if result := s.Ack(context.Background(), ackReq("dlv_first")); result.Outcome != "acknowledged" {
		t.Fatalf("ack = %+v", result)
	}
	result, err := a.RunScript(context.Background(), "reconcile_message_v1",
		[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
		[]string{"inspect", "second", ridA, "behind_head", "3600000", "hr1"})
	if err != nil || result.Status != "blocked" {
		t.Fatalf("stale locator = %+v, %v", result, err)
	}
	if hget(t, a, "hr1:q:"+ridA, "reason_code") != "message_lifecycle_inconsistent" {
		t.Fatal("expected stale-discovery isolation was not reproduced")
	}
}

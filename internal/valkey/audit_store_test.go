package valkey

import (
	"context"
	"errors"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

// TestAuditLogPagesNewestFirst pins the reader: XREVRANGE order across
// entries sharing a millisecond, the exclusive continuation bound, the
// field allowlist, and missing or wrong-type Streams.
func TestAuditLogPagesNewestFirst(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	l := NewAuditLog(a)
	if got, err := l.ListAudit(ctx, 10, ""); err != nil || len(got) != 0 {
		t.Fatalf("missing stream = %v, %v", got, err)
	}
	for _, id := range []string{"1000-0", "1000-1", "1000-2", "2000-0"} {
		a.testDo(t, "XADD", "hr1:audit", id, "event_id", "e"+id, "timestamp_ms", "1000", "actor", "admin_bearer",
			"operation", "dead_letter_replayed", "target", "m1", "request_id", "r", "outcome", "success", "reason", "kept_current",
			"payload", "must-not-leak")
	}
	page, err := l.ListAudit(ctx, 2, "")
	if err != nil || len(page) != 2 || page[0].StreamID != "2000-0" || page[1].StreamID != "1000-2" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	want := administration.AuditEntry{StreamID: "2000-0", EventID: "e2000-0", TimestampMs: 1000, Actor: "admin_bearer",
		Operation: "dead_letter_replayed", Target: "m1", RequestID: "r", Outcome: "success", Reason: "kept_current"}
	if page[0] != want {
		t.Errorf("entry = %+v", page[0])
	}
	page, err = l.ListAudit(ctx, 2, "1000-2")
	if err != nil || len(page) != 2 || page[0].StreamID != "1000-1" || page[1].StreamID != "1000-0" {
		t.Fatalf("second page = %+v, %v", page, err)
	}
	if page, _ := l.ListAudit(ctx, 2, "1000-0"); len(page) != 0 {
		t.Errorf("after the oldest = %+v", page)
	}

	a.testDo(t, "DEL", "hr1:audit")
	a.testDo(t, "SET", "hr1:audit", "x")
	if _, err := l.ListAudit(ctx, 2, ""); !errors.Is(err, administration.ErrStoredWrongType) {
		t.Errorf("wrong type = %v", err)
	}
}

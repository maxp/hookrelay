package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
)

func testRecipientStore(a *Adapter) administration.RecipientRepository {
	return NewRecipientStore(a, 10)
}

// block isolates rid behind a marker the way reconciliation does.
func block(t *testing.T, a *Adapter, rid, detected, reason string) {
	t.Helper()
	a.testDo(t, "HSET", "hr1:q:"+rid, "detected_ms", detected, "reason_code", reason)
	a.testDo(t, "ZADD", "hr1:blocked", detected, rid)
	for _, idx := range []string{"hr1:ready", "hr1:leases", "hr1:retries"} {
		a.testDo(t, "ZREM", idx, rid)
	}
}

// TestListRecipientStates pins each status filter over its index, the
// ascending (score, member) order, cursor continuation, and the blocked
// reason.
func TestListRecipientStates(t *testing.T) {
	a, _ := retryWaitState(t) // ridA retry_wait, ridB ready
	ctx := context.Background()
	store := testRecipientStore(a)
	for i := 0; i < 5; i++ {
		enqueueJSON(t, a, "r"+itoa64(int64(i)), "telegram:42:chat:"+itoa64(int64(-200-i)))
	}
	block(t, a, "telegram:42:chat:-300", "77", "queue_head_mismatch")

	var seen []string
	var after *administration.RecipientCursor
	for {
		page, err := store.ListRecipientStates(ctx, administration.StatusReady, 2, after)
		if err != nil {
			t.Fatal(err)
		}
		for _, it := range page {
			seen = append(seen, it.RecipientIdentity)
		}
		if len(page) < 2 {
			break
		}
		last := page[len(page)-1]
		after = &administration.RecipientCursor{Score: last.Score, Member: last.RecipientIdentity}
	}
	if len(seen) != 6 || seen[0] != ridB {
		t.Errorf("ready pages = %v, want ridB first then the five new heads", seen)
	}

	if items, _ := store.ListRecipientStates(ctx, administration.StatusRetryWait, 10, nil); len(items) != 1 || items[0].RecipientIdentity != ridA ||
		itoa64(items[0].Score) != hget(t, a, "hr1:r:"+ridA+":s", "retry_at_ms") {
		t.Errorf("retry_wait = %+v", items)
	}
	if items, _ := store.ListRecipientStates(ctx, administration.StatusBlocked, 10, nil); len(items) != 1 || items[0].Score != 77 ||
		items[0].ReasonCode != "queue_head_mismatch" {
		t.Errorf("blocked = %+v", items)
	}
	if items, err := store.ListRecipientStates(ctx, administration.StatusLeased, 10, nil); err != nil || len(items) != 0 {
		t.Errorf("leased = %+v, %v", items, err)
	}
}

// TestInspectBlock pins the read-only inspection: marker, queue length,
// bounded head state without the token, head message presence, index
// memberships, and the violated invariants.
func TestInspectBlock(t *testing.T) {
	a, _ := consistentState(t) // ridA leased (a1, a2), ridB ready
	ctx := context.Background()
	store := testRecipientStore(a)
	block(t, a, ridA, "55", "head_message_missing")
	a.testDo(t, "DEL", "hr1:m:a2")
	before := snapshot(t, a)
	got, err := store.InspectBlock(ctx, ridA)
	if err != nil {
		t.Fatal(err)
	}
	assertUnchanged(t, a, before, "inspection")
	if got.Marker == nil || got.Marker.DetectedMs != 55 || got.Marker.ReasonCode != "head_message_missing" || got.QueueLength != 2 ||
		got.Head == nil || got.Head.MessageID != "a1" || got.Head.Status != "leased" || got.Head.Attempt != 1 || got.Head.LeaseExpiresMs == 0 ||
		!got.HeadMessagePresent {
		t.Errorf("inspection = %+v head %+v", got, got.Head)
	}
	if got.Memberships["blocked"] != true || got.Memberships["leases"] || got.Memberships["ready"] || got.Memberships["retries"] {
		t.Errorf("memberships = %v", got.Memberships)
	}
	if strings.Join(got.ViolatedInvariants, ",") != "head_message_missing" {
		t.Errorf("violated = %v", got.ViolatedInvariants)
	}

	// A consistent blocked Recipient has no violations; an unblocked one no
	// marker.
	got, _ = store.InspectBlock(ctx, ridB)
	if got.Marker != nil || len(got.ViolatedInvariants) != 0 || !got.Memberships["ready"] {
		t.Errorf("unblocked inspection = %+v", got)
	}
	a.testDo(t, "SET", "hr1:m:a2", `{"message_id":"a2"}`)
	got, _ = store.InspectBlock(ctx, ridA)
	if len(got.ViolatedInvariants) != 0 {
		t.Errorf("violations after the repair = %v", got.ViolatedInvariants)
	}
}

// TestClearBlockRestoresTheImpliedIndex pins the cleared tuple for every
// head status: marker and blocked member removed, exactly the implied index
// restored, and the mandatory audit event appended.
func TestClearBlockRestoresTheImpliedIndex(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T) (*Adapter, string)
		index string
	}{
		{"leased head", func(t *testing.T) (*Adapter, string) { a, _ := consistentState(t); return a, ridA }, "leases"},
		{"ready head", func(t *testing.T) (*Adapter, string) { a, _ := consistentState(t); return a, ridB }, "ready"},
		{"retry_wait head", func(t *testing.T) (*Adapter, string) { a, _ := retryWaitState(t); return a, ridA }, "retries"},
		{"drained recipient", func(t *testing.T) (*Adapter, string) { a, _ := claimSetup(t); return a, "telegram:42:chat:-9" }, "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, rid := tc.setup(t)
			block(t, a, rid, "55", "queue_head_mismatch")
			result, detail := testRecipientStore(a).ClearBlock(ctx, rid, 55, "queue_head_mismatch", "evt-1", "req-1")
			if result != administration.ClearCleared || detail != tc.index {
				t.Fatalf("clear = %s %s, want cleared %s", result, detail, tc.index)
			}
			if exists(t, a, "hr1:q:"+rid) {
				t.Error("marker kept")
			}
			for _, idx := range []string{"ready", "leases", "retries", "blocked"} {
				_, in := score(t, a, "hr1:"+idx, rid)
				if in != (idx == tc.index) {
					t.Errorf("membership %s = %v", idx, in)
				}
			}
			switch tc.index {
			case "leases":
				if sc, _ := score(t, a, "hr1:leases", rid); itoa64(sc) != hget(t, a, "hr1:r:"+rid+":s", "lease_expires_ms") {
					t.Errorf("lease score = %d", sc)
				}
			case "retries":
				if sc, _ := score(t, a, "hr1:retries", rid); itoa64(sc) != hget(t, a, "hr1:r:"+rid+":s", "retry_at_ms") {
					t.Errorf("retry score = %d", sc)
				}
			}
			entries, err := a.AuditEntries(ctx, 10)
			if err != nil || len(entries) != 1 {
				t.Fatalf("audit = %+v, %v", entries, err)
			}
			e := entries[0]
			if e["operation"] != "recipient_block_cleared" || e["target"] != rid || e["event_id"] != "evt-1" ||
				e["request_id"] != "req-1" || e["actor"] != "admin_bearer" || e["outcome"] != "success" || e["reason"] != "queue_head_mismatch" {
				t.Errorf("audit entry = %+v", e)
			}
		})
	}
}

// TestClearBlockRefusals pins not_found, precondition_failed, ambiguous
// (with the first violated invariant), and wrong_type without mutation.
func TestClearBlockRefusals(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name     string
		setup    func(t *testing.T, a *Adapter)
		detected int64
		reason   string
		want     administration.ClearBlockResult
		detail   string
	}{
		{"no marker", func(t *testing.T, a *Adapter) {}, 55, "queue_head_mismatch", administration.ClearNotFound, ""},
		{"other detection time", func(t *testing.T, a *Adapter) { block(t, a, ridA, "55", "queue_head_mismatch") }, 56, "queue_head_mismatch", administration.ClearPreconditionFailed, ""},
		{"other reason", func(t *testing.T, a *Adapter) { block(t, a, ridA, "55", "queue_head_mismatch") }, 55, "head_state_missing", administration.ClearPreconditionFailed, ""},
		{"queued message missing", func(t *testing.T, a *Adapter) {
			block(t, a, ridA, "55", "head_message_missing")
			a.testDo(t, "DEL", "hr1:m:a2")
		}, 55, "head_message_missing", administration.ClearAmbiguous, "head_message_missing"},
		{"head mismatch", func(t *testing.T, a *Adapter) {
			block(t, a, ridA, "55", "queue_head_mismatch")
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "zz")
		}, 55, "queue_head_mismatch", administration.ClearAmbiguous, "queue_head_mismatch"},
		{"lease without token", func(t *testing.T, a *Adapter) {
			block(t, a, ridA, "55", "head_state_missing")
			a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token")
		}, 55, "head_state_missing", administration.ClearAmbiguous, "head_state_missing"},
		{"blocked index wrong type", func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "55", "reason_code", "queue_head_mismatch")
			a.testDo(t, "SET", "hr1:blocked", "x")
		}, 55, "queue_head_mismatch", administration.ClearWrongType, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := consistentState(t)
			tc.setup(t, a)
			before := snapshot(t, a)
			result, detail := testRecipientStore(a).ClearBlock(ctx, ridA, tc.detected, tc.reason, "evt", "req")
			if result != tc.want || detail != tc.detail {
				t.Errorf("clear = %s %q, want %s %q", result, detail, tc.want, tc.detail)
			}
			assertUnchanged(t, a, before, tc.name)
		})
	}
}

// TestClearBlockArgumentsAndReload pins argument validation and the EVAL
// reload after SCRIPT FLUSH.
func TestClearBlockArgumentsAndReload(t *testing.T) {
	a, _ := consistentState(t)
	ctx := context.Background()
	keys := []string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", "hr1:audit"}
	valid := []string{ridA, "55", "queue_head_mismatch", "evt", "req", "10", "hr1"}
	with := func(i int, v string) []string {
		args := append([]string(nil), valid...)
		args[i] = v
		return args
	}
	for name, args := range map[string][]string{
		"too few":         valid[:6],
		"empty recipient": with(0, ""),
		"zero detection":  with(1, "0"),
		"empty reason":    with(2, ""),
		"empty event id":  with(3, ""),
		"zero bound":      with(5, "0"),
		"prefix mismatch": with(6, "hr2"),
	} {
		if _, err := a.RunScript(ctx, "clear_block_v2", keys, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	block(t, a, ridA, "55", "queue_head_mismatch")
	a.testDo(t, "SCRIPT", "FLUSH")
	if result, _ := testRecipientStore(a).ClearBlock(ctx, ridA, 55, "queue_head_mismatch", "evt", "req"); result != administration.ClearCleared {
		t.Errorf("clear after SCRIPT FLUSH = %s", result)
	}
}

// TestRecipientBlockRoutesOverRealValkey drives list → inspect → clear
// through the real Admin API handler, service, and Recipient store: a
// Recipient isolated by reconciliation is listed, inspected, refused while
// ambiguous, and cleared after the authoritative state is consistent.
func TestRecipientBlockRoutesOverRealValkey(t *testing.T) {
	a, s := consistentState(t)
	a.testDo(t, "DEL", "hr1:m:b1") // ridB: head_message_missing
	if rep := reconcile(t, a, true); rep.BlockReasons["head_message_missing"] != 1 {
		t.Fatalf("reconcile = %+v", rep.BlockReasons)
	}
	svc, err := administration.NewService(administration.ServiceDeps{
		Repo: NewEndpointStore(a), Recipients: NewRecipientStore(a, 10), Audit: NewAuditSink(a),
		AdminSecret: "admin-secret-value-016", Gen: gen.Crypto{},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := administration.Handler(svc)
	call := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-secret-value-016")
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	recipient := `{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-2"}`

	w := call(http.MethodGet, "/admin/v1/recipient-states?status=blocked", "")
	var page struct {
		Items []struct {
			DetectedMs int64  `json:"detected_ms"`
			ReasonCode string `json:"reason_code"`
		} `json:"items"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &page) != nil || len(page.Items) != 1 || page.Items[0].ReasonCode != "head_message_missing" {
		t.Fatalf("list = %d %s", w.Code, w.Body.String())
	}
	detected := itoa64(page.Items[0].DetectedMs)
	clearBody := `{"recipient":` + recipient + `,"expected_detected_ms":` + detected + `,"expected_reason_code":"head_message_missing"}`

	if w := call(http.MethodPost, "/admin/v1/recipient-blocks/inspect", `{"recipient":`+recipient+`}`); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), `"violated_invariants":["head_message_missing"]`) || strings.Contains(w.Body.String(), "dlv_") {
		t.Errorf("inspect = %d %s", w.Code, w.Body.String())
	}
	if w := call(http.MethodPost, "/admin/v1/recipient-blocks/clear", clearBody); w.Code != http.StatusConflict ||
		!strings.Contains(w.Body.String(), "recipient_state_ambiguous") {
		t.Errorf("ambiguous clear = %d %s", w.Code, w.Body.String())
	}

	// The reviewed repair restores the authoritative message; the clear then
	// succeeds and the Recipient becomes claimable again.
	a.testDo(t, "SET", "hr1:m:b1", `{"message_id":"b1"}`)
	if w := call(http.MethodPost, "/admin/v1/recipient-blocks/clear", clearBody); w.Code != http.StatusNoContent {
		t.Fatalf("clear = %d %s", w.Code, w.Body.String())
	}
	if w := call(http.MethodPost, "/admin/v1/recipient-blocks/clear", clearBody); w.Code != http.StatusNotFound {
		t.Errorf("second clear = %d, want 404", w.Code)
	}
	if c := s.Claim(context.Background(), claimReq("op-after-clear", "args", "dlv_after")); c.Outcome != delivery.ClaimClaimed || c.Delivery.MessageID != "b1" {
		t.Errorf("claim after clear = %+v", c)
	}
}

// TestInspectAndClearAgree pins that inspection reports invariants in the
// order clear checks them, so the first violated invariant inspection shows
// is the one clear refuses with, and that marker fields are validated.
func TestInspectAndClearAgree(t *testing.T) {
	ctx := context.Background()
	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"head mismatch and a missing message": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "head_message_id", "zz")
			a.testDo(t, "DEL", "hr1:m:a2")
		},
		"lease without token and a missing message": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HDEL", "hr1:r:"+ridA+":s", "delivery_token")
			a.testDo(t, "DEL", "hr1:m:a2")
		},
		"non-canonical lease deadline": func(t *testing.T, a *Adapter) {
			a.testDo(t, "HSET", "hr1:r:"+ridA+":s", "lease_expires_ms", "0123")
		},
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := consistentState(t)
			block(t, a, ridA, "55", "queue_head_mismatch")
			setup(t, a)
			store := testRecipientStore(a)
			in, err := store.InspectBlock(ctx, ridA)
			if err != nil || len(in.ViolatedInvariants) == 0 {
				t.Fatalf("inspection = %+v, %v", in, err)
			}
			result, detail := store.ClearBlock(ctx, ridA, 55, "queue_head_mismatch", "evt", "req")
			if result != administration.ClearAmbiguous || detail != in.ViolatedInvariants[0] {
				t.Errorf("clear = %s %s, inspection first = %v", result, detail, in.ViolatedInvariants)
			}
		})
	}

	a, _ := consistentState(t)
	a.testDo(t, "HSET", "hr1:q:"+ridA, "reason_code", "queue_head_mismatch")
	a.testDo(t, "ZADD", "hr1:blocked", "55", ridA)
	in, _ := testRecipientStore(a).InspectBlock(ctx, ridA)
	if len(in.ViolatedInvariants) == 0 || in.ViolatedInvariants[0] != "marker_fields" {
		t.Errorf("marker without detected_ms: violated = %v", in.ViolatedInvariants)
	}
}

// TestListBlockedUsesTheMarker pins that the blocked list reports the
// authoritative marker's detection time and flags an index member whose
// marker is missing.
func TestListBlockedUsesTheMarker(t *testing.T) {
	a, _ := consistentState(t)
	a.testDo(t, "HSET", "hr1:q:"+ridA, "detected_ms", "55", "reason_code", "queue_head_mismatch")
	a.testDo(t, "ZADD", "hr1:blocked", "99", ridA) // drifted index score
	a.testDo(t, "ZADD", "hr1:blocked", "100", "telegram:42:chat:-9")
	items, err := testRecipientStore(a).ListRecipientStates(context.Background(), administration.StatusBlocked, 10, nil)
	if err != nil || len(items) != 2 {
		t.Fatalf("items = %+v, %v", items, err)
	}
	if items[0].DetectedMs != 55 || items[0].ReasonCode != "queue_head_mismatch" || items[0].MarkerMissing {
		t.Errorf("marked item = %+v", items[0])
	}
	if !items[1].MarkerMissing || items[1].ReasonCode != "" {
		t.Errorf("unmarked item = %+v", items[1])
	}
}

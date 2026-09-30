package cli

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const (
	dlqMessageID  = "01950000-0000-7000-8000-000000000001"
	dlqListPath   = "/admin/v1/dead-letters"
	dlqGetPath    = dlqListPath + "/" + dlqMessageID
	dlqReplayPath = dlqGetPath + "/replay"
	statePath     = "/admin/v1/messages/" + dlqMessageID + "/delivery-state"
)

func deadLetterBody(cycle int) string {
	return fmt.Sprintf(`{"message_id":%q,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-100"},`+
		`"dead_lettered_ms":1740000000000,"dead_letter_reason":"nack_exhausted","delivery_cycle":%d,`+
		`"attempts":[{"delivery_cycle":%d,"attempt":1,"claimed_ms":1,"lease_expires_ms":2,"completed_ms":2,"outcome":"nack"}]}`, dlqMessageID, cycle, cycle)
}

// TestAdminDLQListAndGet pins the list query, the JSON passthrough, the
// table forms, and get by message id.
func TestAdminDLQListAndGet(t *testing.T) {
	page := `{"items":[` + deadLetterBody(1) + `],"next_cursor":"abc"}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
		"GET " + dlqListPath: respond(http.StatusOK, page),
		"GET " + dlqGetPath:  respond(http.StatusOK, deadLetterBody(1)),
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()

	r := newTestRun(srv.URL)
	if code := r.run("dlq", "list", "--limit", "10", "--cursor", "xyz"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	if q := api.queries[0]; !strings.Contains(q, "limit=10") || !strings.Contains(q, "cursor=xyz") {
		t.Errorf("query = %q", q)
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["next_cursor"] != "abc" || len(out["items"].([]any)) != 1 {
		t.Errorf("stdout = %s (%v)", r.stdout.String(), err)
	}
	r = newTestRun(srv.URL)
	r.tty = true
	r.run("dlq", "list")
	if !strings.Contains(r.stdout.String(), dlqMessageID) || !strings.Contains(r.stdout.String(), "nack_exhausted") || !strings.Contains(r.stdout.String(), "next_cursor") {
		t.Errorf("table = %s", r.stdout.String())
	}

	r = newTestRun(srv.URL)
	r.tty = true
	if code := r.run("dlq", "get", "--message-id", dlqMessageID); code != ExitOK || !strings.Contains(r.stdout.String(), "outcome=nack") ||
		!strings.Contains(r.stdout.String(), "chat_id") {
		t.Errorf("get = %d %s %s", code, r.stdout.String(), r.stderr.String())
	}
	if code := newTestRun(srv.URL).run("dlq", "get"); code != ExitUsage {
		t.Errorf("get without --message-id = %d, want usage", code)
	}
	r.assertNoSecrets(t)
}

// TestAdminDLQReplay pins the confirmed replay: the current cycle is read
// first, --yes and a bounded resolution are required, the resolution is
// sent, and definite refusals fail without reconciliation or retry.
func TestAdminDLQReplay(t *testing.T) {
	replayed := `{"status":"replayed","message_id":"` + dlqMessageID + `","delivery_cycle":2,"queue_position":"head",` +
		`"replayed_ms":1740000000001,"deduplication_resolution":"kept_current"}`
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
		"GET " + dlqGetPath:     respond(http.StatusOK, deadLetterBody(1)),
		"POST " + dlqReplayPath: respond(http.StatusOK, replayed),
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run("dlq", "replay", "--message-id", dlqMessageID, "--deduplication-conflict-resolution", "keep_current", "--yes"); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	if body := api.calls(http.MethodPost, dlqReplayPath)[0].Body; body["deduplication_conflict_resolution"] != "keep_current" {
		t.Errorf("request = %v", body)
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["outcome"] != "replayed" || out["delivery_cycle"] != 2.0 ||
		out["previous_delivery_cycle"] != 1.0 || out["queue_position"] != "head" {
		t.Errorf("stdout = %s", r.stdout.String())
	}

	for name, args := range map[string][]string{
		"no --yes":           {"dlq", "replay", "--message-id", dlqMessageID},
		"no message id":      {"dlq", "replay", "--yes"},
		"unknown resolution": {"dlq", "replay", "--message-id", dlqMessageID, "--deduplication-conflict-resolution", "repoint", "--yes"},
	} {
		if code := newTestRun(srv.URL).run(args...); code != ExitUsage {
			t.Errorf("%s: exit = %d, want usage", name, code)
		}
	}
	if n := len(api.calls(http.MethodPost, dlqReplayPath)); n != 1 {
		t.Errorf("replay requests = %d, want 1", n)
	}

	for status, code := range map[int]string{
		http.StatusConflict: "deduplication_conflict",
		http.StatusNotFound: "dead_letter_not_found",
	} {
		api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
			"GET " + dlqGetPath:     respond(http.StatusOK, deadLetterBody(1)),
			"POST " + dlqReplayPath: respond(status, apiErrorBody(code, "refused")),
		}}
		srv := httptest.NewServer(api)
		r := newTestRun(srv.URL)
		if exit := r.run("dlq", "replay", "--message-id", dlqMessageID, "--yes"); exit != ExitError || !strings.Contains(r.stderr.String(), code) ||
			len(api.calls(http.MethodGet, dlqGetPath)) != 1 {
			t.Errorf("%d: exit %d, gets %d, stderr %s", status, exit, len(api.calls(http.MethodGet, dlqGetPath)), r.stderr.String())
		}
		srv.Close()
	}

	// A message that is not dead-lettered is refused before any replay.
	api = &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET " + dlqGetPath: respond(http.StatusNotFound, apiErrorBody("dead_letter_not_found", "x"))}}
	srv2 := httptest.NewServer(api)
	defer srv2.Close()
	if exit := newTestRun(srv2.URL).run("dlq", "replay", "--message-id", dlqMessageID, "--yes"); exit != ExitError ||
		len(api.calls(http.MethodPost, dlqReplayPath)) != 0 {
		t.Errorf("replay of an absent dead letter: exit %d", exit)
	}
}

func deliveryStateBody(state string, cycle int) string {
	return fmt.Sprintf(`{"message_id":%q,"delivery_cycle":%d,"state":%q}`, dlqMessageID, cycle, state)
}

// TestAdminDLQReplayUncertain pins the uncertain-outcome discipline: no
// retry, one reconciling delivery-state read, a newer cycle reported as
// desired state with the audit caveat, and a failing exit in every case.
func TestAdminDLQReplayUncertain(t *testing.T) {
	for name, tc := range map[string]struct {
		replay  http.HandlerFunc
		state   http.HandlerFunc
		outcome string
		warning string
	}{
		"lost response, newer cycle queued":  {dropConnection, respond(http.StatusOK, deliveryStateBody("queued", 2)), outcomeDesiredStateObserved, "does NOT confirm"},
		"lost response, dead-lettered again": {dropConnection, respond(http.StatusOK, deliveryStateBody("dead_lettered", 2)), outcomeDesiredStateObserved, "does NOT confirm"},
		"503, still dead-lettered": {respond(http.StatusServiceUnavailable, apiErrorBody("dependency_unavailable", "replay outcome is uncertain")),
			respond(http.StatusOK, deliveryStateBody("dead_lettered", 1)), outcomeUncertain, "has not been observed"},
		"lost response, no state retained": {dropConnection, respond(http.StatusNotFound, apiErrorBody("message_not_found", "x")), outcomeUncertain, "no delivery state is retained"},
		"lost response, read fails":        {dropConnection, dropConnection, outcomeUncertain, "could not be read"},
	} {
		t.Run(name, func(t *testing.T) {
			api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
				"GET " + dlqGetPath:     respond(http.StatusOK, deadLetterBody(1)),
				"GET " + statePath:      tc.state,
				"POST " + dlqReplayPath: tc.replay,
			}}
			srv := httptest.NewServer(api)
			defer srv.Close()
			r := newTestRun(srv.URL)
			if code := r.run("dlq", "replay", "--message-id", dlqMessageID, "--yes"); code != ExitError {
				t.Fatalf("exit = %d, want 1", code)
			}
			if n := len(api.calls(http.MethodPost, dlqReplayPath)); n != 1 {
				t.Errorf("replay requests = %d, want exactly 1 (no blind retry)", n)
			}
			if n := len(api.calls(http.MethodGet, statePath)); n != 1 {
				t.Errorf("delivery-state reads = %d, want 1", n)
			}
			var out map[string]any
			if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["outcome"] != tc.outcome || out["previous_delivery_cycle"] != 1.0 {
				t.Errorf("stdout = %s", r.stdout.String())
			}
			if !strings.Contains(r.stderr.String(), tc.warning) {
				t.Errorf("stderr = %s", r.stderr.String())
			}
			r.assertNoSecrets(t)
		})
	}
}

// TestAdminMessageDeliveryState pins the delivery-state command.
func TestAdminMessageDeliveryState(t *testing.T) {
	api := &routedAdminAPI{routes: map[string]http.HandlerFunc{
		"GET " + statePath: respond(http.StatusOK, `{"message_id":"`+dlqMessageID+`","delivery_cycle":2,"state":"queued","queue_position":"behind_head"}`),
	}}
	srv := httptest.NewServer(api)
	defer srv.Close()
	r := newTestRun(srv.URL)
	if code := r.run("message", "delivery-state", "--message-id", dlqMessageID); code != ExitOK {
		t.Fatalf("exit = %d: %s", code, r.stderr.String())
	}
	var out map[string]any
	if err := json.Unmarshal(r.stdout.Bytes(), &out); err != nil || out["state"] != "queued" || out["queue_position"] != "behind_head" {
		t.Errorf("stdout = %s", r.stdout.String())
	}
	r = newTestRun(srv.URL)
	r.tty = true
	if r.run("message", "delivery-state", "--message-id", dlqMessageID); !strings.Contains(r.stdout.String(), "behind_head") {
		t.Errorf("table = %s", r.stdout.String())
	}
	if code := newTestRun(srv.URL).run("message", "delivery-state"); code != ExitUsage {
		t.Errorf("without --message-id = %d, want usage", code)
	}
	if code := newTestRun(srv.URL).run("message", "other"); code != ExitUsage {
		t.Errorf("unknown message command = %d, want usage", code)
	}

	api = &routedAdminAPI{routes: map[string]http.HandlerFunc{"GET " + statePath: respond(http.StatusNotFound, apiErrorBody("message_not_found", "x"))}}
	srv2 := httptest.NewServer(api)
	defer srv2.Close()
	r = newTestRun(srv2.URL)
	if code := r.run("message", "delivery-state", "--message-id", dlqMessageID); code != ExitError || !strings.Contains(r.stderr.String(), "message_not_found") {
		t.Errorf("not found = %d %s", code, r.stderr.String())
	}
}

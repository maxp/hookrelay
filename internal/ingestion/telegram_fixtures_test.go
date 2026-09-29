package ingestion_test

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/model"
)

// fixtureCase is one signed Telegram update and its expected Canonical
// Message classification.
type fixtureCase struct {
	name     string
	body     string
	scope    model.Scope
	id       string // chat_id or user_id
	event    string
	issue    string
	source   string // expected source_event_id; "" when absent
	occurred int64  // expected occurred_ms; 0 when omitted
}

func telegramFixtures() []fixtureCase {
	chat := func(n, event, value, id string, occurred int64) fixtureCase {
		return fixtureCase{event, `{"update_id":` + n + `,"` + event + `":` + value + `}`, model.ScopeChat, id, event, "", n, occurred}
	}
	user := func(n, event, value, id string, occurred int64) fixtureCase {
		return fixtureCase{event, `{"update_id":` + n + `,"` + event + `":` + value + `}`, model.ScopeUser, id, event, "", n, occurred}
	}
	relay := func(name, n, body, event, issue string) fixtureCase {
		return fixtureCase{name, body, model.ScopeRelay, "", event, issue, n, 0}
	}
	return []fixtureCase{
		// Chat rows of the event-to-Recipient table.
		chat("1", "message", `{"message_id":1,"date":1700000001,"chat":{"id":-1001},"from":{"id":7},"reply_to_message":{"date":1600000000,"chat":{"id":-1001}}}`, "-1001", 1700000001000),
		chat("2", "edited_message", `{"date":1600000000,"edit_date":1700000002,"chat":{"id":5},"from":{"id":7}}`, "5", 1700000002000),
		chat("3", "channel_post", `{"date":1700000003,"chat":{"id":-1003}}`, "-1003", 1700000003000),
		chat("4", "edited_channel_post", `{"date":1,"edit_date":1700000004,"chat":{"id":-1003}}`, "-1003", 1700000004000),
		chat("5", "business_message", `{"date":1700000005,"chat":{"id":9},"business_connection_id":"bc1"}`, "9", 1700000005000),
		chat("6", "edited_business_message", `{"date":1,"edit_date":1700000006,"chat":{"id":9}}`, "9", 1700000006000),
		chat("7", "deleted_business_messages", `{"business_connection_id":"bc1","chat":{"id":9},"message_ids":[1,2]}`, "9", 0),
		chat("8", "guest_message", `{"date":1700000008,"chat":{"id":11},"guest_query_id":"g"}`, "11", 1700000008000),
		chat("9", "callback_query", `{"id":"q","from":{"id":7},"message":{"date":1,"chat":{"id":77}},"chat_instance":"-555"}`, "77", 0),
		chat("10", "message_reaction", `{"date":1700000010,"chat":{"id":3},"user":{"id":7},"message_id":1}`, "3", 1700000010000),
		chat("11", "message_reaction_count", `{"date":1700000011,"chat":{"id":3},"message_id":1}`, "3", 1700000011000),
		chat("12", "my_chat_member", `{"date":1700000012,"chat":{"id":-5},"from":{"id":7}}`, "-5", 1700000012000),
		chat("13", "chat_member", `{"date":1700000013,"chat":{"id":-5},"from":{"id":7}}`, "-5", 1700000013000),
		chat("14", "chat_join_request", `{"date":1700000014,"chat":{"id":-5},"from":{"id":7},"user_chat_id":7}`, "-5", 1700000014000),
		chat("15", "chat_boost", `{"chat":{"id":-5},"boost":{"boost_id":"b","add_date":1700000015,"expiration_date":1800000000}}`, "-5", 1700000015000),
		chat("16", "removed_chat_boost", `{"chat":{"id":-5},"boost_id":"b","remove_date":1700000016}`, "-5", 1700000016000),
		chat("17", "stopped_message_generation", `{"chat":{"id":88}}`, "88", 0),
		{"poll_answer with voter_chat", `{"update_id":18,"poll_answer":{"poll_id":"p","voter_chat":{"id":-18},"user":{"id":7},"option_ids":[0]}}`, model.ScopeChat, "-18", "poll_answer", "", "18", 0},

		// User-fallback rows.
		user("20", "inline_query", `{"id":"q","from":{"id":20},"query":"x"}`, "20", 0),
		user("21", "chosen_inline_result", `{"result_id":"r","from":{"id":21},"inline_message_id":"im"}`, "21", 0),
		user("22", "shipping_query", `{"id":"s","from":{"id":22}}`, "22", 0),
		user("23", "pre_checkout_query", `{"id":"p","from":{"id":23}}`, "23", 0),
		user("24", "purchased_paid_media", `{"from":{"id":24},"paid_media_payload":"x"}`, "24", 0),
		user("25", "business_connection", `{"id":"bc","user":{"id":25},"user_chat_id":25,"date":1700000025}`, "25", 1700000025000),
		user("26", "subscription", `{"user":{"id":26}}`, "26", 0),
		user("27", "managed_bot", `{"user":{"id":27}}`, "27", 0),
		{"inline callback_query", `{"update_id":28,"callback_query":{"id":"q","from":{"id":28},"inline_message_id":"im","chat_instance":"-999"}}`, model.ScopeUser, "28", "callback_query", "", "28", 0},
		{"poll_answer without voter_chat", `{"update_id":29,"poll_answer":{"poll_id":"p","user":{"id":29},"option_ids":[0]}}`, model.ScopeUser, "29", "poll_answer", "", "29", 0},

		// Bot scope: only the known poll update, never poll.id as an identifier.
		{"poll", `{"update_id":30,"poll":{"id":"12345","question":"q","options":[]}}`, model.ScopeBot, "", "poll", "", "30", 0},

		// Relay scope with bounded routing issues.
		relay("unknown event field", "40", `{"update_id":40,"future_update":{"chat":{"id":1}}}`, "future_update", model.IssueUnknownEventType),
		relay("no event field", "41", `{"update_id":41}`, "$unknown", model.IssueUnknownEventType),
		relay("only null event fields", "42", `{"update_id":42,"message":null,"poll":null}`, "$unknown", model.IssueUnknownEventType),
		relay("multiple event fields", "43", `{"update_id":43,"message":{"chat":{"id":1}},"poll":{"id":"p"}}`, "$unknown", model.IssueUnknownEventStructure),
		{"null field beside a known one", `{"update_id":44,"message":null,"poll":{"id":"p"}}`, model.ScopeBot, "", "poll", "", "44", 0},
		{"missing chat id", `{"update_id":45,"message":{"date":1700000045,"chat":{"type":"group"}}}`, model.ScopeRelay, "", "message", model.IssueMissingChatID, "45", 1700000045000},
		relay("string chat id", "46", `{"update_id":46,"message":{"chat":{"id":"-46"}}}`, "message", model.IssueInvalidChatIDType),
		relay("fractional chat id", "47", `{"update_id":47,"message":{"chat":{"id":4.7}}}`, "message", model.IssueInvalidChatIDValue),
		relay("missing user id", "48", `{"update_id":48,"inline_query":{"id":"q","from":{}}}`, "inline_query", model.IssueMissingUserID),
		relay("string user id", "49", `{"update_id":49,"inline_query":{"id":"q","from":{"id":"49"}}}`, "inline_query", model.IssueInvalidUserIDType),
		relay("negative user id", "50", `{"update_id":50,"inline_query":{"id":"q","from":{"id":-50}}}`, "inline_query", model.IssueInvalidUserIDValue),
		relay("non-object known event", "51", `{"update_id":51,"message":"text"}`, "message", model.IssueUnknownEventStructure),
	}
}

// fallbackFixtures have no usable update_id: relay scope, no source event,
// and the body_sha256_v1 fallback deduplication key.
var fallbackFixtures = []string{
	`{"message":{"date":1700000060,"chat":{"id":60}}}`,
	`{"update_id":"61","message":{"chat":{"id":61}}}`,
	`{"update_id":-62,"message":{"chat":{"id":62}}}`,
	`{"update_id":6.3,"message":{"chat":{"id":63}}}`,
	`{"update_id":9223372036854775808,"message":{"chat":{"id":64}}}`,
}

func recipientIdentity(scope model.Scope, id string) string {
	switch scope {
	case model.ScopeChat:
		return "telegram:42:chat:" + id
	case model.ScopeUser:
		return "telegram:42:user:" + id
	default:
		return "telegram:42:" + string(scope)
	}
}

// storedMessage finds the message accepted for a platform deduplication key
// and checks that it sits in the expected Recipient queue.
func (e *env) storedMessage(t *testing.T, dedupKey, identity string) model.CanonicalMessage {
	t.Helper()
	digest := sha256.Sum256([]byte("telegram\ntelegram\n42\n" + dedupKey))
	id, err := e.do("HGET", "hr1:d:"+hex.EncodeToString(digest[:]), "message_id").ToString()
	if err != nil {
		t.Fatalf("dedup record for %q: %v", dedupKey, err)
	}
	queue, _ := e.do("LRANGE", "hr1:r:"+identity+":q", "0", "-1").AsStrSlice()
	found := false
	for _, q := range queue {
		found = found || q == id
	}
	if !found {
		t.Errorf("message %s not in queue %s (%v)", id, identity, queue)
	}
	blob, err := e.do("GET", "hr1:m:"+id).ToString()
	if err != nil {
		t.Fatalf("blob %s: %v", id, err)
	}
	var msg model.CanonicalMessage
	if err := json.Unmarshal([]byte(blob), &msg); err != nil {
		t.Fatalf("blob decode: %v\n%s", err, blob)
	}
	return msg
}

// TestTelegramFixturesOverHTTP drives every row of the event-to-Recipient
// table and every routing rule through the signed webhook route and asserts
// the Recipient queue and Canonical Message at the storage seam.
func TestTelegramFixturesOverHTTP(t *testing.T) {
	e := compose(t, nil)
	for _, tc := range telegramFixtures() {
		t.Run(tc.name, func(t *testing.T) {
			if w := e.post(tc.body); w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}
			msg := e.storedMessage(t, tc.source, recipientIdentity(tc.scope, tc.id))
			want := model.Recipient{Scope: tc.scope, BotPlatform: "telegram", BotID: "42"}
			switch tc.scope {
			case model.ScopeChat:
				want.ChatID = tc.id
			case model.ScopeUser:
				want.UserID = tc.id
			}
			if msg.Recipient != want {
				t.Errorf("recipient = %+v, want %+v", msg.Recipient, want)
			}
			if msg.PlatformEventType != tc.event || msg.RoutingIssue != tc.issue || msg.SourceEventID != tc.source {
				t.Errorf("event %q issue %q source %q; want %q %q %q", msg.PlatformEventType, msg.RoutingIssue, msg.SourceEventID, tc.event, tc.issue, tc.source)
			}
			switch {
			case tc.occurred == 0 && msg.OccurredMs != nil:
				t.Errorf("occurred_ms = %d, want omitted", *msg.OccurredMs)
			case tc.occurred != 0 && (msg.OccurredMs == nil || *msg.OccurredMs != tc.occurred):
				t.Errorf("occurred_ms = %v, want %d", msg.OccurredMs, tc.occurred)
			}
			var payload, original map[string]any
			_ = json.Unmarshal(msg.Payload, &payload)
			_ = json.Unmarshal([]byte(tc.body), &original)
			if len(payload) != len(original) {
				t.Errorf("payload lost top-level fields: %s", msg.Payload)
			}
		})
	}

	for i, body := range fallbackFixtures {
		sum := sha256.Sum256([]byte(body))
		key := "body_sha256_v1:" + base64.RawURLEncoding.EncodeToString(sum[:])
		if w := e.post(body); w.Code != http.StatusOK {
			t.Fatalf("fallback %d: status = %d", i, w.Code)
		}
		msg := e.storedMessage(t, key, "telegram:42:relay")
		if msg.Recipient.Scope != model.ScopeRelay || msg.RoutingIssue != model.IssueInvalidSourceEventID || msg.SourceEventID != "" || msg.PlatformEventType != "message" {
			t.Errorf("fallback %d: %+v", i, msg)
		}
		// The fallback key deduplicates the exact bytes.
		before := len(e.keys("hr1:m:*"))
		e.post(body)
		if after := len(e.keys("hr1:m:*")); after != before {
			t.Errorf("fallback %d: repeat created a message", i)
		}
	}

	// Routing issues are counted by bounded reason; the unknown event-field
	// name never becomes a label value.
	families, _ := e.registry.Gather()
	issues := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetValue() == "future_update" {
					t.Errorf("metric %s carries the unknown event name", f.GetName())
				}
			}
			if f.GetName() == "hookrelay_routing_issues_total" {
				for _, l := range m.GetLabel() {
					if l.GetName() == "reason" {
						issues[l.GetValue()] = m.GetCounter().GetValue()
					}
				}
			}
		}
	}
	for reason, want := range map[string]float64{
		model.IssueUnknownEventType:      3,
		model.IssueUnknownEventStructure: 2,
		model.IssueMissingChatID:         1,
		model.IssueInvalidChatIDType:     1,
		model.IssueInvalidChatIDValue:    1,
		model.IssueMissingUserID:         1,
		model.IssueInvalidUserIDType:     1,
		model.IssueInvalidUserIDValue:    1,
		model.IssueInvalidSourceEventID:  float64(len(fallbackFixtures)),
	} {
		if issues[reason] != want {
			t.Errorf("routing_issues_total{reason=%q} = %v, want %v", reason, issues[reason], want)
		}
	}
}

// TestTelegramEventTimeIssues pins that a missing or unusable documented
// timestamp omits occurred_ms, keeps routing, and is counted by reason.
func TestTelegramEventTimeIssues(t *testing.T) {
	e := compose(t, nil)
	for n, tc := range map[string]struct{ date, reason string }{
		"71": {``, "missing"},
		"72": {`,"date":"1700000000"`, "invalid_type"},
		"73": {`,"date":-5`, "invalid_value"},
		"74": {`,"date":1.5`, "invalid_value"},
	} {
		body := `{"update_id":` + n + `,"message":{"chat":{"id":-70}` + tc.date + `,"reply_to_message":{"date":1700000000}}}`
		if w := e.post(body); w.Code != http.StatusOK {
			t.Fatalf("%s: status = %d", tc.reason, w.Code)
		}
		msg := e.storedMessage(t, n, "telegram:42:chat:-70")
		if msg.OccurredMs != nil || msg.Recipient.Scope != model.ScopeChat {
			t.Errorf("%s: occurred %v scope %s", tc.reason, msg.OccurredMs, msg.Recipient.Scope)
		}
	}
	families, _ := e.registry.Gather()
	got := map[string]float64{}
	for _, f := range families {
		if f.GetName() != "hookrelay_event_time_issues_total" {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" {
					got[l.GetValue()] = m.GetCounter().GetValue()
				}
			}
		}
	}
	if got["missing"] != 1 || got["invalid_type"] != 1 || got["invalid_value"] != 2 {
		t.Errorf("event_time_issues_total = %v", got)
	}
	if !strings.Contains(strings.Join(e.keys("hr1:r:*"), ","), "telegram:42:chat:-70") {
		t.Error("events with unusable timestamps were not routed to their chat")
	}
}

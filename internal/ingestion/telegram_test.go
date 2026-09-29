package ingestion

import (
	"crypto/sha256"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maxp/hookrelay/internal/model"
)

func convert(t *testing.T, body string) Conversion {
	t.Helper()
	c, err := telegramConverter{}.Convert(ConvertInput{
		Body:        []byte(body),
		BodySHA256:  sha256.Sum256([]byte(body)),
		BotPlatform: "telegram",
		BotID:       "42",
	})
	if err != nil {
		t.Fatalf("convert %s: %v", body, err)
	}
	return c
}

// TestTelegramChatScopeRows pins the chat-scope rows of the event table,
// the event-time field per row, and the update identity.
func TestTelegramChatScopeRows(t *testing.T) {
	cases := []struct {
		event    string
		value    string
		chatID   string
		occurred int64 // 0 means omitted
	}{
		{"message", `{"message_id":1,"date":1700000000,"chat":{"id":-1001234567890},"from":{"id":7}}`, "-1001234567890", 1700000000000},
		{"edited_message", `{"date":1600000000,"edit_date":1700000001,"chat":{"id":5}}`, "5", 1700000001000},
		{"channel_post", `{"date":1700000002,"chat":{"id":-100}}`, "-100", 1700000002000},
		{"edited_channel_post", `{"date":1,"edit_date":1700000003,"chat":{"id":-100}}`, "-100", 1700000003000},
		{"business_message", `{"date":1700000004,"chat":{"id":9},"business_connection_id":"bc"}`, "9", 1700000004000},
		{"edited_business_message", `{"edit_date":1700000005,"chat":{"id":9}}`, "9", 1700000005000},
		{"deleted_business_messages", `{"chat":{"id":9},"message_ids":[1,2]}`, "9", 0},
		{"guest_message", `{"date":1700000006,"chat":{"id":11}}`, "11", 1700000006000},
		{"my_chat_member", `{"date":1700000007,"chat":{"id":-5},"from":{"id":7}}`, "-5", 1700000007000},
		{"chat_member", `{"date":1700000008,"chat":{"id":-5},"from":{"id":7}}`, "-5", 1700000008000},
		{"chat_join_request", `{"date":1700000009,"chat":{"id":-5},"from":{"id":7}}`, "-5", 1700000009000},
		{"chat_boost", `{"chat":{"id":-5},"boost":{"add_date":1700000010}}`, "-5", 1700000010000},
		{"removed_chat_boost", `{"chat":{"id":-5},"remove_date":1700000011}`, "-5", 1700000011000},
		{"message_reaction", `{"date":1700000012,"chat":{"id":3},"user":{"id":7}}`, "3", 1700000012000},
		{"message_reaction_count", `{"date":1700000013,"chat":{"id":3}}`, "3", 1700000013000},
		{"callback_query", `{"id":"q","from":{"id":7},"message":{"date":1,"chat":{"id":77}},"chat_instance":"ci"}`, "77", 0},
		{"stopped_message_generation", `{"chat":{"id":88}}`, "88", 0},
	}
	for _, tc := range cases {
		t.Run(tc.event, func(t *testing.T) {
			c := convert(t, `{"update_id":10,"`+tc.event+`":`+tc.value+`}`)
			want := model.Recipient{Scope: model.ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: tc.chatID}
			if c.Recipient != want || c.RoutingIssue != "" {
				t.Errorf("recipient = %+v issue %q, want %+v", c.Recipient, c.RoutingIssue, want)
			}
			if c.PlatformEventType != tc.event || c.SourceEventID != "10" || c.DedupKey != "10" {
				t.Errorf("identity = %q/%q/%q", c.PlatformEventType, c.SourceEventID, c.DedupKey)
			}
			switch {
			case tc.occurred == 0 && c.OccurredMs != nil:
				t.Errorf("occurred_ms = %d, want omitted", *c.OccurredMs)
			case tc.occurred != 0 && (c.OccurredMs == nil || *c.OccurredMs != tc.occurred):
				t.Errorf("occurred_ms = %v, want %d", c.OccurredMs, tc.occurred)
			}
		})
	}
}

// TestTelegramClassificationRules pins the non-chat classifications the
// converter must not get wrong while chat scope is exercised end to end.
func TestTelegramClassificationRules(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		scope model.Scope
		id    string
		issue string
		event string
	}{
		{"inline callback_query uses from.id", `{"update_id":1,"callback_query":{"id":"q","from":{"id":7},"inline_message_id":"im"}}`, model.ScopeUser, "7", "", "callback_query"},
		{"inline_query", `{"update_id":1,"inline_query":{"id":"q","from":{"id":8}}}`, model.ScopeUser, "8", "", "inline_query"},
		{"poll_answer voter_chat", `{"update_id":1,"poll_answer":{"poll_id":"p","voter_chat":{"id":-3},"option_ids":[0]}}`, model.ScopeChat, "-3", "", "poll_answer"},
		{"poll_answer user", `{"update_id":1,"poll_answer":{"poll_id":"p","user":{"id":9},"option_ids":[0]}}`, model.ScopeUser, "9", "", "poll_answer"},
		{"poll is bot scope", `{"update_id":1,"poll":{"id":"p"}}`, model.ScopeBot, "", "", "poll"},
		{"missing chat id", `{"update_id":1,"message":{"date":1,"chat":{}}}`, model.ScopeRelay, "", model.IssueMissingChatID, "message"},
		{"string chat id", `{"update_id":1,"message":{"chat":{"id":"5"}}}`, model.ScopeRelay, "", model.IssueInvalidChatIDType, "message"},
		{"float chat id", `{"update_id":1,"message":{"chat":{"id":5.5}}}`, model.ScopeRelay, "", model.IssueInvalidChatIDValue, "message"},
		{"chat id beyond int64", `{"update_id":1,"message":{"chat":{"id":9223372036854775808}}}`, model.ScopeRelay, "", model.IssueInvalidChatIDValue, "message"},
		{"negative user id", `{"update_id":1,"inline_query":{"from":{"id":-8}}}`, model.ScopeRelay, "", model.IssueInvalidUserIDValue, "inline_query"},
		{"zero chat id", `{"update_id":1,"message":{"chat":{"id":0}}}`, model.ScopeRelay, "", model.IssueInvalidChatIDValue, "message"},
		{"non-object known event", `{"update_id":1,"poll":5}`, model.ScopeRelay, "", model.IssueUnknownEventStructure, "poll"},
		{"unknown event", `{"update_id":1,"future_update":{"chat":{"id":5}}}`, model.ScopeRelay, "", model.IssueUnknownEventType, "future_update"},
		{"no event", `{"update_id":1,"message":null}`, model.ScopeRelay, "", model.IssueUnknownEventType, "$unknown"},
		{"two events", `{"update_id":1,"message":{"chat":{"id":1}},"poll":{"id":"p"}}`, model.ScopeRelay, "", model.IssueUnknownEventStructure, "$unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := convert(t, tc.body)
			got := c.Recipient.ChatID + c.Recipient.UserID
			if c.Recipient.Scope != tc.scope || got != tc.id || c.RoutingIssue != tc.issue || c.PlatformEventType != tc.event {
				t.Errorf("= scope %s id %q issue %q event %q; want %s %q %q %q", c.Recipient.Scope, got, c.RoutingIssue, c.PlatformEventType, tc.scope, tc.id, tc.issue, tc.event)
			}
			if err := c.Recipient.Validate(); err != nil {
				t.Errorf("invalid recipient: %v", err)
			}
		})
	}
}

// TestTelegramUpdateIdentity pins update_id parsing and the fallback key.
func TestTelegramUpdateIdentity(t *testing.T) {
	c := convert(t, `{"update_id":9223372036854775807,"message":{"chat":{"id":1}}}`)
	if c.SourceEventID != "9223372036854775807" || c.Recipient.Scope != model.ScopeChat {
		t.Errorf("max int64 update_id = %+v", c)
	}
	for _, body := range []string{
		`{"message":{"chat":{"id":1}}}`,
		`{"update_id":"10","message":{"chat":{"id":1}}}`,
		`{"update_id":-1,"message":{"chat":{"id":1}}}`,
		`{"update_id":1e3,"message":{"chat":{"id":1}}}`,
	} {
		c := convert(t, body)
		sum := sha256.Sum256([]byte(body))
		if c.Recipient.Scope != model.ScopeRelay || c.RoutingIssue != model.IssueInvalidSourceEventID || c.SourceEventID != "" || c.DedupKey != fallbackDedupKey(sum) {
			t.Errorf("%s: %+v", body, c)
		}
		if !strings.HasPrefix(c.DedupKey, "body_sha256_v1:") || strings.ContainsAny(c.DedupKey, "+/=") {
			t.Errorf("fallback key %q", c.DedupKey)
		}
	}
}

// TestTelegramPayloadAndShape pins compact re-encoding with integer
// precision and unknown fields, and the invalid-JSON rejections.
func TestTelegramPayloadAndShape(t *testing.T) {
	c := convert(t, "{ \"update_id\": 12345678901234567,\n \"message\": {\"chat\": {\"id\": 1}, \"text\": \"a<b\", \"x_new\": [1, 2.50]} }")
	want := `{"message":{"chat":{"id":1},"text":"a<b","x_new":[1,2.50]},"update_id":12345678901234567}`
	if string(c.Payload) != want {
		t.Errorf("payload = %s\nwant      %s", c.Payload, want)
	}
	for _, body := range []string{`[1]`, `"s"`, `42`, `{`, `{"a":1} {"b":2}`, strings.Repeat(`{"a":`, 41) + `1` + strings.Repeat(`}`, 41)} {
		if _, err := (telegramConverter{}).Convert(ConvertInput{Body: []byte(body), BotPlatform: "telegram", BotID: "42"}); err != ErrInvalidJSON {
			t.Errorf("%.40s: err = %v, want invalid JSON", body, err)
		}
	}
}

// TestTelegramVerifier pins the bounded failure reasons and the optional
// source allowlist.
func TestTelegramVerifier(t *testing.T) {
	_, allowed, _ := net.ParseCIDR("149.154.160.0/20")
	v := telegramVerifier{}
	header := func(values ...string) http.Header {
		h := http.Header{}
		for _, x := range values {
			h.Add(telegramSecretHeader, x)
		}
		return h
	}
	for name, tc := range map[string]struct {
		v      telegramVerifier
		header http.Header
		ip     string
		ok     bool
		reason string
	}{
		"match":              {v, header("secret-1"), "1.2.3.4", true, ""},
		"missing":            {v, header(), "1.2.3.4", false, ReasonCredentialMissing},
		"repeated":           {v, header("secret-1", "secret-1"), "1.2.3.4", false, ReasonCredentialRepeated},
		"mismatch":           {v, header("secret-2"), "1.2.3.4", false, ReasonCredentialMismatch},
		"no trimming":        {v, header(" secret-1"), "1.2.3.4", false, ReasonCredentialMismatch},
		"allowlisted source": {telegramVerifier{sourceCIDRs: []*net.IPNet{allowed}}, header("secret-1"), "149.154.167.1", true, ""},
		"denied source":      {telegramVerifier{sourceCIDRs: []*net.IPNet{allowed}}, header("secret-1"), "1.2.3.4", false, ReasonSourceIPDenied},
	} {
		ok, reason := tc.v.Verify(VerifyInput{Header: tc.header, SourceIP: net.ParseIP(tc.ip), Credential: "secret-1"})
		if ok != tc.ok || reason != tc.reason {
			t.Errorf("%s: = %v %q, want %v %q", name, ok, reason, tc.ok, tc.reason)
		}
	}
}

// TestSourceIP pins forwarded-address trust: only behind a trusted peer,
// right-to-left, first untrusted hop, malformed chains fall back.
func TestSourceIP(t *testing.T) {
	_, proxies, _ := net.ParseCIDR("10.0.0.0/8")
	trusted := []*net.IPNet{proxies}
	for name, tc := range map[string]struct {
		peer, xff string
		trusted   []*net.IPNet
		want      string
		malformed bool
	}{
		"no proxies ignores header":   {"10.0.0.1:5", "1.1.1.1", nil, "10.0.0.1", false},
		"untrusted peer ignores":      {"8.8.8.8:5", "1.1.1.1", trusted, "8.8.8.8", false},
		"first untrusted from right":  {"10.0.0.1:5", "2.2.2.2, 1.1.1.1, 10.0.0.2", trusted, "1.1.1.1", false},
		"malformed falls back":        {"10.0.0.1:5", "1.1.1.1, garbage", trusted, "10.0.0.1", true},
		"trusted peer without header": {"10.0.0.1:5", "", trusted, "10.0.0.1", false},
	} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = tc.peer
		if tc.xff != "" {
			r.Header.Set("X-Forwarded-For", tc.xff)
		}
		ip, malformed := sourceIP(r, tc.trusted)
		if ip.String() != tc.want || malformed != tc.malformed {
			t.Errorf("%s: = %s %v, want %s %v", name, ip, malformed, tc.want, tc.malformed)
		}
	}
}

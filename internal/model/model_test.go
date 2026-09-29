package model

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestRecipientIdentityAndValidation pins scope serialization and the
// identity-component rules.
func TestRecipientIdentityAndValidation(t *testing.T) {
	for _, tc := range []struct {
		r    Recipient
		want string
	}{
		{Recipient{Scope: ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: "-100123"}, "telegram:42:chat:-100123"},
		{Recipient{Scope: ScopeUser, BotPlatform: "telegram", BotID: "42", UserID: "7"}, "telegram:42:user:7"},
		{Recipient{Scope: ScopeBot, BotPlatform: "telegram", BotID: "42"}, "telegram:42:bot"},
		{Recipient{Scope: ScopeRelay, BotPlatform: "telegram", BotID: "42"}, "telegram:42:relay"},
	} {
		if err := tc.r.Validate(); err != nil {
			t.Errorf("%+v: %v", tc.r, err)
		}
		if got := tc.r.Identity(); got != tc.want {
			t.Errorf("identity = %q, want %q", got, tc.want)
		}
		if back, err := ParseIdentity(tc.want); err != nil || back != tc.r {
			t.Errorf("ParseIdentity(%q) = %+v, %v", tc.want, back, err)
		}
	}
	for name, r := range map[string]Recipient{
		"chat with user_id":  {Scope: ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: "1", UserID: "2"},
		"relay with chat_id": {Scope: ScopeRelay, BotPlatform: "telegram", BotID: "42", ChatID: "1"},
		"colon in chat_id":   {Scope: ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: "1:2"},
		"control in bot_id":  {Scope: ScopeBot, BotPlatform: "telegram", BotID: "4\x002"},
		"empty user_id":      {Scope: ScopeUser, BotPlatform: "telegram", BotID: "42"},
		"bad platform":       {Scope: ScopeBot, BotPlatform: "Telegram", BotID: "42"},
		"unknown scope":      {Scope: "group", BotPlatform: "telegram", BotID: "42"},
		"long bot_id":        {Scope: ScopeBot, BotPlatform: "telegram", BotID: strings.Repeat("1", 129)},
	} {
		if err := r.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// TestCanonicalMessageCodec pins field order, omission of absent optional
// fields, integer precision, and null rejection.
func TestCanonicalMessageCodec(t *testing.T) {
	occurred := int64(1740000000000)
	m := CanonicalMessage{
		MessageID:         "0195-id",
		ReceivedMs:        1740000000123,
		OccurredMs:        &occurred,
		Recipient:         Recipient{Scope: ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: "-100"},
		SourceEventID:     "9007199254740993",
		PlatformEventType: "message",
		Payload:           json.RawMessage(`{"update_id":9007199254740993,"text":"<b>"}`),
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"message_id":"0195-id","received_ms":1740000000123,"occurred_ms":1740000000000,"recipient":{"scope":"chat","bot_platform":"telegram","bot_id":"42","chat_id":"-100"},"source_event_id":"9007199254740993","platform_event_type":"message","payload":{"update_id":9007199254740993,"text":"<b>"}}`
	if string(data) != want {
		t.Errorf("encoded:\n%s\nwant:\n%s", data, want)
	}
	var back CanonicalMessage
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if back.Recipient != m.Recipient || *back.OccurredMs != occurred || string(back.Payload) != string(m.Payload) {
		t.Errorf("round trip = %+v", back)
	}

	relay := CanonicalMessage{
		MessageID:         "0195-id",
		ReceivedMs:        1,
		Recipient:         Recipient{Scope: ScopeRelay, BotPlatform: "telegram", BotID: "42"},
		PlatformEventType: "$unknown",
		Payload:           json.RawMessage(`{}`),
		RoutingIssue:      IssueUnknownEventType,
	}
	data, err = relay.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "null") || strings.Contains(string(data), "occurred_ms") || strings.Contains(string(data), "source_event_id") {
		t.Errorf("absent optional fields encoded: %s", data)
	}
	if !strings.HasSuffix(string(data), `"routing_issue":{"code":"unknown_event_type"}}`) {
		t.Errorf("routing issue encoding: %s", data)
	}

	if err := json.Unmarshal([]byte(`{"message_id":"x","received_ms":1,"occurred_ms":null,"recipient":{"scope":"bot","bot_platform":"telegram","bot_id":"1"},"platform_event_type":"poll","payload":{}}`), &back); err == nil {
		t.Error("null occurred_ms accepted")
	}
	if _, err := json.Marshal(CanonicalMessage{MessageID: "x", Recipient: relay.Recipient, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Error("missing platform_event_type accepted")
	}
}

// TestParseIdentityRejectsMalformed pins the reverse mapping's validation.
func TestParseIdentityRejectsMalformed(t *testing.T) {
	for _, id := range []string{"", "telegram:42", "telegram:42:chat", "telegram:42:bot:1", "telegram:42:group:1", "Telegram:42:bot", "telegram::relay"} {
		if _, err := ParseIdentity(id); err == nil {
			t.Errorf("%q accepted", id)
		}
	}
}

package ingestion

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"math"
	"net"
	"strconv"
	"strings"

	"github.com/maxp/hookrelay/internal/model"
)

// Telegram Verifier failure reasons.
const (
	ReasonCredentialMissing  = "credential_missing"
	ReasonCredentialRepeated = "credential_repeated"
	ReasonCredentialMismatch = "credential_mismatch"
	ReasonSourceIPDenied     = "source_ip_denied"
)

const telegramSecretHeader = "X-Telegram-Bot-Api-Secret-Token"

// telegramVerifier checks the secret-token header and, when configured, the
// Telegram source CIDR allowlist as defense-in-depth.
type telegramVerifier struct {
	sourceCIDRs []*net.IPNet
}

func (telegramVerifier) Headers() []string { return []string{telegramSecretHeader} }

func (v telegramVerifier) Verify(in VerifyInput) (bool, string) {
	values := in.Header.Values(telegramSecretHeader)
	switch {
	case len(values) == 0:
		return false, ReasonCredentialMissing
	case len(values) > 1:
		return false, ReasonCredentialRepeated
	}
	// Fixed-size digests keep the comparison constant-time in the length
	// too. Values are compared exactly, without trimming.
	provided := sha256.Sum256([]byte(values[0]))
	stored := sha256.Sum256([]byte(in.Credential))
	if subtle.ConstantTimeCompare(provided[:], stored[:]) != 1 {
		return false, ReasonCredentialMismatch
	}
	if len(v.sourceCIDRs) > 0 && !ipInAny(in.SourceIP, v.sourceCIDRs) {
		return false, ReasonSourceIPDenied
	}
	return true, ""
}

func ipInAny(ip net.IP, nets []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// telegramRule is one row of the event-to-Recipient table.
type telegramRule struct {
	// chatPath selects the applicable Chat.id; userPath the user fallback.
	chatPath []string
	userPath []string
	bot      bool
	// datePath is the event-time field inside the event object.
	datePath []string
	// resolve overrides the static paths for events with a conditional rule.
	resolve func(event map[string]any) telegramRule
}

var chatID = []string{"chat", "id"}

// telegramEvents is the event-to-Recipient table from the adapter design.
var telegramEvents = map[string]telegramRule{
	"message":                    {chatPath: chatID, datePath: []string{"date"}},
	"edited_message":             {chatPath: chatID, datePath: []string{"edit_date"}},
	"channel_post":               {chatPath: chatID, datePath: []string{"date"}},
	"edited_channel_post":        {chatPath: chatID, datePath: []string{"edit_date"}},
	"business_message":           {chatPath: chatID, datePath: []string{"date"}},
	"edited_business_message":    {chatPath: chatID, datePath: []string{"edit_date"}},
	"deleted_business_messages":  {chatPath: chatID},
	"guest_message":              {chatPath: chatID, datePath: []string{"date"}},
	"message_reaction":           {chatPath: chatID, datePath: []string{"date"}},
	"message_reaction_count":     {chatPath: chatID, datePath: []string{"date"}},
	"my_chat_member":             {chatPath: chatID, datePath: []string{"date"}},
	"chat_member":                {chatPath: chatID, datePath: []string{"date"}},
	"chat_join_request":          {chatPath: chatID, datePath: []string{"date"}},
	"chat_boost":                 {chatPath: chatID, datePath: []string{"boost", "add_date"}},
	"removed_chat_boost":         {chatPath: chatID, datePath: []string{"remove_date"}},
	"stopped_message_generation": {chatPath: chatID},
	"inline_query":               {userPath: []string{"from", "id"}},
	"chosen_inline_result":       {userPath: []string{"from", "id"}},
	"shipping_query":             {userPath: []string{"from", "id"}},
	"pre_checkout_query":         {userPath: []string{"from", "id"}},
	"purchased_paid_media":       {userPath: []string{"from", "id"}},
	"business_connection":        {userPath: []string{"user", "id"}, datePath: []string{"date"}},
	"subscription":               {userPath: []string{"user", "id"}},
	"managed_bot":                {userPath: []string{"user", "id"}},
	"poll":                       {bot: true},
	"callback_query": {resolve: func(event map[string]any) telegramRule {
		// The message chat has priority; from.id is the inline fallback.
		if event["message"] != nil {
			return telegramRule{chatPath: []string{"message", "chat", "id"}}
		}
		return telegramRule{userPath: []string{"from", "id"}}
	}},
	"poll_answer": {resolve: func(event map[string]any) telegramRule {
		if event["voter_chat"] != nil {
			return telegramRule{chatPath: []string{"voter_chat", "id"}}
		}
		return telegramRule{userPath: []string{"user", "id"}}
	}},
}

// telegramConverter maps a Telegram Update to a Conversion.
type telegramConverter struct{}

func (telegramConverter) Convert(in ConvertInput) (Conversion, error) {
	update, payload, err := decodeObject(in.Body)
	if err != nil {
		return Conversion{}, err
	}
	c := Conversion{
		Recipient: model.Recipient{Scope: model.ScopeRelay, BotPlatform: in.BotPlatform, BotID: in.BotID},
		Payload:   payload,
	}

	// Update identity.
	updateID, idOK := canonicalInt(update["update_id"], false)
	if idOK {
		c.SourceEventID = updateID
		c.DedupKey = updateID
	} else {
		c.DedupKey = fallbackDedupKey(in.BodySHA256)
		c.RoutingIssue = model.IssueInvalidSourceEventID
	}

	// Event field selection over non-null fields other than update_id.
	var fields []string
	for k, v := range update {
		if k != "update_id" && v != nil {
			fields = append(fields, k)
		}
	}
	switch {
	case len(fields) == 0:
		c.PlatformEventType = "$unknown"
		c.setIssue(model.IssueUnknownEventType)
		return c, nil
	case len(fields) > 1:
		c.PlatformEventType = "$unknown"
		c.setIssue(model.IssueUnknownEventStructure)
		return c, nil
	}
	name := fields[0]
	c.PlatformEventType = name
	rule, known := telegramEvents[name]
	if !known {
		c.setIssue(model.IssueUnknownEventType)
		return c, nil
	}
	event, isObject := update[name].(map[string]any)
	if !isObject {
		// A known event field must hold an object to be classified.
		c.setIssue(model.IssueUnknownEventStructure)
		return c, nil
	}
	if rule.resolve != nil {
		rule = rule.resolve(event)
	}

	if len(rule.datePath) > 0 {
		c.OccurredMs, c.OccurredIssue = eventTime(lookup(event, rule.datePath))
	}
	if !idOK {
		// Relay scope with invalid_source_event_id already selected.
		return c, nil
	}

	switch {
	case rule.bot:
		c.Recipient.Scope = model.ScopeBot
	case rule.chatPath != nil:
		id, issue := identifier(lookup(event, rule.chatPath), true)
		if issue != "" {
			c.setIssue(chatIssue[issue])
			return c, nil
		}
		c.Recipient.Scope = model.ScopeChat
		c.Recipient.ChatID = id
	case rule.userPath != nil:
		id, issue := identifier(lookup(event, rule.userPath), false)
		if issue != "" {
			c.setIssue(userIssue[issue])
			return c, nil
		}
		c.Recipient.Scope = model.ScopeUser
		c.Recipient.UserID = id
	}
	return c, nil
}

// setIssue records the first routing issue; relay scope already applies.
func (c *Conversion) setIssue(code string) {
	if c.RoutingIssue == "" {
		c.RoutingIssue = code
	}
}

// lookup walks nested objects; a missing or non-object step yields nil.
func lookup(obj map[string]any, path []string) any {
	var cur any = obj
	for _, p := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

// identifier issue classes, mapped to chat or user routing codes.
const (
	idMissing      = "missing"
	idInvalidType  = "type"
	idInvalidValue = "value"
)

// identifier validates a Telegram Chat or User identifier: a nonzero
// integer that fits int64, canonical decimal; user identifiers must be
// positive.
func identifier(v any, allowNegative bool) (string, string) {
	if v == nil {
		return "", idMissing
	}
	if _, ok := v.(json.Number); !ok {
		return "", idInvalidType
	}
	id, ok := canonicalInt(v, allowNegative)
	if !ok || id == "0" {
		return "", idInvalidValue
	}
	return id, ""
}

// Routing issue codes per identifier role and issue class.
var (
	chatIssue = map[string]string{
		idMissing: model.IssueMissingChatID, idInvalidType: model.IssueInvalidChatIDType, idInvalidValue: model.IssueInvalidChatIDValue,
	}
	userIssue = map[string]string{
		idMissing: model.IssueMissingUserID, idInvalidType: model.IssueInvalidUserIDType, idInvalidValue: model.IssueInvalidUserIDValue,
	}
)

// canonicalInt parses an unquoted JSON integer that fits int64 and returns
// its canonical decimal form. Negative values are rejected unless allowed.
func canonicalInt(v any, allowNegative bool) (string, bool) {
	n, ok := v.(json.Number)
	if !ok || strings.ContainsAny(string(n), ".eE") {
		return "", false
	}
	i, err := strconv.ParseInt(string(n), 10, 64)
	if err != nil || (i < 0 && !allowNegative) {
		return "", false
	}
	return strconv.FormatInt(i, 10), true
}

// Bounded event-time issue reasons.
const (
	TimeIssueMissing      = "missing"
	TimeIssueInvalidType  = "invalid_type"
	TimeIssueInvalidValue = "invalid_value" // non-integer or out of range
)

// eventTime converts the event's Telegram seconds to milliseconds. When the
// documented timestamp is missing or unusable, occurred_ms is omitted and
// one bounded issue reason is returned; routing is never affected.
func eventTime(v any) (*int64, string) {
	if v == nil {
		return nil, TimeIssueMissing
	}
	if _, ok := v.(json.Number); !ok {
		return nil, TimeIssueInvalidType
	}
	s, ok := canonicalInt(v, false)
	if !ok {
		return nil, TimeIssueInvalidValue
	}
	sec, _ := strconv.ParseInt(s, 10, 64)
	if sec <= 0 || sec > math.MaxInt64/1000 {
		return nil, TimeIssueInvalidValue
	}
	ms := sec * 1000
	return &ms, ""
}

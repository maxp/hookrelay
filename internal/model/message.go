package model

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// Bounded routing_issue.code values from the message contract.
const (
	IssueUnknownEventStructure = "unknown_event_structure"
	IssueUnknownEventType      = "unknown_event_type"
	IssueInvalidSourceEventID  = "invalid_source_event_id"
	IssueMissingChatID         = "missing_chat_id"
	IssueInvalidChatIDType     = "invalid_chat_id_type"
	IssueInvalidChatIDValue    = "invalid_chat_id_value"
	IssueMissingUserID         = "missing_user_id"
	IssueInvalidUserIDType     = "invalid_user_id_type"
	IssueInvalidUserIDValue    = "invalid_user_id_value"
	IssueUnexpectedJSONShape   = "unexpected_json_shape"
)

// CanonicalMessage is the platform-neutral message envelope. Optional fields
// are absent when empty (OccurredMs nil, SourceEventID and RoutingIssue
// empty); the codec never emits null.
type CanonicalMessage struct {
	MessageID         string
	ReceivedMs        int64
	OccurredMs        *int64
	Recipient         Recipient
	SourceEventID     string
	PlatformEventType string
	// Payload is the compact Canonical Payload JSON value.
	Payload      json.RawMessage
	RoutingIssue string
}

// wire types fix the field order and omission rules of the JSON contract.
type wireRecipient struct {
	Scope       Scope  `json:"scope"`
	BotPlatform string `json:"bot_platform"`
	BotID       string `json:"bot_id"`
	ChatID      string `json:"chat_id,omitempty"`
	UserID      string `json:"user_id,omitempty"`
}

type wireRoutingIssue struct {
	Code string `json:"code"`
}

type wireMessage struct {
	MessageID         string            `json:"message_id"`
	ReceivedMs        int64             `json:"received_ms"`
	OccurredMs        *int64            `json:"occurred_ms,omitempty"`
	Recipient         wireRecipient     `json:"recipient"`
	SourceEventID     string            `json:"source_event_id,omitempty"`
	PlatformEventType string            `json:"platform_event_type"`
	Payload           json.RawMessage   `json:"payload"`
	RoutingIssue      *wireRoutingIssue `json:"routing_issue,omitempty"`
}

// MarshalJSON implements json.Marshaler. encoding/json re-escapes HTML
// characters in its output; storage uses Encode for the exact compact form.
func (m CanonicalMessage) MarshalJSON() ([]byte, error) { return m.Encode() }

// Encode returns the compact Canonical Message contract without HTML
// escaping.
func (m CanonicalMessage) Encode() ([]byte, error) {
	if m.MessageID == "" || m.PlatformEventType == "" || len(m.Payload) == 0 {
		return nil, fmt.Errorf("model: canonical message is missing a required field")
	}
	if err := m.Recipient.Validate(); err != nil {
		return nil, err
	}
	w := wireMessage{
		MessageID:  m.MessageID,
		ReceivedMs: m.ReceivedMs,
		OccurredMs: m.OccurredMs,
		Recipient: wireRecipient{
			Scope:       m.Recipient.Scope,
			BotPlatform: m.Recipient.BotPlatform,
			BotID:       m.Recipient.BotID,
			ChatID:      m.Recipient.ChatID,
			UserID:      m.Recipient.UserID,
		},
		SourceEventID:     m.SourceEventID,
		PlatformEventType: m.PlatformEventType,
		Payload:           m.Payload,
	}
	if m.RoutingIssue != "" {
		w.RoutingIssue = &wireRoutingIssue{Code: m.RoutingIssue}
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(w); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// UnmarshalJSON decodes the contract, keeping integer precision in the
// payload and rejecting explicit nulls for optional fields.
func (m *CanonicalMessage) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for _, optional := range []string{"occurred_ms", "source_event_id", "routing_issue"} {
		if v, ok := raw[optional]; ok && bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return fmt.Errorf("model: %s must be absent rather than null", optional)
		}
	}
	var w wireMessage
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*m = CanonicalMessage{
		MessageID:  w.MessageID,
		ReceivedMs: w.ReceivedMs,
		OccurredMs: w.OccurredMs,
		Recipient: Recipient{
			Scope:       w.Recipient.Scope,
			BotPlatform: w.Recipient.BotPlatform,
			BotID:       w.Recipient.BotID,
			ChatID:      w.Recipient.ChatID,
			UserID:      w.Recipient.UserID,
		},
		SourceEventID:     w.SourceEventID,
		PlatformEventType: w.PlatformEventType,
		Payload:           w.Payload,
	}
	if w.RoutingIssue != nil {
		m.RoutingIssue = w.RoutingIssue.Code
	}
	if m.MessageID == "" || m.PlatformEventType == "" || len(m.Payload) == 0 {
		return fmt.Errorf("model: canonical message is missing a required field")
	}
	return m.Recipient.Validate()
}

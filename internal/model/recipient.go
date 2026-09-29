// Package model holds the values shared between feature modules: Recipient
// Identity and the Canonical Message with its explicit JSON codec. It has no
// HTTP, Valkey, metrics, or process concerns.
package model

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Scope is the Recipient scope.
type Scope string

const (
	ScopeChat  Scope = "chat"
	ScopeUser  Scope = "user"
	ScopeBot   Scope = "bot"
	ScopeRelay Scope = "relay"
)

var botPlatformPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// Recipient is a scope-specific Recipient Identity. ChatID is set only for
// chat scope and UserID only for user scope.
type Recipient struct {
	Scope       Scope
	BotPlatform string
	BotID       string
	ChatID      string
	UserID      string
}

// Validate enforces the identity-component rules of the message contract.
func (r Recipient) Validate() error {
	if !botPlatformPattern.MatchString(r.BotPlatform) {
		return fmt.Errorf("model: invalid bot_platform %q", r.BotPlatform)
	}
	if err := validateIdentifier("bot_id", r.BotID); err != nil {
		return err
	}
	switch r.Scope {
	case ScopeChat:
		if r.UserID != "" {
			return fmt.Errorf("model: chat scope carries a user_id")
		}
		return validateIdentifier("chat_id", r.ChatID)
	case ScopeUser:
		if r.ChatID != "" {
			return fmt.Errorf("model: user scope carries a chat_id")
		}
		return validateIdentifier("user_id", r.UserID)
	case ScopeBot, ScopeRelay:
		if r.ChatID != "" || r.UserID != "" {
			return fmt.Errorf("model: %s scope carries a chat_id or user_id", r.Scope)
		}
		return nil
	default:
		return fmt.Errorf("model: unknown recipient scope %q", r.Scope)
	}
}

// validateIdentifier: 1–128 UTF-8 bytes, no colon, no control characters.
func validateIdentifier(name, v string) error {
	if len(v) == 0 || len(v) > 128 || !utf8.ValidString(v) || strings.ContainsRune(v, ':') {
		return fmt.Errorf("model: invalid %s", name)
	}
	for _, c := range v {
		if c < 0x20 || c == 0x7f || (c >= 0x80 && c <= 0x9f) {
			return fmt.Errorf("model: invalid %s", name)
		}
	}
	return nil
}

// Identity is the internal colon-delimited Recipient storage identity:
// <bot_platform>:<bot_id>:(chat:<chat_id>|user:<user_id>|bot|relay). It is
// never part of a public contract.
func (r Recipient) Identity() string {
	base := r.BotPlatform + ":" + r.BotID + ":"
	switch r.Scope {
	case ScopeChat:
		return base + "chat:" + r.ChatID
	case ScopeUser:
		return base + "user:" + r.UserID
	default:
		return base + string(r.Scope)
	}
}

// ParseIdentity reverses Identity and validates the result.
func ParseIdentity(identity string) (Recipient, error) {
	parts := strings.Split(identity, ":")
	var r Recipient
	switch {
	case len(parts) == 3 && (parts[2] == string(ScopeBot) || parts[2] == string(ScopeRelay)):
		r = Recipient{Scope: Scope(parts[2]), BotPlatform: parts[0], BotID: parts[1]}
	case len(parts) == 4 && parts[2] == string(ScopeChat):
		r = Recipient{Scope: ScopeChat, BotPlatform: parts[0], BotID: parts[1], ChatID: parts[3]}
	case len(parts) == 4 && parts[2] == string(ScopeUser):
		r = Recipient{Scope: ScopeUser, BotPlatform: parts[0], BotID: parts[1], UserID: parts[3]}
	default:
		return Recipient{}, fmt.Errorf("model: malformed recipient identity")
	}
	if err := r.Validate(); err != nil {
		return Recipient{}, err
	}
	return r, nil
}

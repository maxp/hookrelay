// Package ingestion owns Webhook Type registration: the catalog mapping each
// supported Webhook Type to its Bot Platform and verification credential
// kinds. Verification, conversion, and response mapping join the definitions
// with the ingestion slice.
package ingestion

import (
	"fmt"
	"regexp"
)

// WebhookType selects the Verifier and Converter for a webhook route.
type WebhookType string

// BotPlatform names the messaging platform a bot operates in.
type BotPlatform string

// WebhookTypePattern is the route/type validation pattern: 1–32 ASCII
// characters matching [a-z][a-z0-9_-]*.
var WebhookTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// Definition describes one registered Webhook Type.
type Definition struct {
	Type            WebhookType
	Platform        BotPlatform
	CredentialKinds []string
}

// Registry holds the registered Webhook Types.
type Registry struct {
	byType map[WebhookType]Definition
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byType: map[WebhookType]Definition{}}
}

// Register adds a definition; duplicate types are a wiring error.
func (r *Registry) Register(d Definition) error {
	if !WebhookTypePattern.MatchString(string(d.Type)) {
		return fmt.Errorf("webhook type %q does not match %s", d.Type, WebhookTypePattern)
	}
	if d.Platform == "" {
		return fmt.Errorf("webhook type %q has no bot platform", d.Type)
	}
	if len(d.CredentialKinds) == 0 {
		return fmt.Errorf("webhook type %q has no credential kinds", d.Type)
	}
	if _, dup := r.byType[d.Type]; dup {
		return fmt.Errorf("webhook type %q registered twice", d.Type)
	}
	r.byType[d.Type] = d
	return nil
}

// Lookup returns the definition for a type.
func (r *Registry) Lookup(t WebhookType) (Definition, bool) {
	d, ok := r.byType[t]
	return d, ok
}

// Builtin registers the built-in Webhook Types. Telegram is the first
// production Bot Platform adapter.
func Builtin() (*Registry, error) {
	r := NewRegistry()
	if err := r.Register(Definition{
		Type:            "telegram",
		Platform:        "telegram",
		CredentialKinds: []string{"secret_token"},
	}); err != nil {
		return nil, err
	}
	return r, nil
}

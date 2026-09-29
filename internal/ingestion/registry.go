// Package ingestion owns the webhook pipeline: route resolution, Webhook
// Type registration, verification, conversion, response mapping, atomic
// acceptance through a caller-owned MessageAcceptor, and the Telegram
// adapter. The /webhook/ HTTP transport stays local to this package.
package ingestion

import (
	"fmt"
	"net"
	"regexp"
)

// WebhookType selects the Verifier and Converter for a webhook route.
type WebhookType string

// BotPlatform names the messaging platform a bot operates in.
type BotPlatform string

// WebhookTypePattern is the route/type validation pattern: 1–32 ASCII
// characters matching [a-z][a-z0-9_-]*.
var WebhookTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// Definition describes one registered Webhook Type. Verifier, Converter,
// and ResponseMapper stay separate capabilities.
type Definition struct {
	Type            WebhookType
	Platform        BotPlatform
	CredentialKinds []string
	Verifier        Verifier
	Converter       Converter
	ResponseMapper  ResponseMapper
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
	if d.Verifier == nil || d.Converter == nil {
		return fmt.Errorf("webhook type %q needs a verifier and a converter", d.Type)
	}
	if d.ResponseMapper == nil {
		d.ResponseMapper = DefaultResponses{}
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

// BuiltinOptions carries operator configuration for the built-in adapters.
type BuiltinOptions struct {
	// TelegramSourceCIDRs is the optional Telegram source allowlist.
	TelegramSourceCIDRs []*net.IPNet
}

// Builtin registers the built-in Webhook Types. Telegram is the first
// production Bot Platform adapter.
func Builtin(opts BuiltinOptions) (*Registry, error) {
	r := NewRegistry()
	if err := r.Register(Definition{
		Type:            "telegram",
		Platform:        "telegram",
		CredentialKinds: []string{"secret_token"},
		Verifier:        telegramVerifier{sourceCIDRs: opts.TelegramSourceCIDRs},
		Converter:       telegramConverter{},
		ResponseMapper:  DefaultResponses{},
	}); err != nil {
		return nil, err
	}
	return r, nil
}

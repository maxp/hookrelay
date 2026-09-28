package cli

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// randomSecret returns 32 random bytes encoded as base64url without padding —
// 256 bits of entropy, satisfying the ≥64-bit requirement with margin.
// Cryptographic randomness failure aborts the operation; it never falls back
// to non-cryptographic randomness.
func randomSecret() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// randomWebhookID returns wh_<base64url-128-bit-random>, the server-generated
// Webhook Identifier form.
func randomWebhookID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return "wh_" + base64.RawURLEncoding.EncodeToString(buf), nil
}

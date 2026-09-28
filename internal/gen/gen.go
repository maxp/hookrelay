// Package gen provides injectable generation for UUIDv7 identifiers and
// cryptographically random tokens. Production randomness uses crypto/rand and
// never falls back to non-cryptographic randomness; tests use deterministic
// adapters.
package gen

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"

	"github.com/google/uuid"
)

// Gen produces identifiers.
type Gen interface {
	// UUIDv7 returns a syntactically valid UUIDv7 string.
	UUIDv7() string
	// Base64URL returns n random bytes encoded as base64url without padding.
	Base64URL(n int) (string, error)
}

// Crypto is the production generator.
type Crypto struct{}

func (Crypto) UUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Identifier generation failure aborts the caller; it never degrades
		// to a different UUID version.
		panic("uuid v7 generation failed: " + err.Error())
	}
	return id.String()
}

func (Crypto) Base64URL(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand failed: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// Fixed is a deterministic generator for tests.
type Fixed struct {
	UUID   string
	Values []string // returned by Base64URL in order
	calls  int
}

func (f *Fixed) UUIDv7() string { return f.UUID }

func (f *Fixed) Base64URL(n int) (string, error) {
	if f.calls >= len(f.Values) {
		return "", fmt.Errorf("no fixed value for call %d", f.calls)
	}
	v := f.Values[f.calls]
	f.calls++
	return v, nil
}

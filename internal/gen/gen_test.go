package gen

import (
	"strings"
	"testing"
)

// TestCryptoUUIDv7Shape pins UUIDv7 syntax (time-ordered variant bits).
func TestCryptoUUIDv7Shape(t *testing.T) {
	g := Crypto{}
	id := g.UUIDv7()
	if len(id) != 36 || strings.Count(id, "-") != 4 {
		t.Fatalf("uuid shape: %q", id)
	}
	// UUIDv7: version nibble '7' at position 14, variant high bits 10 at 19.
	if id[14] != '7' {
		t.Errorf("version nibble = %q, want '7'", id[14])
	}
	if id[19] != '8' && id[19] != '9' && id[19] != 'a' && id[19] != 'b' {
		t.Errorf("variant nibble = %q, want 8/9/a/b", id[19])
	}
}

// TestCryptoBase64URL pins length and alphabet (no padding, url-safe).
func TestCryptoBase64URL(t *testing.T) {
	v, err := Crypto{}.Base64URL(16)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 22 || strings.ContainsAny(v, "+/=") {
		t.Errorf("base64url(16) = %q, want 22 unpadded url-safe chars", v)
	}
}

// TestFixedDeterminism pins the deterministic test generator.
func TestFixedDeterminism(t *testing.T) {
	g := &Fixed{UUID: "u1", Values: []string{"a", "b"}}
	if g.UUIDv7() != "u1" {
		t.Fatal("fixed uuid")
	}
	a, _ := g.Base64URL(16)
	b, _ := g.Base64URL(16)
	if a != "a" || b != "b" {
		t.Fatalf("fixed values: %q %q", a, b)
	}
	if _, err := g.Base64URL(16); err == nil {
		t.Fatal("exhausted fixed generator must fail, not fall back")
	}
}

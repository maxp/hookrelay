package ingestion

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/maxp/hookrelay/internal/model"
)

// VerifyInput carries only what a Verifier may see: the headers it declared,
// the exact body, the resolved source IP, and the stored credential.
type VerifyInput struct {
	Header     http.Header
	Body       []byte
	SourceIP   net.IP
	Credential string
}

// Verifier authenticates a webhook request. It returns false with one
// bounded reason on failure.
type Verifier interface {
	// Headers lists the only request headers the Verifier receives.
	Headers() []string
	Verify(in VerifyInput) (ok bool, reason string)
}

// ConvertInput is a verified request body with its Bot Identity.
type ConvertInput struct {
	Body        []byte
	BodySHA256  [sha256.Size]byte
	BotPlatform string
	BotID       string
}

// Conversion is the platform-neutral result of a Converter.
type Conversion struct {
	Recipient         model.Recipient
	PlatformEventType string
	SourceEventID     string
	DedupKey          string
	OccurredMs        *int64
	// OccurredIssue is the bounded reason the event's documented timestamp
	// was missing or unusable; empty when occurred_ms is set or the event
	// type has no timestamp.
	OccurredIssue string
	RoutingIssue  string
	Payload       json.RawMessage
}

// ErrInvalidJSON rejects a body that is not a valid JSON object for the
// Webhook Type.
var ErrInvalidJSON = errors.New("ingestion: invalid JSON")

// Converter is a pure transformation from verified bytes to a Conversion.
type Converter interface {
	Convert(in ConvertInput) (Conversion, error)
}

// maxJSONDepth is the maximum nesting depth of a webhook payload.
const maxJSONDepth = 40

// decodeObject decodes a top-level JSON object with integer-aware numbers,
// enforces the nesting depth, and returns the compact re-encoded payload.
func decodeObject(body []byte) (map[string]any, json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, nil, ErrInvalidJSON
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, nil, ErrInvalidJSON
	}
	obj, ok := v.(map[string]any)
	if !ok || depth(v) > maxJSONDepth {
		return nil, nil, ErrInvalidJSON
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(obj); err != nil {
		return nil, nil, ErrInvalidJSON
	}
	return obj, json.RawMessage(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// depth is the nesting depth of a decoded JSON value.
func depth(v any) int {
	var children []any
	switch t := v.(type) {
	case map[string]any:
		for _, c := range t {
			children = append(children, c)
		}
	case []any:
		children = t
	default:
		return 0
	}
	deepest := 0
	for _, c := range children {
		deepest = max(deepest, depth(c))
	}
	return deepest + 1
}

// fallbackDedupKey is body_sha256_v1:<base64url> of the exact request body.
func fallbackDedupKey(sum [sha256.Size]byte) string {
	return "body_sha256_v1:" + base64.RawURLEncoding.EncodeToString(sum[:])
}

// dedupIdentityDigest is the lowercase-hex SHA-256 over
// webhook_type \n bot_platform \n bot_id \n platform_deduplication_key.
func dedupIdentityDigest(webhookType, botPlatform, botID, key string) string {
	sum := sha256.Sum256([]byte(webhookType + "\n" + botPlatform + "\n" + botID + "\n" + key))
	return hex.EncodeToString(sum[:])
}

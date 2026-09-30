package administration

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
)

// Shared list bounds of the paginated Admin API routes.
const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// parseListLimit validates the shared list limit: default 50, range 1–200.
func parseListLimit(limit string) (int, error) {
	if limit == "" {
		return defaultListLimit, nil
	}
	v, err := strconv.Atoi(limit)
	if err != nil || v < 1 || v > maxListLimit {
		return 0, BadRequestError{msg: "limit must be between 1 and 200"}
	}
	return v, nil
}

// decodeCursor decodes an opaque base64url JSON cursor; nil when absent.
// complete reports whether the decoded position names a member.
func decodeCursor[T any](cursor string, complete func(T) bool) (*T, error) {
	if cursor == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	var c T
	if err != nil || json.Unmarshal(raw, &c) != nil || !complete(c) {
		return nil, BadRequestError{msg: "cursor is malformed", code: "invalid_cursor"}
	}
	return &c, nil
}

// encodeCursor is the opaque form of a list position.
func encodeCursor(position any) string {
	raw, _ := json.Marshal(position)
	return base64.RawURLEncoding.EncodeToString(raw)
}

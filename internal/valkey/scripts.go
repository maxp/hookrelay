package valkey

import (
	"embed"
	"fmt"
	"strings"

	"github.com/valkey-io/valkey-go"
)

//go:embed scripts/*.lua
var scriptsFS embed.FS

func init() {
	registry = map[string]*Script{}
	register("endpoint_create_v1", "created conflict bot_endpoint_limit wrong_type")
}

// Result is a typed script result: the bounded status code plus the
// documented positional fields for that status. The parser rejects unknown
// statuses and invalid shapes.
type Result struct {
	Script string
	Status string
	Fields []valkey.ValkeyMessage
}

// Field returns the i-th positional field (0-based, after the status).
func (r *Result) Field(i int) (valkey.ValkeyMessage, error) {
	if i < 0 || i >= len(r.Fields) {
		return valkey.ValkeyMessage{}, fmt.Errorf("valkey: script %s status %s: missing field %d", r.Script, r.Status, i)
	}
	return r.Fields[i], nil
}

func parseResult(script string, statuses []string, msg valkey.ValkeyMessage) (*Result, error) {
	items, err := msg.ToArray()
	if err != nil {
		return nil, fmt.Errorf("valkey: script %s: result is not an array: %w", script, err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("valkey: script %s: empty result array", script)
	}
	status, err := items[0].ToString()
	if err != nil {
		return nil, fmt.Errorf("valkey: script %s: status is not a string: %w", script, err)
	}
	for _, known := range statuses {
		if status == known {
			return &Result{Script: script, Status: status, Fields: items[1:]}, nil
		}
	}
	return nil, fmt.Errorf("valkey: script %s: unknown status %q (expected one of: %s)", script, status, strings.Join(statuses, ", "))
}

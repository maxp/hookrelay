package valkey

import (
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/valkey-io/valkey-go"
)

//go:embed scripts/*.lua
var scriptsFS embed.FS

func init() {
	registry = map[string]*Script{}
	register("endpoint_create_v1", 1, map[string]int{
		"created":            2, // created_ms, updated_ms
		"conflict":           0,
		"bot_endpoint_limit": 0,
		"wrong_type":         0,
	})
}

// Result is a typed script result: the bounded status code plus the
// documented positional fields for that status. The parser rejects unknown
// statuses and tuples whose field count differs from the registered arity.
type Result struct {
	Script string
	Status string
	Fields []valkey.ValkeyMessage
}

func parseResult(script string, arity map[string]int, msg valkey.ValkeyMessage) (*Result, error) {
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
	want, known := arity[status]
	if !known {
		statuses := make([]string, 0, len(arity))
		for s := range arity {
			statuses = append(statuses, s)
		}
		sort.Strings(statuses)
		return nil, fmt.Errorf("valkey: script %s: unknown status %q (expected one of: %s)", script, status, strings.Join(statuses, ", "))
	}
	if got := len(items) - 1; got != want {
		return nil, fmt.Errorf("valkey: script %s status %s: invalid shape: %d fields, want %d", script, status, got, want)
	}
	return &Result{Script: script, Status: status, Fields: items[1:]}, nil
}

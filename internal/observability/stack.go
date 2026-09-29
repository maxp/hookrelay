package observability

import "strings"

// RedactedStack keeps goroutine headers, function names, and file:line
// frames of a Go stack trace and drops argument values, which could echo
// request data.
func RedactedStack(stack []byte) string {
	lines := strings.Split(strings.TrimSpace(string(stack)), "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "\t") && strings.HasSuffix(line, ")") {
			if open := strings.LastIndex(line, "("); open > 0 {
				lines[i] = line[:open] + "(...)"
			}
		}
	}
	return strings.Join(lines, "\n")
}

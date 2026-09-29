package observability

import (
	"strings"
	"testing"
)

// TestRedactedStack pins stack redaction: goroutine headers, function names,
// and file:line frames stay; argument values are dropped.
func TestRedactedStack(t *testing.T) {
	stack := "goroutine 7 [running]:\n" +
		"runtime/debug.Stack()\n" +
		"\t/usr/local/go/src/runtime/debug/stack.go:26 +0x5e\n" +
		"panic({0x7b5a40?, 0xc000012345?})\n" +
		"\t/usr/local/go/src/runtime/panic.go:785 +0x132\n" +
		"github.com/maxp/hookrelay/internal/delivery.(*Maintenance).Run(0xc0001a2000, {0x8d7c80, 0xc0000b4000})\n" +
		"\t/src/internal/delivery/maintenance.go:120 +0x45\n" +
		"created by github.com/maxp/hookrelay/internal/app.(*App).Run in goroutine 1\n"
	got := RedactedStack([]byte(stack))
	for _, want := range []string{
		"goroutine 7 [running]:",
		"panic(...)",
		"github.com/maxp/hookrelay/internal/delivery.(*Maintenance).Run(...)",
		"\t/src/internal/delivery/maintenance.go:120 +0x45",
		"created by github.com/maxp/hookrelay/internal/app.(*App).Run in goroutine 1",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("redacted stack lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "0xc000012345") || strings.Contains(got, "{0x") {
		t.Errorf("argument values kept:\n%s", got)
	}
}

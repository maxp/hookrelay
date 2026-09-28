// Command hookrelay is the single executable providing the serve, admin,
// generate, and version command families plus the healthcheck command used by
// releases and containers. The entry point only delegates; it contains no
// HTTP, domain, Valkey, or observability logic.
package main

import (
	"os"

	"github.com/maxp/hookrelay/internal/cli"
)

func main() {
	os.Exit(cli.Main(os.Args[1:]))
}

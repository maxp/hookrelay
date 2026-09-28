# 02: Application scaffold

**What to build:** The hookrelay executable starts in Compose against a pinned, production-persisting Valkey: two listeners, the full validated configuration surface, contract-shaped structured logs, metrics endpoint, liveness health, graceful shutdown, distroless image, and the CI skeleton. No domain behavior yet — the pipes the vertical slices flow through.

**Blocked by:** 01 (valkey-go client spike — versions must be pinned first).

**Status:** done

- [x] One executable with `serve`, `generate` (consumer-secret, admin-secret, webhook-id), `version`, `healthcheck` commands; version embeds commit/build time/dirty state via linker flags, human and JSON output
- [x] Full `HOOKRELAY_*` configuration surface per the configuration contract: flag > env > default precedence, secret file/env exclusivity, secret length bounds, listener addresses, Valkey URL with redacted userinfo, log level, trusted-proxy and Telegram CIDRs, capacity/retention/lease/maintenance settings; invalid or contradictory configuration exits with code 2
- [x] Production validation enforced where applicable (explicit Valkey URL, nonzero rate limits, AOF + `noeviction` requirement, secure admin cookie, `EXPECTED_PEAK_RATE` dedup capacity formula); settings consumed only by later tickets are validated but not yet enforced
- [x] Structured slog JSON on stdout with the bounded envelope (`timestamp_ms`, `level`, `service`, `version`, `event`, `message`) via centralized attribute handling; redaction rules active at every level; log-write failure never panics
- [x] Public and administrative listeners per configured addresses; `/metrics` (private Prometheus registry) and `/health/live` (no dependency checks) on the admin listener
- [x] Graceful shutdown: 30-second deadline, first signal graceful / second forced, exit codes 0/1/2
- [x] `CGO_ENABLED=0` distroless static image, linux/amd64, nonroot, hardened container settings; `healthcheck` command works against `/health/live` with no shell in the image
- [x] Compose stack: pinned exact Valkey (AOF `everysec`, `noeviction`, named volume, healthcheck) + hookrelay waiting on Valkey health; secrets mounted read-only from the ignored `.secrets/` directory (mode 0600); admin port not published; `.env.example` with non-secret settings; optional observability profile (Prometheus + Grafana)
- [x] CI skeleton: gofmt, unit tests, race-enabled tests, `go vet`, `govulncheck`, linux/amd64 build, production image build
- [x] `/health/ready` exists and reports not-ready at this stage (never falsely ready before the reconciliation gate exists)

## Comments

- 2026-09-28: Implemented. Four packages (config, observability, app, cli) with envelope-contract and validation tests; the full HOOKRELAY_* surface parses with flag > env > default precedence, and production rules (explicit Valkey URL, nonzero rate limits, HTTPS origin, secure cookie, peak-rate capacity formula, retry-delay count) reject at startup with exit 2. Compose verified live: valkey healthy with AOF everysec + noeviction + named volume, hookrelay healthy with hardened container settings, loopback-only port publishes, admin listener bound inside the private network with an explicitly configured secure cookie (the non-loopback bind requires it; exposure control stays the loopback publish). Container runs under the host uid because file-backed compose secrets inherit source-file permissions (0600). `/health/ready` is honestly not-ready (`startup_reconciliation: pending`) and never flips without the gate. CI skeleton: gofmt, unit, race, vet, govulncheck (pinned), linux/amd64 build, production image build. Image builds with embedded version ldflags; distroless entrypoint defaults to `serve` (healthcheck command verified in-container). Build note for later tickets: the compose file-backed secret mount mode cannot be overridden; a real secret-manager or explicit bind-mount policy belongs to the deployment slice if 0600/uid coupling becomes a problem.

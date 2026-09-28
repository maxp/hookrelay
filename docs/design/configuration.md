# Process configuration

The first version uses command-line flags and environment variables only. It has no YAML or TOML process configuration file. Dynamic Webhook Endpoint configuration is managed through the Admin API and stored in Valkey. Secret-file settings are supported where explicitly documented.

Flags override environment variables, which override defaults.

## Listener addresses

```text
HOOKRELAY_PUBLIC_ADDRESS
HOOKRELAY_ADMIN_ADDRESS
```

Defaults:

```text
public = 0.0.0.0:8080
admin  = 127.0.0.1:8081
```

Equivalent flags are `--public-address` and `--admin-address`. Docker Compose may bind the admin listener to `0.0.0.0:8081` inside its private network without publishing the port externally. Production network policy restricts the administrative listener.

## Valkey

```text
HOOKRELAY_VALKEY_URL
```

The local default is:

```text
valkey://127.0.0.1:6379/0
```

Production must explicitly configure the URL and may use `valkeys://` for TLS. Credentials in URL userinfo are always redacted from logs and summaries. The selected Valkey database is dedicated to hookrelay.

## Consumer and Admin secrets

```text
HOOKRELAY_CONSUMER_SECRET_FILE
HOOKRELAY_CONSUMER_SECRET

HOOKRELAY_ADMIN_SECRET_FILE
HOOKRELAY_ADMIN_SECRET
```

For either secret, specifying both file and direct value is a startup error. Production prefers mounted files; local development may use environment values. Values are non-empty and limited to 8192 bytes. A single trailing newline is removed from file input. Secrets never appear in configuration summaries.

## Deployment environment

```text
HOOKRELAY_ENVIRONMENT=development|production
```

This setting has only documented effects:

- production requires AOF and `noeviction` checks;
- production requires nonzero webhook rate limits;
- production disallows an insecure administrative cookie;
- development permits explicitly documented local relaxations;
- the default log level is `debug` in development and `info` in production;
- startup validation severity follows the documented environment contract.

It does not silently change unrelated application semantics.

## Logging

```text
HOOKRELAY_LOG_LEVEL
```

Supported levels are `debug`, `info`, `warn`, and `error`. With no explicit override, development defaults to `debug` and production to `info`. The level is selected at startup; the first version has no runtime log-level API or signal-based toggle. There is no `trace` log level. Payload and secret redaction rules apply at every level, including `debug`.

## Configuration inspection

`GET /debug/config` is available only on the administrative listener and requires Admin authentication. It exposes effective non-secret process settings with their sources (`default`, `env`, `flag`, or `file`). Secret settings expose only whether a value is configured, never the value itself. Valkey URL userinfo is redacted. Webhook Endpoint counts may be shown, but endpoint credentials are never included. The endpoint is read-only and does not change runtime configuration.

## Profiling

```text
HOOKRELAY_PPROF_ENABLED=false
```

When enabled, selected pprof handlers are exposed only on the administrative listener and require Admin Bearer authentication. Browser sessions cannot access them. CPU duration is capped at 30 seconds, trace duration at 5 seconds, and only one profile request may run at once. Access logging to standard output and auditing in Valkey are best effort; a Valkey outage or audit-write failure does not itself block an authorized profile request. This diagnostic exception does not relax authentication or profiling limits.

## Administrative origin and cookie

```text
HOOKRELAY_ADMIN_ORIGIN
HOOKRELAY_ADMIN_COOKIE_SECURE
```

Local defaults are:

```text
HOOKRELAY_ADMIN_ORIGIN=http://127.0.0.1:8081
HOOKRELAY_ADMIN_COOKIE_SECURE=false
```

Production uses an explicit HTTPS origin and secure cookie. An insecure cookie is permitted only when the administrative listener binds to loopback; non-loopback plus insecure cookie is a startup error. The cookie name remains `hookrelay_admin`. Hookrelay does not trust arbitrary forwarded scheme or host headers in place of the configured origin.

## Trusted proxies and source addresses

```text
HOOKRELAY_TRUSTED_PROXY_CIDRS
HOOKRELAY_TELEGRAM_SOURCE_CIDRS
```

Forwarded client addresses are trusted only when the direct peer belongs to the configured trusted-proxy CIDRs. With no trusted proxies, hookrelay ignores forwarded headers and uses the TCP remote address. It selects the first untrusted address walking `X-Forwarded-For` from right to left; malformed chains fall back to the direct peer and generate a warning. Source IP may be logged in clear text but is never a Prometheus label.

Telegram source CIDRs are optional defense-in-depth. When configured, a mismatch fails verification, while the Telegram secret token remains mandatory. Hookrelay never downloads or updates Telegram ranges automatically.

TLS termination is outside hookrelay in the first version. A reverse proxy or load balancer exposes external HTTPS and connects to hookrelay over the trusted deployment network.

## Webhook rate limits

```text
HOOKRELAY_WEBHOOK_GLOBAL_RATE
HOOKRELAY_WEBHOOK_GLOBAL_BURST
HOOKRELAY_WEBHOOK_ENDPOINT_RATE
HOOKRELAY_WEBHOOK_ENDPOINT_BURST
HOOKRELAY_MAX_INFLIGHT_WEBHOOKS
```

Rates are positive decimal requests per second, and bursts are nonnegative integer token capacities. Zero disables a limit in development. Production requires nonzero global and endpoint limits. These configure process-local token buckets rather than a distributed quota. Maximum in-flight webhooks defaults to 100 and is enforced independently of rate.

## Valkey client resources

```text
HOOKRELAY_VALKEY_MAX_CONNECTIONS=20
HOOKRELAY_VALKEY_MIN_IDLE=2
```

Client resources are bounded. These accepted initial values are verified against the selected `valkey-go` connection and multiplexing model during the client spike.

## Queue capacity and retention

```text
HOOKRELAY_MAX_QUEUED_MESSAGES
HOOKRELAY_MAX_QUEUED_MESSAGES_PER_RECIPIENT
HOOKRELAY_MEMORY_ACCEPTANCE_STOP_PERCENT

HOOKRELAY_DEDUP_RETENTION
HOOKRELAY_DEDUP_MIN_RETENTION
HOOKRELAY_MAX_DEDUP_RECORDS
HOOKRELAY_EXPECTED_PEAK_RATE

HOOKRELAY_DLQ_RETENTION
```

Accepted defaults include:

```text
MAX_QUEUED_MESSAGES               = 100000
MAX_QUEUED_MESSAGES_PER_RECIPIENT = 1000
MEMORY_ACCEPTANCE_STOP_PERCENT    = 90
DEDUP_RETENTION                   = 168h
DEDUP_MIN_RETENTION               = 24h
MAX_DEDUP_RECORDS                 = 1000000
DLQ_RETENTION                     = 720h
```

Durations use Go duration syntax. Production requires `EXPECTED_PEAK_RATE` and validates that deduplication capacity can preserve minimum retention.

## Delivery and retry policy

```text
HOOKRELAY_INITIAL_LEASE_DURATION=60s
HOOKRELAY_LEASE_EXTENSION_DURATION=60s
HOOKRELAY_MAX_LEASE_LIFETIME=5m
HOOKRELAY_MAX_DELIVERY_ATTEMPTS=4
HOOKRELAY_RETRY_DELAYS=1s,5s,30s
HOOKRELAY_RETRY_JITTER_MIN=0.5
HOOKRELAY_RETRY_JITTER_MAX=1.0
```

The retry-delay count must equal maximum attempts minus one. Jitter satisfies:

```text
0 <= min <= max <= 1
```

All invalid or contradictory configuration is rejected at startup rather than silently corrected.

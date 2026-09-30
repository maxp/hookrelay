// Package config parses the hookrelay process configuration from command-line
// flags and environment variables and validates it at startup. It returns an
// immutable value and never connects to Valkey or starts runtime processes.
//
// Precedence: flags override environment variables, which override defaults.
// All invalid or contradictory configuration is rejected here rather than
// silently corrected.
package config

import (
	"flag"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment selects the deployment environment.
type Environment string

const (
	Development Environment = "development"
	Production  Environment = "production"
)

// Secret source bounds per the configuration contract. Entropy is the
// operator's responsibility; length validation cannot prove entropy.
const (
	minSecretBytes = 16
	maxSecretBytes = 8192
)

// Config is the immutable process configuration.
type Config struct {
	// Listeners.
	PublicAddress string
	AdminAddress  string

	// Valkey.
	ValkeyURL string

	// Secrets. Empty means not yet resolved; Serve resolves them from files
	// or environment at startup and never logs the values.
	ConsumerSecret     string
	ConsumerSecretFile string
	AdminSecret        string
	AdminSecretFile    string

	// Deployment environment and logging.
	Environment Environment
	LogLevel    string

	// Administrative origin and cookie policy.
	AdminOrigin       *url.URL
	AdminCookieSecure bool

	// PprofEnabled is validated in Milestone 1; the profiling handlers
	// arrive with a later slice.
	PprofEnabled bool

	// Source-address trust and adapter defense-in-depth.
	TrustedProxyCIDRs   []*net.IPNet
	TelegramSourceCIDRs []*net.IPNet

	// Webhook process protections.
	WebhookGlobalRate    float64
	WebhookGlobalBurst   int
	WebhookEndpointRate  float64
	WebhookEndpointBurst int
	MaxInflightWebhooks  int

	// Valkey client resources.
	ValkeyMaxConnections int
	ValkeyMinIdle        int

	// Queue capacity and retention.
	MaxQueuedMessages             int
	MaxQueuedMessagesPerRecipient int
	MemoryAcceptanceStopPercent   int

	// Deduplication.
	DedupRetention    time.Duration
	DedupMinRetention time.Duration
	MaxDedupRecords   int64
	ExpectedPeakRate  float64

	// Dead-letter retention.
	DLQRetention time.Duration

	// Delivery and retry policy.
	InitialLeaseDuration   time.Duration
	LeaseExtensionDuration time.Duration
	MaxLeaseLifetime       time.Duration
	MaxDeliveryAttempts    int
	RetryDelays            []time.Duration
	RetryJitterMin         float64
	RetryJitterMax         float64
	MaxActiveLeases        int
	MaxWaitingClaims       int
	// ClaimNotifications wakes waiting claims early (ADR 0008).
	ClaimNotifications bool

	// valkeyURLProvided records whether the Valkey URL was explicitly
	// supplied (flag or environment) as opposed to the local default.
	valkeyURLProvided bool

	// Maintenance.
	MaintenanceInterval             time.Duration
	MaintenanceIntervalJitter       time.Duration
	MaintenanceBatchSize            int
	MaintenanceMaxContinuousBatches int
}

// Production reports whether production validation severity applies.
func (c *Config) Production() bool { return c.Environment == Production }

// Load builds the configuration from command-line arguments and the given
// environment lookup. getenv is injectable for tests; production callers pass
// os.Getenv.
func Load(args []string, getenv func(string) string) (*Config, error) {
	flags := map[string]*string{}
	values := map[string]*string{}
	set := func(name string) *string {
		v := new(string)
		flags[name] = v
		values[name] = v
		return v
	}

	publicAddress := set("public-address")
	adminAddress := set("admin-address")
	valkeyURL := set("valkey-url")
	logLevel := set("log-level")
	environment := set("environment")
	adminOrigin := set("admin-origin")

	fs := newFlagSet("hookrelay serve")
	fs.StringVar(publicAddress, "public-address", "", "public listener address (env HOOKRELAY_PUBLIC_ADDRESS)")
	fs.StringVar(adminAddress, "admin-address", "", "administrative listener address (env HOOKRELAY_ADMIN_ADDRESS)")
	fs.StringVar(valkeyURL, "valkey-url", "", "Valkey URL (env HOOKRELAY_VALKEY_URL)")
	fs.StringVar(logLevel, "log-level", "", "log level: debug, info, warn, error (env HOOKRELAY_LOG_LEVEL)")
	fs.StringVar(environment, "environment", "", "deployment environment: development, production (env HOOKRELAY_ENVIRONMENT)")
	fs.StringVar(adminOrigin, "admin-origin", "", "administrative origin URL (env HOOKRELAY_ADMIN_ORIGIN)")
	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("parse flags: %w", err)
	}

	// Flags override environment, which overrides the documented default.
	envOr := func(flag, env, def string) string {
		if isSet(flags[flag]) {
			return *flags[flag]
		}
		if v := getenv(env); v != "" {
			return v
		}
		return def
	}
	get := func(env string) string { return getenv(env) }
	getInt := func(env string, def int) (int, error) {
		raw := get(env)
		if raw == "" {
			return def, nil
		}
		n, err := strconv.Atoi(raw)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", env, err)
		}
		return n, nil
	}
	// getBool accepts true/1 and false/0; empty keeps the default.
	getBool := func(env string, def bool) (bool, error) {
		switch raw := get(env); raw {
		case "":
			return def, nil
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		default:
			return false, fmt.Errorf("%s: must be a boolean, got %q", env, raw)
		}
	}
	getFloat := func(env string, def float64) (float64, error) {
		raw := get(env)
		if raw == "" {
			return def, nil
		}
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", env, err)
		}
		return f, nil
	}
	getInt64 := func(env string, def int64) (int64, error) {
		raw := get(env)
		if raw == "" {
			return def, nil
		}
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", env, err)
		}
		return n, nil
	}
	getDuration := func(env string, def time.Duration) (time.Duration, error) {
		raw := get(env)
		if raw == "" {
			return def, nil
		}
		d, err := time.ParseDuration(raw)
		if err != nil {
			return 0, fmt.Errorf("%s: %w", env, err)
		}
		return d, nil
	}

	c := &Config{
		PublicAddress:      envOr("public-address", "HOOKRELAY_PUBLIC_ADDRESS", "0.0.0.0:8080"),
		AdminAddress:       envOr("admin-address", "HOOKRELAY_ADMIN_ADDRESS", "127.0.0.1:8081"),
		ValkeyURL:          envOr("valkey-url", "HOOKRELAY_VALKEY_URL", "valkey://127.0.0.1:6379/0"),
		valkeyURLProvided:  isSet(flags["valkey-url"]) || getenv("HOOKRELAY_VALKEY_URL") != "",
		LogLevel:           envOr("log-level", "HOOKRELAY_LOG_LEVEL", ""),
		ConsumerSecret:     get("HOOKRELAY_CONSUMER_SECRET"),
		ConsumerSecretFile: get("HOOKRELAY_CONSUMER_SECRET_FILE"),
		AdminSecret:        get("HOOKRELAY_ADMIN_SECRET"),
		AdminSecretFile:    get("HOOKRELAY_ADMIN_SECRET_FILE"),
	}

	envRaw := envOr("environment", "HOOKRELAY_ENVIRONMENT", string(Development))
	switch Environment(envRaw) {
	case Development, Production:
		c.Environment = Environment(envRaw)
	default:
		return nil, fmt.Errorf("HOOKRELAY_ENVIRONMENT: must be development or production, got %q", envRaw)
	}
	if c.LogLevel == "" {
		if c.Production() {
			c.LogLevel = "info"
		} else {
			c.LogLevel = "debug"
		}
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("HOOKRELAY_LOG_LEVEL: must be debug, info, warn, or error, got %q", c.LogLevel)
	}

	originRaw := envOr("admin-origin", "HOOKRELAY_ADMIN_ORIGIN", "http://127.0.0.1:8081")
	origin, err := url.Parse(originRaw)
	if err != nil || origin.Scheme == "" || origin.Host == "" {
		return nil, fmt.Errorf("HOOKRELAY_ADMIN_ORIGIN: must be an absolute URL, got %q", originRaw)
	}
	c.AdminOrigin = origin

	// The session cookie is Secure by default only in production.
	if c.AdminCookieSecure, err = getBool("HOOKRELAY_ADMIN_COOKIE_SECURE", c.Production()); err != nil {
		return nil, err
	}
	if c.PprofEnabled, err = getBool("HOOKRELAY_PPROF_ENABLED", false); err != nil {
		return nil, err
	}

	parseCIDRs := func(env string) ([]*net.IPNet, error) {
		raw := get(env)
		if raw == "" {
			return nil, nil
		}
		var out []*net.IPNet
		for _, part := range strings.Split(raw, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			_, ipnet, err := net.ParseCIDR(part)
			if err != nil {
				return nil, fmt.Errorf("%s: invalid CIDR %q", env, part)
			}
			out = append(out, ipnet)
		}
		return out, nil
	}
	if c.TrustedProxyCIDRs, err = parseCIDRs("HOOKRELAY_TRUSTED_PROXY_CIDRS"); err != nil {
		return nil, err
	}
	if c.TelegramSourceCIDRs, err = parseCIDRs("HOOKRELAY_TELEGRAM_SOURCE_CIDRS"); err != nil {
		return nil, err
	}

	if c.WebhookGlobalRate, err = getFloat("HOOKRELAY_WEBHOOK_GLOBAL_RATE", 0); err != nil {
		return nil, err
	}
	if c.WebhookGlobalBurst, err = getInt("HOOKRELAY_WEBHOOK_GLOBAL_BURST", 0); err != nil {
		return nil, err
	}
	if c.WebhookEndpointRate, err = getFloat("HOOKRELAY_WEBHOOK_ENDPOINT_RATE", 0); err != nil {
		return nil, err
	}
	if c.WebhookEndpointBurst, err = getInt("HOOKRELAY_WEBHOOK_ENDPOINT_BURST", 0); err != nil {
		return nil, err
	}
	if c.MaxInflightWebhooks, err = getInt("HOOKRELAY_MAX_INFLIGHT_WEBHOOKS", 100); err != nil {
		return nil, err
	}
	if c.ValkeyMaxConnections, err = getInt("HOOKRELAY_VALKEY_MAX_CONNECTIONS", 20); err != nil {
		return nil, err
	}
	if c.ValkeyMinIdle, err = getInt("HOOKRELAY_VALKEY_MIN_IDLE", 2); err != nil {
		return nil, err
	}
	if c.MaxQueuedMessages, err = getInt("HOOKRELAY_MAX_QUEUED_MESSAGES", 100000); err != nil {
		return nil, err
	}
	if c.MaxQueuedMessagesPerRecipient, err = getInt("HOOKRELAY_MAX_QUEUED_MESSAGES_PER_RECIPIENT", 1000); err != nil {
		return nil, err
	}
	if c.MemoryAcceptanceStopPercent, err = getInt("HOOKRELAY_MEMORY_ACCEPTANCE_STOP_PERCENT", 90); err != nil {
		return nil, err
	}
	if c.DedupRetention, err = getDuration("HOOKRELAY_DEDUP_RETENTION", 168*time.Hour); err != nil {
		return nil, err
	}
	if c.DedupMinRetention, err = getDuration("HOOKRELAY_DEDUP_MIN_RETENTION", 24*time.Hour); err != nil {
		return nil, err
	}
	if c.MaxDedupRecords, err = getInt64("HOOKRELAY_MAX_DEDUP_RECORDS", 1000000); err != nil {
		return nil, err
	}
	if c.ExpectedPeakRate, err = getFloat("HOOKRELAY_EXPECTED_PEAK_RATE", 0); err != nil {
		return nil, err
	}
	if c.DLQRetention, err = getDuration("HOOKRELAY_DLQ_RETENTION", 720*time.Hour); err != nil {
		return nil, err
	}
	if c.InitialLeaseDuration, err = getDuration("HOOKRELAY_INITIAL_LEASE_DURATION", 60*time.Second); err != nil {
		return nil, err
	}
	if c.LeaseExtensionDuration, err = getDuration("HOOKRELAY_LEASE_EXTENSION_DURATION", 60*time.Second); err != nil {
		return nil, err
	}
	if c.MaxLeaseLifetime, err = getDuration("HOOKRELAY_MAX_LEASE_LIFETIME", 5*time.Minute); err != nil {
		return nil, err
	}
	if c.MaxDeliveryAttempts, err = getInt("HOOKRELAY_MAX_DELIVERY_ATTEMPTS", 4); err != nil {
		return nil, err
	}
	if c.RetryDelays, err = getRetryDelays(get("HOOKRELAY_RETRY_DELAYS"), []time.Duration{time.Second, 5 * time.Second, 30 * time.Second}); err != nil {
		return nil, err
	}
	if c.RetryJitterMin, err = getFloat("HOOKRELAY_RETRY_JITTER_MIN", 0.5); err != nil {
		return nil, err
	}
	if c.RetryJitterMax, err = getFloat("HOOKRELAY_RETRY_JITTER_MAX", 1.0); err != nil {
		return nil, err
	}
	if c.MaxActiveLeases, err = getInt("HOOKRELAY_MAX_ACTIVE_LEASES", 100); err != nil {
		return nil, err
	}
	if c.MaxWaitingClaims, err = getInt("HOOKRELAY_MAX_WAITING_CLAIMS", 20); err != nil {
		return nil, err
	}
	if c.ClaimNotifications, err = getBool("HOOKRELAY_CLAIM_NOTIFICATIONS", true); err != nil {
		return nil, err
	}
	if c.MaintenanceInterval, err = getDuration("HOOKRELAY_MAINTENANCE_INTERVAL", time.Second); err != nil {
		return nil, err
	}
	if c.MaintenanceIntervalJitter, err = getDuration("HOOKRELAY_MAINTENANCE_INTERVAL_JITTER", 250*time.Millisecond); err != nil {
		return nil, err
	}
	if c.MaintenanceBatchSize, err = getInt("HOOKRELAY_MAINTENANCE_BATCH_SIZE", 100); err != nil {
		return nil, err
	}
	if c.MaintenanceMaxContinuousBatches, err = getInt("HOOKRELAY_MAINTENANCE_MAX_CONTINUOUS_BATCHES", 5); err != nil {
		return nil, err
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func getRetryDelays(raw string, def []time.Duration) ([]time.Duration, error) {
	if raw == "" {
		return def, nil
	}
	var out []time.Duration
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("HOOKRELAY_RETRY_DELAYS: empty entry in %q", raw)
		}
		d, err := time.ParseDuration(part)
		if err != nil {
			return nil, fmt.Errorf("HOOKRELAY_RETRY_DELAYS: %w", err)
		}
		out = append(out, d)
	}
	return out, nil
}

func isSet(v *string) bool { return v != nil && *v != "" }

func newFlagSet(name string) *flag.FlagSet { return flag.NewFlagSet(name, flag.ContinueOnError) }

// LoadSecrets resolves a secret from its file source or direct environment
// value. Specifying both is an error. File input has one trailing newline
// removed. Values must be 16–8192 bytes after trimming.
func LoadSecrets(direct, file string, readFile func(string) ([]byte, error), label string) (string, error) {
	if direct != "" && file != "" {
		return "", fmt.Errorf("%s: both the secret file and the direct value are configured; configure exactly one", label)
	}
	value := direct
	if file != "" {
		data, err := readFile(file)
		if err != nil {
			return "", fmt.Errorf("%s: read secret file: %w", label, err)
		}
		value = string(data)
		value = strings.TrimSuffix(value, "\n")
		value = strings.TrimSuffix(value, "\r")
	}
	if value == "" {
		return "", fmt.Errorf("%s: no secret configured", label)
	}
	if n := len(value); n < minSecretBytes || n > maxSecretBytes {
		return "", fmt.Errorf("%s: secret must be %d–%d bytes, got %d bytes", label, minSecretBytes, maxSecretBytes, n)
	}
	return value, nil
}

// RedactedValkeyURL returns the Valkey URL with userinfo replaced by a
// placeholder, safe for logs and configuration summaries.
func (c *Config) RedactedValkeyURL() string {
	u, err := url.Parse(c.ValkeyURL)
	if err != nil {
		return "(invalid valkey url)"
	}
	if u.User != nil {
		u.User = url.UserPassword("redacted", "redacted")
	}
	return u.String()
}

// osReadFile is the production file reader used by Serve.
func OSReadFile(name string) ([]byte, error) { return os.ReadFile(name) }

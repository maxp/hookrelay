package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func getenv(mapEnv map[string]string) func(string) string {
	return func(key string) string { return mapEnv[key] }
}

func devEnv() map[string]string {
	return map[string]string{}
}

// TestLoadDefaults pins the documented local defaults.
func TestLoadDefaults(t *testing.T) {
	c, err := Load(nil, getenv(devEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PublicAddress != "0.0.0.0:8080" {
		t.Errorf("PublicAddress = %q", c.PublicAddress)
	}
	if c.AdminAddress != "127.0.0.1:8081" {
		t.Errorf("AdminAddress = %q", c.AdminAddress)
	}
	if c.ValkeyURL != "valkey://127.0.0.1:6379/0" {
		t.Errorf("ValkeyURL = %q", c.ValkeyURL)
	}
	if c.Environment != Development {
		t.Errorf("Environment = %q", c.Environment)
	}
	if c.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug default in development", c.LogLevel)
	}
	if !c.AdminCookieSecure {
		// dev default is insecure cookie on loopback admin listener — allowed.
		if !adminAddressIsLoopback(c.AdminAddress) {
			t.Errorf("insecure cookie with non-loopback admin address must fail validation")
		}
	}
	if c.MaxQueuedMessages != 100000 || c.MaxQueuedMessagesPerRecipient != 1000 {
		t.Errorf("queue capacity defaults: %d / %d", c.MaxQueuedMessages, c.MaxQueuedMessagesPerRecipient)
	}
	if c.DedupRetention != 168*time.Hour || c.DedupMinRetention != 24*time.Hour {
		t.Errorf("dedup retention defaults: %s / %s", c.DedupRetention, c.DedupMinRetention)
	}
	if c.MaxDedupRecords != 1000000 || c.DLQRetention != 720*time.Hour {
		t.Errorf("dedup records / dlq defaults: %d / %s", c.MaxDedupRecords, c.DLQRetention)
	}
	if c.InitialLeaseDuration != 60*time.Second || c.MaxLeaseLifetime != 5*time.Minute {
		t.Errorf("lease defaults: %s / %s", c.InitialLeaseDuration, c.MaxLeaseLifetime)
	}
	if len(c.RetryDelays) != 3 {
		t.Errorf("retry delays default: %v", c.RetryDelays)
	}
	if c.MaxActiveLeases != 100 || c.MaxWaitingClaims != 20 {
		t.Errorf("limits defaults: %d / %d", c.MaxActiveLeases, c.MaxWaitingClaims)
	}
	if !c.ClaimNotifications {
		t.Errorf("claim notifications default off")
	}
	if c.MaintenanceInterval != time.Second || c.MaintenanceBatchSize != 100 || c.MaintenanceMaxContinuousBatches != 5 {
		t.Errorf("maintenance defaults wrong")
	}
	if c.RedactedValkeyURL() != "valkey://127.0.0.1:6379/0" {
		t.Errorf("RedactedValkeyURL = %q", c.RedactedValkeyURL())
	}
}

// TestFlagOverridesEnvOverridesDefault pins the precedence contract.
func TestFlagOverridesEnvOverridesDefault(t *testing.T) {
	env := map[string]string{"HOOKRELAY_PUBLIC_ADDRESS": "0.0.0.0:9000"}
	c, err := Load(nil, getenv(env))
	if err != nil {
		t.Fatalf("Load env: %v", err)
	}
	if c.PublicAddress != "0.0.0.0:9000" {
		t.Errorf("env not applied: %q", c.PublicAddress)
	}
	c, err = Load([]string{"-public-address", "0.0.0.0:9001"}, getenv(env))
	if err != nil {
		t.Fatalf("Load flag: %v", err)
	}
	if c.PublicAddress != "0.0.0.0:9001" {
		t.Errorf("flag did not override env: %q", c.PublicAddress)
	}
}

// TestProductionValidation covers the documented production-only rules.
func TestProductionValidation(t *testing.T) {
	base := map[string]string{
		"HOOKRELAY_ENVIRONMENT":           "production",
		"HOOKRELAY_VALKEY_URL":            "valkeys://valkey.internal:6379/0",
		"HOOKRELAY_ADMIN_COOKIE_SECURE":   "true",
		"HOOKRELAY_ADMIN_ORIGIN":          "https://admin.internal:8081",
		"HOOKRELAY_WEBHOOK_GLOBAL_RATE":   "100",
		"HOOKRELAY_WEBHOOK_ENDPOINT_RATE": "50",
		"HOOKRELAY_EXPECTED_PEAK_RATE":    "10",
	}
	if _, err := Load(nil, getenv(base)); err != nil {
		t.Fatalf("valid production config rejected: %v", err)
	}

	cases := []struct {
		name     string
		drop     string
		override map[string]string
		wantSub  string
	}{
		{"missing valkey url", "HOOKRELAY_VALKEY_URL", nil, "production must explicitly configure"},
		{"explicit insecure cookie", "", map[string]string{"HOOKRELAY_ADMIN_COOKIE_SECURE": "false"}, "insecure administrative cookie"},
		{"missing peak rate", "HOOKRELAY_EXPECTED_PEAK_RATE", nil, "positive expected peak rate"},
		{"zero global rate", "HOOKRELAY_WEBHOOK_GLOBAL_RATE", nil, "nonzero rate limit"},
		{"zero endpoint rate", "HOOKRELAY_WEBHOOK_ENDPOINT_RATE", nil, "nonzero rate limit"},
		{"http origin", "", map[string]string{"HOOKRELAY_ADMIN_ORIGIN": "http://admin.internal:8081"}, "HTTPS origin"},
	}
	for _, tc := range cases {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		if tc.drop != "" {
			delete(env, tc.drop)
		}
		for k, v := range tc.override {
			env[k] = v
		}
		_, err := Load(nil, getenv(env))
		if err == nil {
			t.Errorf("%s: expected error", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.wantSub) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.wantSub)
		}
	}
}

// TestLogDefaultsAndLevels pins the level vocabulary and environment defaults.
func TestLogDefaultsAndLevels(t *testing.T) {
	dev, err := Load(nil, getenv(devEnv()))
	if err != nil || dev.LogLevel != "debug" {
		t.Fatalf("dev default: %v %q", err, dev.LogLevel)
	}
	prodEnv := map[string]string{"HOOKRELAY_ENVIRONMENT": "production", "HOOKRELAY_VALKEY_URL": "valkey://v:6379", "HOOKRELAY_ADMIN_COOKIE_SECURE": "true", "HOOKRELAY_ADMIN_ORIGIN": "https://admin.internal:8081", "HOOKRELAY_EXPECTED_PEAK_RATE": "10", "HOOKRELAY_WEBHOOK_GLOBAL_RATE": "1", "HOOKRELAY_WEBHOOK_ENDPOINT_RATE": "1"}
	prod, err := Load(nil, getenv(prodEnv))
	if err != nil {
		t.Fatalf("prod: %v", err)
	}
	if prod.LogLevel != "info" {
		t.Errorf("prod default = %q, want info", prod.LogLevel)
	}
	env := devEnv()
	env["HOOKRELAY_LOG_LEVEL"] = "trace"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "debug, info, warn, or error") {
		t.Errorf("trace level accepted: %v", err)
	}
}

// TestDedupCapacityFormula pins max_dedup_records >= expected_peak_rate *
// dedup_min_retention_seconds.
func TestDedupCapacityFormula(t *testing.T) {
	// 1,000,000 records / 86,400 s minimum retention ≈ 11.57/s supported.
	env := devEnv()
	env["HOOKRELAY_EXPECTED_PEAK_RATE"] = "11"
	if _, err := Load(nil, getenv(env)); err != nil {
		t.Fatalf("peak rate within capacity rejected: %v", err)
	}
	env["HOOKRELAY_EXPECTED_PEAK_RATE"] = "12"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "supported") {
		t.Errorf("peak rate above capacity accepted: %v", err)
	}
}

// TestRetryDelayCountRule pins the retry-delay count == attempts-1 rule.
func TestRetryDelayCountRule(t *testing.T) {
	env := devEnv()
	env["HOOKRELAY_MAX_DELIVERY_ATTEMPTS"] = "4"
	env["HOOKRELAY_RETRY_DELAYS"] = "1s,5s"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "minus one") {
		t.Errorf("mismatched retry delays accepted: %v", err)
	}
	env["HOOKRELAY_RETRY_DELAYS"] = "1s,5s,30s"
	if _, err := Load(nil, getenv(env)); err != nil {
		t.Fatalf("matched retry delays rejected: %v", err)
	}
}

// TestJitterBounds pins 0 <= min <= max <= 1.
func TestJitterBounds(t *testing.T) {
	for _, tc := range []struct{ min, max string }{
		{"0.6", "0.5"}, // min > max
		{"-0.1", "1"},  // min < 0
		{"0", "1.1"},   // max > 1
	} {
		env := devEnv()
		env["HOOKRELAY_RETRY_JITTER_MIN"] = tc.min
		env["HOOKRELAY_RETRY_JITTER_MAX"] = tc.max
		if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "0 <= min") {
			t.Errorf("jitter %v..%v accepted: %v", tc.min, tc.max, err)
		}
	}
}

// TestInsecureCookieLoopbackRule pins non-loopback + insecure cookie = error.
func TestInsecureCookieLoopbackRule(t *testing.T) {
	env := devEnv()
	env["HOOKRELAY_ADMIN_ADDRESS"] = "0.0.0.0:8081"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Errorf("insecure cookie on non-loopback accepted: %v", err)
	}
	env["HOOKRELAY_ADMIN_COOKIE_SECURE"] = "true"
	if _, err := Load(nil, getenv(env)); err != nil {
		t.Fatalf("secure cookie on non-loopback rejected: %v", err)
	}
}

// TestCIDRValidation pins CIDR parsing for the two CIDR settings.
func TestCIDRValidation(t *testing.T) {
	env := devEnv()
	env["HOOKRELAY_TRUSTED_PROXY_CIDRS"] = "10.0.0.0/8, 172.16.0.0/12"
	c, err := Load(nil, getenv(env))
	if err != nil {
		t.Fatalf("valid CIDRs rejected: %v", err)
	}
	if len(c.TrustedProxyCIDRs) != 2 {
		t.Errorf("TrustedProxyCIDRs = %v", c.TrustedProxyCIDRs)
	}
	env["HOOKRELAY_TELEGRAM_SOURCE_CIDRS"] = "not-a-cidr"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "invalid CIDR") {
		t.Errorf("invalid CIDR accepted: %v", err)
	}
}

// TestLoadSecrets covers exclusivity, file trailing-newline trim, and bounds.
func TestLoadSecrets(t *testing.T) {
	dir := t.TempDir()
	readFile := os.ReadFile
	write := func(name, content string) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	secret := write("consumer", "0123456789abcdef\n")
	got, err := LoadSecrets("", secret, readFile, "consumer secret")
	if err != nil || got != "0123456789abcdef" {
		t.Errorf("file secret: %q %v", got, err)
	}

	if _, err := LoadSecrets("0123456789abcdef", secret, readFile, "consumer secret"); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Errorf("both sources accepted: %v", err)
	}

	short := write("short", "too-short\n")
	if _, err := LoadSecrets("", short, readFile, "consumer secret"); err == nil || !strings.Contains(err.Error(), "16–8192 bytes") {
		t.Errorf("short secret accepted: %v", err)
	}

	long := write("long", strings.Repeat("x", maxSecretBytes+1))
	if _, err := LoadSecrets("", long, readFile, "consumer secret"); err == nil || !strings.Contains(err.Error(), "16–8192 bytes") {
		t.Errorf("long secret accepted: %v", err)
	}

	maxLen := write("max", strings.Repeat("x", maxSecretBytes)+"\n")
	if _, err := LoadSecrets("", maxLen, readFile, "consumer secret"); err != nil {
		t.Errorf("max-length secret rejected: %v", err)
	}

	if _, err := LoadSecrets("", "", readFile, "consumer secret"); err == nil || !strings.Contains(err.Error(), "no secret configured") {
		t.Errorf("absent secret accepted: %v", err)
	}
}

// TestEnvironmentVocabulary pins the environment whitelist.
func TestEnvironmentVocabulary(t *testing.T) {
	env := devEnv()
	env["HOOKRELAY_ENVIRONMENT"] = "staging"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "development or production") {
		t.Errorf("staging accepted: %v", err)
	}
}

// TestPprofEnabledValidation pins the Milestone 1 contract for a setting
// consumed by a later slice: parsed and validated, default false.
func TestPprofEnabledValidation(t *testing.T) {
	for raw, want := range map[string]bool{"": false, "false": false, "0": false, "true": true, "1": true} {
		c, err := Load(nil, getenv(map[string]string{"HOOKRELAY_PPROF_ENABLED": raw}))
		if err != nil || c.PprofEnabled != want {
			t.Errorf("%q: PprofEnabled = %v, %v; want %v", raw, c != nil && c.PprofEnabled, err, want)
		}
	}
	if _, err := Load(nil, getenv(map[string]string{"HOOKRELAY_PPROF_ENABLED": "yes"})); err == nil || !strings.Contains(err.Error(), "HOOKRELAY_PPROF_ENABLED") {
		t.Errorf("invalid value accepted: %v", err)
	}
}

// TestValkeyMaxConnectionsMinimum pins the lower bound that lets the
// pipelined ring and the blocking pool both exist within the limit.
func TestValkeyMaxConnectionsMinimum(t *testing.T) {
	if _, err := Load(nil, getenv(map[string]string{"HOOKRELAY_VALKEY_MAX_CONNECTIONS": "1", "HOOKRELAY_VALKEY_MIN_IDLE": "1"})); err == nil {
		t.Error("a single connection accepted")
	}
	if _, err := Load(nil, getenv(map[string]string{"HOOKRELAY_VALKEY_MAX_CONNECTIONS": "2", "HOOKRELAY_VALKEY_MIN_IDLE": "1"})); err != nil {
		t.Errorf("two connections rejected: %v", err)
	}
}

// TestClaimNotifications pins the notifier switch parsing.
func TestClaimNotifications(t *testing.T) {
	env := devEnv()
	env["HOOKRELAY_CLAIM_NOTIFICATIONS"] = "false"
	c, err := Load(nil, getenv(env))
	if err != nil || c.ClaimNotifications {
		t.Fatalf("false: %v, %v", c, err)
	}
	env["HOOKRELAY_CLAIM_NOTIFICATIONS"] = "maybe"
	if _, err := Load(nil, getenv(env)); err == nil || !strings.Contains(err.Error(), "boolean") {
		t.Errorf("invalid value accepted: %v", err)
	}
}

func TestUIGrafanaURL(t *testing.T) {
	env := devEnv()
	c, err := Load(nil, getenv(env))
	if err != nil || c.UIGrafanaURL != "" {
		t.Fatalf("default: %q, %v", c.UIGrafanaURL, err)
	}
	env["HOOKRELAY_UI_GRAFANA_URL"] = "https://grafana.example/d/hookrelay"
	if c, err := Load(nil, getenv(env)); err != nil || c.UIGrafanaURL != "https://grafana.example/d/hookrelay" {
		t.Errorf("env: %v, %v", c, err)
	}
	if c, err := Load([]string{"--ui-grafana-url", "http://g:3000/x"}, getenv(env)); err != nil || c.UIGrafanaURL != "http://g:3000/x" {
		t.Errorf("flag: %v, %v", c, err)
	}
	for _, bad := range []string{"grafana.example", "javascript:alert(1)", "ftp://g/x", "https://user:pw@g/x", "/relative"} {
		env["HOOKRELAY_UI_GRAFANA_URL"] = bad
		_, err := Load(nil, getenv(env))
		if err == nil || !strings.Contains(err.Error(), "HOOKRELAY_UI_GRAFANA_URL") {
			t.Errorf("%q accepted: %v", bad, err)
		}
		if err != nil && strings.Contains(err.Error(), "pw") {
			t.Errorf("error echoes the value: %v", err)
		}
	}
}

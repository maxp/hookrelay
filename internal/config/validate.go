package config

import (
	"fmt"
	"net"
	"net/url"
)

// validate enforces the documented startup validation contract. Production
// adds stricter requirements; runtime Valkey checks (AOF, noeviction,
// scripting) join this gate when the storage adapter exists.
func (c *Config) validate() error {
	if err := validatePositive("HOOKRELAY_WEBHOOK_GLOBAL_RATE", c.WebhookGlobalRate, c.Production()); err != nil {
		return err
	}
	if err := validateNonNegative("HOOKRELAY_WEBHOOK_GLOBAL_BURST", c.WebhookGlobalBurst); err != nil {
		return err
	}
	if err := validatePositive("HOOKRELAY_WEBHOOK_ENDPOINT_RATE", c.WebhookEndpointRate, c.Production()); err != nil {
		return err
	}
	if err := validateNonNegative("HOOKRELAY_WEBHOOK_ENDPOINT_BURST", c.WebhookEndpointBurst); err != nil {
		return err
	}
	if c.MaxInflightWebhooks <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_INFLIGHT_WEBHOOKS: must be positive, got %d", c.MaxInflightWebhooks)
	}
	if c.ValkeyMaxConnections <= 0 {
		return fmt.Errorf("HOOKRELAY_VALKEY_MAX_CONNECTIONS: must be positive, got %d", c.ValkeyMaxConnections)
	}
	if c.ValkeyMinIdle <= 0 {
		return fmt.Errorf("HOOKRELAY_VALKEY_MIN_IDLE: must be positive, got %d", c.ValkeyMinIdle)
	}
	if c.ValkeyMinIdle > c.ValkeyMaxConnections {
		return fmt.Errorf("HOOKRELAY_VALKEY_MIN_IDLE (%d) must not exceed HOOKRELAY_VALKEY_MAX_CONNECTIONS (%d)", c.ValkeyMinIdle, c.ValkeyMaxConnections)
	}
	if c.MaxQueuedMessages <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_QUEUED_MESSAGES: must be positive, got %d", c.MaxQueuedMessages)
	}
	if c.MaxQueuedMessagesPerRecipient <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_QUEUED_MESSAGES_PER_RECIPIENT: must be positive, got %d", c.MaxQueuedMessagesPerRecipient)
	}
	if c.MemoryAcceptanceStopPercent < 1 || c.MemoryAcceptanceStopPercent > 100 {
		return fmt.Errorf("HOOKRELAY_MEMORY_ACCEPTANCE_STOP_PERCENT: must be 1–100, got %d", c.MemoryAcceptanceStopPercent)
	}
	if c.DedupRetention <= 0 {
		return fmt.Errorf("HOOKRELAY_DEDUP_RETENTION: must be positive, got %s", c.DedupRetention)
	}
	if c.DedupMinRetention <= 0 {
		return fmt.Errorf("HOOKRELAY_DEDUP_MIN_RETENTION: must be positive, got %s", c.DedupMinRetention)
	}
	if c.DedupMinRetention > c.DedupRetention {
		return fmt.Errorf("HOOKRELAY_DEDUP_MIN_RETENTION (%s) must not exceed HOOKRELAY_DEDUP_RETENTION (%s)", c.DedupMinRetention, c.DedupRetention)
	}
	if c.MaxDedupRecords <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_DEDUP_RECORDS: must be positive, got %d", c.MaxDedupRecords)
	}
	if c.ExpectedPeakRate < 0 {
		return fmt.Errorf("HOOKRELAY_EXPECTED_PEAK_RATE: must not be negative, got %v", c.ExpectedPeakRate)
	}
	if c.Production() && c.ExpectedPeakRate == 0 {
		return fmt.Errorf("HOOKRELAY_EXPECTED_PEAK_RATE: production requires a positive expected peak rate")
	}
	// Capacity must be able to preserve the minimum retention window at the
	// expected peak rate of newly accepted deduplication identities.
	if c.ExpectedPeakRate > 0 {
		supportedRate := float64(c.MaxDedupRecords) / c.DedupMinRetention.Seconds()
		if c.ExpectedPeakRate > supportedRate {
			return fmt.Errorf(
				"HOOKRELAY_EXPECTED_PEAK_RATE (%v/s) exceeds the rate supported by HOOKRELAY_MAX_DEDUP_RECORDS=%d over HOOKRELAY_DEDUP_MIN_RETENTION=%s (supported: %.2f/s)",
				c.ExpectedPeakRate, c.MaxDedupRecords, c.DedupMinRetention, supportedRate)
		}
	}
	if c.DLQRetention <= 0 {
		return fmt.Errorf("HOOKRELAY_DLQ_RETENTION: must be positive, got %s", c.DLQRetention)
	}
	if c.InitialLeaseDuration <= 0 {
		return fmt.Errorf("HOOKRELAY_INITIAL_LEASE_DURATION: must be positive, got %s", c.InitialLeaseDuration)
	}
	if c.LeaseExtensionDuration <= 0 {
		return fmt.Errorf("HOOKRELAY_LEASE_EXTENSION_DURATION: must be positive, got %s", c.LeaseExtensionDuration)
	}
	if c.MaxLeaseLifetime <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_LEASE_LIFETIME: must be positive, got %s", c.MaxLeaseLifetime)
	}
	if c.InitialLeaseDuration > c.MaxLeaseLifetime {
		return fmt.Errorf("HOOKRELAY_INITIAL_LEASE_DURATION (%s) must not exceed HOOKRELAY_MAX_LEASE_LIFETIME (%s)", c.InitialLeaseDuration, c.MaxLeaseLifetime)
	}
	if c.LeaseExtensionDuration > c.MaxLeaseLifetime {
		return fmt.Errorf("HOOKRELAY_LEASE_EXTENSION_DURATION (%s) must not exceed HOOKRELAY_MAX_LEASE_LIFETIME (%s)", c.LeaseExtensionDuration, c.MaxLeaseLifetime)
	}
	if c.MaxDeliveryAttempts <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_DELIVERY_ATTEMPTS: must be positive, got %d", c.MaxDeliveryAttempts)
	}
	if len(c.RetryDelays) != c.MaxDeliveryAttempts-1 {
		return fmt.Errorf("HOOKRELAY_RETRY_DELAYS: the retry-delay count (%d) must equal HOOKRELAY_MAX_DELIVERY_ATTEMPTS minus one (%d)", len(c.RetryDelays), c.MaxDeliveryAttempts-1)
	}
	for _, d := range c.RetryDelays {
		if d <= 0 {
			return fmt.Errorf("HOOKRELAY_RETRY_DELAYS: delays must be positive, got %s", d)
		}
	}
	if c.RetryJitterMin < 0 || c.RetryJitterMax < c.RetryJitterMin || c.RetryJitterMax > 1 {
		return fmt.Errorf("HOOKRELAY_RETRY_JITTER_MIN/MAX: must satisfy 0 <= min (%v) <= max (%v) <= 1", c.RetryJitterMin, c.RetryJitterMax)
	}
	if c.MaxActiveLeases <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_ACTIVE_LEASES: must be positive, got %d", c.MaxActiveLeases)
	}
	if c.MaxWaitingClaims <= 0 {
		return fmt.Errorf("HOOKRELAY_MAX_WAITING_CLAIMS: must be positive, got %d", c.MaxWaitingClaims)
	}
	if c.MaintenanceInterval <= 0 {
		return fmt.Errorf("HOOKRELAY_MAINTENANCE_INTERVAL: must be positive, got %s", c.MaintenanceInterval)
	}
	if c.MaintenanceIntervalJitter < 0 {
		return fmt.Errorf("HOOKRELAY_MAINTENANCE_INTERVAL_JITTER: must not be negative, got %s", c.MaintenanceIntervalJitter)
	}
	if c.MaintenanceBatchSize <= 0 {
		return fmt.Errorf("HOOKRELAY_MAINTENANCE_BATCH_SIZE: must be positive, got %d", c.MaintenanceBatchSize)
	}
	if c.MaintenanceMaxContinuousBatches <= 0 {
		return fmt.Errorf("HOOKRELAY_MAINTENANCE_MAX_CONTINUOUS_BATCHES: must be positive, got %d", c.MaintenanceMaxContinuousBatches)
	}

	// Valkey URL: scheme must be valkey or valkeys; production requires an
	// explicitly configured URL (the default is local development only).
	if _, err := parseValkeyURL(c.ValkeyURL); err != nil {
		return fmt.Errorf("HOOKRELAY_VALKEY_URL: %w", err)
	}
	if c.Production() && !c.valkeyURLProvided {
		return fmt.Errorf("HOOKRELAY_VALKEY_URL: production must explicitly configure the Valkey URL")
	}

	// Administrative cookie: an insecure cookie is permitted only when the
	// administrative listener binds to a loopback address.
	if !c.AdminCookieSecure && !adminAddressIsLoopback(c.AdminAddress) {
		return fmt.Errorf("HOOKRELAY_ADMIN_COOKIE_SECURE=false is only permitted when the administrative listener (%s) binds to loopback", c.AdminAddress)
	}
	if c.Production() && !c.AdminCookieSecure {
		return fmt.Errorf("HOOKRELAY_ADMIN_COOKIE_SECURE: production disallows an insecure administrative cookie")
	}
	if c.AdminOrigin.Scheme != "http" && c.AdminOrigin.Scheme != "https" {
		return fmt.Errorf("HOOKRELAY_ADMIN_ORIGIN: scheme must be http or https, got %q", c.AdminOrigin.Scheme)
	}
	if c.Production() && c.AdminOrigin.Scheme != "https" {
		return fmt.Errorf("HOOKRELAY_ADMIN_ORIGIN: production requires an HTTPS origin, got %q", c.AdminOrigin.String())
	}
	return nil
}

func validatePositive(env string, v float64, required bool) error {
	if v < 0 {
		return fmt.Errorf("%s: must not be negative, got %v", env, v)
	}
	if required && v == 0 {
		return fmt.Errorf("%s: production requires a nonzero rate limit", env)
	}
	return nil
}

func validateNonNegative(env string, v int) error {
	if v < 0 {
		return fmt.Errorf("%s: must not be negative, got %d", env, v)
	}
	return nil
}

func parseValkeyURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "valkey" && u.Scheme != "valkeys" {
		return nil, fmt.Errorf("scheme must be valkey or valkeys, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("host is required")
	}
	return u, nil
}

func adminAddressIsLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

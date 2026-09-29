// Package delivery owns the ordered-delivery use cases (claim, and later
// acknowledgement, negative acknowledgement, extension) and the Consumer API
// HTTP transport. Storage arrives through the narrow interfaces declared
// here; callers never see Valkey keys or scripts.
package delivery

import "context"

// ClaimRequest is one atomic claim check.
type ClaimRequest struct {
	OperationID string
	// ArgsDigest binds the operation to its arguments (operation_id and
	// wait_ms); a replay with other arguments is an operation conflict.
	ArgsDigest string
	// Token is the candidate Delivery Token for a new lease; TokenDigest is
	// its SHA-256, the key of the token record.
	Token              string
	TokenDigest        string
	ConsumerInstanceID string
	// RecordEmpty records an empty outcome for replay. A waiting claim
	// records only its final empty check.
	RecordEmpty bool
}

// ClaimOutcome is the bounded result of a claim check.
type ClaimOutcome string

const (
	ClaimClaimed               ClaimOutcome = "claimed"
	ClaimReplayActive          ClaimOutcome = "replay_active"
	ClaimReplayEmpty           ClaimOutcome = "replay_empty"
	ClaimNoLongerActive        ClaimOutcome = "claim_no_longer_active"
	ClaimOperationConflict     ClaimOutcome = "operation_conflict"
	ClaimLimitExceeded         ClaimOutcome = "limit_exceeded"
	ClaimEmpty                 ClaimOutcome = "empty"
	ClaimDependencyUnavailable ClaimOutcome = "dependency_unavailable"
	ClaimInternalFailure       ClaimOutcome = "internal_failure"
)

// Delivery is the active Delivery Attempt returned with a claimed message.
type Delivery struct {
	Token          string
	MessageID      string
	DeliveryCycle  int64
	Attempt        int64
	ClaimedMs      int64
	LeaseExpiresMs int64
}

// ClaimResult carries the outcome, the attempt, the stored Canonical
// Message JSON, and the number of Recipients newly blocked during the scan.
type ClaimResult struct {
	Outcome         ClaimOutcome
	Delivery        Delivery
	MessageJSON     []byte
	BlockedDetected int64
}

// Claimer runs the atomic claim transition.
type Claimer interface {
	Claim(ctx context.Context, req ClaimRequest) ClaimResult
}

// Stats is the delivery state snapshot behind the delivery gauges.
type Stats struct {
	ActiveLeases      int64
	ReadyRecipients   int64
	BlockedRecipients int64
	QueuedMessages    int64
	RetriesWaiting    int64
}

// StatsReader reads the delivery gauges' sources.
type StatsReader interface {
	Stats(ctx context.Context) (Stats, error)
}

// AckRequest acknowledges one Delivery Attempt by its token.
type AckRequest struct {
	Token       string
	TokenDigest string
}

// AckOutcome is the bounded result of an acknowledgement.
type AckOutcome string

const (
	AckAcknowledged          AckOutcome = "acknowledged"
	AckAlreadyAcknowledged   AckOutcome = "already_acknowledged"
	AckAlreadyNacked         AckOutcome = "already_nacked"
	AckNotFound              AckOutcome = "not_found"
	AckStale                 AckOutcome = "stale"
	AckRecipientBlocked      AckOutcome = "recipient_blocked"
	AckDependencyUnavailable AckOutcome = "dependency_unavailable"
	AckInternalFailure       AckOutcome = "internal_failure"
)

// AckResult carries the recorded acknowledgement; RecipientIdentity,
// DeliveryCycle, Attempt, and ClaimedMs are set only for a first
// acknowledgement.
type AckResult struct {
	Outcome           AckOutcome
	MessageID         string
	AcknowledgedMs    int64
	RecipientIdentity string
	DeliveryCycle     int64
	Attempt           int64
	ClaimedMs         int64
}

// Acknowledger runs the atomic acknowledgement transition.
type Acknowledger interface {
	Ack(ctx context.Context, req AckRequest) AckResult
}

// NackRequest negatively acknowledges one Delivery Attempt by its token.
type NackRequest struct {
	Token       string
	TokenDigest string
	// ReasonCode is empty or a bounded consumer-supplied code.
	ReasonCode string
	// RetryDelaysMs[n-1] is the drawn effective delay after failed attempt
	// n; the transition picks the entry of the attempt it fails.
	RetryDelaysMs []int64
	MaxAttempts   int
}

// NackOutcome is the bounded result of a negative acknowledgement.
type NackOutcome string

const (
	NackRetryScheduled      NackOutcome = "retry_scheduled"
	NackAlreadyNacked       NackOutcome = "already_nacked"
	NackAlreadyAcknowledged NackOutcome = "already_acknowledged"
	NackNotFound            NackOutcome = "not_found"
	NackStale               NackOutcome = "stale"
	NackRecipientBlocked    NackOutcome = "recipient_blocked"
	// NackAttemptsExhausted refuses the last attempt without mutation until
	// the dead-letter transition exists (Milestone 2 ticket 05).
	NackAttemptsExhausted     NackOutcome = "attempts_exhausted"
	NackDependencyUnavailable NackOutcome = "dependency_unavailable"
	NackInternalFailure       NackOutcome = "internal_failure"
)

// NackResult carries the scheduled retry or the recorded result of an
// earlier nack (Result names the recorded result kind). RecipientIdentity,
// DeliveryCycle, ClaimedMs, and CompletedMs are set only for a first nack.
type NackResult struct {
	Outcome           NackOutcome
	Result            string
	MessageID         string
	Attempt           int64
	RetryAtMs         int64
	RecipientIdentity string
	DeliveryCycle     int64
	ClaimedMs         int64
	CompletedMs       int64
}

// NegativeAcknowledger runs the atomic negative-acknowledgement transition.
type NegativeAcknowledger interface {
	Nack(ctx context.Context, req NackRequest) NackResult
}

// DueEntry is one due maintenance index member: the Recipient and the
// deadline (retry_at_ms or lease_expires_ms) it is due at.
type DueEntry struct {
	RecipientIdentity string
	DueMs             int64
}

// DueBatch is one bounded, oldest-first read of due entries. NowMs is the
// authoritative Valkey time the entries were compared against.
type DueBatch struct {
	NowMs   int64
	Entries []DueEntry
}

// ActivationOutcome is the bounded result of one retry activation.
type ActivationOutcome string

const (
	ActivationActivated             ActivationOutcome = "activated"
	ActivationNotDue                ActivationOutcome = "not_due"
	ActivationRecipientBlocked      ActivationOutcome = "recipient_blocked"
	ActivationDependencyUnavailable ActivationOutcome = "dependency_unavailable"
	ActivationInternalFailure       ActivationOutcome = "internal_failure"
)

// ActivationResult carries the activated head and its next attempt.
type ActivationResult struct {
	Outcome   ActivationOutcome
	MessageID string
	Attempt   int64
}

// RetryActivator reads due retries and runs the atomic activation
// transition, which re-validates authoritative state before mutating.
type RetryActivator interface {
	DueRetries(ctx context.Context, limit int) (DueBatch, error)
	ActivateRetry(ctx context.Context, recipientIdentity string) ActivationResult
}

// ExpiryOutcome is the bounded result of one lease expiry.
type ExpiryOutcome string

const (
	ExpiryRetryScheduled   ExpiryOutcome = "retry_scheduled"
	ExpiryNotDue           ExpiryOutcome = "not_due"
	ExpiryRecipientBlocked ExpiryOutcome = "recipient_blocked"
	// ExpiryAttemptsExhausted leaves the last attempt's lease in place
	// until the dead-letter transition exists (Milestone 2 ticket 05).
	ExpiryAttemptsExhausted     ExpiryOutcome = "attempts_exhausted"
	ExpiryDependencyUnavailable ExpiryOutcome = "dependency_unavailable"
	ExpiryInternalFailure       ExpiryOutcome = "internal_failure"
)

// ExpiryResult carries the expired attempt and its scheduled retry.
type ExpiryResult struct {
	Outcome       ExpiryOutcome
	MessageID     string
	Attempt       int64
	RetryAtMs     int64
	DeliveryCycle int64
	ClaimedMs     int64
	ExpiredMs     int64
	// ConsumerInstanceID is the diagnostics identifier recorded at claim
	// (empty when absent).
	ConsumerInstanceID string
}

// LeaseExpirer reads due leases and runs the atomic expiry transition,
// which re-validates authoritative state before mutating.
type LeaseExpirer interface {
	DueLeases(ctx context.Context, limit int) (DueBatch, error)
	ExpireLease(ctx context.Context, recipientIdentity string, retryDelaysMs []int64, maxAttempts int) ExpiryResult
}

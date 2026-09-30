package administration

import "context"

// DeadLetterCursor is the position after the last listed DLQ member
// (newest first: descending score, then descending member).
type DeadLetterCursor struct {
	Score  int64  `json:"s"`
	Member string `json:"m"`
}

// DeadLetter is one dead-letter record's safe metadata: never payload,
// Delivery Token, or deduplication digest. RecordMissing flags a DLQ member
// without its record (a reconciliation finding, skipped by the listing).
type DeadLetter struct {
	MessageID         string
	RecipientIdentity string
	DeadLetteredMs    int64
	Reason            string
	DeliveryCycle     int64
	RecordMissing     bool
	// Attempts and Archived are read only by GetDeadLetter.
	Attempts []AttemptEntry
	Archived *ArchivedCycles
}

// AttemptEntry is one completed Delivery Attempt from the retained history.
type AttemptEntry struct {
	DeliveryCycle      int64  `json:"delivery_cycle"`
	Attempt            int64  `json:"attempt"`
	ClaimedMs          int64  `json:"claimed_ms"`
	LeaseExpiresMs     int64  `json:"lease_expires_ms"`
	CompletedMs        int64  `json:"completed_ms"`
	Outcome            string `json:"outcome"`
	ReasonCode         string `json:"reason_code,omitempty"`
	ConsumerInstanceID string `json:"consumer_instance_id,omitempty"`
}

// ArchivedCycles is the history's leading summary of folded cycles.
type ArchivedCycles struct {
	ArchivedCycles   int64 `json:"archived_cycles"`
	ArchivedAttempts int64 `json:"archived_attempts"`
	FirstArchivedMs  int64 `json:"first_archived_ms"`
	LastArchivedMs   int64 `json:"last_archived_ms"`
}

// ReplayResult is the bounded outcome of the audited replay transition.
type ReplayResult string

const (
	ReplayReplayed              ReplayResult = "replayed"
	ReplayNotFound              ReplayResult = "not_found"
	ReplayMessageMissing        ReplayResult = "message_missing"
	ReplayRecipientBlocked      ReplayResult = "recipient_blocked"
	ReplayDeduplicationConflict ReplayResult = "deduplication_conflict"
	ReplayWrongType             ReplayResult = "wrong_type"
	ReplayUnavailable           ReplayResult = "dependency_unavailable"
	// ReplayUncertain: the transition may have run; neither success nor
	// failure may be reported.
	ReplayUncertain ReplayResult = "uncertain"
)

// Deduplication conflict resolutions.
const (
	ResolutionReject      = "reject"
	ResolutionKeepCurrent = "keep_current"
)

// Replay is the replay transition's result; the fields are set for
// ReplayReplayed.
type Replay struct {
	Result                  ReplayResult
	DeliveryCycle           int64
	QueuePosition           string // head | after_active_head | after_pending_replay
	ReplayedMs              int64
	DeduplicationResolution string // not_conflicting | kept_current
	RecipientIdentity       string
}

// DeadLetterRepository is the DLQ storage the Valkey adapter implements.
type DeadLetterRepository interface {
	// ListDeadLetters returns at most limit DLQ members newest first after
	// the cursor, each with its record's metadata (or RecordMissing).
	ListDeadLetters(ctx context.Context, limit int, after *DeadLetterCursor) ([]DeadLetter, error)
	// GetDeadLetter reads one record with its attempt history; nil when the
	// message is not dead-lettered.
	GetDeadLetter(ctx context.Context, messageID string) (*DeadLetter, error)
	// ReplayDeadLetter runs the audited replay transition.
	ReplayDeadLetter(ctx context.Context, messageID, resolution, eventID, requestID string) Replay
}

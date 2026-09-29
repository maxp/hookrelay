package administration

import "context"

// RecipientStatus is a Recipient-state list filter; each maps to one
// derived index.
type RecipientStatus string

const (
	StatusReady     RecipientStatus = "ready"
	StatusLeased    RecipientStatus = "leased"
	StatusRetryWait RecipientStatus = "retry_wait"
	StatusBlocked   RecipientStatus = "blocked"
)

// RecipientCursor is the position after the last listed index member.
type RecipientCursor struct {
	Score  int64  `json:"s"`
	Member string `json:"m"`
}

// RecipientStateItem is one index member: the internal Recipient identity
// (converted to structured fields before it leaves the service), the index
// score (ready sequence, lease deadline, retry time, or detection time; the
// cursor position), and, for blocked Recipients, the authoritative marker's
// detection time and reason, or MarkerMissing for a member without one.
type RecipientStateItem struct {
	RecipientIdentity string
	Score             int64
	DetectedMs        int64
	ReasonCode        string
	MarkerMissing     bool
}

// BlockMarker is a Recipient block marker's safe fields.
type BlockMarker struct {
	DetectedMs int64
	ReasonCode string
}

// BlockHead is the bounded head-state view: never a Delivery Token.
type BlockHead struct {
	MessageID      string
	Status         string
	DeliveryCycle  int64
	Attempt        int64
	LeaseExpiresMs int64
	RetryAtMs      int64
}

// BlockInspection is the read-only inspection result.
type BlockInspection struct {
	Marker             *BlockMarker
	QueueLength        int64
	Head               *BlockHead
	HeadMessagePresent bool
	Memberships        map[string]bool // ready, leases, retries, blocked
	ViolatedInvariants []string
}

// ClearBlockResult is the bounded outcome of the audited clear transition.
type ClearBlockResult string

const (
	ClearCleared            ClearBlockResult = "cleared"
	ClearNotFound           ClearBlockResult = "not_found"
	ClearPreconditionFailed ClearBlockResult = "precondition_failed"
	ClearAmbiguous          ClearBlockResult = "ambiguous"
	ClearWrongType          ClearBlockResult = "wrong_type"
	ClearUnavailable        ClearBlockResult = "dependency_unavailable"
	// ClearUncertain: the transition may have run; neither success nor
	// failure may be reported.
	ClearUncertain ClearBlockResult = "uncertain"
)

// RecipientRepository is the Recipient-state storage the Valkey adapter
// implements.
type RecipientRepository interface {
	// ListRecipientStates returns at most limit members of the status index
	// in ascending (score, member) order after the cursor.
	ListRecipientStates(ctx context.Context, status RecipientStatus, limit int, after *RecipientCursor) ([]RecipientStateItem, error)
	// InspectBlock reads a Recipient's block and state without mutation.
	InspectBlock(ctx context.Context, recipientIdentity string) (BlockInspection, error)
	// ClearBlock runs the preconditioned, audited clear transition. detail
	// is the restored index for cleared, or the first violated invariant
	// for ambiguous.
	ClearBlock(ctx context.Context, recipientIdentity string, expectedDetectedMs int64, expectedReasonCode, eventID, requestID string) (result ClearBlockResult, detail string)
}

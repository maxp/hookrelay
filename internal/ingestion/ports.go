package ingestion

import (
	"context"
	"errors"
)

// Endpoint is the stored Webhook Endpoint as ingestion needs it. The
// credential value is secret-bearing and only ever reaches a Verifier.
type Endpoint struct {
	BotID           string
	Enabled         bool
	CredentialKind  string
	CredentialValue string
}

// ErrStoredWrongType reports an unexpected stored key type.
var ErrStoredWrongType = errors.New("ingestion: stored structure has an unexpected type")

// EndpointLookup reads one Webhook Endpoint. It returns (nil, nil) when the
// endpoint does not exist.
type EndpointLookup interface {
	LookupEndpoint(ctx context.Context, webhookType, identifier string) (*Endpoint, error)
}

// AcceptRequest is one atomic acceptance: the candidate message, its
// Deduplication Identity digest, and the exact-body digest.
type AcceptRequest struct {
	MessageID           string
	DedupIdentityDigest string
	BodyDigest          string
	ReceivedMs          int64
	OccurredMs          *int64
	MessageJSON         []byte
	RecipientIdentity   string
}

// AcceptOutcome is the bounded result of atomic acceptance.
type AcceptOutcome string

const (
	AcceptAccepted          AcceptOutcome = "accepted"
	AcceptDuplicate         AcceptOutcome = "duplicate"
	AcceptDuplicateConflict AcceptOutcome = "duplicate_conflict"
	AcceptRecipientBlocked  AcceptOutcome = "recipient_blocked"
	AcceptRecipientCapacity AcceptOutcome = "recipient_capacity"
	AcceptGlobalCapacity    AcceptOutcome = "global_capacity"
	AcceptDedupCapacity     AcceptOutcome = "dedup_capacity"
	// AcceptDependencyUnavailable covers Valkey failures and uncertain
	// script outcomes. The platform retries; a retry of an already accepted
	// message is proven duplicate.
	AcceptDependencyUnavailable AcceptOutcome = "dependency_unavailable"
	// AcceptInternalFailure covers unexpected stored types and inconsistent
	// recipient state: corruption that retrying cannot fix.
	AcceptInternalFailure AcceptOutcome = "internal_failure"
)

// AcceptResult carries the outcome; MessageID is the original message for
// duplicates, AcceptedMs the Valkey acceptance time.
type AcceptResult struct {
	Outcome    AcceptOutcome
	MessageID  string
	AcceptedMs int64
}

// MessageAcceptor atomically accepts a message or proves it duplicate. It
// hides deduplication, queues, indexes, counters, and scripts.
type MessageAcceptor interface {
	Accept(ctx context.Context, req AcceptRequest) AcceptResult
}

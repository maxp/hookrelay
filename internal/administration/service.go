// Package administration owns Admin Bearer authentication, Webhook Endpoint
// management, and administrative audit intent. Its HTTP transport stays
// local; storage behavior arrives through the narrow interfaces declared
// here and implemented by the Valkey adapter.
package administration

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/gen"
)

// EndpointRepository is the storage interface the Valkey adapter implements.
type EndpointRepository interface {
	CreateEndpoint(ctx context.Context, e Endpoint, eventID, operation, requestID string) (createdMs, updatedMs int64, result CreateEndpointResult)
	GetEndpoint(ctx context.Context, webhookType, identifier string) (*Endpoint, error)
	// SetEndpointEnabled runs the audited enable/disable transition under
	// the expected entity version (nil when no If-Match was sent).
	// The Endpoint (safe fields only) is returned for updated and unchanged.
	SetEndpointEnabled(ctx context.Context, ref EndpointRef, enabled bool, expected *EntityVersion, eventID, requestID string) (*Endpoint, SetEnabledResult)
	// DeleteEndpoint runs the audited delete of a disabled endpoint under
	// the expected entity version (nil when no If-Match was sent).
	// The DeletedEndpoint is returned for deleted.
	DeleteEndpoint(ctx context.Context, ref EndpointRef, botPlatform string, expected *EntityVersion, eventID, requestID string) (*DeletedEndpoint, DeleteResult)
	// ListBotEndpoints reads every endpoint of one Bot Identity (at most
	// 100), newest first (descending created_ms, then member).
	ListBotEndpoints(ctx context.Context, botPlatform, botID string) ([]EndpointListing, error)
	// ListEndpoints pages the global listing newest first (descending
	// created_ms, then descending member), strictly after the cursor.
	ListEndpoints(ctx context.Context, limit int, after *EndpointCursor) ([]EndpointListing, error)
}

// EndpointRef names one Webhook Endpoint.
type EndpointRef struct {
	Type       string
	Identifier string
}

// String is "<webhook_type>:<webhook_identifier>": the listing member and
// the audit target.
func (r EndpointRef) String() string { return r.Type + ":" + r.Identifier }

// DeletedEndpoint is the safe identity of a deleted endpoint.
type DeletedEndpoint struct {
	BotID          string
	CredentialKind string
	GenerationID   string
	ConfigVersion  int64
	DeletedMs      int64
}

// EntityVersion is a parsed strong entity tag.
type EntityVersion struct {
	GenerationID  string
	ConfigVersion int64
}

// SetEnabledResult is the bounded outcome of the enable/disable transition.
type SetEnabledResult string

const (
	SetEnabledUpdated              SetEnabledResult = "updated"
	SetEnabledUnchanged            SetEnabledResult = "unchanged"
	SetEnabledNotFound             SetEnabledResult = "not_found"
	SetEnabledPreconditionRequired SetEnabledResult = "precondition_required"
	SetEnabledPreconditionFailed   SetEnabledResult = "precondition_failed"
	SetEnabledWrongType            SetEnabledResult = "wrong_type"
	SetEnabledUnavailable          SetEnabledResult = "dependency_unavailable"
	// SetEnabledUncertain: the transition may have run; neither success nor
	// failure may be reported.
	SetEnabledUncertain SetEnabledResult = "uncertain"
)

// DeleteResult is the bounded outcome of the delete transition.
type DeleteResult string

const (
	DeleteDeleted              DeleteResult = "deleted"
	DeleteAbsent               DeleteResult = "absent"
	DeletePreconditionRequired DeleteResult = "precondition_required"
	DeletePreconditionFailed   DeleteResult = "precondition_failed"
	DeleteMustBeDisabled       DeleteResult = "must_be_disabled"
	DeleteWrongType            DeleteResult = "wrong_type"
	DeleteUnavailable          DeleteResult = "dependency_unavailable"
	// DeleteUncertain: the transition may have run; neither success nor
	// failure may be reported.
	DeleteUncertain DeleteResult = "uncertain"
)

// EndpointCursor is the position after the last listed endpoint.
type EndpointCursor struct {
	CreatedMs int64  `json:"created_ms"`
	ID        string `json:"id"` // <webhook_type>:<webhook_identifier>
}

// EndpointListing is one listing-index member with its record. A member
// whose record is missing, of the wrong type, or malformed carries the
// Orphan reason and no Endpoint; listings skip it and the cursor advances.
type EndpointListing struct {
	Member    string
	CreatedMs int64
	Endpoint  *Endpoint
	Orphan    string
}

// Orphan reasons of an EndpointListing.
const (
	OrphanMissing   = "missing"
	OrphanWrongType = "wrong_type"
	OrphanMalformed = "malformed"
	// OrphanUnknownType: the record's Webhook Type is no longer registered.
	OrphanUnknownType = "unknown_webhook_type"
)

// Endpoint mirrors the stored record without importing the adapter package.
type Endpoint struct {
	Type            string
	Identifier      string
	BotPlatform     string
	BotID           string
	Enabled         bool
	CredentialKind  string
	CredentialValue string
	GenerationID    string
	CreatedMs       int64
	UpdatedMs       int64
	ConfigVersion   int64
}

// Create result codes mirror the adapter's bounded outcomes.
type CreateEndpointResult string

const (
	CreateOK          CreateEndpointResult = "created"
	CreateConflict    CreateEndpointResult = "conflict"
	CreateBotLimit    CreateEndpointResult = "bot_endpoint_limit"
	CreateWrongType   CreateEndpointResult = "wrong_type"
	CreateUnavailable CreateEndpointResult = "dependency_unavailable"
	// CreateUncertain: the transition may have run, fully or partially.
	// Neither success nor failure may be reported to the caller.
	CreateUncertain CreateEndpointResult = "uncertain"
)

// ErrStoredWrongType reports an unexpected stored key type: an inconsistency
// the current slice does not repair.
var ErrStoredWrongType = errors.New("administration: stored structure has an unexpected type")

// TypeCatalog is the narrow view of the ingestion registry: platform and
// credential-kind allowlist per Webhook Type.
type TypeCatalog interface {
	Lookup(webhookType string) (platform string, credentialKinds []string, ok bool)
	// KnownPlatform reports whether a registered type belongs to the Bot
	// Platform.
	KnownPlatform(botPlatform string) bool
}

// AuditSink records best-effort audit events outside Lua transitions. The
// returned error is only counted; it never changes the caller's outcome.
type AuditSink interface {
	AppendRejectedAuth(ctx context.Context, eventID, requestID, target string) error
}

// ServiceDeps carries the administrative service collaborators.
type ServiceDeps struct {
	Repo EndpointRepository
	// Recipients serves the Recipient-state and block routes; nil leaves
	// them unregistered.
	Recipients RecipientRepository
	// DeadLetters serves the DLQ routes; nil leaves them unregistered.
	DeadLetters DeadLetterRepository
	// Messages serves the delivery-state route; nil leaves it unregistered.
	Messages MessageStateRepository
	Catalog  TypeCatalog
	Audit    AuditSink
	// AuditLog serves the audit listing; nil leaves it unregistered.
	AuditLog AuditLog
	// AdminSecret must already be resolved and validated by the
	// configuration layer.
	AdminSecret string
	Gen         gen.Gen
	// Ready is signalled when a replay or block clear makes a Recipient
	// claimable, so a waiting claim can wake early; nil disables the hint.
	Ready ReadySignal
	// Logger receives feature events and best-effort audit copies; nil discards them.
	Logger *slog.Logger
	// Registerer receives the administrative audit metrics; nil keeps them
	// on a private registry.
	Registerer prometheus.Registerer
}

// ReadySignal receives a hint that claimable work may exist; the delivery
// notifier implements it and maps any source it does not list to
// "unknown".
type ReadySignal interface {
	Signal(source string)
}

// Ready-signal sources of this module.
const (
	readySourceReplay     = "replay"
	readySourceBlockClear = "block_clear"
)

// signalReady forwards a ready hint when a signal is wired.
func (s *Service) signalReady(source string) {
	if s.ready != nil {
		s.ready.Signal(source)
	}
}

// Service implements the administrative use cases.
type Service struct {
	repo        EndpointRepository
	recipients  RecipientRepository
	deadLetters DeadLetterRepository
	messages    MessageStateRepository
	catalog     TypeCatalog
	audit       AuditSink
	auditLog    AuditLog
	ready       ReadySignal
	// adminSecretDigest is the SHA-256 of the Admin Secret: comparing
	// fixed-size digests keeps the check constant-time in the secret length.
	adminSecretDigest [sha256.Size]byte
	gen               gen.Gen
	log               *slog.Logger
	metrics           *metrics
}

// NewService composes the administrative service.
func NewService(d ServiceDeps) (*Service, error) {
	reg := d.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m, err := newMetrics(reg)
	if err != nil {
		return nil, err
	}
	log := d.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Service{
		repo:              d.Repo,
		recipients:        d.Recipients,
		deadLetters:       d.DeadLetters,
		messages:          d.Messages,
		catalog:           d.Catalog,
		audit:             d.Audit,
		auditLog:          d.AuditLog,
		ready:             d.Ready,
		adminSecretDigest: sha256.Sum256([]byte(d.AdminSecret)),
		gen:               d.Gen,
		log:               log,
		metrics:           m,
	}, nil
}

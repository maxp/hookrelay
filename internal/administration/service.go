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
}

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
	Catalog    TypeCatalog
	Audit      AuditSink
	// AdminSecret must already be resolved and validated by the
	// configuration layer.
	AdminSecret string
	Gen         gen.Gen
	// Logger receives feature events and best-effort audit copies; nil discards them.
	Logger *slog.Logger
	// Registerer receives the administrative audit metrics; nil keeps them
	// on a private registry.
	Registerer prometheus.Registerer
}

// Service implements the administrative use cases.
type Service struct {
	repo       EndpointRepository
	recipients RecipientRepository
	catalog    TypeCatalog
	audit      AuditSink
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
		catalog:           d.Catalog,
		audit:             d.Audit,
		adminSecretDigest: sha256.Sum256([]byte(d.AdminSecret)),
		gen:               d.Gen,
		log:               log,
		metrics:           m,
	}, nil
}

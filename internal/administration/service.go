// Package administration owns Admin Bearer authentication, Webhook Endpoint
// management, and administrative audit intent. Its HTTP transport stays
// local; storage behavior arrives through the narrow interfaces declared
// here and implemented by the Valkey adapter.
package administration

import (
	"context"
	"errors"

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
)

// ErrStoredWrongType reports an unexpected stored key type: an inconsistency
// the current slice does not repair.
var ErrStoredWrongType = errors.New("administration: stored structure has an unexpected type")

// TypeCatalog is the narrow view of the ingestion registry: platform and
// credential-kind allowlist per Webhook Type.
type TypeCatalog interface {
	Lookup(webhookType string) (platform string, credentialKinds []string, ok bool)
}

// AuditSink records best-effort audit events outside Lua transitions.
type AuditSink interface {
	AppendRejectedAuth(ctx context.Context, eventID, requestID, target string)
}

// Service implements the administrative use cases.
type Service struct {
	repo        EndpointRepository
	catalog     TypeCatalog
	audit       AuditSink
	adminSecret []byte
	gen         gen.Gen
}

// NewService composes the administrative service. adminSecret must already
// be resolved and validated by the configuration layer.
func NewService(repo EndpointRepository, catalog TypeCatalog, audit AuditSink, adminSecret string, g gen.Gen) *Service {
	return &Service{repo: repo, catalog: catalog, audit: audit, adminSecret: []byte(adminSecret), gen: g}
}

package administration

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/maxp/hookrelay/internal/jsonbody"
)

// requestIDKey carries the per-request identifier from the auth middleware
// to the handlers.
type requestIDKeyType struct{}

var requestIDKey requestIDKeyType

func requestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// Audit actors of authenticated administrative requests.
const (
	actorAdminBearer  = "admin_bearer"
	actorAdminSession = "admin_session"
)

// actorKey carries the authenticated audit actor from the auth middleware.
type actorKeyType struct{}

var actorKey actorKeyType

// actorFrom returns the request's audit actor; admin_bearer when unset.
func actorFrom(ctx context.Context) string {
	if v, ok := ctx.Value(actorKey).(string); ok {
		return v
	}
	return actorAdminBearer
}

// Handler builds the administrative API routes. Authentication applies only
// to the API routes; health and metrics stay on the surrounding admin mux
// without an Admin Secret (they are protected by the listener placement).
func Handler(svc *Service) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /admin/v1/webhooks", svc.auth(http.HandlerFunc(svc.handleCreate)))
	mux.Handle("GET /admin/v1/webhooks", svc.auth(http.HandlerFunc(svc.handleListWebhooks)))
	mux.Handle("GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}", svc.auth(http.HandlerFunc(svc.handleGet)))
	mux.Handle("PATCH /admin/v1/webhooks/{webhook_type}/{webhook_identifier}", svc.auth(http.HandlerFunc(svc.handlePatch)))
	mux.Handle("DELETE /admin/v1/webhooks/{webhook_type}/{webhook_identifier}", svc.auth(http.HandlerFunc(svc.handleDelete)))
	mux.Handle("GET /admin/v1/bots/{bot_platform}/{bot_id}/webhooks", svc.auth(http.HandlerFunc(svc.handleBotWebhooks)))
	if svc.recipients != nil {
		mux.Handle("GET /admin/v1/recipient-states", svc.auth(http.HandlerFunc(svc.handleListRecipientStates)))
		mux.Handle("POST /admin/v1/recipient-blocks/inspect", svc.auth(http.HandlerFunc(svc.handleInspectBlock)))
		mux.Handle("POST /admin/v1/recipient-blocks/clear", svc.auth(http.HandlerFunc(svc.handleClearBlock)))
	}
	if svc.deadLetters != nil {
		mux.Handle("GET /admin/v1/dead-letters", svc.auth(http.HandlerFunc(svc.handleListDeadLetters)))
		mux.Handle("GET /admin/v1/dead-letters/{message_id}", svc.auth(http.HandlerFunc(svc.handleGetDeadLetter)))
		mux.Handle("POST /admin/v1/dead-letters/{message_id}/replay", svc.auth(http.HandlerFunc(svc.handleReplayDeadLetter)))
		mux.Handle("POST /admin/v1/dead-letters/{message_id}/payload", svc.auth(http.HandlerFunc(svc.handleDeadLetterPayload)))
		mux.Handle("DELETE /admin/v1/dead-letters/{message_id}", svc.auth(http.HandlerFunc(svc.handleDeleteDeadLetter)))
	}
	if svc.messages != nil {
		mux.Handle("GET /admin/v1/messages/{message_id}/delivery-state", svc.auth(http.HandlerFunc(svc.handleDeliveryState)))
	}
	return mux
}

// auth enforces the Admin Secret Bearer token: constant-time comparison,
// uniform 401, rejected authentication audited best effort.
func (s *Service) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := s.gen.UUIDv7()
		w.Header().Set("X-Request-Id", requestID)
		// Administrative responses may carry payloads or CSRF tokens.
		w.Header().Set("Cache-Control", "no-store")
		ctx := context.WithValue(r.Context(), requestIDKey, requestID)
		r = r.WithContext(context.WithValue(ctx, actorKey, actorAdminBearer))

		if !s.secretMatches(bearerToken(r.Header.Get("Authorization"))) {
			// Best-effort audit; refusal never depends on audit success.
			s.recordRejectedAuth(r.Context(), requestID)
			writeError(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Admin Secret", requestID)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(header, prefix))
}

// secretMatches compares the provided token with the Admin Secret in
// constant time. Both sides are reduced to fixed-size SHA-256 digests first,
// so neither the content nor the length of the secret leaks through timing.
func (s *Service) secretMatches(provided string) bool {
	digest := sha256.Sum256([]byte(provided))
	return subtle.ConstantTimeCompare(digest[:], s.adminSecretDigest[:]) == 1
}

func (s *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())

	var req CreateRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if req.Enabled == nil {
		enabled := true
		req.Enabled = &enabled
	}

	view, err := s.CreateWebhook(r.Context(), req, requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	w.Header().Set("Location", fmt.Sprintf("/admin/v1/webhooks/%s/%s", view.Type, view.Identifier))
	writeEndpoint(w, http.StatusCreated, view)
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	ref, ok := endpointPath(r)
	if !ok {
		writeAPIError(w, NotFoundError{}, requestID)
		return
	}

	view, err := s.GetWebhook(r.Context(), ref.Type, ref.Identifier)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeEndpoint(w, http.StatusOK, view)
}

// writeEndpoint writes the entity ETag and the safe endpoint representation.
func writeEndpoint(w http.ResponseWriter, status int, view EndpointView) {
	w.Header().Set("ETag", entityTag(view))
	writeJSON(w, status, endpointBody(view))
}

// entityTag is the strong ETag "<generation_id>:<config_version>".
func entityTag(view EndpointView) string {
	return fmt.Sprintf("%q", fmt.Sprintf("%s:%d", view.GenerationID, view.ConfigVersion))
}

// endpointBody is the safe endpoint representation: never the credential
// value, Valkey keys, or the public scheme and host.
func endpointBody(view EndpointView) endpointResponse {
	return endpointResponse{
		WebhookType:       view.Type,
		WebhookIdentifier: view.Identifier,
		BotPlatform:       view.BotPlatform,
		BotID:             view.BotID,
		Enabled:           view.Enabled,
		Credential:        credentialResponse{Kind: view.CredentialKind, Configured: view.CredentialSet},
		GenerationID:      view.GenerationID,
		ConfigVersion:     view.ConfigVersion,
		CreatedMs:         view.CreatedMs,
		UpdatedMs:         view.UpdatedMs,
		WebhookPath:       fmt.Sprintf("/webhook/%s/%s", view.Type, view.Identifier),
	}
}

// CreateRequest is the strict create body. Unknown fields are rejected.
type CreateRequest struct {
	WebhookType       string          `json:"webhook_type"`
	WebhookIdentifier string          `json:"webhook_identifier"`
	BotID             string          `json:"bot_id"`
	Credential        CredentialInput `json:"credential"`
	Enabled           *bool           `json:"enabled"`
}

type CredentialInput struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type credentialResponse struct {
	Kind       string `json:"kind"`
	Configured bool   `json:"configured"`
}

type endpointResponse struct {
	WebhookType       string             `json:"webhook_type"`
	WebhookIdentifier string             `json:"webhook_identifier"`
	BotPlatform       string             `json:"bot_platform"`
	BotID             string             `json:"bot_id"`
	Enabled           bool               `json:"enabled"`
	Credential        credentialResponse `json:"credential"`
	GenerationID      string             `json:"generation_id"`
	ConfigVersion     int64              `json:"config_version"`
	CreatedMs         int64              `json:"created_ms"`
	UpdatedMs         int64              `json:"updated_ms"`
	WebhookPath       string             `json:"webhook_path"`
}

// decodeBody applies the shared strict JSON discipline and maps its failure
// classes to the bounded Admin API errors.
func decodeBody(r *http.Request, dst any) error {
	err := jsonbody.RequireJSON(r.Header.Get("Content-Type"))
	if err == nil {
		err = jsonbody.Decode(r.Body, dst)
	}
	switch {
	case err == nil:
		return nil
	case errors.Is(err, jsonbody.ErrUnsupportedMediaType):
		return BadRequestError{msg: err.Error(), code: "unsupported_media_type", status: http.StatusUnsupportedMediaType}
	case errors.Is(err, jsonbody.ErrTooLarge):
		return BadRequestError{msg: err.Error(), code: "request_too_large", status: http.StatusRequestEntityTooLarge}
	default:
		return BadRequestError{msg: err.Error()}
	}
}

// writeAPIError maps a failure to its bounded envelope. Anything that is not
// an apiError is an internal bug and never exposes its message.
func writeAPIError(w http.ResponseWriter, err error, requestID string) {
	var ae apiError
	if errors.As(err, &ae) {
		writeError(w, ae.HTTPStatus(), ae.ErrorCode(), ae.Error(), requestID)
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", requestID)
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	writeJSON(w, status, errorEnvelope{Error: errorBody{Code: code, Message: message, RequestID: requestID}})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	data, err := json.Marshal(body)
	if err != nil {
		return
	}
	w.Write(data)
	w.Write([]byte("\n"))
}

package administration

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/maxp/hookrelay/internal/gen"
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

// Body limits from the Admin API contract.
const (
	maxBodyBytes = 16 << 10
	maxJSONDepth = 40
)

// Handler builds the administrative API routes. Authentication applies only
// to the API routes; health and metrics stay on the surrounding admin mux
// without an Admin Secret (they are protected by the listener placement).
func Handler(svc *Service, g gen.Gen) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /admin/v1/webhooks", svc.auth(g, http.HandlerFunc(svc.handleCreate)))
	mux.Handle("GET /admin/v1/webhooks/{webhook_type}/{webhook_identifier}", svc.auth(g, http.HandlerFunc(svc.handleGet)))
	return mux
}

// auth enforces the Admin Secret Bearer token: constant-time comparison,
// uniform 401, rejected authentication audited best effort.
func (s *Service) auth(g gen.Gen, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := g.UUIDv7()
		w.Header().Set("X-Request-Id", requestID)
		r = r.WithContext(context.WithValue(r.Context(), requestIDKey, requestID))

		provided := bearerToken(r.Header.Get("Authorization"))
		if !constantTimeEquals(s.adminSecret, []byte(provided)) {
			// Best-effort audit; refusal never depends on audit success.
			if s.audit != nil {
				s.audit.AppendRejectedAuth(r.Context(), g.UUIDv7(), requestID, "admin_api")
			}
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

// constantTimeEquals compares secrets without leaking length or content.
func constantTimeEquals(a, b []byte) bool {
	return hmac.Equal(a, b)
}

func (s *Service) handleCreate(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())

	var req CreateRequest
	if err := decodeStrict(r.Body, &req); err != nil {
		writeDecodeError(w, err, requestID)
		return
	}
	if req.Enabled == nil {
		enabled := true
		req.Enabled = &enabled
	}

	e, err := s.CreateWebhook(r.Context(), req, requestID)
	if err != nil {
		writeUseCaseError(w, err, requestID)
		return
	}

	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%s:%d", e.GenerationID, e.ConfigVersion)))
	w.Header().Set("Location", fmt.Sprintf("/admin/v1/webhooks/%s/%s", e.Type, e.Identifier))
	writeJSON(w, http.StatusCreated, endpointResponse{
		WebhookType:       e.Type,
		WebhookIdentifier: e.Identifier,
		BotPlatform:       e.BotPlatform,
		BotID:             e.BotID,
		Enabled:           e.Enabled,
		Credential:        credentialResponse{Kind: e.CredentialKind, Configured: true},
		GenerationID:      e.GenerationID,
		ConfigVersion:     e.ConfigVersion,
		CreatedMs:         e.CreatedMs,
		UpdatedMs:         e.UpdatedMs,
		WebhookPath:       fmt.Sprintf("/webhook/%s/%s", e.Type, e.Identifier),
	})
}

func (s *Service) handleGet(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	webhookType := r.PathValue("webhook_type")
	identifier := r.PathValue("webhook_identifier")

	if !isValidWebhookType(webhookType) || !identifierPattern.MatchString(identifier) {
		// Unknown shapes are indistinguishable from unknown endpoints so
		// endpoint registration is not disclosed.
		writeError(w, http.StatusNotFound, "webhook_endpoint_not_found", "webhook endpoint not found", requestID)
		return
	}

	view, err := s.GetWebhook(r.Context(), webhookType, identifier)
	if err != nil {
		writeUseCaseError(w, err, requestID)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf("%q", fmt.Sprintf("%s:%d", view.GenerationID, view.ConfigVersion)))
	writeJSON(w, http.StatusOK, endpointResponse{
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
	})
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

// decodeStrict enforces the 16 KiB limit, JSON content framing, maximum
// nesting depth, and unknown-field rejection.
func decodeStrict(body io.Reader, dst any) error {
	data, err := io.ReadAll(io.LimitReader(body, maxBodyBytes+1))
	if err != nil {
		return BadRequestError{msg: "read body"}
	}
	if len(data) > maxBodyBytes {
		return BadRequestError{msg: "request body larger than 16 KiB", code: "request_too_large"}
	}
	if err := checkDepth(data, maxJSONDepth); err != nil {
		return err
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return BadRequestError{msg: "invalid request body"}
	}
	// Trailing non-whitespace data is rejected.
	if dec.More() {
		return BadRequestError{msg: "trailing data after JSON body"}
	}
	return nil
}

// checkDepth walks JSON tokens to enforce the maximum nesting depth.
func checkDepth(data []byte, max int) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	depth := 0
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return BadRequestError{msg: "invalid request body"}
		}
		switch tok.(type) {
		case json.Delim:
			switch tok.(json.Delim) {
			case '{', '[':
				depth++
				if depth > max {
					return BadRequestError{msg: "maximum JSON nesting depth exceeded"}
				}
			case '}', ']':
				depth--
			}
		}
	}
}

func writeDecodeError(w http.ResponseWriter, err error, requestID string) {
	var bre BadRequestError
	if asBadRequest(err, &bre) {
		writeError(w, http.StatusBadRequest, bre.ErrorCode(), bre.Error(), requestID)
		return
	}
	writeError(w, http.StatusBadRequest, "invalid_request", err.Error(), requestID)
}

func writeUseCaseError(w http.ResponseWriter, err error, requestID string) {
	switch e := err.(type) {
	case BadRequestError:
		writeError(w, http.StatusBadRequest, e.ErrorCode(), e.Error(), requestID)
	case NotFoundError:
		writeError(w, http.StatusNotFound, e.ErrorCode(), e.Error(), requestID)
	case ConflictError:
		writeError(w, http.StatusConflict, e.ErrorCode(), e.Error(), requestID)
	case DependencyError:
		writeError(w, http.StatusServiceUnavailable, e.ErrorCode(), e.Error(), requestID)
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", requestID)
	}
}

func asBadRequest(err error, target *BadRequestError) bool {
	if bre, ok := err.(BadRequestError); ok {
		*target = bre
		return true
	}
	return false
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

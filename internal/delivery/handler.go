package delivery

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/jsonbody"
	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
)

// Claim contract bounds.
const (
	DefaultWaitMs = 30000
	MaxWaitMs     = 30000
)

// AckTimeout and NackTimeout are the acknowledgement request deadlines.
const (
	AckTimeout  = 5 * time.Second
	NackTimeout = 5 * time.Second
)

// Long-poll defaults from the delivery design.
const (
	DefaultRecheckInterval  = 250 * time.Millisecond
	DefaultRecheckJitter    = 50 * time.Millisecond
	DefaultMaxWaitingClaims = 20
	// claimDeadlineSlack extends the claim request deadline past wait_ms.
	claimDeadlineSlack = 5 * time.Second
)

var (
	tokenPattern      = regexp.MustCompile(`^dlv_[A-Za-z0-9_-]{22}$`)
	uuidV7Pattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-7[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
	instanceIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,64}$`)
	reasonCodePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
)

// HandlerDeps carries the Consumer API collaborators.
type HandlerDeps struct {
	Claimer              Claimer
	Acknowledger         Acknowledger
	NegativeAcknowledger NegativeAcknowledger
	Stats                StatsReader
	// Attempts counts completed attempts; maintenance shares the instance.
	Attempts *AttemptMetrics
	// RetryPolicy chooses the retry delay after a failed attempt.
	RetryPolicy RetryPolicy
	// Uniform draws the retry jitter in [0, 1) (rand.Float64 when nil).
	Uniform func() float64
	// ConsumerSecret must already be resolved and validated.
	ConsumerSecret string
	// MaxWaitingClaims bounds concurrent waiting claims in this process
	// (DefaultMaxWaitingClaims when zero).
	MaxWaitingClaims int
	// RecheckInterval and RecheckJitter pace the long-poll rechecks
	// (defaults 250 ms and 0–50 ms).
	RecheckInterval time.Duration
	RecheckJitter   time.Duration
	Gen             gen.Gen
	Clock           gen.Clock
	// Logger receives feature events; nil discards them.
	Logger *slog.Logger
	// Registerer receives the delivery metrics; nil keeps them private.
	Registerer prometheus.Registerer
}

// Handler serves the Consumer API routes on the public listener.
type Handler struct {
	d            HandlerDeps
	mux          *http.ServeMux
	log          *slog.Logger
	metrics      *metrics
	secretDigest [sha256.Size]byte
	// waitSlots holds one token per waiting claim.
	waitSlots chan struct{}
	// stopping is closed when shutdown begins; waiting claims end with 503.
	stopping     chan struct{}
	stopOnce     sync.Once
	shuttingDown atomic.Bool
}

// NewHandler composes the Consumer API.
func NewHandler(d HandlerDeps) (*Handler, error) {
	if d.Clock == nil || d.Gen == nil || d.Claimer == nil || d.Acknowledger == nil || d.NegativeAcknowledger == nil || d.Attempts == nil {
		return nil, errors.New("delivery: Claimer, Acknowledger, NegativeAcknowledger, Attempts, Gen, and Clock are required")
	}
	if err := d.RetryPolicy.Validate(); err != nil {
		return nil, err
	}
	if d.Uniform == nil {
		d.Uniform = rand.Float64
	}
	reg := d.Registerer
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	if d.MaxWaitingClaims <= 0 {
		d.MaxWaitingClaims = DefaultMaxWaitingClaims
	}
	if d.RecheckInterval <= 0 {
		d.RecheckInterval = DefaultRecheckInterval
	}
	if d.RecheckJitter < 0 {
		d.RecheckJitter = 0
	} else if d.RecheckJitter == 0 {
		d.RecheckJitter = DefaultRecheckJitter
	}
	h := &Handler{
		d: d, mux: http.NewServeMux(), secretDigest: sha256.Sum256([]byte(d.ConsumerSecret)),
		waitSlots: make(chan struct{}, d.MaxWaitingClaims), stopping: make(chan struct{}),
	}
	m, err := newMetrics(reg, func() float64 { return float64(len(h.waitSlots)) })
	if err != nil {
		return nil, err
	}
	h.metrics = m
	h.log = d.Logger
	if h.log == nil {
		h.log = slog.New(slog.DiscardHandler)
	}
	h.mux.Handle("POST /v1/deliveries/claim", h.auth(h.handleClaim))
	h.mux.Handle("POST /v1/deliveries/ack", h.auth(h.handleAck))
	h.mux.Handle("POST /v1/deliveries/nack", h.auth(h.handleNack))
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) { h.mux.ServeHTTP(w, r) }

// call is the per-request context passed to handlers.
type call struct {
	requestID  string
	instanceID string
	start      time.Time
}

// auth enforces the shared Consumer Secret: constant-time comparison of
// fixed-size digests and a uniform 401. Every response carries X-Request-Id.
func (h *Handler) auth(next func(http.ResponseWriter, *http.Request, call)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c := call{requestID: h.d.Gen.UUIDv7(), start: h.d.Clock.Now()}
		w.Header().Set("X-Request-Id", c.requestID)
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		provided := sha256.Sum256([]byte(strings.TrimPrefix(header, prefix)))
		if !strings.HasPrefix(header, prefix) || subtle.ConstantTimeCompare(provided[:], h.secretDigest[:]) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthenticated", "missing or invalid Consumer Secret", c.requestID)
			return
		}
		// The instance identifier is diagnostics only; an unusable value is
		// ignored rather than rejected.
		if id := r.Header.Get("Consumer-Instance-Id"); instanceIDPattern.MatchString(id) {
			c.instanceID = id
		}
		next(w, r, c)
	})
}

type claimRequest struct {
	OperationID string `json:"operation_id"`
	WaitMs      *int64 `json:"wait_ms"`
}

func (h *Handler) handleClaim(w http.ResponseWriter, r *http.Request, c call) {
	var req claimRequest
	if status, code, msg, ok := decode(r, &req); !ok {
		h.metrics.claims.WithLabelValues("invalid_request").Inc()
		writeError(w, status, code, msg, c.requestID)
		return
	}
	waitMs := int64(DefaultWaitMs)
	if req.WaitMs != nil {
		waitMs = *req.WaitMs
	}
	switch {
	case !uuidV7Pattern.MatchString(req.OperationID):
		h.metrics.claims.WithLabelValues("invalid_request").Inc()
		writeError(w, http.StatusBadRequest, "invalid_request", "operation_id must be a UUIDv7", c.requestID)
		return
	case waitMs < 0 || waitMs > MaxWaitMs:
		h.metrics.claims.WithLabelValues("invalid_request").Inc()
		writeError(w, http.StatusBadRequest, "invalid_request", "wait_ms must be between 0 and 30000", c.requestID)
		return
	}

	suffix, err := h.d.Gen.Base64URL(16)
	if err != nil {
		h.metrics.claims.WithLabelValues("internal_error").Inc()
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", c.requestID)
		return
	}
	token := "dlv_" + suffix
	tokenDigest := sha256.Sum256([]byte(token))
	argsDigest := sha256.Sum256([]byte(req.OperationID + "\n" + strconv.FormatInt(waitMs, 10)))

	if h.shuttingDown.Load() {
		h.respondShutdown(w, c)
		return
	}
	wait := time.Duration(waitMs) * time.Millisecond
	if wait > 0 {
		// Process-local waiting-claim limit.
		select {
		case h.waitSlots <- struct{}{}:
			defer func() { <-h.waitSlots }()
		default:
			h.metrics.claims.WithLabelValues("waiting_limit_exceeded").Inc()
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusTooManyRequests, "consumer_limit_exceeded", "the waiting claim limit is reached", c.requestID)
			return
		}
	}

	// Route deadline: wait_ms plus slack.
	ctx, cancel := context.WithTimeout(r.Context(), wait+claimDeadlineSlack)
	defer cancel()
	claimReq := ClaimRequest{
		OperationID:        req.OperationID,
		ArgsDigest:         hex.EncodeToString(argsDigest[:]),
		Token:              token,
		TokenDigest:        hex.EncodeToString(tokenDigest[:]),
		ConsumerInstanceID: c.instanceID,
	}
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	final := wait == 0
	for {
		// Only the final check at the deadline records an empty outcome, so
		// rechecks under the same operation_id never replay an early empty.
		claimReq.RecordEmpty = final
		res := h.d.Claimer.Claim(ctx, claimReq)
		if res.Outcome == ClaimDependencyUnavailable && r.Context().Err() != nil {
			// The client left while the check ran; its failure is the
			// cancellation, not a Valkey outage. A lease the check may have
			// created is recovered by repeating the same operation_id.
			h.metrics.claims.WithLabelValues("cancelled").Inc()
			return
		}
		if res.Outcome != ClaimEmpty || final {
			h.respondClaim(w, c, req.OperationID, res)
			return
		}
		pause := time.NewTimer(h.recheckDelay())
		select {
		case <-pause.C:
		case <-deadline.C:
			pause.Stop()
			final = true
		case <-h.stopping:
			pause.Stop()
			h.respondShutdown(w, c)
			return
		case <-r.Context().Done():
			// The client left before a lease: abandon the wait. A lease that
			// raced with the disconnect is recovered by repeating the same
			// operation_id.
			pause.Stop()
			h.metrics.claims.WithLabelValues("cancelled").Inc()
			return
		}
	}
}

// recheckDelay is the recheck interval plus uniform jitter.
func (h *Handler) recheckDelay() time.Duration {
	return h.d.RecheckInterval + rand.N(h.d.RecheckJitter+1)
}

func (h *Handler) respondShutdown(w http.ResponseWriter, c call) {
	h.metrics.claims.WithLabelValues("shutting_down").Inc()
	w.Header().Set("Retry-After", "1")
	writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "the service is shutting down; retry the same operation_id", c.requestID)
}

// Shutdown ends outstanding long polls with 503 + Retry-After and refuses
// new claims. It is called before the listeners drain.
func (h *Handler) Shutdown() {
	h.stopOnce.Do(func() {
		h.shuttingDown.Store(true)
		close(h.stopping)
	})
}

func (h *Handler) respondClaim(w http.ResponseWriter, c call, operationID string, res ClaimResult) {
	h.metrics.claims.WithLabelValues(string(res.Outcome)).Inc()
	if res.BlockedDetected > 0 {
		observability.LogEvent(h.log, slog.LevelError, "recipient_blocked_detected", "claim isolated inconsistent Recipient state behind a block marker",
			"request_id", c.requestID, "blocked_detected", res.BlockedDetected, "error_code", "recipient_blocked")
	}
	switch res.Outcome {
	case ClaimClaimed, ClaimReplayActive:
		var msg model.CanonicalMessage
		if err := json.Unmarshal(res.MessageJSON, &msg); err != nil {
			observability.LogEvent(h.log, slog.LevelError, "delivery_claim_failed", "stored message is unreadable",
				"request_id", c.requestID, "message_id", res.Delivery.MessageID, "error_code", "internal_error")
			writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", c.requestID)
			return
		}
		fields := []any{
			"request_id", c.requestID, "operation_id", operationID, "message_id", res.Delivery.MessageID,
			"recipient_scope", string(msg.Recipient.Scope), "bot_platform", msg.Recipient.BotPlatform, "bot_id", msg.Recipient.BotID,
			"delivery_cycle", res.Delivery.DeliveryCycle, "attempt", res.Delivery.Attempt,
			"replay", res.Outcome == ClaimReplayActive, "duration_ms", h.d.Clock.Now().Sub(c.start).Milliseconds(),
		}
		if msg.Recipient.ChatID != "" {
			fields = append(fields, "chat_id", msg.Recipient.ChatID)
		}
		if msg.Recipient.UserID != "" {
			fields = append(fields, "user_id", msg.Recipient.UserID)
		}
		if c.instanceID != "" {
			fields = append(fields, "consumer_instance_id", c.instanceID)
		}
		observability.LogEvent(h.log, slog.LevelInfo, "delivery_claimed", "delivery claimed", fields...)
		writeJSON(w, http.StatusOK, claimResponse{
			Delivery: deliveryBody{
				DeliveryToken:  res.Delivery.Token,
				DeliveryCycle:  res.Delivery.DeliveryCycle,
				Attempt:        res.Delivery.Attempt,
				ClaimedMs:      res.Delivery.ClaimedMs,
				LeaseExpiresMs: res.Delivery.LeaseExpiresMs,
			},
			Message: json.RawMessage(res.MessageJSON),
		})
	case ClaimEmpty, ClaimReplayEmpty:
		w.WriteHeader(http.StatusNoContent)
	case ClaimNoLongerActive:
		writeError(w, http.StatusConflict, "claim_no_longer_active", "the claimed delivery attempt has already completed", c.requestID)
	case ClaimOperationConflict:
		writeError(w, http.StatusConflict, "operation_conflict", "operation_id was already used with other arguments", c.requestID)
	case ClaimLimitExceeded:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "consumer_limit_exceeded", "the active lease limit is reached", c.requestID)
	case ClaimInternalFailure:
		observability.LogEvent(h.log, slog.LevelError, "delivery_claim_failed", "claim hit unexpected stored state",
			"request_id", c.requestID, "operation_id", operationID, "error_code", "internal_error")
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", c.requestID)
	default:
		observability.LogEvent(h.log, slog.LevelError, "delivery_claim_failed", "claim could not reach Valkey",
			"request_id", c.requestID, "operation_id", operationID, "error_code", "dependency_unavailable")
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "dependency unavailable; repeat the same operation_id", c.requestID)
	}
}

// decode applies the shared strict body discipline and maps its failures to
// the Consumer API error codes.
func decode(r *http.Request, dst any) (status int, code, message string, ok bool) {
	err := jsonbody.RequireJSON(r.Header.Get("Content-Type"))
	if err == nil {
		err = jsonbody.Decode(r.Body, dst)
	}
	switch {
	case err == nil:
		return 0, "", "", true
	case errors.Is(err, jsonbody.ErrUnsupportedMediaType):
		return http.StatusUnsupportedMediaType, "unsupported_media_type", err.Error(), false
	case errors.Is(err, jsonbody.ErrTooLarge):
		return http.StatusRequestEntityTooLarge, "request_too_large", err.Error(), false
	default:
		return http.StatusBadRequest, "invalid_request", err.Error(), false
	}
}

type deliveryBody struct {
	DeliveryToken  string `json:"delivery_token"`
	DeliveryCycle  int64  `json:"delivery_cycle"`
	Attempt        int64  `json:"attempt"`
	ClaimedMs      int64  `json:"claimed_ms"`
	LeaseExpiresMs int64  `json:"lease_expires_ms"`
}

type claimResponse struct {
	Delivery deliveryBody    `json:"delivery"`
	Message  json.RawMessage `json:"message"`
}

type errorBody struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, code, message, requestID string) {
	var b errorBody
	b.Error.Code, b.Error.Message, b.Error.RequestID = code, message, requestID
	writeJSON(w, status, b)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// RefreshGauges updates the delivery gauges from Valkey; the application
// runs it on the readiness monitor cadence rather than on scrape.
func (h *Handler) RefreshGauges(ctx context.Context) {
	if h.d.Stats == nil {
		return
	}
	s, err := h.d.Stats.Stats(ctx)
	if err != nil {
		return
	}
	h.metrics.activeLeases.Set(float64(s.ActiveLeases))
	h.metrics.readyRecipients.Set(float64(s.ReadyRecipients))
	h.metrics.blockedRecipients.Set(float64(s.BlockedRecipients))
	h.metrics.queueMessages.Set(float64(s.QueuedMessages))
	h.metrics.retriesWaiting.Set(float64(s.RetriesWaiting))
}

type ackRequest struct {
	DeliveryToken string `json:"delivery_token"`
}

type ackResponse struct {
	Status         string `json:"status"`
	MessageID      string `json:"message_id"`
	AcknowledgedMs int64  `json:"acknowledged_ms"`
}

func (h *Handler) handleAck(w http.ResponseWriter, r *http.Request, c call) {
	var req ackRequest
	if status, code, msg, ok := decode(r, &req); !ok {
		writeError(w, status, code, msg, c.requestID)
		return
	}
	if !tokenPattern.MatchString(req.DeliveryToken) {
		writeError(w, http.StatusBadRequest, "invalid_request", "delivery_token is malformed", c.requestID)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), AckTimeout)
	defer cancel()
	digest := sha256.Sum256([]byte(req.DeliveryToken))
	res := h.d.Acknowledger.Ack(ctx, AckRequest{Token: req.DeliveryToken, TokenDigest: hex.EncodeToString(digest[:])})

	switch res.Outcome {
	case AckAcknowledged, AckAlreadyAcknowledged:
		if res.Outcome == AckAcknowledged {
			h.logAcknowledged(c, res)
		}
		writeJSON(w, http.StatusOK, ackResponse{Status: "acknowledged", MessageID: res.MessageID, AcknowledgedMs: res.AcknowledgedMs})
	case AckNotFound:
		writeError(w, http.StatusNotFound, "delivery_token_not_found", "unknown or expired delivery token", c.requestID)
	case AckStale:
		writeError(w, http.StatusConflict, "stale_delivery_token", "the delivery attempt is no longer active", c.requestID)
	case AckAlreadyNacked:
		writeError(w, http.StatusConflict, "delivery_already_nacked", "the delivery attempt was negatively acknowledged", c.requestID)
	case AckRecipientBlocked:
		writeError(w, http.StatusConflict, "recipient_blocked", "the recipient is blocked pending operator recovery", c.requestID)
	case AckInternalFailure:
		observability.LogEvent(h.log, slog.LevelError, "delivery_ack_failed", "acknowledgement hit unexpected stored state",
			"request_id", c.requestID, "error_code", "internal_error")
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", c.requestID)
	default:
		observability.LogEvent(h.log, slog.LevelError, "delivery_ack_failed", "acknowledgement could not reach Valkey",
			"request_id", c.requestID, "error_code", "dependency_unavailable")
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "dependency unavailable; repeat the acknowledgement", c.requestID)
	}
}

// logAcknowledged records the feature event and the completed attempt.
func (h *Handler) logAcknowledged(c call, res AckResult) {
	fields := []any{
		"request_id", c.requestID, "message_id", res.MessageID,
		"delivery_cycle", res.DeliveryCycle, "attempt", res.Attempt,
		"duration_ms", h.d.Clock.Now().Sub(c.start).Milliseconds(),
	}
	fields, scope := h.recipientFields(c, res.RecipientIdentity, fields)
	h.d.Attempts.Observe(scope, "acknowledged", res.ClaimedMs, res.AcknowledgedMs)
	observability.LogEvent(h.log, slog.LevelInfo, "delivery_acknowledged", "delivery acknowledged", fields...)
}

// recipientFields appends the Recipient and consumer diagnostics to event
// fields and returns the bounded recipient_scope label.
func (h *Handler) recipientFields(c call, recipientIdentity string, fields []any) ([]any, string) {
	fields, scope := recipientEventFields(recipientIdentity, fields)
	if c.instanceID != "" {
		fields = append(fields, "consumer_instance_id", c.instanceID)
	}
	return fields, scope
}

// recipientEventFields appends the Recipient's safe identifiers to event
// fields and returns the bounded recipient_scope label.
func recipientEventFields(recipientIdentity string, fields []any) ([]any, string) {
	scope := "unknown"
	if rcpt, err := model.ParseIdentity(recipientIdentity); err == nil {
		scope = string(rcpt.Scope)
		fields = append(fields, "recipient_scope", scope, "bot_platform", rcpt.BotPlatform, "bot_id", rcpt.BotID)
		if rcpt.ChatID != "" {
			fields = append(fields, "chat_id", rcpt.ChatID)
		}
		if rcpt.UserID != "" {
			fields = append(fields, "user_id", rcpt.UserID)
		}
	}
	return fields, scope
}

type nackRequest struct {
	DeliveryToken string  `json:"delivery_token"`
	ReasonCode    *string `json:"reason_code"`
}

type nackResponse struct {
	Status    string `json:"status"`
	MessageID string `json:"message_id"`
	Attempt   int64  `json:"attempt"`
	RetryAtMs int64  `json:"retry_at_ms"`
}

func (h *Handler) handleNack(w http.ResponseWriter, r *http.Request, c call) {
	var req nackRequest
	if status, code, msg, ok := decode(r, &req); !ok {
		writeError(w, status, code, msg, c.requestID)
		return
	}
	if !tokenPattern.MatchString(req.DeliveryToken) {
		writeError(w, http.StatusBadRequest, "invalid_request", "delivery_token is malformed", c.requestID)
		return
	}
	reason := ""
	if req.ReasonCode != nil {
		if !reasonCodePattern.MatchString(*req.ReasonCode) {
			writeError(w, http.StatusBadRequest, "invalid_request", "reason_code must be 1-64 characters of [A-Za-z0-9_.:-]", c.requestID)
			return
		}
		reason = *req.ReasonCode
	}
	ctx, cancel := context.WithTimeout(r.Context(), NackTimeout)
	defer cancel()
	digest := sha256.Sum256([]byte(req.DeliveryToken))
	res := h.d.NegativeAcknowledger.Nack(ctx, NackRequest{
		Token: req.DeliveryToken, TokenDigest: hex.EncodeToString(digest[:]), ReasonCode: reason,
		RetryDelaysMs: h.d.RetryPolicy.DrawDelaysMs(h.d.Uniform), MaxAttempts: h.d.RetryPolicy.MaxAttempts,
	})

	switch res.Outcome {
	case NackRetryScheduled, NackAlreadyNacked:
		if res.Outcome == NackRetryScheduled {
			h.logNacked(c, res, reason)
		}
		writeJSON(w, http.StatusOK, nackResponse{Status: res.Result, MessageID: res.MessageID, Attempt: res.Attempt, RetryAtMs: res.RetryAtMs})
	case NackAlreadyAcknowledged:
		writeError(w, http.StatusConflict, "delivery_already_acknowledged", "the delivery attempt was acknowledged", c.requestID)
	case NackNotFound:
		writeError(w, http.StatusNotFound, "delivery_token_not_found", "unknown or expired delivery token", c.requestID)
	case NackStale:
		writeError(w, http.StatusConflict, "stale_delivery_token", "the delivery attempt is no longer active", c.requestID)
	case NackRecipientBlocked:
		writeError(w, http.StatusConflict, "recipient_blocked", "the recipient is blocked pending operator recovery", c.requestID)
	case NackAttemptsExhausted, NackInternalFailure:
		observability.LogEvent(h.log, slog.LevelError, "delivery_nack_failed", "negative acknowledgement hit unexpected stored state",
			"request_id", c.requestID, "error_code", "internal_error", "outcome", string(res.Outcome))
		writeError(w, http.StatusInternalServerError, "internal_error", "unexpected internal error", c.requestID)
	default:
		observability.LogEvent(h.log, slog.LevelError, "delivery_nack_failed", "negative acknowledgement could not reach Valkey",
			"request_id", c.requestID, "error_code", "dependency_unavailable")
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "dependency_unavailable", "dependency unavailable; repeat the negative acknowledgement", c.requestID)
	}
}

// logNacked records the feature event and the failed attempt.
func (h *Handler) logNacked(c call, res NackResult, reason string) {
	fields := []any{
		"request_id", c.requestID, "message_id", res.MessageID,
		"delivery_cycle", res.DeliveryCycle, "attempt", res.Attempt, "retry_at_ms", res.RetryAtMs,
		"duration_ms", h.d.Clock.Now().Sub(c.start).Milliseconds(),
	}
	if reason != "" {
		fields = append(fields, "reason_code", reason)
	}
	fields, scope := h.recipientFields(c, res.RecipientIdentity, fields)
	h.d.Attempts.Observe(scope, "nack", res.ClaimedMs, res.CompletedMs)
	observability.LogEvent(h.log, slog.LevelInfo, "delivery_nacked", "delivery negatively acknowledged; retry scheduled", fields...)
}

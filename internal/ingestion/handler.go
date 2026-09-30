package ingestion

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/ratelimit"
)

// RoutePrefix is the webhook route prefix on the public listener.
const RoutePrefix = "/webhook/"

// MaxBodyBytes is the webhook request body hard limit.
const MaxBodyBytes = 256 << 10

// DefaultRequestTimeout is the webhook request deadline; it covers body
// reading so a slow sender cannot hold a request open.
const DefaultRequestTimeout = 10 * time.Second

// DefaultMaxInflight is the default in-flight webhook limit.
const DefaultMaxInflight = 100

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// HandlerDeps carries the webhook pipeline collaborators.
type HandlerDeps struct {
	Registry       *Registry
	Endpoints      EndpointLookup
	Acceptor       MessageAcceptor
	Gen            gen.Gen
	Clock          gen.Clock
	TrustedProxies []*net.IPNet
	// Capacity reads the global acceptance capacity; nil disables the
	// capacity stop conditions.
	Capacity CapacityReader
	// OnAcceptanceStop is called when an acceptance hits a global queue,
	// deduplication, or memory stop condition.
	OnAcceptanceStop func()
	// RateLimits are the process-local token buckets (zero rates disable).
	RateLimits RateLimits
	// MemoryStopPercent stops new acceptance at this share of Valkey
	// maxmemory (0, or maxmemory 0, disables the stop).
	MemoryStopPercent int
	// DedupRetention and DedupMinRetention describe deduplication
	// retention: records younger than the minimum are never evicted early.
	DedupRetention    time.Duration
	DedupMinRetention time.Duration
	// DedupEvictor runs proactive early eviction in MaintainCapacity; nil
	// disables it (acceptance-time eviction is in the acceptor).
	DedupEvictor DedupEvictor
	// Ready is signalled after each acceptance so a waiting claim can wake
	// early; nil disables the hint.
	Ready ReadySignal
	// MaxInflight bounds concurrent webhook requests past route resolution
	// (DefaultMaxInflight when zero).
	MaxInflight int
	// RequestTimeout is the request deadline (DefaultRequestTimeout when
	// zero).
	RequestTimeout time.Duration
	// Logger receives feature events; nil discards them.
	Logger *slog.Logger
	// Registerer receives the ingestion metrics; nil keeps them private.
	Registerer prometheus.Registerer
}

// Handler serves POST /webhook/{webhook_type}/{webhook_identifier}.
type Handler struct {
	d        HandlerDeps
	log      *slog.Logger
	metrics  *metrics
	inflight chan struct{}
	limiter  *ratelimit.Limiter
	// memoryStopped is the last evaluated memory stop condition.
	memoryStopped atomic.Bool
}

// NewHandler composes the webhook pipeline.
func NewHandler(d HandlerDeps) (*Handler, error) {
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
	if d.Clock == nil {
		return nil, errors.New("ingestion: a Clock is required")
	}
	if d.MaxInflight <= 0 {
		d.MaxInflight = DefaultMaxInflight
	}
	if d.RequestTimeout <= 0 {
		d.RequestTimeout = DefaultRequestTimeout
	}
	h := &Handler{d: d, log: log, metrics: m, inflight: make(chan struct{}, d.MaxInflight), limiter: newRateLimiter(d.RateLimits, d.Clock.Now())}
	if err := m.registerInflight(reg, func() float64 { return float64(len(h.inflight)) }); err != nil {
		return nil, err
	}
	return h, nil
}

// request is the per-request pipeline state used for logging and metrics.
type request struct {
	id          string
	start       time.Time
	typeLabel   string
	webhookType string
	identifier  string
	def         Definition
	endpoint    *Endpoint
	sourceIP    net.IP
	bodySize    int
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req := &request{id: h.d.Gen.UUIDv7(), start: h.d.Clock.Now(), typeLabel: unknownLabel}
	w.Header().Set("X-Request-Id", req.id)
	ctx, cancel := context.WithTimeout(r.Context(), h.d.RequestTimeout)
	defer cancel()
	// The body read shares the request deadline.
	_ = http.NewResponseController(w).SetReadDeadline(req.start.Add(h.d.RequestTimeout))
	outcome := h.serve(w, r.WithContext(ctx), req)
	h.metrics.requests.WithLabelValues(req.typeLabel, string(outcome)).Inc()
	h.metrics.duration.WithLabelValues(req.typeLabel).Observe(h.d.Clock.Now().Sub(req.start).Seconds())
}

// serve runs the pipeline in the documented order: resolve route, verify,
// convert, deduplicate and accept, respond.
func (h *Handler) serve(w http.ResponseWriter, r *http.Request, req *request) Outcome {
	// Route shape is checked on the escaped path, so encoded slashes,
	// Unicode, whitespace, colons, and dot segments never match.
	parts := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), RoutePrefix), "/")
	if len(parts) != 2 || !WebhookTypePattern.MatchString(parts[0]) || !identifierPattern.MatchString(parts[1]) {
		return h.respond(w, DefaultResponses{}, OutcomeUnknownEndpoint)
	}
	req.webhookType, req.identifier = parts[0], parts[1]
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		return h.respond(w, DefaultResponses{}, OutcomeMethodNotAllowed)
	}

	def, ok := h.d.Registry.Lookup(WebhookType(req.webhookType))
	if !ok {
		return h.respond(w, DefaultResponses{}, OutcomeUnknownEndpoint)
	}
	req.def, req.typeLabel = def, req.webhookType

	endpoint, err := h.d.Endpoints.LookupEndpoint(r.Context(), req.webhookType, req.identifier)
	if err != nil {
		outcome := OutcomeDependencyUnavailable
		if errors.Is(err, ErrStoredWrongType) {
			outcome = OutcomeInternalError
		}
		h.logFailure(req, outcome, "endpoint lookup failed")
		return h.respond(w, def.ResponseMapper, outcome)
	}
	// Unknown and disabled endpoints are indistinguishable.
	if endpoint == nil || !endpoint.Enabled {
		return h.respond(w, def.ResponseMapper, OutcomeUnknownEndpoint)
	}
	req.endpoint = endpoint

	// Rate limits: checked before the in-flight slot and the body read.
	if ok, retryAfter := h.limiter.Allow(req.webhookType+":"+req.identifier, h.d.Clock.Now()); !ok {
		w.Header().Set("Retry-After", strconv.Itoa(retryAfter))
		return h.reject(w, req, OutcomeRateLimited)
	}

	// Process protection: bounded in-flight requests past route resolution,
	// acquired before the body is read.
	select {
	case h.inflight <- struct{}{}:
		defer func() { <-h.inflight }()
	default:
		return h.reject(w, req, OutcomeOverloaded)
	}

	var malformedChain bool
	req.sourceIP, malformedChain = sourceIP(r, h.d.TrustedProxies)
	if malformedChain {
		observability.LogEvent(h.log, slog.LevelWarn, "forwarded_chain_malformed", "malformed X-Forwarded-For chain; using the direct peer",
			"request_id", req.id, "source_ip", req.sourceIP.String())
	}

	if !acceptableEncoding(r.Header.Values("Content-Encoding")) || !acceptableMediaType(r.Header.Get("Content-Type")) {
		return h.reject(w, req, OutcomeUnsupportedMediaType)
	}
	if r.ContentLength > MaxBodyBytes {
		return h.reject(w, req, OutcomeOversizedBody)
	}
	body, sum, err := readBody(r.Body)
	req.bodySize = len(body)
	h.metrics.bodyBytes.WithLabelValues(req.typeLabel).Observe(float64(len(body)))
	switch {
	case errors.Is(err, errBodyTooLarge):
		return h.reject(w, req, OutcomeOversizedBody)
	case err != nil && (errors.Is(err, os.ErrDeadlineExceeded) || r.Context().Err() != nil):
		return h.reject(w, req, OutcomeRequestTimeout)
	case err != nil:
		return h.reject(w, req, OutcomeBodyReadFailed)
	}

	verifyHeader := http.Header{}
	for _, name := range def.Verifier.Headers() {
		if v := r.Header.Values(name); len(v) > 0 {
			verifyHeader[http.CanonicalHeaderKey(name)] = v
		}
	}
	if ok, reason := def.Verifier.Verify(VerifyInput{Header: verifyHeader, Body: body, SourceIP: req.sourceIP, Credential: endpoint.CredentialValue}); !ok {
		observability.LogEvent(h.log, slog.LevelWarn, "webhook_verification_failed", "webhook verification failed",
			append(req.logFields(), "reason", reason, "body_size", req.bodySize)...)
		return h.respond(w, def.ResponseMapper, OutcomeVerificationFailure)
	}

	conv, err := def.Converter.Convert(ConvertInput{Body: body, BodySHA256: sum, BotPlatform: string(def.Platform), BotID: endpoint.BotID})
	if err != nil {
		return h.reject(w, req, OutcomeInvalidJSON)
	}
	if conv.OccurredIssue != "" {
		h.metrics.eventTimeIssues.WithLabelValues(string(def.Platform), conv.OccurredIssue).Inc()
		observability.LogEvent(h.log, slog.LevelWarn, "event_time_invalid", "platform event timestamp is missing or unusable; occurred_ms omitted",
			append(req.logFields(), "platform_event_type", loggedEventType(conv.PlatformEventType), "reason", conv.OccurredIssue)...)
	}
	return h.accept(w, r.Context(), req, conv, sum)
}

func (h *Handler) accept(w http.ResponseWriter, ctx context.Context, req *request, conv Conversion, sum [sha256.Size]byte) Outcome {
	msg := model.CanonicalMessage{
		MessageID:         h.d.Gen.UUIDv7(),
		ReceivedMs:        h.d.Clock.Now().UnixMilli(),
		OccurredMs:        conv.OccurredMs,
		Recipient:         conv.Recipient,
		SourceEventID:     conv.SourceEventID,
		PlatformEventType: conv.PlatformEventType,
		Payload:           conv.Payload,
		RoutingIssue:      conv.RoutingIssue,
	}
	data, err := msg.Encode()
	if err != nil {
		h.logFailure(req, OutcomeInternalError, "canonical message encoding failed")
		return h.respond(w, req.def.ResponseMapper, OutcomeInternalError)
	}

	var result AcceptResult
	if h.memoryStopped.Load() {
		// Valkey memory reached the stop share: no new message is written;
		// the platform retries once draining frees memory.
		result = AcceptResult{Outcome: AcceptMemoryStop}
	} else {
		result = h.d.Acceptor.Accept(ctx, AcceptRequest{
			MessageID:           msg.MessageID,
			DedupIdentityDigest: dedupIdentityDigest(req.webhookType, string(req.def.Platform), req.endpoint.BotID, conv.DedupKey),
			BodyDigest:          hex.EncodeToString(sum[:]),
			ReceivedMs:          msg.ReceivedMs,
			OccurredMs:          msg.OccurredMs,
			MessageJSON:         data,
			RecipientIdentity:   msg.Recipient.Identity(),
		})
	}
	if result.EarlyEvicted > 0 {
		h.metrics.dedupEarlyEvictions.Add(float64(result.EarlyEvicted))
	}

	fields := append(req.logFields(),
		"recipient_scope", string(msg.Recipient.Scope),
		"platform_event_type", loggedEventType(conv.PlatformEventType),
		"body_size", req.bodySize,
		"canonical_payload_size", len(conv.Payload),
	)
	if msg.Recipient.ChatID != "" {
		fields = append(fields, "chat_id", msg.Recipient.ChatID)
	}
	if msg.Recipient.UserID != "" {
		fields = append(fields, "user_id", msg.Recipient.UserID)
	}

	outcome := outcomeOf(result.Outcome)
	switch result.Outcome {
	case AcceptAccepted:
		if h.d.Ready != nil {
			h.d.Ready.Signal(readySourceAccept)
		}
		h.metrics.accepted.WithLabelValues(string(req.def.Platform), string(msg.Recipient.Scope)).Inc()
		if conv.RoutingIssue != "" {
			h.metrics.routingIssues.WithLabelValues(string(req.def.Platform), conv.RoutingIssue).Inc()
		}
	case AcceptDuplicate, AcceptDuplicateConflict:
		h.metrics.duplicates.WithLabelValues(req.typeLabel).Inc()
		if result.Outcome == AcceptDuplicateConflict {
			h.metrics.dedupConflicts.WithLabelValues(req.typeLabel).Inc()
		}
	case AcceptDedupCapacity:
		h.metrics.dedupCapacity.Inc()
	}
	if (result.Outcome == AcceptGlobalCapacity || result.Outcome == AcceptDedupCapacity) && h.d.OnAcceptanceStop != nil {
		// Surface the stop condition immediately; the periodic capacity
		// probe restores acceptance once capacity returns.
		h.d.OnAcceptanceStop()
	}

	status, _ := req.def.ResponseMapper.Status(outcome)
	fields = append(fields, "status", status, "duration_ms", h.d.Clock.Now().Sub(req.start).Milliseconds())
	switch outcome {
	case OutcomeAccepted:
		observability.LogEvent(h.log, slog.LevelInfo, "webhook_accepted", "webhook accepted",
			append(fields, "message_id", result.MessageID)...)
	case OutcomeDuplicate:
		level := slog.LevelInfo
		if result.Outcome == AcceptDuplicateConflict {
			// Same Deduplication Identity, different bytes: still a
			// duplicate, surfaced for investigation.
			level = slog.LevelWarn
			fields = append(fields, "dedup_conflict", true)
		}
		observability.LogEvent(h.log, level, "webhook_duplicate", "webhook duplicate",
			append(fields, "message_id", result.MessageID)...)
	case OutcomeInternalError:
		observability.LogEvent(h.log, slog.LevelError, "webhook_failed", "webhook acceptance hit inconsistent storage state",
			append(fields, "outcome", string(outcome), "error_code", string(outcome), "reason_code", string(result.Outcome))...)
	default:
		observability.LogEvent(h.log, slog.LevelWarn, "webhook_rejected", "webhook not accepted",
			append(fields, "outcome", string(outcome), "reason_code", string(result.Outcome))...)
	}
	return h.respond(w, req.def.ResponseMapper, outcome)
}

// outcomeOf maps an acceptance result to the request outcome.
func outcomeOf(a AcceptOutcome) Outcome {
	switch a {
	case AcceptAccepted:
		return OutcomeAccepted
	case AcceptDuplicate, AcceptDuplicateConflict:
		return OutcomeDuplicate
	case AcceptRecipientBlocked:
		return OutcomeRecipientBlocked
	case AcceptRecipientCapacity, AcceptGlobalCapacity, AcceptDedupCapacity, AcceptMemoryStop:
		return OutcomeCapacityRejection
	case AcceptInternalFailure:
		return OutcomeInternalError
	default:
		return OutcomeDependencyUnavailable
	}
}

// maxLoggedEventType bounds an unknown, sender-chosen event field name in
// logs; the Canonical Message keeps it in full.
const maxLoggedEventType = 64

func loggedEventType(t string) string {
	if len(t) <= maxLoggedEventType {
		return t
	}
	return t[:maxLoggedEventType] + "…"
}

// reject logs a pre-acceptance rejection and responds.
func (h *Handler) reject(w http.ResponseWriter, req *request, o Outcome) Outcome {
	status, _ := req.def.ResponseMapper.Status(o)
	observability.LogEvent(h.log, slog.LevelInfo, "webhook_rejected", "webhook rejected",
		append(req.logFields(), "outcome", string(o), "status", status, "body_size", req.bodySize)...)
	return h.respond(w, req.def.ResponseMapper, o)
}

func (h *Handler) logFailure(req *request, o Outcome, message string) {
	observability.LogEvent(h.log, slog.LevelError, "webhook_failed", message,
		append(req.logFields(), "outcome", string(o), "error_code", string(o))...)
}

// respond writes the empty platform response.
func (h *Handler) respond(w http.ResponseWriter, mapper ResponseMapper, o Outcome) Outcome {
	status, retryAfter := mapper.Status(o)
	if retryAfter && w.Header().Get("Retry-After") == "" {
		w.Header().Set("Retry-After", "1")
	}
	w.WriteHeader(status)
	return o
}

// logFields returns the request, endpoint, and Bot Identity log fields.
func (r *request) logFields() []any {
	fields := []any{"request_id", r.id, "webhook_type", r.webhookType, "webhook_identifier", r.identifier}
	if r.endpoint != nil {
		fields = append(fields, "bot_platform", string(r.def.Platform), "bot_id", r.endpoint.BotID)
	}
	if r.sourceIP != nil {
		fields = append(fields, "source_ip", r.sourceIP.String())
	}
	return fields
}

var errBodyTooLarge = errors.New("ingestion: body exceeds the limit")

// readBody reads at most MaxBodyBytes+1 bytes, hashing them during the read.
func readBody(body io.Reader) ([]byte, [sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	hasher := sha256.New()
	data, err := io.ReadAll(io.TeeReader(io.LimitReader(body, MaxBodyBytes+1), hasher))
	if err != nil {
		return nil, sum, err
	}
	if len(data) > MaxBodyBytes {
		return data, sum, errBodyTooLarge
	}
	hasher.Sum(sum[:0])
	return data, sum, nil
}

// acceptableMediaType accepts application/json with an optional UTF-8
// charset, parsed as a media type.
func acceptableMediaType(contentType string) bool {
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "application/json" {
		return false
	}
	for name, value := range params {
		if name != "charset" || !strings.EqualFold(value, "utf-8") {
			return false
		}
	}
	return true
}

// acceptableEncoding accepts only an absent or identity Content-Encoding.
func acceptableEncoding(values []string) bool {
	for _, v := range values {
		for _, enc := range strings.Split(v, ",") {
			if !strings.EqualFold(strings.TrimSpace(enc), "identity") {
				return false
			}
		}
	}
	return true
}

// Capacity is the global acceptance capacity snapshot. NowMs is Valkey
// time; OldestDedupAcceptedMs is the oldest live deduplication record's
// acceptance time (0 without live records); MaxMemoryBytes 0 means Valkey
// has no maxmemory.
type Capacity struct {
	NowMs                 int64
	QueuedMessages        int64
	MaxQueuedMessages     int64
	DedupRecords          int64
	MaxDedupRecords       int64
	OldestDedupAcceptedMs int64
	UsedMemoryBytes       int64
	MaxMemoryBytes        int64
}

// CapacityReader reads the global acceptance capacity.
type CapacityReader interface {
	Capacity(ctx context.Context) (Capacity, error)
}

// AcceptingWebhooks evaluates the global stop conditions: new messages are
// accepted only while the global queue is below its cap, the live
// deduplication records are below theirs or the oldest can be evicted
// early (it is at least the minimum retention old), Valkey memory is below
// the stop share of maxmemory, and Valkey answers. It refreshes the
// deduplication gauges and the memory stop the handler enforces. A full
// individual Recipient does not stop acceptance.
func (h *Handler) AcceptingWebhooks(ctx context.Context) bool {
	if h.d.Capacity == nil {
		return true
	}
	c, err := h.d.Capacity.Capacity(ctx)
	if err != nil {
		return false
	}
	memoryStop := h.d.MemoryStopPercent > 0 && c.MaxMemoryBytes > 0 &&
		c.UsedMemoryBytes*100 >= int64(h.d.MemoryStopPercent)*c.MaxMemoryBytes
	if h.memoryStopped.Swap(memoryStop) != memoryStop {
		level, message := slog.LevelInfo, "valkey memory below the acceptance stop; acceptance resumes"
		if memoryStop {
			level, message = slog.LevelWarn, "valkey memory reached the acceptance stop; new messages are refused while draining continues"
		}
		observability.LogEvent(h.log, level, "memory_acceptance_stop", message,
			"stopped", memoryStop, "used_memory_bytes", c.UsedMemoryBytes, "max_memory_bytes", c.MaxMemoryBytes)
	}
	h.metrics.dedupRecords.Set(float64(c.DedupRecords))
	h.metrics.dedupRecordCapacity.Set(float64(c.MaxDedupRecords))
	full := c.DedupRecords >= c.MaxDedupRecords
	oldestAge := time.Duration(0)
	if c.OldestDedupAcceptedMs > 0 {
		oldestAge = time.Duration(c.NowMs-c.OldestDedupAcceptedMs) * time.Millisecond
	}
	h.metrics.dedupOldestAge.Set(oldestAge.Seconds())
	effective := h.d.DedupRetention
	if full && c.OldestDedupAcceptedMs > 0 {
		// At the cap, the oldest live record bounds how far back
		// duplicates are still detected.
		effective = min(effective, oldestAge)
	}
	h.metrics.dedupEffectiveRetention.Set(effective.Seconds())
	minRetention := h.d.DedupMinRetention
	if minRetention <= 0 {
		// Without a minimum, nothing is evicted early (as in the acceptor).
		minRetention = h.d.DedupRetention
	}
	dedupStop := full && (c.OldestDedupAcceptedMs == 0 || minRetention <= 0 || oldestAge < minRetention)
	return c.QueuedMessages < c.MaxQueuedMessages && !dedupStop && !memoryStop
}

// MaintainCapacity is the ingestion part of a maintenance round: it
// re-evaluates the stop conditions (the memory sample) and, while the
// deduplication cap is reached, runs one bounded early-eviction batch.
func (h *Handler) MaintainCapacity(ctx context.Context) {
	h.AcceptingWebhooks(ctx)
	if h.d.DedupEvictor == nil {
		return
	}
	// Evictions made before a failure still count.
	n, err := h.d.DedupEvictor.EvictDedup(ctx)
	if n > 0 {
		h.metrics.dedupEarlyEvictions.Add(float64(n))
	}
	if err != nil {
		observability.LogEvent(h.log, slog.LevelWarn, "dedup_eviction_failed", "deduplication early eviction stopped",
			"error_code", "dependency_unavailable", "evicted", n)
	}
}

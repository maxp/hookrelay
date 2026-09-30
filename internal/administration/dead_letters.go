package administration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/maxp/hookrelay/internal/observability"
)

// messageIDPattern is the canonical lowercase UUID form of a Message
// Identifier; other shapes cannot name a dead letter.
var messageIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// DeadLetterView is one dead letter's safe metadata.
type DeadLetterView struct {
	MessageID        string          `json:"message_id"`
	Recipient        RecipientInput  `json:"recipient"`
	DeadLetteredMs   int64           `json:"dead_lettered_ms"`
	DeadLetterReason string          `json:"dead_letter_reason"`
	DeliveryCycle    int64           `json:"delivery_cycle"`
	Attempts         []AttemptEntry  `json:"attempts,omitempty"`
	ArchivedCycles   *ArchivedCycles `json:"archived_cycles_summary,omitempty"`
}

// DeadLetterPage is one list page with the opaque continuation cursor.
type DeadLetterPage struct {
	Items      []DeadLetterView `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

func deadLetterView(d DeadLetter) (DeadLetterView, bool) {
	rcpt, ok := recipientOf(d.RecipientIdentity)
	if !ok {
		return DeadLetterView{}, false
	}
	return DeadLetterView{MessageID: d.MessageID, Recipient: rcpt, DeadLetteredMs: d.DeadLetteredMs,
		DeadLetterReason: d.Reason, DeliveryCycle: d.DeliveryCycle, Attempts: d.Attempts, ArchivedCycles: d.Archived}, true
}

// ListDeadLetters pages the global DLQ newest first by safe metadata.
func (s *Service) ListDeadLetters(ctx context.Context, limit, cursor string) (DeadLetterPage, error) {
	n, err := parseListLimit(limit)
	if err != nil {
		return DeadLetterPage{}, err
	}
	after, err := decodeCursor(cursor, func(c DeadLetterCursor) bool { return c.Member != "" })
	if err != nil {
		return DeadLetterPage{}, err
	}
	items, err := s.deadLetters.ListDeadLetters(ctx, n+1, after)
	if err != nil {
		return DeadLetterPage{}, DependencyError{}
	}
	page := DeadLetterPage{Items: []DeadLetterView{}}
	for i, it := range items {
		if i == n {
			last := items[n-1]
			page.NextCursor = encodeCursor(DeadLetterCursor{Score: last.DeadLetteredMs, Member: last.MessageID})
			break
		}
		if it.RecordMissing {
			// A member without its record is reconciliation's concern; the
			// cursor still advances past it.
			continue
		}
		if v, ok := deadLetterView(it); ok {
			page.Items = append(page.Items, v)
		}
	}
	return page, nil
}

// GetDeadLetter reads one dead letter with its retained attempt history.
func (s *Service) GetDeadLetter(ctx context.Context, messageID string) (DeadLetterView, error) {
	if !messageIDPattern.MatchString(messageID) {
		return DeadLetterView{}, deadLetterNotFound()
	}
	d, err := s.deadLetters.GetDeadLetter(ctx, messageID)
	if err != nil {
		return DeadLetterView{}, DependencyError{}
	}
	if d == nil {
		return DeadLetterView{}, deadLetterNotFound()
	}
	v, ok := deadLetterView(*d)
	if !ok {
		return DeadLetterView{}, DependencyError{detail: "stored dead-letter record is malformed"}
	}
	return v, nil
}

func deadLetterNotFound() error {
	return StatusError{Status: http.StatusNotFound, Code: "dead_letter_not_found", Msg: "dead-letter message not found"}
}

// ReplayRequest is the strict replay body; an empty body replays with the
// default reject resolution.
type ReplayRequest struct {
	DeduplicationConflictResolution *string `json:"deduplication_conflict_resolution"`
}

// ReplayView is the replayed response.
type ReplayView struct {
	Status                  string `json:"status"`
	MessageID               string `json:"message_id"`
	DeliveryCycle           int64  `json:"delivery_cycle"`
	QueuePosition           string `json:"queue_position"`
	ReplayedMs              int64  `json:"replayed_ms"`
	DeduplicationResolution string `json:"deduplication_resolution"`
}

// ReplayDeadLetter runs the audited replay. An uncertain outcome is
// reported as such: the caller reconciles instead of retrying.
func (s *Service) ReplayDeadLetter(ctx context.Context, messageID string, req ReplayRequest, requestID string) (ReplayView, error) {
	resolution := ResolutionReject
	if req.DeduplicationConflictResolution != nil {
		resolution = *req.DeduplicationConflictResolution
	}
	if resolution != ResolutionReject && resolution != ResolutionKeepCurrent {
		return ReplayView{}, BadRequestError{msg: "deduplication_conflict_resolution must be reject or keep_current"}
	}
	if !messageIDPattern.MatchString(messageID) {
		s.metrics.replays.WithLabelValues(string(ReplayNotFound)).Inc()
		return ReplayView{}, deadLetterNotFound()
	}
	actor := actorFrom(ctx)
	eventID := s.gen.UUIDv7()
	r := s.deadLetters.ReplayDeadLetter(ctx, messageID, resolution, actor, eventID, requestID)
	switch r.Result {
	case ReplayReplayed, ReplayNotFound, ReplayMessageMissing, ReplayRecipientBlocked, ReplayDeduplicationConflict, ReplayUncertain:
		s.metrics.replays.WithLabelValues(string(r.Result)).Inc()
	default:
		s.metrics.replays.WithLabelValues(replayFailed).Inc()
	}
	switch r.Result {
	case ReplayReplayed:
		s.metrics.auditEvents.WithLabelValues(opDeadLetterReplayed, outcomeSuccess).Inc()
		s.logAuditAs(actor, eventID, opDeadLetterReplayed, messageID, requestID, outcomeSuccess)
		fields := []any{"request_id", requestID, "message_id", messageID, "delivery_cycle", r.DeliveryCycle,
			"queue_position", r.QueuePosition, "deduplication_resolution", r.DeduplicationResolution}
		fields = append(fields, recipientLogFields(r.RecipientIdentity)...)
		observability.LogEvent(s.log, slog.LevelInfo, "delivery_replayed", "dead-letter message replayed", fields...)
		if r.QueuePosition == "head" {
			s.signalReady(readySourceReplay)
		}
		return ReplayView{Status: "replayed", MessageID: messageID, DeliveryCycle: r.DeliveryCycle, QueuePosition: r.QueuePosition,
			ReplayedMs: r.ReplayedMs, DeduplicationResolution: r.DeduplicationResolution}, nil
	case ReplayNotFound:
		return ReplayView{}, deadLetterNotFound()
	case ReplayRecipientBlocked:
		return ReplayView{}, StatusError{Status: http.StatusConflict, Code: "recipient_blocked",
			Msg: "the recipient is blocked: recover the block before replay"}
	case ReplayDeduplicationConflict:
		return ReplayView{}, StatusError{Status: http.StatusConflict, Code: "deduplication_conflict",
			Msg: "the deduplication identity now maps to another message: replay with keep_current to keep that mapping"}
	case ReplayMessageMissing:
		s.logReplayFailure(requestID, messageID, "message_missing")
		return ReplayView{}, DependencyError{detail: "the dead-letter message is missing: reconciliation holds readiness until it is corrected"}
	case ReplayUncertain:
		s.logReplayFailure(requestID, messageID, "outcome_uncertain")
		return ReplayView{}, DependencyError{detail: "replay outcome is uncertain: read the dead letter and the audit before any retry"}
	case ReplayWrongType:
		s.logReplayFailure(requestID, messageID, "wrong_type")
		return ReplayView{}, DependencyError{detail: "stored structure has an unexpected type"}
	default:
		s.logReplayFailure(requestID, messageID, "dependency_unavailable")
		return ReplayView{}, DependencyError{}
	}
}

// recipientLogFields are the structured Recipient fields of a feature
// event; none for an unparseable identity.
func recipientLogFields(identity string) []any {
	rcpt, ok := recipientOf(identity)
	if !ok {
		return nil
	}
	fields := []any{"recipient_scope", rcpt.Scope, "bot_platform", rcpt.BotPlatform, "bot_id", rcpt.BotID}
	if rcpt.ChatID != "" {
		fields = append(fields, "chat_id", rcpt.ChatID)
	}
	if rcpt.UserID != "" {
		fields = append(fields, "user_id", rcpt.UserID)
	}
	return fields
}

func (s *Service) logReplayFailure(requestID, messageID, reason string) {
	observability.LogEvent(s.log, slog.LevelError, "dead_letter_replay_failed", "dead-letter replay not confirmed",
		"request_id", requestID, "message_id", messageID, "error_code", "dependency_unavailable", "reason_code", reason)
}

func (s *Service) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	q := r.URL.Query()
	page, err := s.ListDeadLetters(r.Context(), q.Get("limit"), q.Get("cursor"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Service) handleGetDeadLetter(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	v, err := s.GetDeadLetter(r.Context(), r.PathValue("message_id"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	w.Header().Set("ETag", deadLetterTag(DeadLetterVersion{DeliveryCycle: v.DeliveryCycle, DeadLetteredMs: v.DeadLetteredMs}))
	writeJSON(w, http.StatusOK, v)
}

// deadLetterTag is the strong ETag "<delivery_cycle>:<dead_lettered_ms>".
func deadLetterTag(v DeadLetterVersion) string {
	return fmt.Sprintf(`"%d:%d"`, v.DeliveryCycle, v.DeadLetteredMs)
}

// deadLetterTagPattern is one strong dead-letter entity tag.
var deadLetterTagPattern = regexp.MustCompile(`^"([1-9][0-9]{0,14}):([1-9][0-9]{0,14})"$`)

// parseDeadLetterIfMatch reads the optional If-Match precondition. Absent
// → nil; present but not exactly one strong tag → 400.
func parseDeadLetterIfMatch(r *http.Request) (*DeadLetterVersion, error) {
	values := r.Header.Values("If-Match")
	if len(values) == 0 {
		return nil, nil
	}
	m := deadLetterTagPattern.FindStringSubmatch(strings.TrimSpace(values[0]))
	if len(values) != 1 || m == nil {
		return nil, BadRequestError{msg: `If-Match must be one strong entity tag "<delivery_cycle>:<dead_lettered_ms>"`}
	}
	cycle, _ := strconv.ParseInt(m[1], 10, 64)
	dead, _ := strconv.ParseInt(m[2], 10, 64)
	return &DeadLetterVersion{DeliveryCycle: cycle, DeadLetteredMs: dead}, nil
}

// DeleteDeadLetter permanently deletes the reviewed dead-letter entry with
// its mandatory audit. An absent entry is success without audit. An
// uncertain outcome is reported as such: the caller reconciles instead of
// retrying.
func (s *Service) DeleteDeadLetter(ctx context.Context, messageID string, expected *DeadLetterVersion, requestID string) error {
	if !messageIDPattern.MatchString(messageID) {
		// A shape that cannot name a dead letter names an absent one.
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionAbsent).Inc()
		return nil
	}
	actor := actorFrom(ctx)
	eventID := s.gen.UUIDv7()
	d := s.deadLetters.DeleteDeadLetter(ctx, messageID, expected, actor, eventID, requestID)
	switch d.Result {
	case DeleteDLQDeleted:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionDeleted).Inc()
		s.metrics.auditEvents.WithLabelValues(opDeadLetterDeleted, outcomeSuccess).Inc()
		s.logAuditAs(actor, eventID, opDeadLetterDeleted, messageID, requestID, outcomeSuccess)
		fields := []any{"request_id", requestID, "message_id", messageID, "actor", actor, "dead_letter_reason", d.Reason}
		fields = append(fields, recipientLogFields(d.RecipientIdentity)...)
		observability.LogEvent(s.log, slog.LevelInfo, opDeadLetterDeleted, "dead-letter message permanently deleted", fields...)
		return nil
	case DeleteDLQAbsent:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionAbsent).Inc()
		return nil
	case DeleteDLQPreconditionRequired:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionRefused).Inc()
		return StatusError{Status: http.StatusPreconditionRequired, Code: "precondition_required",
			Msg: "If-Match with the current ETag is required: read the dead letter first"}
	case DeleteDLQPreconditionFailed:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionRefused).Inc()
		return StatusError{Status: http.StatusPreconditionFailed, Code: "precondition_failed",
			Msg: "the dead letter changed: read it again for the current ETag"}
	case DeleteDLQRecipientBlocked:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionRefused).Inc()
		return StatusError{Status: http.StatusConflict, Code: "recipient_blocked",
			Msg: "the recipient is blocked: recover the block before deleting its dead letters"}
	default:
		s.metrics.dlqDeletions.WithLabelValues(dlqDeletionUnavailable).Inc()
		reason, detail := "dependency_unavailable", ""
		switch d.Result {
		case DeleteDLQUncertain:
			reason, detail = "outcome_uncertain", "deletion outcome is uncertain: read the dead letter and the audit before any retry"
		case DeleteDLQWrongType:
			reason, detail = "wrong_type", "stored structure has an unexpected type"
		}
		observability.LogEvent(s.log, slog.LevelError, "dead_letter_delete_failed", "dead-letter deletion not confirmed",
			"request_id", requestID, "message_id", messageID, "error_code", "dependency_unavailable", "reason_code", reason)
		return DependencyError{detail: detail}
	}
}

func (s *Service) handleDeleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	messageID := r.PathValue("message_id")
	expected, err := parseDeadLetterIfMatch(r)
	if err != nil && messageIDPattern.MatchString(messageID) {
		// A malformed tag is 400 only for an existing entry: deleting an
		// absent entry answers 204 with or without If-Match, so the
		// deletion below runs without a tag and reports the absence.
		var se StatusError
		if _, getErr := s.GetDeadLetter(r.Context(), messageID); !errors.As(getErr, &se) || se.Code != "dead_letter_not_found" {
			writeAPIError(w, err, requestID)
			return
		}
		expected = nil
	}
	if err := s.DeleteDeadLetter(r.Context(), messageID, expected, requestID); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Service) handleReplayDeadLetter(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	var req ReplayRequest
	if r.ContentLength != 0 {
		if err := decodeBody(r, &req); err != nil {
			writeAPIError(w, err, requestID)
			return
		}
	}
	v, err := s.ReplayDeadLetter(r.Context(), r.PathValue("message_id"), req, requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// PayloadView is the privileged payload response: the stored Canonical
// Message in its Consumer API representation.
type PayloadView struct {
	MessageID      string          `json:"message_id"`
	DeliveryCycle  int64           `json:"delivery_cycle"`
	DeadLetteredMs int64           `json:"dead_lettered_ms"`
	Message        json.RawMessage `json:"message"`
}

// ViewDeadLetterPayload discloses a dead letter's Canonical Message after
// its access audit is appended in the same storage operation. Nothing is
// disclosed when the audit is not confirmed.
func (s *Service) ViewDeadLetterPayload(ctx context.Context, messageID, requestID string) (PayloadView, error) {
	if !messageIDPattern.MatchString(messageID) {
		s.metrics.payloadViews.WithLabelValues(payloadViewNotFound).Inc()
		return PayloadView{}, deadLetterNotFound()
	}
	actor := actorFrom(ctx)
	eventID := s.gen.UUIDv7()
	p := s.deadLetters.ViewPayload(ctx, messageID, actor, eventID, requestID)
	switch p.Result {
	case PayloadDisclosed:
		s.metrics.payloadViews.WithLabelValues(payloadViewDisclosed).Inc()
		s.metrics.auditEvents.WithLabelValues(opDeadLetterPayloadViewed, outcomeSuccess).Inc()
		s.logAuditAs(actor, eventID, opDeadLetterPayloadViewed, messageID, requestID, outcomeSuccess)
		fields := []any{"request_id", requestID, "message_id", messageID, "actor", actor, "delivery_cycle", p.DeliveryCycle}
		if rcpt, ok := recipientOf(p.RecipientIdentity); ok {
			fields = append(fields, "recipient_scope", rcpt.Scope, "bot_platform", rcpt.BotPlatform)
		}
		observability.LogEvent(s.log, slog.LevelInfo, opDeadLetterPayloadViewed, "dead-letter payload disclosed", fields...)
		return PayloadView{MessageID: messageID, DeliveryCycle: p.DeliveryCycle, DeadLetteredMs: p.DeadLetteredMs, Message: p.Message}, nil
	case PayloadNotFound:
		s.metrics.payloadViews.WithLabelValues(payloadViewNotFound).Inc()
		return PayloadView{}, deadLetterNotFound()
	default:
		s.metrics.payloadViews.WithLabelValues(payloadViewUnavailable).Inc()
		reason, detail := "dependency_unavailable", "payload not disclosed: the access audit could not be confirmed"
		switch p.Result {
		case PayloadMessageMissing:
			reason, detail = "message_missing", "the dead-letter message is missing: reconciliation holds readiness until it is corrected"
		case PayloadWrongType:
			reason, detail = "wrong_type", "stored structure has an unexpected type"
		}
		observability.LogEvent(s.log, slog.LevelError, "dead_letter_payload_failed", "dead-letter payload not disclosed",
			"request_id", requestID, "message_id", messageID, "error_code", "dependency_unavailable", "reason_code", reason)
		return PayloadView{}, DependencyError{detail: detail}
	}
}

func (s *Service) handleDeadLetterPayload(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	if r.ContentLength != 0 {
		// The only accepted body is an empty JSON object.
		var empty struct{}
		if err := decodeBody(r, &empty); err != nil {
			writeAPIError(w, err, requestID)
			return
		}
	}
	v, err := s.ViewDeadLetterPayload(r.Context(), r.PathValue("message_id"), requestID)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, v)
}

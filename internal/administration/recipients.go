package administration

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
)

// RecipientInput is the structured Recipient of the block routes and the
// list items; the internal serialized identity never leaves the service.
type RecipientInput struct {
	Scope       string `json:"scope"`
	BotPlatform string `json:"bot_platform"`
	BotID       string `json:"bot_id"`
	ChatID      string `json:"chat_id,omitempty"`
	UserID      string `json:"user_id,omitempty"`
}

func (r RecipientInput) identity() (string, error) {
	rcpt := model.Recipient{Scope: model.Scope(r.Scope), BotPlatform: r.BotPlatform, BotID: r.BotID, ChatID: r.ChatID, UserID: r.UserID}
	if err := rcpt.Validate(); err != nil {
		return "", BadRequestError{msg: "recipient: " + err.Error()}
	}
	return rcpt.Identity(), nil
}

func recipientOf(identity string) (RecipientInput, bool) {
	r, err := model.ParseIdentity(identity)
	if err != nil {
		return RecipientInput{}, false
	}
	return RecipientInput{Scope: string(r.Scope), BotPlatform: r.BotPlatform, BotID: r.BotID, ChatID: r.ChatID, UserID: r.UserID}, true
}

// RecipientStateView is one list item: the Recipient, its status, and the
// status's index score under its own name.
type RecipientStateView struct {
	Recipient      RecipientInput `json:"recipient"`
	Status         string         `json:"status"`
	ReadySequence  *int64         `json:"ready_sequence,omitempty"`
	LeaseExpiresMs *int64         `json:"lease_expires_ms,omitempty"`
	RetryAtMs      *int64         `json:"retry_at_ms,omitempty"`
	DetectedMs     *int64         `json:"detected_ms,omitempty"`
	ReasonCode     string         `json:"reason_code,omitempty"`
	// MarkerMissing flags a blocked-index member without a marker: a
	// reconciliation finding, not a clearable block.
	MarkerMissing bool `json:"marker_missing,omitempty"`
}

// RecipientStatePage is one list page with the opaque continuation cursor.
type RecipientStatePage struct {
	Items      []RecipientStateView `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// ListRecipientStates pages the Recipients in one state over its derived
// index, ascending by index score (ready sequence, lease deadline, retry
// time, or detection time).
func (s *Service) ListRecipientStates(ctx context.Context, status, limit, cursor string) (RecipientStatePage, error) {
	st := RecipientStatus(status)
	switch st {
	case StatusReady, StatusLeased, StatusRetryWait, StatusBlocked:
	default:
		return RecipientStatePage{}, BadRequestError{msg: "status must be ready, leased, retry_wait, or blocked"}
	}
	n, err := parseListLimit(limit)
	if err != nil {
		return RecipientStatePage{}, err
	}
	after, err := decodeCursor(cursor, func(c RecipientCursor) bool { return c.Member != "" })
	if err != nil {
		return RecipientStatePage{}, err
	}
	items, err := s.recipients.ListRecipientStates(ctx, st, n+1, after)
	if err != nil {
		return RecipientStatePage{}, DependencyError{}
	}
	page := RecipientStatePage{Items: []RecipientStateView{}}
	for i, it := range items {
		if i == n {
			last := items[n-1]
			page.NextCursor = encodeCursor(RecipientCursor{Score: last.Score, Member: last.RecipientIdentity})
			break
		}
		rcpt, ok := recipientOf(it.RecipientIdentity)
		if !ok {
			// A malformed index member is reconciliation's concern; the
			// cursor still advances past it.
			continue
		}
		v := RecipientStateView{Recipient: rcpt, Status: status}
		score := it.Score
		switch st {
		case StatusReady:
			v.ReadySequence = &score
		case StatusLeased:
			v.LeaseExpiresMs = &score
		case StatusRetryWait:
			v.RetryAtMs = &score
		case StatusBlocked:
			detected := it.DetectedMs
			if it.MarkerMissing {
				detected = score
			}
			v.DetectedMs = &detected
			v.ReasonCode = it.ReasonCode
			v.MarkerMissing = it.MarkerMissing
		}
		page.Items = append(page.Items, v)
	}
	return page, nil
}

// InspectBlock reads one Recipient's block and state without mutation.
func (s *Service) InspectBlock(ctx context.Context, r RecipientInput) (BlockInspection, error) {
	rid, err := r.identity()
	if err != nil {
		return BlockInspection{}, err
	}
	out, err := s.recipients.InspectBlock(ctx, rid)
	if err != nil {
		return BlockInspection{}, DependencyError{}
	}
	return out, nil
}

// ClearBlockRequest is the strict clear body.
type ClearBlockRequest struct {
	Recipient          *RecipientInput `json:"recipient"`
	ExpectedDetectedMs *int64          `json:"expected_detected_ms"`
	ExpectedReasonCode *string         `json:"expected_reason_code"`
}

// ClearBlock runs the preconditioned, audited clear. It never repairs
// authoritative state; an uncertain outcome is reported as such.
func (s *Service) ClearBlock(ctx context.Context, req ClearBlockRequest, requestID string) error {
	if req.Recipient == nil {
		return BadRequestError{msg: "recipient is required"}
	}
	rid, err := req.Recipient.identity()
	if err != nil {
		return err
	}
	if req.ExpectedDetectedMs == nil || req.ExpectedReasonCode == nil || *req.ExpectedDetectedMs <= 0 || *req.ExpectedReasonCode == "" {
		return StatusError{Status: http.StatusPreconditionRequired, Code: "precondition_required",
			Msg: "expected_detected_ms and expected_reason_code from the current marker are required"}
	}
	eventID := s.gen.UUIDv7()
	result, detail := s.recipients.ClearBlock(ctx, rid, *req.ExpectedDetectedMs, *req.ExpectedReasonCode, eventID, requestID)
	switch result {
	case ClearCleared:
		s.metrics.auditEvents.WithLabelValues(opRecipientBlockCleared, outcomeSuccess).Inc()
		s.logAudit(eventID, opRecipientBlockCleared, rid, requestID, outcomeSuccess)
		fields := []any{"request_id", requestID, "reason_code", *req.ExpectedReasonCode, "restored_index", detail,
			"recipient_scope", req.Recipient.Scope, "bot_platform", req.Recipient.BotPlatform, "bot_id", req.Recipient.BotID}
		if req.Recipient.ChatID != "" {
			fields = append(fields, "chat_id", req.Recipient.ChatID)
		}
		if req.Recipient.UserID != "" {
			fields = append(fields, "user_id", req.Recipient.UserID)
		}
		observability.LogEvent(s.log, slog.LevelInfo, opRecipientBlockCleared, "recipient block cleared", fields...)
		if detail == "ready" {
			s.signalReady(readySourceBlockClear)
		}
		return nil
	case ClearNotFound:
		return StatusError{Status: http.StatusNotFound, Code: "recipient_block_not_found", Msg: "the recipient is not blocked"}
	case ClearPreconditionFailed:
		return StatusError{Status: http.StatusPreconditionFailed, Code: "precondition_failed",
			Msg: "the marker changed: inspect the block again"}
	case ClearAmbiguous:
		return StatusError{Status: http.StatusConflict, Code: "recipient_state_ambiguous",
			Msg: "authoritative state violates " + detail + ": the block stays"}
	case ClearUncertain:
		s.logClearFailure(requestID, "outcome_uncertain")
		return DependencyError{detail: "clear outcome is uncertain: inspect the block and the audit before any retry"}
	case ClearWrongType:
		s.logClearFailure(requestID, "wrong_type")
		return DependencyError{detail: "stored structure has an unexpected type"}
	default:
		s.logClearFailure(requestID, "dependency_unavailable")
		return DependencyError{}
	}
}

func (s *Service) logClearFailure(requestID, reason string) {
	observability.LogEvent(s.log, slog.LevelError, "recipient_block_clear_failed", "recipient block clear not confirmed",
		"request_id", requestID, "error_code", "dependency_unavailable", "reason_code", reason)
}

func (s *Service) handleListRecipientStates(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	q := r.URL.Query()
	page, err := s.ListRecipientStates(r.Context(), q.Get("status"), q.Get("limit"), q.Get("cursor"))
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

type inspectRequest struct {
	Recipient *RecipientInput `json:"recipient"`
}

type markerView struct {
	DetectedMs int64  `json:"detected_ms"`
	ReasonCode string `json:"reason_code"`
}

type headView struct {
	MessageID      string `json:"message_id"`
	Status         string `json:"status"`
	DeliveryCycle  int64  `json:"delivery_cycle"`
	Attempt        int64  `json:"attempt"`
	LeaseExpiresMs int64  `json:"lease_expires_ms,omitempty"`
	RetryAtMs      int64  `json:"retry_at_ms,omitempty"`
}

type inspectionView struct {
	Marker             *markerView     `json:"marker"`
	QueueLength        int64           `json:"queue_length"`
	Head               *headView       `json:"head"`
	HeadMessagePresent bool            `json:"head_message_present"`
	Memberships        map[string]bool `json:"memberships"`
	ViolatedInvariants []string        `json:"violated_invariants"`
}

func (s *Service) handleInspectBlock(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	var req inspectRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if req.Recipient == nil {
		writeAPIError(w, BadRequestError{msg: "recipient is required"}, requestID)
		return
	}
	in, err := s.InspectBlock(r.Context(), *req.Recipient)
	if err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	v := inspectionView{
		QueueLength: in.QueueLength, HeadMessagePresent: in.HeadMessagePresent,
		Memberships: map[string]bool{}, ViolatedInvariants: []string{},
	}
	for _, name := range []string{"ready", "leases", "retries", "blocked"} {
		v.Memberships[name] = in.Memberships[name]
	}
	v.ViolatedInvariants = append(v.ViolatedInvariants, in.ViolatedInvariants...)
	if in.Marker != nil {
		v.Marker = &markerView{DetectedMs: in.Marker.DetectedMs, ReasonCode: in.Marker.ReasonCode}
	}
	if in.Head != nil {
		v.Head = &headView{MessageID: in.Head.MessageID, Status: in.Head.Status, DeliveryCycle: in.Head.DeliveryCycle,
			Attempt: in.Head.Attempt, LeaseExpiresMs: in.Head.LeaseExpiresMs, RetryAtMs: in.Head.RetryAtMs}
	}
	writeJSON(w, http.StatusOK, v)
}

func (s *Service) handleClearBlock(w http.ResponseWriter, r *http.Request) {
	requestID := requestIDFrom(r.Context())
	var req ClearBlockRequest
	if err := decodeBody(r, &req); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	if err := s.ClearBlock(r.Context(), req, requestID); err != nil {
		writeAPIError(w, err, requestID)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

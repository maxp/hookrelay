package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/maxp/hookrelay/internal/administration"
)

type deadLetterStore struct {
	a *Adapter
}

// NewDeadLetterStore returns the DLQ storage used by the Admin API.
func NewDeadLetterStore(a *Adapter) administration.DeadLetterRepository {
	return &deadLetterStore{a: a}
}

// ListDeadLetters pages hr1:dlq in descending (score, member) order and
// reads each member's record. A member without a record is returned with
// RecordMissing so the cursor can advance past it.
func (s *deadLetterStore) ListDeadLetters(ctx context.Context, limit int, after *administration.DeadLetterCursor) ([]administration.DeadLetter, error) {
	c := s.a.client
	from := "+inf"
	if after != nil {
		from = strconv.FormatInt(after.Score, 10)
	}
	var out []administration.DeadLetter
	// Members sharing the cursor score are skipped down to the cursor
	// member; read further pages until the limit is filled or the index ends.
	for offset := int64(0); len(out) < limit; {
		page, err := c.Do(ctx, c.B().Zrange().Key("hr1:dlq").Min(from).Max("-inf").Byscore().Rev().Limit(offset, int64(limit)).Withscores().Build()).AsZScores()
		if err != nil {
			return nil, err
		}
		for _, z := range page {
			sc := int64(z.Score)
			if after != nil && sc == after.Score && z.Member >= after.Member {
				continue
			}
			if len(out) < limit {
				out = append(out, administration.DeadLetter{MessageID: z.Member, DeadLetteredMs: sc})
			}
		}
		if len(page) < limit {
			break
		}
		offset += int64(len(page))
	}
	for i := range out {
		d, err := s.record(ctx, out[i].MessageID)
		if err != nil {
			return nil, err
		}
		if d == nil {
			out[i].RecordMissing = true
			continue
		}
		// The index score stays: it is the cursor position, and
		// reconciliation keeps it equal to the record's dead_lettered_ms.
		d.DeadLetteredMs = out[i].DeadLetteredMs
		out[i] = *d
	}
	return out, nil
}

// record reads hr1:dl:<message_id>; nil when absent or not a Hash.
func (s *deadLetterStore) record(ctx context.Context, messageID string) (*administration.DeadLetter, error) {
	c := s.a.client
	m, err := c.Do(ctx, c.B().Hgetall().Key("hr1:dl:"+messageID).Build()).AsStrMap()
	if err != nil {
		if isWrongType(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	dead, _ := strconv.ParseInt(m["dead_lettered_ms"], 10, 64)
	cycle, _ := strconv.ParseInt(m["delivery_cycle"], 10, 64)
	return &administration.DeadLetter{
		MessageID: messageID, RecipientIdentity: m["recipient_identity"], DeadLetteredMs: dead,
		Reason: m["dead_letter_reason"], DeliveryCycle: cycle,
	}, nil
}

// GetDeadLetter reads one record and its retained attempt history.
func (s *deadLetterStore) GetDeadLetter(ctx context.Context, messageID string) (*administration.DeadLetter, error) {
	d, err := s.record(ctx, messageID)
	if err != nil || d == nil {
		return nil, err
	}
	c := s.a.client
	raw, err := c.Do(ctx, c.B().Lrange().Key("hr1:a:"+messageID).Start(0).Stop(-1).Build()).AsStrSlice()
	if err != nil && !isWrongType(err) {
		return nil, err
	}
	for _, entry := range raw {
		var kind struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal([]byte(entry), &kind) != nil {
			continue
		}
		switch kind.Kind {
		case "attempt":
			var a administration.AttemptEntry
			if json.Unmarshal([]byte(entry), &a) == nil {
				d.Attempts = append(d.Attempts, a)
			}
		case "archived_cycles_summary":
			var sum administration.ArchivedCycles
			if json.Unmarshal([]byte(entry), &sum) == nil {
				d.Archived = &sum
			}
		}
	}
	return d, nil
}

// ReplayDeadLetter runs replay_dlq_v2.
func (s *deadLetterStore) ReplayDeadLetter(ctx context.Context, messageID, resolution, eventID, requestID string) administration.Replay {
	res, err := s.a.RunScript(ctx, "replay_dlq_v2",
		[]string{"hr1:dlq", "hr1:ready", "hr1:ready_seq", "hr1:retries", "hr1:leases", "hr1:blocked", "hr1:stats:queued_messages", auditKey},
		[]string{messageID, resolution, eventID, requestID, "hr1"})
	if err != nil {
		if errors.Is(err, ErrNotDispatched) {
			return administration.Replay{Result: administration.ReplayUnavailable}
		}
		// The replay may have run: never report success or failure.
		return administration.Replay{Result: administration.ReplayUncertain}
	}
	if res.Status != "replayed" {
		return administration.Replay{Result: administration.ReplayResult(res.Status)}
	}
	out, err := parseReplayed(res)
	if err != nil {
		return administration.Replay{Result: administration.ReplayUncertain}
	}
	return out
}

func parseReplayed(res *Result) (administration.Replay, error) {
	f := res.Fields
	cycle, err1 := f[0].AsInt64()
	position, err2 := f[1].ToString()
	replayed, err3 := f[2].AsInt64()
	resolution, err4 := f[3].ToString()
	rid, err5 := f[4].ToString()
	if err := errors.Join(err1, err2, err3, err4, err5); err != nil {
		return administration.Replay{}, fmt.Errorf("valkey: replay_dlq_v2: result shape: %w", err)
	}
	return administration.Replay{Result: administration.ReplayReplayed, DeliveryCycle: cycle, QueuePosition: position,
		ReplayedMs: replayed, DeduplicationResolution: resolution, RecipientIdentity: rid}, nil
}

// ViewPayload runs dlq_payload_v1: the access audit is appended in the
// same operation that returns the blob, so a disclosed payload is always
// audited. A blob that is not a JSON object is refused as wrong_type
// (already audited, never disclosed).
func (s *deadLetterStore) ViewPayload(ctx context.Context, messageID, actor, eventID, requestID string) administration.Payload {
	res, err := s.a.RunScript(ctx, "dlq_payload_v1", []string{auditKey}, []string{messageID, actor, eventID, requestID, "hr1"})
	if err != nil {
		return administration.Payload{Result: administration.PayloadUnavailable}
	}
	if res.Status != "disclosed" {
		return administration.Payload{Result: administration.PayloadResult(res.Status)}
	}
	f := res.Fields
	blob, err1 := f[0].ToString()
	cycle, err2 := f[1].AsInt64()
	dead, err3 := f[2].AsInt64()
	rid, err4 := f[3].ToString()
	if errors.Join(err1, err2, err3, err4) != nil {
		return administration.Payload{Result: administration.PayloadUnavailable}
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal([]byte(blob), &obj) != nil {
		return administration.Payload{Result: administration.PayloadWrongType}
	}
	return administration.Payload{Result: administration.PayloadDisclosed, Message: json.RawMessage(blob),
		DeliveryCycle: cycle, DeadLetteredMs: dead, RecipientIdentity: rid}
}

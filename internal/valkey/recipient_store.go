package valkey

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/maxp/hookrelay/internal/administration"
)

// recipientIndexes maps each Recipient-state filter to its derived index.
var recipientIndexes = map[administration.RecipientStatus]string{
	administration.StatusReady:     "hr1:ready",
	administration.StatusLeased:    "hr1:leases",
	administration.StatusRetryWait: "hr1:retries",
	administration.StatusBlocked:   "hr1:blocked",
}

type recipientStore struct {
	a *Adapter
	// bound is how many queued messages must have a stored blob (the
	// per-recipient queue limit), as in reconciliation.
	bound int
}

// NewRecipientStore returns the Recipient-state storage used by the Admin
// API. bound is the per-recipient message check bound.
func NewRecipientStore(a *Adapter, bound int) administration.RecipientRepository {
	return &recipientStore{a: a, bound: bound}
}

// ListRecipientStates pages one index in ascending (score, member) order.
func (s *recipientStore) ListRecipientStates(ctx context.Context, status administration.RecipientStatus, limit int,
	after *administration.RecipientCursor) ([]administration.RecipientStateItem, error) {
	key, ok := recipientIndexes[status]
	if !ok {
		return nil, fmt.Errorf("valkey: unknown recipient status %q", status)
	}
	c := s.a.client
	from := "-inf"
	if after != nil {
		from = strconv.FormatInt(after.Score, 10)
	}
	var out []administration.RecipientStateItem
	// Members sharing the cursor score are skipped up to the cursor member;
	// read further pages until the limit is filled or the index ends.
	for offset := int64(0); len(out) < limit; {
		page, err := c.Do(ctx, c.B().Zrange().Key(key).Min(from).Max("+inf").Byscore().Limit(offset, int64(limit)).Withscores().Build()).AsZScores()
		if err != nil {
			return nil, err
		}
		for _, z := range page {
			sc := int64(z.Score)
			if after != nil && sc == after.Score && z.Member <= after.Member {
				continue
			}
			if len(out) < limit {
				out = append(out, administration.RecipientStateItem{RecipientIdentity: z.Member, Score: sc})
			}
		}
		if len(page) < limit {
			break
		}
		offset += int64(len(page))
	}
	if status == administration.StatusBlocked {
		// The marker is authoritative; the index score may have drifted.
		for i := range out {
			m, err := c.Do(ctx, c.B().Hmget().Key("hr1:q:"+out[i].RecipientIdentity).Field("detected_ms", "reason_code").Build()).ToArray()
			if err != nil {
				if isWrongType(err) {
					out[i].MarkerMissing = true
					continue
				}
				return nil, err
			}
			detected, errD := m[0].AsInt64()
			reason, errR := m[1].ToString()
			if isNil(errD) && isNil(errR) {
				out[i].MarkerMissing = true
				continue
			}
			out[i].DetectedMs, out[i].ReasonCode = detected, reason
		}
	}
	return out, nil
}

// InspectBlock runs inspect_block_v1: one atomic read that applies the
// checks and order of clear_block_v1, so the first violated invariant is
// the one a clear would refuse with. It never returns a token or payload.
func (s *recipientStore) InspectBlock(ctx context.Context, rid string) (administration.BlockInspection, error) {
	res, err := s.a.RunScript(ctx, "inspect_block_v1",
		[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
		[]string{rid, strconv.Itoa(s.bound), "hr1"})
	if err != nil {
		return administration.BlockInspection{}, err
	}
	f := res.Fields
	str := func(i int) string {
		v, _ := f[i].ToString()
		return v
	}
	num := func(i int) int64 {
		v, err := f[i].AsInt64()
		if err != nil {
			v, _ = strconv.ParseInt(str(i), 10, 64)
		}
		return v
	}
	out := administration.BlockInspection{
		QueueLength:        num(3),
		HeadMessagePresent: num(10) == 1,
		Memberships: map[string]bool{
			"ready": num(11) == 1, "leases": num(12) == 1, "retries": num(13) == 1, "blocked": num(14) == 1,
		},
	}
	if str(0) == "present" {
		out.Marker = &administration.BlockMarker{DetectedMs: num(1), ReasonCode: str(2)}
	}
	if str(5) != "" || str(4) != "" {
		out.Head = &administration.BlockHead{
			MessageID: str(4), Status: str(5), DeliveryCycle: num(6), Attempt: num(7), LeaseExpiresMs: num(8), RetryAtMs: num(9),
		}
	}
	if out.ViolatedInvariants, err = f[15].AsStrSlice(); err != nil {
		return administration.BlockInspection{}, fmt.Errorf("valkey: inspect_block_v1: invariants shape: %w", err)
	}
	return out, nil
}

// ClearBlock runs clear_block_v1.
func (s *recipientStore) ClearBlock(ctx context.Context, rid string, expectedDetectedMs int64, expectedReasonCode, eventID, requestID string) (administration.ClearBlockResult, string) {
	res, err := s.a.RunScript(ctx, "clear_block_v1",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:retries", "hr1:blocked", auditKey},
		[]string{rid, strconv.FormatInt(expectedDetectedMs, 10), expectedReasonCode, eventID, requestID, strconv.Itoa(s.bound), "hr1"})
	if err != nil {
		if errors.Is(err, ErrNotDispatched) {
			return administration.ClearUnavailable, ""
		}
		// The clear may have run: never report success or failure.
		return administration.ClearUncertain, ""
	}
	switch res.Status {
	case "cleared", "ambiguous":
		detail, err := res.Fields[0].ToString()
		if err != nil {
			return administration.ClearUncertain, ""
		}
		return administration.ClearBlockResult(res.Status), detail
	default:
		return administration.ClearBlockResult(res.Status), ""
	}
}

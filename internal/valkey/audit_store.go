package valkey

import (
	"context"
	"fmt"
	"strconv"

	"github.com/valkey-io/valkey-go"

	"github.com/maxp/hookrelay/internal/administration"
)

type auditLog struct{ a *Adapter }

// NewAuditLog returns the administrative audit Stream reader.
func NewAuditLog(a *Adapter) administration.AuditLog { return &auditLog{a: a} }

// ListAudit pages hr1:audit newest first with XREVRANGE, strictly before
// the given entry ID (an exclusive "(" bound). Only the allowlisted event
// fields are copied; anything else stored in an entry is dropped.
func (l *auditLog) ListAudit(ctx context.Context, limit int, before string) ([]administration.AuditEntry, error) {
	c := l.a.client
	end := "+"
	if before != "" {
		end = "(" + before
	}
	msg, err := c.Do(ctx, c.B().Xrevrange().Key(auditKey).End(end).Start("-").Count(int64(limit)).Build()).ToMessage()
	if err != nil {
		if isWrongType(err) {
			return nil, administration.ErrStoredWrongType
		}
		return nil, fmt.Errorf("valkey: audit xrevrange: %w", err)
	}
	items, err := msg.ToArray()
	if err != nil {
		return nil, fmt.Errorf("valkey: audit shape: %w", err)
	}
	out := make([]administration.AuditEntry, 0, len(items))
	for _, item := range items {
		id, fields, err := streamEntry(item)
		if err != nil {
			return nil, err
		}
		e := administration.AuditEntry{StreamID: id, EventID: fields["event_id"], Actor: fields["actor"], Operation: fields["operation"],
			Target: fields["target"], RequestID: fields["request_id"], Outcome: fields["outcome"], Reason: fields["reason"]}
		if ts, err := strconv.ParseInt(fields["timestamp_ms"], 10, 64); err == nil && ts > 0 {
			e.TimestampMs = ts
		}
		out = append(out, e)
	}
	return out, nil
}

// streamEntry decodes one stream entry into its ID and field map.
func streamEntry(item valkey.ValkeyMessage) (string, map[string]string, error) {
	parts, err := item.ToArray()
	if err != nil || len(parts) != 2 {
		return "", nil, fmt.Errorf("valkey: audit entry shape")
	}
	id, err := parts[0].ToString()
	if err != nil {
		return "", nil, fmt.Errorf("valkey: audit entry id: %w", err)
	}
	pairs, err := parts[1].ToArray()
	if err != nil {
		return "", nil, fmt.Errorf("valkey: audit fields shape: %w", err)
	}
	fields := make(map[string]string, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		k, _ := pairs[i].ToString()
		v, _ := pairs[i+1].ToString()
		fields[k] = v
	}
	return id, fields, nil
}

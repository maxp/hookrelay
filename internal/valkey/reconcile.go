package valkey

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/observability"
)

// ReconcileOptions control one reconciliation pass.
type ReconcileOptions struct {
	// Full also recomputes hr1:stats:queued_messages. Only the startup pass
	// is full: ingestion and delivery are not serving yet, so the scanned
	// total cannot race live transitions (the compare-and-set still guards).
	Full bool
	// MessageCheckBound is how many queued messages per Recipient must have
	// a stored blob (the per-recipient queue limit).
	MessageCheckBound int
	// BatchSize bounds SCAN/ZSCAN pages and dedup script batches.
	BatchSize int
	// Gen supplies audit event identifiers (gen.Crypto when nil).
	Gen gen.Gen
	// Logger receives best-effort stdout copies of reconciliation audit events.
	Logger *slog.Logger
}

// ReconcileReport summarizes one pass with bounded finding kinds.
type ReconcileReport struct {
	Recipients int
	// Findings counts bounded kinds: repaired, drained, blocked,
	// already_blocked, due_lease, unhandled, dedup_expired_removed,
	// dedup_restored, dedup_orphans_removed, dedup_skipped,
	// counter_repaired, counter_unverified.
	Findings map[string]int
	// BlockReasons counts newly created markers by bounded reason.
	BlockReasons map[string]int
}

// Hold reports why readiness must stay false, or "" when the pass allows
// readiness.
func (r ReconcileReport) Hold() string {
	switch {
	case r.Findings["unhandled"] > 0:
		return "unhandled_inconsistency"
	case r.Findings["due_lease"] > 0:
		return "due_lease"
	}
	return ""
}

// Reconcile validates every structure Milestone 1 implements in bounded
// batches, repairs derived indexes and counters, and isolates ambiguous
// Recipient state behind block markers. It never uses KEYS.
func (a *Adapter) Reconcile(ctx context.Context, opts ReconcileOptions) (ReconcileReport, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.MessageCheckBound <= 0 {
		opts.MessageCheckBound = 1000
	}
	if opts.Gen == nil {
		opts.Gen = gen.Crypto{}
	}
	rep := ReconcileReport{Findings: map[string]int{}, BlockReasons: map[string]int{}}
	// Scan administrative records at startup and after loss of readiness, not
	// on every one-second connectivity probe while the service is healthy.
	if err := a.checkAdminRecords(ctx); err != nil {
		return rep, fmt.Errorf("valkey: administrative structure: %w", err)
	}

	observedCounter := ""
	if opts.Full {
		v, err := a.client.Do(ctx, a.client.B().Get().Key("hr1:stats:queued_messages").Build()).ToString()
		if err != nil && !isNil(err) {
			return rep, fmt.Errorf("valkey: reconcile counter read: %w", err)
		}
		observedCounter = v
	}

	// Recipients from every structure that can name one.
	seen := map[string]struct{}{}
	total, countable := int64(0), true
	visit := func(rid string) error {
		if rid == "" {
			return nil
		}
		if _, done := seen[rid]; done {
			return nil
		}
		seen[rid] = struct{}{}
		n, err := a.reconcileRecipient(ctx, opts.Gen, opts.Logger, rid, opts.MessageCheckBound, &rep)
		if err != nil {
			return err
		}
		if n < 0 {
			countable = false
		} else {
			total += n
		}
		return nil
	}
	if err := a.scanKeys(ctx, "hr1:r:*", opts.BatchSize, func(key string) error {
		rest := strings.TrimPrefix(key, "hr1:r:")
		switch {
		case strings.HasSuffix(rest, ":q"):
			return visit(strings.TrimSuffix(rest, ":q"))
		case strings.HasSuffix(rest, ":s"):
			return visit(strings.TrimSuffix(rest, ":s"))
		}
		return nil
	}); err != nil {
		return rep, err
	}
	if err := a.scanKeys(ctx, "hr1:q:*", opts.BatchSize, func(key string) error {
		return visit(strings.TrimPrefix(key, "hr1:q:"))
	}); err != nil {
		return rep, err
	}
	for _, index := range []string{"hr1:ready", "hr1:leases", "hr1:blocked"} {
		if err := a.scanMembers(ctx, index, opts.BatchSize, func(members []string) error {
			for _, m := range members {
				if err := visit(m); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return rep, err
		}
	}
	rep.Recipients = len(seen)

	// Deduplication records and their age index, both directions.
	var batch []string
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		err := a.reconcileDedup(ctx, batch, &rep)
		batch = batch[:0]
		return err
	}
	collect := func(digests []string) error {
		for _, d := range digests {
			batch = append(batch, d)
			if len(batch) >= opts.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := a.scanKeys(ctx, "hr1:d:*", opts.BatchSize, func(key string) error {
		return collect([]string{strings.TrimPrefix(key, "hr1:d:")})
	}); err != nil {
		return rep, err
	}
	if err := a.scanMembers(ctx, "hr1:dedup_age", opts.BatchSize, collect); err != nil {
		return rep, err
	}
	if err := flush(); err != nil {
		return rep, err
	}

	// The queued-message counter, on the full pass only.
	if opts.Full {
		if !countable || rep.Findings["unhandled"] > 0 {
			rep.Findings["counter_unverified"]++
		} else {
			res, err := a.RunScript(ctx, "reconcile_counter_v1", []string{"hr1:stats:queued_messages"},
				[]string{observedCounter, strconv.FormatInt(total, 10)})
			if err != nil {
				return rep, fmt.Errorf("valkey: reconcile counter: %w", err)
			}
			switch res.Status {
			case "repaired":
				rep.Findings["counter_repaired"]++
				a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", "stats:queued_messages", "counter_repaired")
			case "precondition_failed", "wrong_type":
				rep.Findings["counter_unverified"]++
			}
		}
	}
	return rep, nil
}

// reconcileRecipient runs reconcile_recipient_v1 and returns the counted
// queue length (-1 when uncountable).
func (a *Adapter) reconcileRecipient(ctx context.Context, g gen.Gen, log *slog.Logger, rid string, bound int, rep *ReconcileReport) (int64, error) {
	res, err := a.RunScript(ctx, "reconcile_recipient_v1",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked"},
		[]string{rid, strconv.Itoa(bound), "hr1"})
	if err != nil {
		return 0, fmt.Errorf("valkey: reconcile recipient: %w", err)
	}
	repairs, err1 := res.Fields[0].AsInt64()
	length, err2 := res.Fields[1].AsInt64()
	reason, err3 := res.Fields[2].ToString()
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, fmt.Errorf("valkey: reconcile recipient: result shape")
	}
	switch res.Status {
	case "consistent":
	case "blocked":
		rep.Findings["blocked"]++
		rep.BlockReasons[reason]++
		a.auditRepair(ctx, g, log, "recipient_blocked", rid, reason)
	case "already_blocked":
		rep.Findings["already_blocked"]++
		if repairs > 0 {
			rep.Findings["repaired"]++
			a.auditRepair(ctx, g, log, "reconciliation_repair", rid, "blocked_index_aligned")
		}
	default: // repaired, drained, due_lease, unhandled
		rep.Findings[res.Status]++
		if res.Status == "repaired" || res.Status == "drained" {
			a.auditRepair(ctx, g, log, "reconciliation_repair", rid, res.Status)
		}
	}
	return length, nil
}

func (a *Adapter) reconcileDedup(ctx context.Context, digests []string, rep *ReconcileReport) error {
	res, err := a.RunScript(ctx, "reconcile_dedup_v1", []string{"hr1:dedup_age"}, append([]string{"hr1"}, digests...))
	if err != nil {
		return fmt.Errorf("valkey: reconcile dedup: %w", err)
	}
	if res.Status == "wrong_type" {
		rep.Findings["unhandled"]++
		return nil
	}
	for i, kind := range []string{"dedup_expired_removed", "dedup_restored", "dedup_orphans_removed", "dedup_skipped"} {
		n, err := res.Fields[i].AsInt64()
		if err != nil {
			return fmt.Errorf("valkey: reconcile dedup: result shape")
		}
		rep.Findings[kind] += int(n)
		if kind == "dedup_skipped" {
			rep.Findings["unhandled"] += int(n)
		}
	}
	return nil
}

// scanKeys walks the keyspace with SCAN MATCH in bounded pages.
func (a *Adapter) scanKeys(ctx context.Context, pattern string, count int, fn func(string) error) error {
	cursor := uint64(0)
	for {
		entry, err := a.client.Do(ctx, a.client.B().Scan().Cursor(cursor).Match(pattern).Count(int64(count)).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("valkey: scan %s: %w", pattern, err)
		}
		for _, key := range entry.Elements {
			if err := fn(key); err != nil {
				return err
			}
		}
		if entry.Cursor == 0 {
			return nil
		}
		cursor = entry.Cursor
	}
}

// scanMembers walks a sorted set with ZSCAN in bounded pages, passing the
// members of each page.
func (a *Adapter) scanMembers(ctx context.Context, key string, count int, fn func([]string) error) error {
	t, err := a.keyType(ctx, key)
	if err != nil {
		return err
	}
	if t == "none" {
		return nil // a fresh deployment has no index yet
	}
	if t != "zset" {
		return fmt.Errorf("valkey: index %s has unexpected type %q", key, t)
	}
	cursor := uint64(0)
	for {
		entry, err := a.client.Do(ctx, a.client.B().Zscan().Key(key).Cursor(cursor).Count(int64(count)).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("valkey: zscan %s: %w", key, err)
		}
		members := make([]string, 0, len(entry.Elements)/2)
		for i := 0; i < len(entry.Elements); i += 2 {
			members = append(members, entry.Elements[i])
		}
		if err := fn(members); err != nil {
			return err
		}
		if entry.Cursor == 0 {
			return nil
		}
		cursor = entry.Cursor
	}
}

// auditRepair appends a best-effort audit event for a reconciliation
// repair or block; failures never stop reconciliation.
func (a *Adapter) auditRepair(ctx context.Context, g gen.Gen, log *slog.Logger, operation, target, reason string) {
	eventID := g.UUIDv7()
	if log != nil {
		observability.LogEvent(log, slog.LevelInfo, "administrative_audit", "administrative audit event",
			"event_id", eventID, "actor", "reconciliation", "operation", operation,
			"target", target, "outcome", "success", "reason_code", reason)
	}
	now, err := a.serverTimeMs(ctx)
	if err != nil {
		return
	}
	a.client.Do(ctx, a.client.B().Arbitrary(
		"XADD", auditKey, "MAXLEN", "~", "1000000", "*",
		"event_id", eventID,
		"timestamp_ms", strconv.FormatInt(now, 10),
		"actor", "reconciliation",
		"operation", operation,
		"target", target,
		"outcome", "success",
		"reason", reason,
	).Build())
}

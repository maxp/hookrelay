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
	// already_blocked, due_lease, due_retry, unhandled,
	// dedup_expired_removed, dedup_restored, dedup_orphans_removed,
	// dedup_skipped, dlq_orphans_removed, dlq_restored, dlq_invalid,
	// dlq_message_missing, counter_repaired, counter_unverified, and (from
	// ReconcileAndProcessDue) due_leases_processed, due_retries_processed.
	Findings map[string]int
	// BlockReasons counts newly created markers by bounded reason.
	BlockReasons map[string]int
	// DueLeases and DueRetries name the Recipients whose lease or retry is
	// due. They are reported without mutation; the caller runs the
	// registered expiry and activation transitions before readiness.
	DueLeases  []string
	DueRetries []string
	// DeadLetterMessagesMissing samples (at most maxMissingSample) the
	// dead-letter entries whose Canonical Message is missing (safe message
	// identifiers); Findings["dlq_message_missing"] counts all of them.
	DeadLetterMessagesMissing []string
}

// maxMissingSample bounds the missing-message identifiers kept in a report.
const maxMissingSample = 100

// Hold reports why readiness must stay false, or "" when the pass allows
// readiness.
func (r ReconcileReport) Hold() string {
	switch {
	case r.Findings["unhandled"] > 0:
		return "unhandled_inconsistency"
	case r.Findings["dlq_message_missing"] > 0:
		// Not isolatable behind a Recipient block: clearing a block cannot
		// certify DLQ integrity.
		return "dead_letter_message_missing"
	}
	return ""
}

// Reconcile validates every persisted delivery structure in bounded
// batches, repairs derived indexes and counters, isolates ambiguous
// Recipient state behind block markers, and reports due leases and retries
// for the caller to execute. It never uses KEYS.
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
	for _, index := range []string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"} {
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

	// Dead-letter entries from both the index and the authoritative Hashes.
	var dlBatch []string
	dlReported := map[string]bool{} // ids already counted as invalid or missing
	dlFlush := func() error {
		if len(dlBatch) == 0 {
			return nil
		}
		err := a.reconcileDLQ(ctx, opts.Gen, opts.Logger, dlBatch, dlReported, &rep)
		dlBatch = dlBatch[:0]
		return err
	}
	dlCollect := func(ids []string) error {
		for _, id := range ids {
			dlBatch = append(dlBatch, id)
			if len(dlBatch) >= opts.BatchSize {
				if err := dlFlush(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	dlqType, err := a.keyType(ctx, "hr1:dlq")
	if err != nil {
		return rep, err
	}
	if dlqType != "none" && dlqType != "zset" {
		rep.Findings["unhandled"]++
	} else {
		if err := a.scanMembers(ctx, "hr1:dlq", opts.BatchSize, dlCollect); err != nil {
			return rep, err
		}
		if err := a.scanKeys(ctx, "hr1:dl:*", opts.BatchSize, func(key string) error {
			return dlCollect([]string{strings.TrimPrefix(key, "hr1:dl:")})
		}); err != nil {
			return rep, err
		}
		if err := dlFlush(); err != nil {
			return rep, err
		}
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

// stateFindings are the findings that describe the state a pass observed
// rather than work it did; a verifying pass replaces them instead of adding.
var stateFindings = []string{
	"unhandled", "already_blocked", "due_lease", "due_retry", "dedup_skipped",
	"dlq_invalid", "dlq_message_missing", "counter_unverified",
}

// ReconcileAndProcessDue is the startup and recovery composition: one pass,
// then — when leases or retries are due — the due work through process
// (the registered expiry and activation transitions, with their events and
// metrics), then a verifying pass. The merged report adds the first pass's
// repairs to the second pass's observations; due work left after the
// verifying pass (e.g. a retry that fell due meanwhile) is not a hold,
// because background maintenance processes it after readiness.
func (a *Adapter) ReconcileAndProcessDue(ctx context.Context, opts ReconcileOptions,
	process func(ctx context.Context, leases, retries []string)) (ReconcileReport, error) {
	first, err := a.Reconcile(ctx, opts)
	if err != nil || len(first.DueLeases)+len(first.DueRetries) == 0 {
		return first, err
	}
	process(ctx, first.DueLeases, first.DueRetries)
	second, err := a.Reconcile(ctx, opts)
	if err != nil {
		return second, err
	}
	merged := second
	merged.Findings = map[string]int{}
	for k, n := range first.Findings {
		merged.Findings[k] = n
	}
	for _, k := range stateFindings {
		delete(merged.Findings, k)
	}
	for k, n := range second.Findings {
		merged.Findings[k] += n
	}
	// A Recipient blocked by the first pass is seen as already blocked by
	// the verifying pass; count it once, as the new block.
	if n := merged.Findings["already_blocked"] - first.Findings["blocked"]; n > 0 {
		merged.Findings["already_blocked"] = n
	} else {
		delete(merged.Findings, "already_blocked")
	}
	merged.Findings["due_leases_processed"] = len(first.DueLeases)
	merged.Findings["due_retries_processed"] = len(first.DueRetries)
	merged.BlockReasons = map[string]int{}
	for _, r := range []map[string]int{first.BlockReasons, second.BlockReasons} {
		for reason, n := range r {
			merged.BlockReasons[reason] += n
		}
	}
	return merged, nil
}

// reconcileRecipient runs reconcile_recipient_v2 and returns the counted
// queue length (-1 when uncountable).
func (a *Adapter) reconcileRecipient(ctx context.Context, g gen.Gen, log *slog.Logger, rid string, bound int, rep *ReconcileReport) (int64, error) {
	res, err := a.RunScript(ctx, "reconcile_recipient_v2",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:retries"},
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
	case "due_lease":
		rep.Findings["due_lease"]++
		rep.DueLeases = append(rep.DueLeases, rid)
	case "due_retry":
		rep.Findings["due_retry"]++
		rep.DueRetries = append(rep.DueRetries, rid)
	default: // repaired, drained, unhandled
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

// reconcileDLQ runs reconcile_dlq_v1 for one batch of message ids. An id
// seen from both the index and the key scan is reported once; repairs and
// ambiguities are audited best effort.
func (a *Adapter) reconcileDLQ(ctx context.Context, g gen.Gen, log *slog.Logger, ids []string, reported map[string]bool, rep *ReconcileReport) error {
	res, err := a.RunScript(ctx, "reconcile_dlq_v1", []string{"hr1:dlq"}, append([]string{"hr1"}, ids...))
	if err != nil {
		return fmt.Errorf("valkey: reconcile dlq: %w", err)
	}
	if res.Status == "wrong_type" {
		rep.Findings["unhandled"]++
		return nil
	}
	lists := make([][]string, 4)
	for i := range lists {
		if lists[i], err = res.Fields[i].AsStrSlice(); err != nil {
			return fmt.Errorf("valkey: reconcile dlq: result shape")
		}
	}
	removed, restored, invalid, missing := lists[0], lists[1], lists[2], lists[3]
	for _, id := range removed {
		rep.Findings["dlq_orphans_removed"]++
		a.auditRepair(ctx, g, log, "reconciliation_repair", id, "dlq_orphan_removed")
	}
	for _, id := range restored {
		rep.Findings["dlq_restored"]++
		a.auditRepair(ctx, g, log, "reconciliation_repair", id, "dlq_restored")
	}
	for _, id := range invalid {
		if reported[id] {
			continue
		}
		reported[id] = true
		rep.Findings["dlq_invalid"]++
		rep.Findings["unhandled"]++
		a.auditRepair(ctx, g, log, "dead_letter_ambiguity", id, "dead_letter_record_invalid")
	}
	for _, id := range missing {
		if reported[id] {
			continue
		}
		reported[id] = true
		rep.Findings["dlq_message_missing"]++
		if len(rep.DeadLetterMessagesMissing) < maxMissingSample {
			rep.DeadLetterMessagesMissing = append(rep.DeadLetterMessagesMissing, id)
		}
		if log != nil {
			observability.LogEvent(log, slog.LevelError, "dead_letter_message_missing",
				"dead-letter entry lost its Canonical Message; readiness held for incident review",
				"message_id", id, "error_code", "internal_error")
		}
		a.auditRepair(ctx, g, log, "dead_letter_ambiguity", id, "dead_letter_message_missing")
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

// ConsistencyIssue is one bounded (kind, resolution) pair with its count,
// the shape of hookrelay_consistency_issues_total.
type ConsistencyIssue struct {
	Kind       string
	Resolution string
	Count      int
}

// ConsistencyIssues maps the pass's findings onto bounded kinds and
// resolutions: repaired, removed, restored, blocked, kept, held, skipped.
func (r ReconcileReport) ConsistencyIssues() []ConsistencyIssue {
	f := r.Findings
	var out []ConsistencyIssue
	add := func(kind, resolution string, n int) {
		if n > 0 {
			out = append(out, ConsistencyIssue{Kind: kind, Resolution: resolution, Count: n})
		}
	}
	add("derived_index_drift", "repaired", f["repaired"])
	add("stale_index_entry", "removed", f["drained"])
	for reason, n := range r.BlockReasons {
		add(reason, "blocked", n)
	}
	add("existing_block", "kept", f["already_blocked"])
	// Invalid dedup and dead-letter records are also counted as unhandled;
	// report them once.
	add("dedup_record_invalid", "held", f["dedup_skipped"])
	add("dead_letter_record_invalid", "held", f["dlq_invalid"])
	add("unhandled_state", "held", f["unhandled"]-f["dedup_skipped"]-f["dlq_invalid"])
	add("dead_letter_message_missing", "held", f["dlq_message_missing"])
	add("dead_letter_index_orphan", "removed", f["dlq_orphans_removed"])
	add("dead_letter_index_missing", "restored", f["dlq_restored"])
	add("dedup_record_expired", "removed", f["dedup_expired_removed"])
	add("dedup_index_missing", "restored", f["dedup_restored"])
	add("dedup_index_orphan", "removed", f["dedup_orphans_removed"])
	add("queued_counter_drift", "repaired", f["counter_repaired"])
	add("queued_counter_unverified", "skipped", f["counter_unverified"])
	return out
}

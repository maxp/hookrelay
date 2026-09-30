package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

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
	// BatchSize bounds SCAN/ZSCAN pages and script batches.
	BatchSize int
	// DedupRetention bounds validation of live dedup records referenced by
	// message metadata. Zero uses the accepted default of seven days.
	DedupRetention time.Duration
	// Gen supplies audit event identifiers (gen.Crypto when nil).
	Gen gen.Gen
	// Logger receives best-effort stdout copies of reconciliation audit events.
	Logger *slog.Logger
	// AdminSecret, when set, runs the Admin Secret generation check (and
	// rotation) before anything else.
	AdminSecret string
}

// ReconcileReport summarizes one pass with bounded finding kinds.
type ReconcileReport struct {
	Recipients int
	// Findings counts bounded kinds: repaired, drained, blocked,
	// already_blocked, due_lease, due_retry, unhandled,
	// dedup_expired_removed, dedup_restored, dedup_orphans_removed,
	// dedup_skipped, dlq_orphans_removed, dlq_restored, dlq_invalid,
	// dlq_message_missing, counter_repaired, counter_unverified,
	// admin_auth_initialized, admin_auth_rotated, session_orphans_removed,
	// sessions_removed, session_index_restored, endpoint_index_removed,
	// endpoint_bot_restored, endpoint_listing_restored,
	// endpoint_listing_score_repaired, endpoint_invalid,
	// active_attempt_repaired, active_attempt_legacy,
	// active_attempt_<detailed_reason>, and (from ReconcileAndProcessDue)
	// due_leases_processed, due_retries_processed.
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
	case r.Findings["endpoint_invalid"] > 0:
		return "webhook_endpoint_inconsistent"
	case r.Findings["dlq_message_missing"] > 0:
		return "dead_letter_message_missing"
	case r.Findings["message_lifecycle_inconsistent"] > 0:
		return "message_lifecycle_inconsistent"
	case r.Findings["unhandled"] > 0:
		return "unhandled_inconsistency"
	}
	return ""
}

// Reconcile validates persisted administration and delivery structures in
// bounded batches, repairs derived indexes and counters, isolates ambiguous
// Recipient state behind block markers, and reports due leases and retries
// for the caller to execute. It never uses KEYS.
func (a *Adapter) Reconcile(ctx context.Context, opts ReconcileOptions) (ReconcileReport, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.MessageCheckBound <= 0 {
		opts.MessageCheckBound = 1000
	}
	if opts.DedupRetention <= 0 {
		opts.DedupRetention = 168 * time.Hour
	}
	if opts.Gen == nil {
		opts.Gen = gen.Crypto{}
	}
	rep := ReconcileReport{Findings: map[string]int{}, BlockReasons: map[string]int{}}
	// The Admin Secret generation comes first: browser sessions of a
	// replaced secret must be revoked before readiness.
	if opts.AdminSecret != "" {
		out, err := a.EnsureAdminAuth(ctx, opts.AdminSecret, opts.Gen)
		if err != nil {
			if opts.Logger != nil {
				reason := "dependency_unavailable"
				if errors.Is(err, errAdminAuthInconsistent) {
					reason = strings.TrimPrefix(err.Error(), errAdminAuthInconsistent.Error()+": ")
				}
				observability.LogEvent(opts.Logger, slog.LevelError, "admin_auth_inconsistent",
					"the Admin Secret generation could not be confirmed", "error_code", "internal_error", "reason_code", reason)
			}
			return rep, fmt.Errorf("valkey: admin secret generation: %w", err)
		}
		switch out.Result {
		case "initialized", "rotated":
			rep.Findings["admin_auth_"+out.Result]++
			if opts.Logger != nil {
				observability.LogEvent(opts.Logger, slog.LevelWarn, "admin_secret_generation_"+out.Result,
					"Admin Secret generation "+out.Result, "revoked_sessions", out.Revoked)
			}
		}
	}
	if err := a.reconcileSessions(ctx, opts, &rep); err != nil {
		return rep, fmt.Errorf("valkey: administrative sessions: %w", err)
	}
	// Reconcile Webhook Endpoint derived indexes before validating the
	// authoritative records. Reverse cleanup runs before restoration so stale
	// members do not create a false Bot Identity capacity hold.
	if err := a.reconcileEndpoints(ctx, opts, &rep); err != nil {
		return rep, fmt.Errorf("valkey: webhook endpoints: %w", err)
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
	isolatedRecipients := map[string]bool{}
	total, countable := int64(0), true
	visit := func(rid string) error {
		if rid == "" {
			return nil
		}
		if _, done := seen[rid]; done {
			return nil
		}
		seen[rid] = struct{}{}
		n, status, err := a.reconcileRecipient(ctx, opts.Gen, opts.Logger, rid, opts.MessageCheckBound, &rep)
		if err != nil {
			return err
		}
		if status == "blocked" || status == "already_blocked" || status == "unhandled" {
			isolatedRecipients[rid] = true
			// The Recipient scan may be reached again from another derived
			// source after creating a marker; keep the original incident state.
		}
		if (status == "consistent" || status == "repaired") && a.recipientHasNonDueLease(ctx, rid) {
			if err := a.reconcileAttempt(ctx, opts.Gen, opts.Logger, rid, &rep); err != nil {
				return err
			}
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

	// Complete message lifecycle validation runs after the existing DLQ pass,
	// so already-reported dead-letter corruption is not double-counted. If
	// Recipient reconciliation already found unhandled structural state, its
	// incident remains authoritative and message scanning cannot locate safely.
	if rep.Findings["unhandled"] == 0 {
		if err := a.reconcileMessages(ctx, opts, isolatedRecipients, dlReported, &rep); err != nil {
			return rep, fmt.Errorf("valkey: message lifecycle: %w", err)
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
	"dlq_invalid", "dlq_message_missing", "counter_unverified", "endpoint_invalid",
	"active_attempt_legacy", "message_lifecycle_inconsistent", "message_legacy",
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

// reconcileRecipient runs reconcile_recipient_v3 and returns the counted
// queue length (-1 when uncountable) and its bounded status.
func (a *Adapter) reconcileRecipient(ctx context.Context, g gen.Gen, log *slog.Logger, rid string, bound int, rep *ReconcileReport) (int64, string, error) {
	res, err := a.RunScript(ctx, "reconcile_recipient_v3",
		[]string{"hr1:ready", "hr1:ready_seq", "hr1:leases", "hr1:blocked", "hr1:retries"},
		[]string{rid, strconv.Itoa(bound), "hr1"})
	if err != nil {
		return 0, "", fmt.Errorf("valkey: reconcile recipient: %w", err)
	}
	repairs, err1 := res.Fields[0].AsInt64()
	length, err2 := res.Fields[1].AsInt64()
	reason, err3 := res.Fields[2].ToString()
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, "", fmt.Errorf("valkey: reconcile recipient: result shape")
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
	return length, res.Status, nil
}

// recipientHasNonDueLease is a cheap post-reconcile guard. The script remains
// authoritative and returns not_leased/changed if the state changes before it
// executes; this read only avoids inspecting ready and retry-wait Recipients.
func (a *Adapter) recipientHasNonDueLease(ctx context.Context, rid string) bool {
	status, err1 := a.client.Do(ctx, a.client.B().Hget().Key("hr1:r:"+rid+":s").Field("status").Build()).ToString()
	deadline, err2 := a.client.Do(ctx, a.client.B().Hget().Key("hr1:r:"+rid+":s").Field("lease_expires_ms").Build()).AsInt64()
	if err1 != nil || err2 != nil || status != "leased" {
		return false
	}
	now, err := a.serverTimeMs(ctx)
	return err == nil && deadline > now
}

// reconcileAttempt validates the secret-bearing active Delivery Attempt
// cross-links without logging or auditing a token. A plaintext-token/digest
// mismatch is detected in Go because Valkey Lua has no SHA-256 implementation;
// the block-mode script fences and performs only the atomic isolation write.
func (a *Adapter) reconcileAttempt(ctx context.Context, g gen.Gen, log *slog.Logger, rid string, rep *ReconcileReport) error {
	stateKey := "hr1:r:" + rid + ":s"
	for read := 0; read < 2; read++ {
		values, err := a.client.Do(ctx, a.client.B().Hmget().Key(stateKey).Field("status", "delivery_token", "delivery_token_digest").Build()).ToArray()
		if err != nil {
			if isWrongType(err) || isNil(err) {
				return nil // reconcile_attempt_v1 classifies the persisted structure
			}
			return fmt.Errorf("valkey: reconcile attempt state: %w", err)
		}
		toString := func(i int) string {
			if i >= len(values) {
				return ""
			}
			v, e := values[i].ToString()
			if e != nil {
				return ""
			}
			return v
		}
		if toString(0) != "leased" {
			return nil
		}
		token, digest := toString(1), toString(2)
		mode, expected, detail := "verify", digest, ""
		validDigest := len(digest) == 64
		for i := 0; validDigest && i < len(digest); i++ {
			validDigest = digest[i] >= '0' && digest[i] <= '9' || digest[i] >= 'a' && digest[i] <= 'f'
		}
		if digest != "" && token != "" {
			sum := sha256.Sum256([]byte(token))
			computed := hex.EncodeToString(sum[:])
			if validDigest && computed != digest {
				mode, expected, detail = "block", "", "token_digest_mismatch"
			} else if !validDigest {
				// Fence with a syntactically valid digest so Lua can classify the
				// persisted malformed digest as active_attempt_state.
				expected = computed
			}
		}
		res, err := a.RunScript(ctx, "reconcile_attempt_v1",
			[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
			[]string{mode, rid, expected, detail, strconv.FormatInt(ClaimOpTTL.Milliseconds(), 10), "hr1"})
		if err != nil {
			return fmt.Errorf("valkey: reconcile attempt: %w", err)
		}
		switch res.Status {
		case "consistent", "not_leased":
			return nil
		case "changed":
			continue // one fenced reread; a second change waits for the next pass
		case "legacy":
			rep.Findings["active_attempt_legacy"]++
			return nil
		case "repaired":
			rep.Findings["active_attempt_repaired"]++
			a.auditRepair(ctx, g, log, "reconciliation_repair", rid, "active_attempt_lease_index_repaired")
			return nil
		case "already_blocked":
			rep.Findings["already_blocked"]++
			return nil
		case "blocked":
			reason, e := res.Fields[0].ToString()
			if e != nil {
				return fmt.Errorf("valkey: reconcile attempt: result shape")
			}
			rep.Findings["blocked"]++
			rep.Findings["active_attempt_"+reason]++
			rep.BlockReasons["active_attempt_inconsistent"]++
			if log != nil {
				observability.LogEvent(log, slog.LevelError, "active_attempt_inconsistent",
					"Recipient isolated because its active Delivery Attempt records disagree",
					"recipient_identity", rid, "reason_code", reason, "error_code", "internal_error")
			}
			a.auditRepair(ctx, g, log, "recipient_blocked", rid, "active_attempt_inconsistent")
			return nil
		case "wrong_type":
			rep.Findings["unhandled"]++
			return nil
		default:
			return fmt.Errorf("valkey: reconcile attempt: unexpected result %q", res.Status)
		}
	}
	return nil
}

// reconcileMessages discovers message IDs from every first-version lifecycle
// family, then validates each ID once. Queue discovery supplies the only safe
// Recipient locator; IDs found from blobs/metadata/history/DLQ/success remain
// unlocatable unless also present in a queue.
func (a *Adapter) reconcileMessages(ctx context.Context, opts ReconcileOptions, isolatedRecipients, dlReported map[string]bool, rep *ReconcileReport) error {
	type locator struct {
		rid      string
		position string
	}
	ids := map[string]locator{}
	add := func(id string) {
		if id != "" {
			if _, exists := ids[id]; !exists {
				ids[id] = locator{position: "none"}
			}
		}
	}
	if err := a.scanKeys(ctx, "hr1:r:*:q", opts.BatchSize, func(key string) error {
		rest := strings.TrimSuffix(strings.TrimPrefix(key, "hr1:r:"), ":q")
		if rest == "" {
			return nil
		}
		t, err := a.keyType(ctx, key)
		if err != nil {
			return err
		}
		if t != "list" {
			return nil // recipient reconciliation owns this incompatibility
		}
		var offset int64
		for {
			page, err := a.client.Do(ctx, a.client.B().Lrange().Key(key).Start(offset).Stop(offset+int64(opts.BatchSize)-1).Build()).AsStrSlice()
			if err != nil {
				return err
			}
			for i, id := range page {
				position := "behind_head"
				if offset+int64(i) == 0 {
					position = "head"
				}
				if previous, exists := ids[id]; exists && previous.rid != "" && previous.rid != rest {
					ids[id] = locator{position: "none"} // duplicated across queues: no safe owner
				} else {
					ids[id] = locator{rid: rest, position: position}
				}
			}
			if len(page) < opts.BatchSize {
				break
			}
			offset += int64(len(page))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, prefix := range []string{"hr1:m:", "hr1:mi:", "hr1:a:", "hr1:dl:", "hr1:success:"} {
		if err := a.scanKeys(ctx, prefix+"*", opts.BatchSize, func(key string) error {
			add(strings.TrimPrefix(key, prefix))
			return nil
		}); err != nil {
			return err
		}
	}

	for id, loc := range ids {
		if dlReported[id] || isolatedRecipients[loc.rid] {
			continue
		}
		res, err := a.RunScript(ctx, "reconcile_message_v1",
			[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
			[]string{"inspect", id, loc.rid, loc.position, strconv.FormatInt(opts.DedupRetention.Milliseconds(), 10), "hr1"})
		if err != nil {
			return err
		}
		reason := func() (string, error) {
			if len(res.Fields) == 0 {
				return "", fmt.Errorf("valkey: reconcile message: result shape")
			}
			return res.Fields[0].ToString()
		}
		switch res.Status {
		case "consistent", "legacy":
			// Accepted pre-M2 compatibility is observable through the script
			// status but is not itself a consistency issue.
		case "already_blocked":
			// Recipient reconciliation already counted the existing marker.
		case "blocked":
			detail, e := reason()
			if e != nil {
				return e
			}
			rep.Findings["blocked"]++
			rep.Findings["message_lifecycle_"+detail]++
			rep.BlockReasons["message_lifecycle_inconsistent"]++
			a.auditRepair(ctx, opts.Gen, opts.Logger, "recipient_blocked", loc.rid, "message_lifecycle_inconsistent")
		case "orphan_records":
			removed, err := a.RunScript(ctx, "reconcile_message_v1",
				[]string{"hr1:ready", "hr1:leases", "hr1:retries", "hr1:blocked"},
				[]string{"delete_orphans", id, "", "none", strconv.FormatInt(opts.DedupRetention.Milliseconds(), 10), "hr1"})
			if err != nil {
				return err
			}
			if removed.Status == "removed_orphans" {
				n, e := removed.Fields[0].AsInt64()
				if e != nil {
					return fmt.Errorf("valkey: reconcile message: result shape")
				}
				rep.Findings["message_orphans_removed"] += int(n)
				a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", id, "message_orphan_records_removed")
			} else if removed.Status != "consistent" {
				rep.Findings["message_lifecycle_inconsistent"]++
				rep.Findings["unhandled"]++
			}
		case "inconsistent", "wrong_type":
			detail, e := reason()
			if e != nil {
				return e
			}
			rep.Findings["message_lifecycle_inconsistent"]++
			rep.Findings["message_lifecycle_"+detail]++
			rep.Findings["unhandled"]++
			if opts.Logger != nil {
				observability.LogEvent(opts.Logger, slog.LevelError, "message_lifecycle_inconsistent",
					"message lifecycle could not be reconciled", "message_id", id,
					"reason_code", detail, "error_code", "internal_error")
			}
		default:
			return fmt.Errorf("valkey: reconcile message: unexpected result %q", res.Status)
		}
	}
	return nil
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
	add("active_attempt_lease_index_drift", "repaired", f["active_attempt_repaired"])
	add("active_attempt_legacy", "kept", f["active_attempt_legacy"])
	add("message_legacy", "kept", f["message_legacy"])
	add("message_orphan_records", "removed", f["message_orphans_removed"])
	for _, reason := range []string{
		"message_invalid", "message_orphan", "message_missing", "metadata_invalid", "history_invalid",
		"pending_state_missing", "pending_state_invalid", "dead_letter_invalid", "dead_letter_overlap",
		"success_invalid", "success_overlap", "dedup_record_invalid", "marker_invalid", "lifecycle_ambiguous",
	} {
		add("message_lifecycle_"+reason, "held", f["message_lifecycle_"+reason])
	}
	for _, reason := range []string{
		"active_attempt_structure", "active_attempt_state", "token_digest_mismatch",
		"token_missing", "token_type", "token_mismatch", "claim_operation_missing",
		"claim_operation_type", "claim_operation_mismatch", "token_ttl", "claim_operation_ttl",
	} {
		add("active_attempt_"+reason, "blocked", f["active_attempt_"+reason])
	}
	add("stale_index_entry", "removed", f["drained"])
	for reason, n := range r.BlockReasons {
		// Active-attempt blocks keep one bounded marker reason, while the
		// metric uses the detailed bounded cross-link reason below.
		if reason != "active_attempt_inconsistent" {
			add(reason, "blocked", n)
		}
	}
	add("existing_block", "kept", f["already_blocked"])
	// Invalid dedup and dead-letter records are also counted as unhandled;
	// report them once.
	add("dedup_record_invalid", "held", f["dedup_skipped"])
	add("dead_letter_record_invalid", "held", f["dlq_invalid"])
	add("endpoint_record_invalid", "held", f["endpoint_invalid"])
	add("unhandled_state", "held", f["unhandled"]-f["dedup_skipped"]-f["dlq_invalid"]-f["endpoint_invalid"])
	add("dead_letter_message_missing", "held", f["dlq_message_missing"])
	add("dead_letter_index_orphan", "removed", f["dlq_orphans_removed"])
	add("dead_letter_index_missing", "restored", f["dlq_restored"])
	add("dedup_record_expired", "removed", f["dedup_expired_removed"])
	add("dedup_index_missing", "restored", f["dedup_restored"])
	add("dedup_index_orphan", "removed", f["dedup_orphans_removed"])
	add("queued_counter_drift", "repaired", f["counter_repaired"])
	add("queued_counter_unverified", "skipped", f["counter_unverified"])
	add("session_index_orphan", "removed", f["session_orphans_removed"])
	add("session_invalid", "removed", f["sessions_removed"])
	add("session_index_missing", "restored", f["session_index_restored"])
	add("endpoint_index_orphan", "removed", f["endpoint_index_removed"])
	add("endpoint_bot_index_missing", "restored", f["endpoint_bot_restored"])
	add("endpoint_listing_missing", "restored", f["endpoint_listing_restored"])
	add("endpoint_listing_score_drift", "repaired", f["endpoint_listing_score_repaired"])
	return out
}

// reconcileSessions checks every browser session found in the index or by
// key, repairing the index from the authoritative Hash and deleting
// sessions that can no longer be valid. Sessions are disposable, so no
// finding here holds readiness; a wrong-typed structure does.
func (a *Adapter) reconcileSessions(ctx context.Context, opts ReconcileOptions, rep *ReconcileReport) error {
	seen := map[string]struct{}{}
	visit := func(digest string) error {
		if _, done := seen[digest]; done {
			return nil
		}
		seen[digest] = struct{}{}
		if !validSessionDigest(digest) {
			return fmt.Errorf("valkey: invalid session identifier %q", digest)
		}
		res, err := a.RunScript(ctx, "reconcile_session_v1", []string{adminAuthKey, adminSessionsKey}, []string{digest, "hr1"})
		if err != nil {
			return err
		}
		switch res.Status {
		case "orphan_removed":
			rep.Findings["session_orphans_removed"]++
			a.auditRepair(ctx, opts.Gen, opts.Logger, "session_index_repaired", "session", "session_index_orphan")
		case "removed":
			reason, _ := res.Fields[0].ToString()
			rep.Findings["sessions_removed"]++
			a.auditRepair(ctx, opts.Gen, opts.Logger, "session_index_repaired", "session", "session_"+reason)
		case "restored":
			rep.Findings["session_index_restored"]++
			a.auditRepair(ctx, opts.Gen, opts.Logger, "session_index_repaired", "session", "session_index_missing")
		case "wrong_type":
			return fmt.Errorf("valkey: session %s structures have an unexpected type", digest[:8])
		}
		return nil
	}
	if err := a.scanMembers(ctx, adminSessionsKey, opts.BatchSize, func(members []string) error {
		for _, m := range members {
			if err := visit(m); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return a.scanKeys(ctx, "hr1:admin_session:*", opts.BatchSize, func(key string) error {
		return visit(strings.TrimPrefix(key, "hr1:admin_session:"))
	})
}

// reconcileEndpoints checks both directions between authoritative Webhook
// Endpoint Hashes and their rebuildable Bot Identity/global listing indexes.
func (a *Adapter) reconcileEndpoints(ctx context.Context, opts ReconcileOptions, rep *ReconcileReport) error {
	invalidReported := map[string]bool{}
	reportInvalid := func(member, reason string) {
		key := member + "\x00" + reason
		if invalidReported[key] {
			return
		}
		invalidReported[key] = true
		rep.Findings["endpoint_invalid"]++
		rep.Findings["unhandled"]++
		if opts.Logger != nil {
			observability.LogEvent(opts.Logger, slog.LevelError, "webhook_endpoint_inconsistent",
				"Webhook Endpoint state could not be reconciled", "target", member,
				"reason_code", reason, "error_code", "internal_error")
		}
	}
	apply := func(mode, member, platform, botID string) error {
		res, err := a.RunScript(ctx, "reconcile_endpoint_v1", []string{"hr1:webhooks"},
			[]string{mode, member, platform, botID, "hr1"})
		if err != nil {
			return err
		}
		switch res.Status {
		case "consistent", "absent":
			return nil
		case "orphan_removed":
			reason, err := res.Fields[0].ToString()
			if err != nil {
				return fmt.Errorf("valkey: reconcile endpoint: result shape")
			}
			rep.Findings["endpoint_index_removed"]++
			a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", member, reason)
			return nil
		case "repaired":
			botRepair, err1 := res.Fields[0].AsInt64()
			listingRepair, err2 := res.Fields[1].AsInt64()
			if err1 != nil || err2 != nil {
				return fmt.Errorf("valkey: reconcile endpoint: result shape")
			}
			if botRepair > 0 {
				rep.Findings["endpoint_bot_restored"] += int(botRepair)
				a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", member, "endpoint_bot_index_restored")
			}
			switch listingRepair {
			case 0:
			case 1:
				rep.Findings["endpoint_listing_restored"]++
				a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", member, "endpoint_listing_restored")
			case 2:
				rep.Findings["endpoint_listing_score_repaired"]++
				a.auditRepair(ctx, opts.Gen, opts.Logger, "reconciliation_repair", member, "endpoint_listing_score_repaired")
			default:
				return fmt.Errorf("valkey: reconcile endpoint: result shape")
			}
			return nil
		case "invalid", "wrong_type":
			reason, err := res.Fields[0].ToString()
			if err != nil {
				return fmt.Errorf("valkey: reconcile endpoint: result shape")
			}
			reportInvalid(member, reason)
			return nil
		default:
			return fmt.Errorf("valkey: reconcile endpoint: unexpected result %q", res.Status)
		}
	}

	// 1. Reverse Bot Identity memberships.
	if err := a.scanKeys(ctx, "hr1:bot:*:webhooks", opts.BatchSize, func(key string) error {
		parts := strings.Split(strings.TrimSuffix(strings.TrimPrefix(key, "hr1:bot:"), ":webhooks"), ":")
		if len(parts) != 2 || parts[0] != "telegram" || !validAdminBotID(parts[1]) {
			reportInvalid(key, "bot_index_key_invalid")
			return nil
		}
		return a.scanSetMembers(ctx, key, opts.BatchSize, func(members []string) error {
			for _, member := range members {
				if err := apply("bot_member", member, parts[0], parts[1]); err != nil {
					return err
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}

	// 2. Reverse global listing memberships.
	if err := a.scanMembers(ctx, "hr1:webhooks", opts.BatchSize, func(members []string) error {
		for _, member := range members {
			if err := apply("listing_member", member, "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// 3. Authoritative endpoint Hashes restore both indexes.
	return a.scanKeys(ctx, "hr1:wh:*", opts.BatchSize, func(key string) error {
		member := strings.TrimPrefix(key, "hr1:wh:")
		if err := apply("endpoint", member, "", ""); err != nil {
			return err
		}
		return nil
	})
}

// scanSetMembers walks a Set with SSCAN in bounded pages.
func (a *Adapter) scanSetMembers(ctx context.Context, key string, count int, fn func([]string) error) error {
	t, err := a.keyType(ctx, key)
	if err != nil {
		return err
	}
	if t == "none" {
		return nil
	}
	if t != "set" {
		return fmt.Errorf("valkey: index %s has unexpected type %q", key, t)
	}
	cursor := uint64(0)
	for {
		entry, err := a.client.Do(ctx, a.client.B().Sscan().Key(key).Cursor(cursor).Count(int64(count)).Build()).AsScanEntry()
		if err != nil {
			return fmt.Errorf("valkey: sscan %s: %w", key, err)
		}
		if err := fn(entry.Elements); err != nil {
			return err
		}
		if entry.Cursor == 0 {
			return nil
		}
		cursor = entry.Cursor
	}
}

func validSessionDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !(s[i] >= '0' && s[i] <= '9' || s[i] >= 'a' && s[i] <= 'f') {
			return false
		}
	}
	return true
}

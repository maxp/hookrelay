package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"text/tabwriter"
)

// recipientFlags identify one Recipient by its structured fields.
type recipientFlags struct {
	botPlatform, botID, scope, chatID, userID string
}

func (r *recipientFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&r.botPlatform, "bot-platform", "", "Bot Platform (required)")
	fs.StringVar(&r.botID, "bot-id", "", "Bot Identifier (required)")
	fs.StringVar(&r.scope, "scope", "", "Recipient scope: chat, user, bot, or relay (required)")
	fs.StringVar(&r.chatID, "chat-id", "", "chat identifier (chat scope)")
	fs.StringVar(&r.userID, "user-id", "", "user identifier (user scope)")
}

// body returns the structured Recipient, or an error for an incomplete or
// contradictory combination.
func (r *recipientFlags) body() (map[string]string, error) {
	if r.botPlatform == "" || r.botID == "" || r.scope == "" {
		return nil, errors.New("--bot-platform, --bot-id, and --scope are required")
	}
	out := map[string]string{"scope": r.scope, "bot_platform": r.botPlatform, "bot_id": r.botID}
	switch r.scope {
	case "chat":
		if r.chatID == "" || r.userID != "" {
			return nil, errors.New("chat scope needs --chat-id and no --user-id")
		}
		out["chat_id"] = r.chatID
	case "user":
		if r.userID == "" || r.chatID != "" {
			return nil, errors.New("user scope needs --user-id and no --chat-id")
		}
		out["user_id"] = r.userID
	case "bot", "relay":
		if r.chatID != "" || r.userID != "" {
			return nil, fmt.Errorf("%s scope takes no --chat-id or --user-id", r.scope)
		}
	default:
		return nil, errors.New("--scope must be chat, user, bot, or relay")
	}
	return out, nil
}

func runRecipients(args []string, env adminIO) int {
	if len(args) < 1 {
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
	switch args[0] {
	case "list":
		return adminRecipientsList(args[1:], env)
	case "inspect-block":
		return adminRecipientsInspect(args[1:], env)
	case "clear-block":
		return adminRecipientsClear(args[1:], env)
	default:
		fmt.Fprintf(env.Stderr, "hookrelay admin: unknown recipients command %q\n", args[0])
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
}

type recipientStatePage struct {
	Items []struct {
		Recipient      map[string]string `json:"recipient"`
		Status         string            `json:"status"`
		ReadySequence  *int64            `json:"ready_sequence,omitempty"`
		LeaseExpiresMs *int64            `json:"lease_expires_ms,omitempty"`
		RetryAtMs      *int64            `json:"retry_at_ms,omitempty"`
		DetectedMs     *int64            `json:"detected_ms,omitempty"`
		ReasonCode     string            `json:"reason_code,omitempty"`
	} `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func adminRecipientsList(args []string, env adminIO) int {
	const name = "hookrelay admin recipients list"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	status := fs.String("status", "", "ready, leased, retry_wait, or blocked (required)")
	limit := fs.Int("limit", 0, "page size 1-200 (default 50)")
	cursor := fs.String("cursor", "", "next_cursor of the previous page")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *status == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --status is required\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	q := url.Values{"status": {*status}}
	if *limit != 0 {
		q.Set("limit", strconv.Itoa(*limit))
	}
	if *cursor != "" {
		q.Set("cursor", *cursor)
	}
	code, data, err := client.do(http.MethodGet, "/admin/v1/recipient-states?"+q.Encode(), nil)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if code != http.StatusOK {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
	var page recipientStatePage
	if err := json.Unmarshal(data, &page); err != nil {
		fmt.Fprintf(env.Stderr, "%s: unreadable response: %v\n", name, err)
		return ExitError
	}
	if client.format == "json" {
		if writeJSONResult(env.Stdout, page) != nil {
			return ExitError
		}
		return ExitOK
	}
	tw := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "SCOPE\tBOT_PLATFORM\tBOT_ID\tCHAT_ID\tUSER_ID\tSTATUS\tSCORE\tREASON_CODE")
	for _, it := range page.Items {
		score := int64(0)
		for _, v := range []*int64{it.ReadySequence, it.LeaseExpiresMs, it.RetryAtMs, it.DetectedMs} {
			if v != nil {
				score = *v
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n", it.Recipient["scope"], it.Recipient["bot_platform"], it.Recipient["bot_id"],
			it.Recipient["chat_id"], it.Recipient["user_id"], it.Status, score, it.ReasonCode)
	}
	if page.NextCursor != "" {
		fmt.Fprintf(tw, "next_cursor\t%s\n", page.NextCursor)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

// inspection is the block inspection response.
type inspection struct {
	Marker *struct {
		DetectedMs int64  `json:"detected_ms"`
		ReasonCode string `json:"reason_code"`
	} `json:"marker"`
	QueueLength int64 `json:"queue_length"`
	Head        *struct {
		MessageID      string `json:"message_id"`
		Status         string `json:"status"`
		DeliveryCycle  int64  `json:"delivery_cycle"`
		Attempt        int64  `json:"attempt"`
		LeaseExpiresMs int64  `json:"lease_expires_ms,omitempty"`
		RetryAtMs      int64  `json:"retry_at_ms,omitempty"`
	} `json:"head"`
	HeadMessagePresent bool            `json:"head_message_present"`
	Memberships        map[string]bool `json:"memberships"`
	ViolatedInvariants []string        `json:"violated_invariants"`
}

func (c *adminClient) inspectBlock(recipient map[string]string) (*inspection, error) {
	code, data, err := c.do(http.MethodPost, "/admin/v1/recipient-blocks/inspect", map[string]any{"recipient": recipient})
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, decodeAPIError(code, data)
	}
	var in inspection
	if err := json.Unmarshal(data, &in); err != nil {
		return nil, fmt.Errorf("unreadable inspection response: %w", err)
	}
	return &in, nil
}

func (c *adminClient) printInspection(in *inspection) error {
	if c.format == "json" {
		return writeJSONResult(c.env.Stdout, in)
	}
	tw := tabwriter.NewWriter(c.env.Stdout, 0, 0, 2, ' ', 0)
	if in.Marker != nil {
		fmt.Fprintf(tw, "marker\tdetected_ms=%d reason_code=%s\n", in.Marker.DetectedMs, in.Marker.ReasonCode)
	} else {
		fmt.Fprintf(tw, "marker\tnone\n")
	}
	fmt.Fprintf(tw, "queue_length\t%d\n", in.QueueLength)
	if in.Head != nil {
		fmt.Fprintf(tw, "head\t%s %s cycle=%d attempt=%d lease_expires_ms=%d retry_at_ms=%d\n", in.Head.MessageID, in.Head.Status,
			in.Head.DeliveryCycle, in.Head.Attempt, in.Head.LeaseExpiresMs, in.Head.RetryAtMs)
	}
	fmt.Fprintf(tw, "head_message_present\t%t\n", in.HeadMessagePresent)
	for _, idx := range []string{"ready", "leases", "retries", "blocked"} {
		fmt.Fprintf(tw, "member_%s\t%t\n", idx, in.Memberships[idx])
	}
	fmt.Fprintf(tw, "violated_invariants\t%v\n", in.ViolatedInvariants)
	return tw.Flush()
}

func adminRecipientsInspect(args []string, env adminIO) int {
	const name = "hookrelay admin recipients inspect-block"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	var rf recipientFlags
	rf.register(fs)
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	recipient, err := rf.body()
	if err != nil || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	in, err := client.inspectBlock(recipient)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if client.printInspection(in) != nil {
		return ExitError
	}
	return ExitOK
}

type clearResult struct {
	Outcome    string      `json:"outcome"`
	Inspection *inspection `json:"inspection,omitempty"`
}

func adminRecipientsClear(args []string, env adminIO) int {
	const name = "hookrelay admin recipients clear-block"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	var rf recipientFlags
	rf.register(fs)
	detected := fs.Int64("expected-detected-ms", 0, "detected_ms of the inspected marker (required)")
	reason := fs.String("expected-reason-code", "", "reason_code of the inspected marker (required)")
	yes := fs.Bool("yes", false, "confirm the audited clear (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	recipient, err := rf.body()
	switch {
	case err != nil:
	case *detected <= 0 || *reason == "":
		err = errors.New("--expected-detected-ms and --expected-reason-code from inspect-block are required")
	case !*yes:
		err = errors.New("--yes is required: clearing a block is an audited mutation")
	case fs.NArg() > 0:
		err = errors.New("unexpected arguments")
	}
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	code, data, err := client.do(http.MethodPost, "/admin/v1/recipient-blocks/clear", map[string]any{
		"recipient": recipient, "expected_detected_ms": *detected, "expected_reason_code": *reason,
	})
	switch {
	case err != nil:
		return client.reconcileClear(recipient, err.Error())
	case code == http.StatusNoContent:
		if writeJSONResult(env.Stdout, clearResult{Outcome: "cleared"}) != nil {
			return ExitError
		}
		return ExitOK
	case code >= 500:
		return client.reconcileClear(recipient, decodeAPIError(code, data).Error())
	default:
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
}

// reconcileClear handles a clear whose outcome is unknown. It never
// retries: it inspects the Recipient once and reports what it observed. A
// missing marker does not prove that this request cleared it or that the
// mandatory audit event was appended, so the command still fails.
func (c *adminClient) reconcileClear(recipient map[string]string, cause string) int {
	const name = "hookrelay admin recipients clear-block"
	fmt.Fprintf(c.env.Stderr, "%s: outcome uncertain: %s\n", name, cause)
	fmt.Fprintf(c.env.Stderr, "%s: not retrying; inspecting the recipient to reconcile\n", name)
	result := clearResult{Outcome: outcomeUncertain}
	in, err := c.inspectBlock(recipient)
	switch {
	case err != nil:
		fmt.Fprintf(c.env.Stderr, "WARNING: the recipient could not be inspected (%v); follow the runbook's uncertain-clear reconciliation. Do not retry the clear.\n", err)
	case in.Marker == nil:
		result.Outcome = outcomeDesiredStateObserved
		result.Inspection = in
		fmt.Fprintln(c.env.Stderr, "WARNING: the block marker is gone, but this does NOT confirm the clear: it cannot prove that this request "+
			"cleared it or that the mandatory audit event was written. Check the audit before any further mutation.")
	default:
		result.Inspection = in
		fmt.Fprintln(c.env.Stderr, "WARNING: the recipient is still blocked; the clear may or may not have run. Follow the runbook's "+
			"uncertain-clear reconciliation. Do not retry blindly.")
	}
	_ = writeJSONResult(c.env.Stdout, result)
	return ExitError
}

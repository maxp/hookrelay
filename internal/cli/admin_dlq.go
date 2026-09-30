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

func runDLQ(args []string, env adminIO) int {
	if len(args) < 1 {
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
	switch args[0] {
	case "list":
		return adminDLQList(args[1:], env)
	case "get":
		return adminDLQGet(args[1:], env)
	case "replay":
		return adminDLQReplay(args[1:], env)
	case "payload":
		return adminDLQPayload(args[1:], env)
	case "delete":
		return adminDLQDelete(args[1:], env)
	default:
		fmt.Fprintf(env.Stderr, "hookrelay admin: unknown dlq command %q\n", args[0])
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
}

// deadLetter is one dead letter's safe metadata as returned by the API.
type deadLetter struct {
	MessageID        string            `json:"message_id"`
	Recipient        map[string]string `json:"recipient"`
	DeadLetteredMs   int64             `json:"dead_lettered_ms"`
	DeadLetterReason string            `json:"dead_letter_reason"`
	DeliveryCycle    int64             `json:"delivery_cycle"`
	Attempts         []struct {
		DeliveryCycle      int64  `json:"delivery_cycle"`
		Attempt            int64  `json:"attempt"`
		ClaimedMs          int64  `json:"claimed_ms"`
		LeaseExpiresMs     int64  `json:"lease_expires_ms"`
		CompletedMs        int64  `json:"completed_ms"`
		Outcome            string `json:"outcome"`
		ReasonCode         string `json:"reason_code,omitempty"`
		ConsumerInstanceID string `json:"consumer_instance_id,omitempty"`
	} `json:"attempts,omitempty"`
	ArchivedCycles *struct {
		ArchivedCycles   int64 `json:"archived_cycles"`
		ArchivedAttempts int64 `json:"archived_attempts"`
		FirstArchivedMs  int64 `json:"first_archived_ms"`
		LastArchivedMs   int64 `json:"last_archived_ms"`
	} `json:"archived_cycles_summary,omitempty"`
}

type deadLetterPage struct {
	Items      []deadLetter `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

func adminDLQList(args []string, env adminIO) int {
	const name = "hookrelay admin dlq list"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	limit := fs.Int("limit", 0, "page size 1-200 (default 50)")
	cursor := fs.String("cursor", "", "next_cursor of the previous page")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: unexpected arguments\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	q := url.Values{}
	if *limit != 0 {
		q.Set("limit", strconv.Itoa(*limit))
	}
	if *cursor != "" {
		q.Set("cursor", *cursor)
	}
	path := "/admin/v1/dead-letters"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	code, data, err := client.do(http.MethodGet, path, nil)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if code != http.StatusOK {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
	var page deadLetterPage
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
	fmt.Fprintln(tw, "MESSAGE_ID\tDEAD_LETTERED_MS\tREASON\tCYCLE\tSCOPE\tBOT_PLATFORM\tBOT_ID\tCHAT_ID\tUSER_ID")
	for _, d := range page.Items {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\n", d.MessageID, d.DeadLetteredMs, d.DeadLetterReason, d.DeliveryCycle,
			d.Recipient["scope"], d.Recipient["bot_platform"], d.Recipient["bot_id"], d.Recipient["chat_id"], d.Recipient["user_id"])
	}
	if page.NextCursor != "" {
		fmt.Fprintf(tw, "next_cursor\t%s\n", page.NextCursor)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

// getDeadLetterTagged is getDeadLetter that also returns the entity ETag.
func (c *adminClient) getDeadLetterTagged(messageID string) (*deadLetter, string, error) {
	code, headers, data, err := c.doHeaders(http.MethodGet, "/admin/v1/dead-letters/"+url.PathEscape(messageID), nil, nil)
	if err != nil {
		return nil, "", err
	}
	if code != http.StatusOK {
		return nil, "", decodeAPIError(code, data)
	}
	var d deadLetter
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, "", fmt.Errorf("unreadable dead-letter response: %w", err)
	}
	return &d, headers.Get("ETag"), nil
}

// getDeadLetter reads one dead letter. It returns an apiError 404 for a
// message that is not dead-lettered and a plain error for transport or
// shape failures.
func (c *adminClient) getDeadLetter(messageID string) (*deadLetter, error) {
	code, data, err := c.do(http.MethodGet, "/admin/v1/dead-letters/"+url.PathEscape(messageID), nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, decodeAPIError(code, data)
	}
	var d deadLetter
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, fmt.Errorf("unreadable dead-letter response: %w", err)
	}
	return &d, nil
}

func (c *adminClient) printDeadLetter(d *deadLetter) error {
	if c.format == "json" {
		return writeJSONResult(c.env.Stdout, d)
	}
	tw := tabwriter.NewWriter(c.env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "message_id\t%s\n", d.MessageID)
	for _, k := range []string{"scope", "bot_platform", "bot_id", "chat_id", "user_id"} {
		if v := d.Recipient[k]; v != "" {
			fmt.Fprintf(tw, "%s\t%s\n", k, v)
		}
	}
	fmt.Fprintf(tw, "dead_lettered_ms\t%d\n", d.DeadLetteredMs)
	fmt.Fprintf(tw, "dead_letter_reason\t%s\n", d.DeadLetterReason)
	fmt.Fprintf(tw, "delivery_cycle\t%d\n", d.DeliveryCycle)
	if s := d.ArchivedCycles; s != nil {
		fmt.Fprintf(tw, "archived_cycles\tcycles=%d attempts=%d first_ms=%d last_ms=%d\n", s.ArchivedCycles, s.ArchivedAttempts, s.FirstArchivedMs, s.LastArchivedMs)
	}
	for _, a := range d.Attempts {
		fmt.Fprintf(tw, "attempt\tcycle=%d attempt=%d outcome=%s claimed_ms=%d completed_ms=%d reason_code=%s consumer_instance_id=%s\n",
			a.DeliveryCycle, a.Attempt, a.Outcome, a.ClaimedMs, a.CompletedMs, a.ReasonCode, a.ConsumerInstanceID)
	}
	return tw.Flush()
}

func adminDLQGet(args []string, env adminIO) int {
	const name = "hookrelay admin dlq get"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	messageID := fs.String("message-id", "", "Message Identifier (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *messageID == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --message-id is required\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	d, err := client.getDeadLetter(*messageID)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if client.printDeadLetter(d) != nil {
		return ExitError
	}
	return ExitOK
}

// adminDLQPayload prints a dead letter's Canonical Message. The server
// discloses it only after appending the access audit, so every successful
// call is an audited view; a failed call is simply reported (repeating it
// is another audited view, not a mutation).
func adminDLQPayload(args []string, env adminIO) int {
	const name = "hookrelay admin dlq payload"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	messageID := fs.String("message-id", "", "Message Identifier (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *messageID == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --message-id is required\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	code, data, err := client.do(http.MethodPost, "/admin/v1/dead-letters/"+url.PathEscape(*messageID)+"/payload", nil)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if code != http.StatusOK {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
	var body json.RawMessage
	if err := json.Unmarshal(data, &body); err != nil {
		fmt.Fprintf(env.Stderr, "%s: unreadable response: %v\n", name, err)
		return ExitError
	}
	fmt.Fprintf(env.Stderr, "%s: this payload access was recorded in the administrative audit\n", name)
	if writeJSONResult(env.Stdout, body) != nil {
		return ExitError
	}
	return ExitOK
}

// dlqDeleteResult is the stdout result of a deletion: confirmed, or the
// reconciliation observation when the outcome is unknown.
type dlqDeleteResult struct {
	Outcome    string      `json:"outcome"`
	MessageID  string      `json:"message_id"`
	DeadLetter *deadLetter `json:"dead_letter,omitempty"`
}

// adminDLQDelete reads the dead letter and its ETag, then permanently
// deletes exactly that entry under If-Match. A missing entry is an error
// here (a typo is not reported as success) although the API answers 204.
func adminDLQDelete(args []string, env adminIO) int {
	const name = "hookrelay admin dlq delete"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	messageID := fs.String("message-id", "", "Message Identifier (required)")
	yes := fs.Bool("yes", false, "confirm the audited permanent deletion (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	var err error
	switch {
	case *messageID == "":
		err = errors.New("--message-id is required")
	case !*yes:
		err = errors.New("--yes is required: permanent deletion is an audited mutation")
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
	_, etag, err := client.getDeadLetterTagged(*messageID)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	status, _, data, err := client.doHeaders(http.MethodDelete, "/admin/v1/dead-letters/"+url.PathEscape(*messageID), nil, map[string]string{"If-Match": etag})
	switch {
	case err != nil:
		return client.reconcileDLQDelete(*messageID, fmt.Sprintf("the request failed: %v", err))
	case status == http.StatusNoContent:
		if writeJSONResult(env.Stdout, dlqDeleteResult{Outcome: outcomeDeleted, MessageID: *messageID}) != nil {
			return ExitError
		}
		return ExitOK
	case status >= 500:
		return client.reconcileDLQDelete(*messageID, fmt.Sprintf("the server reported %v", decodeAPIError(status, data)))
	case status == http.StatusPreconditionFailed:
		fmt.Fprintf(env.Stderr, "%s: %v; the dead letter changed since it was read (replayed or dead-lettered again) — not retrying, read it again\n",
			name, decodeAPIError(status, data))
		return ExitError
	default:
		apiErr := decodeAPIError(status, data)
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, apiErr)
		if apiErr.Code == "recipient_blocked" {
			fmt.Fprintf(env.Stderr, "%s: the Recipient is blocked; follow the Recipient block recovery runbook before deleting its dead letters\n", name)
		}
		return ExitError
	}
}

// reconcileDLQDelete handles a deletion whose outcome is unknown. It never
// retries: observed absence does not prove that this request deleted the
// dead letter or that its mandatory audit event was written.
func (c *adminClient) reconcileDLQDelete(messageID, cause string) int {
	const name = "hookrelay admin dlq delete"
	fmt.Fprintf(c.env.Stderr, "%s: outcome uncertain for %s: %s\n", name, messageID, cause)
	fmt.Fprintf(c.env.Stderr, "%s: not retrying; reading the dead letter to reconcile\n", name)
	result := dlqDeleteResult{Outcome: outcomeUncertain, MessageID: messageID}
	d, err := c.getDeadLetter(messageID)
	var ae *apiError
	switch {
	case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
		result.Outcome = outcomeDesiredStateObserved
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is no longer dead-lettered, but this does NOT confirm that this request deleted it (it may have been "+
			"replayed or expired) or that the mandatory audit event was written. Check the audit before any further mutation.\n", messageID)
	case err == nil:
		result.DeadLetter = d
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is still dead-lettered; operator reconciliation is required. Do not retry blindly.\n", messageID)
	default:
		fmt.Fprintf(c.env.Stderr, "WARNING: %s could not be read (%v); operator reconciliation is required. Do not retry blindly.\n", messageID, err)
	}
	_ = writeJSONResult(c.env.Stdout, result)
	return ExitError
}

// replayResult is the stdout result of a replay: the API response on
// success, or the reconciliation observation when the outcome is unknown.
type replayResult struct {
	Outcome                 string         `json:"outcome"`
	MessageID               string         `json:"message_id"`
	DeliveryCycle           int64          `json:"delivery_cycle,omitempty"`
	QueuePosition           string         `json:"queue_position,omitempty"`
	ReplayedMs              int64          `json:"replayed_ms,omitempty"`
	DeduplicationResolution string         `json:"deduplication_resolution,omitempty"`
	PreviousDeliveryCycle   int64          `json:"previous_delivery_cycle"`
	DeliveryState           *deliveryState `json:"delivery_state,omitempty"`
}

func adminDLQReplay(args []string, env adminIO) int {
	const name = "hookrelay admin dlq replay"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	messageID := fs.String("message-id", "", "Message Identifier (required)")
	resolution := fs.String("deduplication-conflict-resolution", "reject", "reject or keep_current")
	yes := fs.Bool("yes", false, "confirm the audited replay (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	var err error
	switch {
	case *messageID == "":
		err = errors.New("--message-id is required")
	case *resolution != "reject" && *resolution != "keep_current":
		err = errors.New("--deduplication-conflict-resolution must be reject or keep_current")
	case !*yes:
		err = errors.New("--yes is required: replay is an audited mutation")
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

	// The current cycle is read first so a lost response can be reconciled
	// against it.
	before, err := client.getDeadLetter(*messageID)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	code, data, err := client.do(http.MethodPost, "/admin/v1/dead-letters/"+url.PathEscape(*messageID)+"/replay",
		map[string]string{"deduplication_conflict_resolution": *resolution})
	switch {
	case err != nil:
		return client.reconcileReplay(*messageID, before.DeliveryCycle, err.Error())
	case code == http.StatusOK:
		var r replayResult
		if err := json.Unmarshal(data, &r); err != nil {
			return client.reconcileReplay(*messageID, before.DeliveryCycle, "the success response was unreadable")
		}
		r.Outcome, r.MessageID, r.PreviousDeliveryCycle = "replayed", *messageID, before.DeliveryCycle
		if writeJSONResult(env.Stdout, r) != nil {
			return ExitError
		}
		return ExitOK
	case code >= 500:
		return client.reconcileReplay(*messageID, before.DeliveryCycle, decodeAPIError(code, data).Error())
	default:
		// A 4xx refusal is definite: the replay did not run.
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
}

// reconcileReplay handles a replay whose outcome is unknown. It never
// retries: it reads the message's delivery state once and reports what it
// observed. A newer Delivery Cycle (queued, active, acknowledged, or
// dead-lettered again) is the desired state; the command still fails in
// every case, because no observation proves that this request replayed the
// message and appended the mandatory audit event.
func (c *adminClient) reconcileReplay(messageID string, previousCycle int64, cause string) int {
	const name = "hookrelay admin dlq replay"
	fmt.Fprintf(c.env.Stderr, "%s: outcome uncertain for %s: %s\n", name, messageID, cause)
	fmt.Fprintf(c.env.Stderr, "%s: not retrying; reading the delivery state to reconcile\n", name)
	result := replayResult{Outcome: outcomeUncertain, MessageID: messageID, PreviousDeliveryCycle: previousCycle}
	st, err := c.getDeliveryState(messageID)
	var ae *apiError
	switch {
	case err == nil && st.DeliveryCycle > previousCycle:
		result.Outcome = outcomeDesiredStateObserved
		result.DeliveryState = st
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is %s in delivery cycle %d, so a replay ran, but this does NOT confirm that this request "+
			"performed it or that the mandatory audit event was written. Check the audit before any further mutation.\n", messageID, st.State, st.DeliveryCycle)
	case err == nil:
		result.DeliveryState = st
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is %s in delivery cycle %d; the replay has not been observed but may still be in flight. "+
			"Check the audit and read the delivery state again before any retry. Do not retry blindly.\n", messageID, st.State, st.DeliveryCycle)
	case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
		fmt.Fprintf(c.env.Stderr, "WARNING: no delivery state is retained for %s (it may have been replayed and acknowledged longer ago than "+
			"the success retention); check the audit for dead_letter_replayed before any further mutation. Do not retry the replay.\n", messageID)
	default:
		fmt.Fprintf(c.env.Stderr, "WARNING: the delivery state of %s could not be read (%v); follow the reconciliation runbook. Do not retry the replay.\n", messageID, err)
	}
	_ = writeJSONResult(c.env.Stdout, result)
	return ExitError
}

// deliveryState is the delivery-state response.
type deliveryState struct {
	MessageID     string `json:"message_id"`
	DeliveryCycle int64  `json:"delivery_cycle"`
	State         string `json:"state"`
	QueuePosition string `json:"queue_position,omitempty"`
}

func (c *adminClient) getDeliveryState(messageID string) (*deliveryState, error) {
	code, data, err := c.do(http.MethodGet, "/admin/v1/messages/"+url.PathEscape(messageID)+"/delivery-state", nil)
	if err != nil {
		return nil, err
	}
	if code != http.StatusOK {
		return nil, decodeAPIError(code, data)
	}
	var st deliveryState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("unreadable delivery-state response: %w", err)
	}
	return &st, nil
}

func runMessage(args []string, env adminIO) int {
	if len(args) < 1 || args[0] != "delivery-state" {
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
	const name = "hookrelay admin message delivery-state"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	messageID := fs.String("message-id", "", "Message Identifier (required)")
	if err := fs.Parse(args[1:]); err != nil {
		return ExitUsage
	}
	if *messageID == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --message-id is required\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	st, err := client.getDeliveryState(*messageID)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if client.format == "json" {
		if writeJSONResult(env.Stdout, st) != nil {
			return ExitError
		}
		return ExitOK
	}
	tw := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "message_id\t%s\nstate\t%s\ndelivery_cycle\t%d\n", st.MessageID, st.State, st.DeliveryCycle)
	if st.QueuePosition != "" {
		fmt.Fprintf(tw, "queue_position\t%s\n", st.QueuePosition)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

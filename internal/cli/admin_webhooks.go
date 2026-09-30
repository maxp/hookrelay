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

// webhookPage is one endpoint list page as returned by the API.
type webhookPage struct {
	Items      []webhookResponse `json:"items"`
	NextCursor string            `json:"next_cursor,omitempty"`
}

func adminWebhookList(args []string, env adminIO) int {
	const name = "hookrelay admin webhook list"
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
	path := "/admin/v1/webhooks"
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	return client.printWebhookPage(name, path)
}

// printWebhookPage reads one endpoint collection and prints it.
func (c *adminClient) printWebhookPage(name, path string) int {
	code, data, err := c.do(http.MethodGet, path, nil)
	if err != nil {
		fmt.Fprintf(c.env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if code != http.StatusOK {
		fmt.Fprintf(c.env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
	var page webhookPage
	if err := json.Unmarshal(data, &page); err != nil {
		fmt.Fprintf(c.env.Stderr, "%s: unreadable response: %v\n", name, err)
		return ExitError
	}
	if c.format == "json" {
		if writeJSONResult(c.env.Stdout, page) != nil {
			return ExitError
		}
		return ExitOK
	}
	tw := tabwriter.NewWriter(c.env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "WEBHOOK_TYPE\tWEBHOOK_IDENTIFIER\tBOT_PLATFORM\tBOT_ID\tENABLED\tCREDENTIAL_KIND\tCONFIG_VERSION\tCREATED_MS")
	for _, e := range page.Items {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%t\t%s\t%d\t%d\n", e.WebhookType, e.WebhookIdentifier, e.BotPlatform, e.BotID,
			e.Enabled, e.Credential.Kind, e.ConfigVersion, e.CreatedMs)
	}
	if page.NextCursor != "" {
		fmt.Fprintf(tw, "next_cursor\t%s\n", page.NextCursor)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

// endpointFlags parses --type, --identifier, and the mandatory --yes of an
// endpoint mutation.
func endpointFlags(name string, args []string, env adminIO) (client *adminClient, webhookType, identifier string, code int) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	t := fs.String("type", "", "Webhook Type (required)")
	id := fs.String("identifier", "", "Webhook Identifier (required)")
	yes := fs.Bool("yes", false, "confirm the audited mutation (required)")
	if err := fs.Parse(args); err != nil {
		return nil, "", "", ExitUsage
	}
	switch {
	case *t == "" || *id == "" || fs.NArg() > 0:
		fmt.Fprintf(env.Stderr, "%s: --type and --identifier are required\n", name)
		return nil, "", "", ExitUsage
	case !*yes:
		fmt.Fprintf(env.Stderr, "%s: --yes is required: this is an audited mutation\n", name)
		return nil, "", "", ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return nil, "", "", ExitUsage
	}
	return client, *t, *id, ExitOK
}

// adminWebhookSetEnabled reads the endpoint and its ETag, then PATCHes the
// enabled flag under If-Match. A stale ETag is reported, never retried.
func adminWebhookSetEnabled(args []string, env adminIO, enabled bool) int {
	name := "hookrelay admin webhook " + map[bool]string{true: "enable", false: "disable"}[enabled]
	client, webhookType, identifier, code := endpointFlags(name, args, env)
	if code != ExitOK {
		return code
	}
	read, etag, err := client.getWebhookTagged(webhookType, identifier)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	status, _, data, err := client.doHeaders(http.MethodPatch, webhookPath(webhookType, identifier),
		map[string]bool{"enabled": enabled}, map[string]string{"If-Match": etag})
	switch {
	case err != nil:
		return client.reconcileSetEnabled(name, read, enabled, fmt.Sprintf("the request failed: %v", err))
	case status == http.StatusOK:
		var w webhookResponse
		if err := json.Unmarshal(data, &w); err != nil {
			return client.reconcileSetEnabled(name, read, enabled, "the success response was unreadable")
		}
		if w.ConfigVersion == read.ConfigVersion {
			fmt.Fprintf(env.Stderr, "%s: the endpoint was already %s; nothing changed\n", name, map[bool]string{true: "enabled", false: "disabled"}[enabled])
		}
		if client.printEndpoint(&w) != nil {
			return ExitError
		}
		return ExitOK
	case status >= 500:
		return client.reconcileSetEnabled(name, read, enabled, fmt.Sprintf("the server reported %v", decodeAPIError(status, data)))
	case status == http.StatusPreconditionFailed:
		fmt.Fprintf(env.Stderr, "%s: %v; the endpoint changed since it was read — not retrying, read it again\n", name, decodeAPIError(status, data))
		return ExitError
	default:
		// A 4xx refusal is definite: nothing was changed.
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(status, data))
		return ExitError
	}
}

// reconcileSetEnabled handles an enable/disable whose outcome is unknown.
// It never retries: it re-reads the endpoint and reports whether the
// desired flag is observed in the same generation. That does not prove
// this request changed it or that its audit event was written.
func (c *adminClient) reconcileSetEnabled(name string, read *webhookResponse, enabled bool, cause string) int {
	target := read.WebhookType + ":" + read.WebhookIdentifier
	fmt.Fprintf(c.env.Stderr, "%s: outcome uncertain for %s: %s\n", name, target, cause)
	fmt.Fprintf(c.env.Stderr, "%s: not retrying; reading %s to reconcile\n", name, target)
	result := uncertainResult{Outcome: outcomeUncertain, WebhookType: read.WebhookType, WebhookIdentifier: read.WebhookIdentifier}
	w, err := c.getWebhook(read.WebhookType, read.WebhookIdentifier)
	switch {
	case err == nil && w.GenerationID == read.GenerationID && w.Enabled == enabled && (w.ConfigVersion > read.ConfigVersion || read.Enabled == enabled):
		result.Outcome, result.Endpoint = outcomeDesiredStateObserved, w
		fmt.Fprintf(c.env.Stderr, "WARNING: %s shows enabled=%t, but this does NOT confirm that this request changed it or that the mandatory audit event was written. "+
			"Check the audit before any further mutation.\n", target, enabled)
	case err == nil:
		result.Endpoint = w
		fmt.Fprintf(c.env.Stderr, "WARNING: %s does not show the requested state in the generation that was read; operator reconciliation is required. Do not retry blindly.\n", target)
	default:
		fmt.Fprintf(c.env.Stderr, "WARNING: %s could not be read (%v); operator reconciliation is required. Do not retry blindly.\n", target, err)
	}
	_ = c.printUncertain(result)
	return ExitError
}

// outcomeDeleted is the result of a confirmed deletion.
const outcomeDeleted = "deleted"

// adminWebhookDelete reads the endpoint and its ETag, then deletes it
// under If-Match. The server refuses an enabled endpoint.
func adminWebhookDelete(args []string, env adminIO) int {
	const name = "hookrelay admin webhook delete"
	client, webhookType, identifier, code := endpointFlags(name, args, env)
	if code != ExitOK {
		return code
	}
	_, etag, err := client.getWebhookTagged(webhookType, identifier)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	status, _, data, err := client.doHeaders(http.MethodDelete, webhookPath(webhookType, identifier), nil, map[string]string{"If-Match": etag})
	switch {
	case err != nil:
		return client.reconcileDelete(name, webhookType, identifier, fmt.Sprintf("the request failed: %v", err))
	case status == http.StatusNoContent:
		if client.printUncertain(uncertainResult{Outcome: outcomeDeleted, WebhookType: webhookType, WebhookIdentifier: identifier}) != nil {
			return ExitError
		}
		return ExitOK
	case status >= 500:
		return client.reconcileDelete(name, webhookType, identifier, fmt.Sprintf("the server reported %v", decodeAPIError(status, data)))
	case status == http.StatusPreconditionFailed:
		fmt.Fprintf(env.Stderr, "%s: %v; the endpoint changed since it was read — not retrying, read it again\n", name, decodeAPIError(status, data))
		return ExitError
	default:
		apiErr := decodeAPIError(status, data)
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, apiErr)
		if apiErr.Code == "endpoint_must_be_disabled" {
			fmt.Fprintf(env.Stderr, "%s: run hookrelay admin webhook disable --type %s --identifier %s --yes first\n", name, webhookType, identifier)
		}
		return ExitError
	}
}

// reconcileDelete handles a deletion whose outcome is unknown. It never
// retries: observed absence does not prove that this request deleted the
// endpoint or that its mandatory audit event was written.
func (c *adminClient) reconcileDelete(name, webhookType, identifier, cause string) int {
	target := webhookType + ":" + identifier
	fmt.Fprintf(c.env.Stderr, "%s: outcome uncertain for %s: %s\n", name, target, cause)
	fmt.Fprintf(c.env.Stderr, "%s: not retrying; reading %s to reconcile\n", name, target)
	result := uncertainResult{Outcome: outcomeUncertain, WebhookType: webhookType, WebhookIdentifier: identifier}
	w, err := c.getWebhook(webhookType, identifier)
	var ae *apiError
	switch {
	case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
		result.Outcome = outcomeDesiredStateObserved
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is absent, but this does NOT confirm that this request deleted it or that the mandatory audit event was written. "+
			"Check the audit before any further mutation.\n", target)
	case err == nil:
		result.Endpoint = w
		fmt.Fprintf(c.env.Stderr, "WARNING: %s still exists; operator reconciliation is required. Do not retry blindly.\n", target)
	default:
		fmt.Fprintf(c.env.Stderr, "WARNING: %s could not be read (%v); operator reconciliation is required. Do not retry blindly.\n", target, err)
	}
	_ = c.printUncertain(result)
	return ExitError
}

// adminBotWebhooks lists every endpoint of one Bot Identity.
func adminBotWebhooks(args []string, env adminIO) int {
	const name = "hookrelay admin bot webhooks"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	platform := fs.String("platform", "", "Bot Platform (required)")
	botID := fs.String("bot-id", "", "Bot Identifier (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *platform == "" || *botID == "" || fs.NArg() > 0 {
		fmt.Fprintf(env.Stderr, "%s: --platform and --bot-id are required\n", name)
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitUsage
	}
	return client.printWebhookPage(name, "/admin/v1/bots/"+url.PathEscape(*platform)+"/"+url.PathEscape(*botID)+"/webhooks")
}

package cli

import (
	"encoding/json"
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

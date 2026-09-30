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

// auditEntry is one administrative audit event as returned by the API.
type auditEntry struct {
	StreamID    string `json:"stream_id"`
	EventID     string `json:"event_id,omitempty"`
	TimestampMs int64  `json:"timestamp_ms,omitempty"`
	Actor       string `json:"actor,omitempty"`
	Operation   string `json:"operation,omitempty"`
	Target      string `json:"target,omitempty"`
	RequestID   string `json:"request_id,omitempty"`
	Outcome     string `json:"outcome,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

type auditPage struct {
	Items      []auditEntry `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

// adminAuditList prints one page of the administrative audit, newest first.
func adminAuditList(args []string, env adminIO) int {
	const name = "hookrelay admin audit list"
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
	path := "/admin/v1/audit"
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
	var page auditPage
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
	fmt.Fprintln(tw, "TIMESTAMP_MS\tACTOR\tOPERATION\tTARGET\tOUTCOME\tREASON\tREQUEST_ID\tEVENT_ID")
	for _, e := range page.Items {
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", e.TimestampMs, e.Actor, e.Operation, e.Target, e.Outcome, e.Reason, e.RequestID, e.EventID)
	}
	if page.NextCursor != "" {
		fmt.Fprintf(tw, "next_cursor\t%s\n", page.NextCursor)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

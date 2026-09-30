package cli

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"text/tabwriter"
)

// operationsSummary is the operations summary as returned by the API.
type operationsSummary struct {
	GeneratedMs int64 `json:"generated_ms"`
	Readiness   *struct {
		Ready                 bool   `json:"ready"`
		AcceptingWebhooks     bool   `json:"accepting_webhooks"`
		StartupReconciliation string `json:"startup_reconciliation"`
	} `json:"readiness,omitempty"`
	Valkey struct {
		UsedMemoryBytes int64 `json:"used_memory_bytes"`
		MaxMemoryBytes  int64 `json:"maxmemory_bytes"`
	} `json:"valkey"`
	Queues struct {
		QueuedMessages         int64 `json:"queued_messages"`
		ReadyRecipients        int64 `json:"ready_recipients"`
		LeasedRecipients       int64 `json:"leased_recipients"`
		RetryWaitRecipients    int64 `json:"retry_wait_recipients"`
		BlockedRecipients      int64 `json:"blocked_recipients"`
		EarliestLeaseExpiresMs int64 `json:"earliest_lease_expires_ms,omitempty"`
		EarliestRetryAtMs      int64 `json:"earliest_retry_at_ms,omitempty"`
	} `json:"queues"`
	DeadLetters struct {
		Count                int64 `json:"count"`
		OldestDeadLetteredMs int64 `json:"oldest_dead_lettered_ms,omitempty"`
		NewestDeadLetteredMs int64 `json:"newest_dead_lettered_ms,omitempty"`
	} `json:"dead_letters"`
	WebhookEndpoints struct {
		Count int64 `json:"count"`
	} `json:"webhook_endpoints"`
	AdminSessions struct {
		Indexed int64 `json:"indexed"`
	} `json:"admin_sessions"`
	Audit struct {
		Length int64 `json:"length"`
	} `json:"audit"`
	Links struct {
		GrafanaURL string `json:"grafana_url,omitempty"`
	} `json:"links"`
}

// adminOperationsSummary prints the operations summary.
func adminOperationsSummary(args []string, env adminIO) int {
	const name = "hookrelay admin operations summary"
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
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
	code, data, err := client.do(http.MethodGet, "/admin/v1/operations/summary", nil)
	if err != nil {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, err)
		return ExitError
	}
	if code != http.StatusOK {
		fmt.Fprintf(env.Stderr, "%s: %v\n", name, decodeAPIError(code, data))
		return ExitError
	}
	var s operationsSummary
	if err := json.Unmarshal(data, &s); err != nil {
		fmt.Fprintf(env.Stderr, "%s: unreadable response: %v\n", name, err)
		return ExitError
	}
	if client.format == "json" {
		if writeJSONResult(env.Stdout, s) != nil {
			return ExitError
		}
		return ExitOK
	}
	tw := tabwriter.NewWriter(env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "generated_ms\t%d\n", s.GeneratedMs)
	if r := s.Readiness; r != nil {
		fmt.Fprintf(tw, "ready\t%t\naccepting_webhooks\t%t\nstartup_reconciliation\t%s\n", r.Ready, r.AcceptingWebhooks, r.StartupReconciliation)
	}
	fmt.Fprintf(tw, "valkey_used_memory_bytes\t%d\nvalkey_maxmemory_bytes\t%d\n", s.Valkey.UsedMemoryBytes, s.Valkey.MaxMemoryBytes)
	q := s.Queues
	fmt.Fprintf(tw, "queued_messages\t%d\nready_recipients\t%d\nleased_recipients\t%d\nretry_wait_recipients\t%d\nblocked_recipients\t%d\n",
		q.QueuedMessages, q.ReadyRecipients, q.LeasedRecipients, q.RetryWaitRecipients, q.BlockedRecipients)
	if q.EarliestLeaseExpiresMs != 0 {
		fmt.Fprintf(tw, "earliest_lease_expires_ms\t%d\n", q.EarliestLeaseExpiresMs)
	}
	if q.EarliestRetryAtMs != 0 {
		fmt.Fprintf(tw, "earliest_retry_at_ms\t%d\n", q.EarliestRetryAtMs)
	}
	fmt.Fprintf(tw, "dead_letters\t%d\n", s.DeadLetters.Count)
	if s.DeadLetters.Count > 0 {
		fmt.Fprintf(tw, "oldest_dead_lettered_ms\t%d\nnewest_dead_lettered_ms\t%d\n", s.DeadLetters.OldestDeadLetteredMs, s.DeadLetters.NewestDeadLetteredMs)
	}
	fmt.Fprintf(tw, "webhook_endpoints\t%d\nadmin_sessions_indexed\t%d\naudit_length\t%d\n", s.WebhookEndpoints.Count, s.AdminSessions.Indexed, s.Audit.Length)
	if s.Links.GrafanaURL != "" {
		fmt.Fprintf(tw, "grafana_url\t%s\n", s.Links.GrafanaURL)
	}
	if tw.Flush() != nil {
		return ExitError
	}
	return ExitOK
}

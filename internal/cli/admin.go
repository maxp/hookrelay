package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/term"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/ingestion"
)

// defaultAdminURL is the loopback administrative listener.
const defaultAdminURL = "http://127.0.0.1:8081"

// adminRequestTimeout bounds one Admin API request.
const adminRequestTimeout = 30 * time.Second

// adminIO carries the process environment of the admin client so tests can
// substitute every input and output.
type adminIO struct {
	Getenv      func(string) string
	ReadFile    func(string) ([]byte, error)
	Stdout      io.Writer
	Stderr      io.Writer
	StdinIsTTY  bool
	StdoutIsTTY bool
	// ReadSecret reads the Admin Secret from the terminal without echo.
	ReadSecret func() (string, error)
	// NewWebhookID generates the client-side wh_ identifier.
	NewWebhookID func() (string, error)
	HTTPClient   *http.Client
}

func osAdminIO() adminIO {
	return adminIO{
		Getenv:      os.Getenv,
		ReadFile:    os.ReadFile,
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
		StdinIsTTY:  term.IsTerminal(int(os.Stdin.Fd())),
		StdoutIsTTY: term.IsTerminal(int(os.Stdout.Fd())),
		ReadSecret: func() (string, error) {
			fmt.Fprint(os.Stderr, "Admin secret: ")
			b, err := term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(os.Stderr)
			return string(b), err
		},
		NewWebhookID: randomWebhookID,
		HTTPClient:   &http.Client{Timeout: adminRequestTimeout},
	}
}

// Admin runs the administrative HTTP client. It speaks only to the Admin
// API and never to Valkey.
func Admin(args []string) int {
	return runAdmin(args, osAdminIO())
}

const adminUsage = `usage:
  hookrelay admin webhook create --type <webhook_type> --bot-id <bot_id> [--identifier <id>]
      [--credential-kind <kind>] [--credential-file <path>] [--disabled] [common flags]
  hookrelay admin webhook get --type <webhook_type> --identifier <id> [common flags]
  hookrelay admin webhook list [--limit <n>] [--cursor <c>] [common flags]
  hookrelay admin webhook enable|disable|delete --type <webhook_type> --identifier <id> --yes [common flags]
  hookrelay admin recipients list --status <ready|leased|retry_wait|blocked> [--limit <n>] [--cursor <c>] [common flags]
  hookrelay admin recipients inspect-block <recipient> [common flags]
  hookrelay admin recipients clear-block <recipient> --expected-detected-ms <ms>
      --expected-reason-code <code> --yes [common flags]
  hookrelay admin dlq list [--limit <n>] [--cursor <c>] [common flags]
  hookrelay admin dlq get --message-id <id> [common flags]
  hookrelay admin dlq replay --message-id <id> [--deduplication-conflict-resolution reject|keep_current]
      --yes [common flags]
  hookrelay admin message delivery-state --message-id <id> [common flags]

recipient: --bot-platform <p> --bot-id <id> --scope <chat|user|bot|relay> [--chat-id <id> | --user-id <id>]

common flags:
  --admin-url <url>          Admin API base URL (HOOKRELAY_ADMIN_URL, default ` + defaultAdminURL + `)
  --admin-secret-file <path> Admin Secret file (then HOOKRELAY_ADMIN_SECRET_FILE,
                             HOOKRELAY_ADMIN_SECRET, then a hidden prompt on a terminal)
  --output table|json        result format (table on a terminal, json otherwise)

The webhook credential comes from --credential-file or HOOKRELAY_WEBHOOK_CREDENTIAL.
`

func runAdmin(args []string, env adminIO) int {
	if len(args) >= 1 && args[0] == "recipients" {
		return runRecipients(args[1:], env)
	}
	if len(args) >= 1 && args[0] == "dlq" {
		return runDLQ(args[1:], env)
	}
	if len(args) >= 1 && args[0] == "message" {
		return runMessage(args[1:], env)
	}
	if len(args) < 2 || args[0] != "webhook" {
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
	switch args[1] {
	case "create":
		return adminWebhookCreate(args[2:], env)
	case "get":
		return adminWebhookGet(args[2:], env)
	case "list":
		return adminWebhookList(args[2:], env)
	case "enable":
		return adminWebhookSetEnabled(args[2:], env, true)
	case "disable":
		return adminWebhookSetEnabled(args[2:], env, false)
	case "delete":
		return adminWebhookDelete(args[2:], env)
	default:
		fmt.Fprintf(env.Stderr, "hookrelay admin: unknown webhook command %q\n", args[1])
		fmt.Fprint(env.Stderr, adminUsage)
		return ExitUsage
	}
}

// commonFlags are shared by every admin command.
type commonFlags struct {
	adminURL        string
	adminSecretFile string
	output          string
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.adminURL, "admin-url", "", "Admin API base URL")
	fs.StringVar(&c.adminSecretFile, "admin-secret-file", "", "file holding the Admin Secret")
	fs.StringVar(&c.output, "output", "", "output format: table or json")
}

// adminClient is a resolved, ready-to-use Admin API client.
type adminClient struct {
	baseURL *url.URL
	secret  string
	format  string
	env     adminIO
}

// resolve applies the documented precedence for URL, secret, and format.
// Every failure here is a usage error.
func (c *commonFlags) resolve(env adminIO) (*adminClient, error) {
	format := c.output
	if format == "" {
		format = "json"
		if env.StdoutIsTTY {
			format = "table"
		}
	}
	if format != "table" && format != "json" {
		return nil, fmt.Errorf("--output must be table or json")
	}

	raw := c.adminURL
	if raw == "" {
		raw = env.Getenv("HOOKRELAY_ADMIN_URL")
	}
	if raw == "" {
		raw = defaultAdminURL
	}
	base, err := url.Parse(raw)
	if err != nil || (base.Scheme != "http" && base.Scheme != "https") || base.Host == "" {
		return nil, fmt.Errorf("admin URL must be an absolute http or https URL")
	}

	secret, err := c.resolveSecret(env)
	if err != nil {
		return nil, err
	}
	return &adminClient{baseURL: base, secret: secret, format: format, env: env}, nil
}

// resolveSecret: --admin-secret-file → HOOKRELAY_ADMIN_SECRET_FILE →
// HOOKRELAY_ADMIN_SECRET → hidden prompt on a terminal. The secret is never
// accepted as a command-line value.
func (c *commonFlags) resolveSecret(env adminIO) (string, error) {
	path := c.adminSecretFile
	if path == "" {
		path = env.Getenv("HOOKRELAY_ADMIN_SECRET_FILE")
	}
	var secret string
	switch {
	case path != "":
		data, err := env.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read admin secret file: %w", err)
		}
		secret = strings.TrimSuffix(string(data), "\n")
	case env.Getenv("HOOKRELAY_ADMIN_SECRET") != "":
		secret = env.Getenv("HOOKRELAY_ADMIN_SECRET")
	case env.StdinIsTTY && env.ReadSecret != nil:
		s, err := env.ReadSecret()
		if err != nil {
			return "", fmt.Errorf("read admin secret: %w", err)
		}
		secret = s
	default:
		return "", fmt.Errorf("no Admin Secret: use --admin-secret-file, HOOKRELAY_ADMIN_SECRET_FILE, or HOOKRELAY_ADMIN_SECRET")
	}
	if secret == "" {
		return "", fmt.Errorf("the Admin Secret is empty")
	}
	return secret, nil
}

// webhookResponse is the safe endpoint representation returned by the API.
type webhookResponse struct {
	WebhookType       string `json:"webhook_type"`
	WebhookIdentifier string `json:"webhook_identifier"`
	BotPlatform       string `json:"bot_platform"`
	BotID             string `json:"bot_id"`
	Enabled           bool   `json:"enabled"`
	Credential        struct {
		Kind       string `json:"kind"`
		Configured bool   `json:"configured"`
	} `json:"credential"`
	GenerationID  string `json:"generation_id"`
	ConfigVersion int64  `json:"config_version"`
	CreatedMs     int64  `json:"created_ms"`
	UpdatedMs     int64  `json:"updated_ms"`
	WebhookPath   string `json:"webhook_path"`
}

// apiError is the bounded Admin API error envelope.
type apiError struct {
	Status    int
	Code      string
	Message   string
	RequestID string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s: %s", e.Code, e.Message)
	if e.RequestID != "" {
		msg += fmt.Sprintf(" (request_id %s)", e.RequestID)
	}
	return msg
}

// do sends one request and returns the status and body. A returned error is
// transport-level: the request may or may not have reached the server.
func (c *adminClient) do(method, path string, body any) (int, []byte, error) {
	status, _, data, err := c.doHeaders(method, path, body, nil)
	return status, data, err
}

// doHeaders is do with extra request headers, also returning the response
// headers.
func (c *adminClient) doHeaders(method, path string, body any, headers map[string]string) (int, http.Header, []byte, error) {
	var rd io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, nil, err
		}
		rd = bytes.NewReader(data)
	}
	rawQuery := ""
	if i := strings.IndexByte(path, '?'); i >= 0 {
		path, rawQuery = path[:i], path[i+1:]
	}
	target := c.baseURL.JoinPath(path)
	target.RawQuery = rawQuery
	req, err := http.NewRequestWithContext(context.Background(), method, target.String(), rd)
	if err != nil {
		return 0, nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Authorization", "Bearer "+c.secret)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.env.HTTPClient.Do(req)
	if err != nil {
		return 0, nil, nil, safeTransportError(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return resp.StatusCode, resp.Header, nil, fmt.Errorf("read response: %w", err)
	}
	return resp.StatusCode, resp.Header, data, nil
}

// safeTransportError drops the request URL (which could carry userinfo) and
// keeps only the operation and the underlying cause.
func safeTransportError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s request failed: %w", ue.Op, ue.Err)
	}
	return err
}

func decodeAPIError(status int, data []byte) *apiError {
	var env struct {
		Error struct {
			Code      string `json:"code"`
			Message   string `json:"message"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err != nil || env.Error.Code == "" {
		return &apiError{Status: status, Code: "unexpected_response", Message: fmt.Sprintf("HTTP %d", status)}
	}
	return &apiError{Status: status, Code: env.Error.Code, Message: env.Error.Message, RequestID: env.Error.RequestID}
}

// getWebhook reads one endpoint. It returns (nil, apiError 404) for a
// missing endpoint and a plain error for transport or shape failures.
func (c *adminClient) getWebhook(webhookType, identifier string) (*webhookResponse, error) {
	w, _, err := c.getWebhookTagged(webhookType, identifier)
	return w, err
}

// getWebhookTagged is getWebhook that also returns the entity ETag.
func (c *adminClient) getWebhookTagged(webhookType, identifier string) (*webhookResponse, string, error) {
	status, headers, data, err := c.doHeaders(http.MethodGet, webhookPath(webhookType, identifier), nil, nil)
	if err != nil {
		return nil, "", err
	}
	if status != http.StatusOK {
		return nil, "", decodeAPIError(status, data)
	}
	var w webhookResponse
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, "", fmt.Errorf("unreadable endpoint response: %w", err)
	}
	return &w, headers.Get("ETag"), nil
}

func webhookPath(webhookType, identifier string) string {
	return "/admin/v1/webhooks/" + url.PathEscape(webhookType) + "/" + url.PathEscape(identifier)
}

func adminWebhookGet(args []string, env adminIO) int {
	fs := flag.NewFlagSet("hookrelay admin webhook get", flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	webhookType := fs.String("type", "", "Webhook Type (required)")
	identifier := fs.String("identifier", "", "Webhook Identifier (required)")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	if *webhookType == "" || *identifier == "" || fs.NArg() > 0 {
		fmt.Fprintln(env.Stderr, "hookrelay admin webhook get: --type and --identifier are required")
		return ExitUsage
	}
	client, err := common.resolve(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "hookrelay admin webhook get: %v\n", err)
		return ExitUsage
	}

	w, err := client.getWebhook(*webhookType, *identifier)
	if err != nil {
		fmt.Fprintf(env.Stderr, "hookrelay admin webhook get: %v\n", err)
		return ExitError
	}
	if err := client.printEndpoint(w); err != nil {
		return ExitError
	}
	return ExitOK
}

func adminWebhookCreate(args []string, env adminIO) int {
	fs := flag.NewFlagSet("hookrelay admin webhook create", flag.ContinueOnError)
	fs.SetOutput(env.Stderr)
	var common commonFlags
	common.register(fs)
	webhookType := fs.String("type", "", "Webhook Type (required)")
	botID := fs.String("bot-id", "", "Bot Identifier (required)")
	identifier := fs.String("identifier", "", "Webhook Identifier (generated when omitted)")
	credentialKind := fs.String("credential-kind", "", "credential kind (defaults to the only kind of the Webhook Type)")
	credentialFile := fs.String("credential-file", "", "file holding the webhook credential")
	disabled := fs.Bool("disabled", false, "create the endpoint disabled")
	if err := fs.Parse(args); err != nil {
		return ExitUsage
	}
	usageErr := func(format string, a ...any) int {
		fmt.Fprintf(env.Stderr, "hookrelay admin webhook create: "+format+"\n", a...)
		return ExitUsage
	}
	if *webhookType == "" || *botID == "" || fs.NArg() > 0 {
		return usageErr("--type and --bot-id are required")
	}
	kind := *credentialKind
	if kind == "" {
		d, ok := types.Lookup(ingestion.WebhookType(*webhookType))
		if !ok || len(d.CredentialKinds) != 1 {
			return usageErr("--credential-kind is required for webhook type %q", *webhookType)
		}
		kind = d.CredentialKinds[0]
	}
	credential, err := resolveCredential(*credentialFile, env)
	if err != nil {
		return usageErr("%v", err)
	}
	client, err := common.resolve(env)
	if err != nil {
		return usageErr("%v", err)
	}

	// The identifier is fixed before the request so a lost response can be
	// reconciled by reading this exact identity. It is never regenerated.
	id := *identifier
	if id == "" {
		if id, err = env.NewWebhookID(); err != nil {
			fmt.Fprintf(env.Stderr, "hookrelay admin webhook create: %v\n", err)
			return ExitError
		}
	}
	enabled := !*disabled
	req := administration.CreateRequest{
		WebhookType:       *webhookType,
		WebhookIdentifier: id,
		BotID:             *botID,
		Credential:        administration.CredentialInput{Kind: kind, Value: credential},
		Enabled:           &enabled,
	}

	status, data, err := client.do(http.MethodPost, "/admin/v1/webhooks", req)
	switch {
	case err != nil:
		return client.reconcileCreate(req, fmt.Sprintf("the request failed: %v", err))
	case status == http.StatusCreated:
		var w webhookResponse
		if err := json.Unmarshal(data, &w); err != nil {
			return client.reconcileCreate(req, "the success response was unreadable")
		}
		if err := client.printEndpoint(&w); err != nil {
			return ExitError
		}
		return ExitOK
	case status >= 500:
		return client.reconcileCreate(req, fmt.Sprintf("the server reported %v", decodeAPIError(status, data)))
	default:
		// A 4xx refusal is definite: the create was rejected before any
		// mutation.
		fmt.Fprintf(env.Stderr, "hookrelay admin webhook create: %v\n", decodeAPIError(status, data))
		return ExitError
	}
}

// resolveCredential reads the webhook credential from --credential-file or
// HOOKRELAY_WEBHOOK_CREDENTIAL; configuring both is an error. There is no
// command-line value flag.
func resolveCredential(path string, env adminIO) (string, error) {
	fromEnv := env.Getenv("HOOKRELAY_WEBHOOK_CREDENTIAL")
	switch {
	case path != "" && fromEnv != "":
		return "", fmt.Errorf("configure the credential with --credential-file or HOOKRELAY_WEBHOOK_CREDENTIAL, not both")
	case path != "":
		data, err := env.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("read credential file: %w", err)
		}
		value := strings.TrimSuffix(string(data), "\n")
		if value == "" {
			return "", fmt.Errorf("the credential file is empty")
		}
		return value, nil
	case fromEnv != "":
		return fromEnv, nil
	default:
		return "", fmt.Errorf("a credential is required: use --credential-file or HOOKRELAY_WEBHOOK_CREDENTIAL")
	}
}

// Create outcomes that could not be confirmed.
const (
	outcomeDesiredStateObserved = "desired_state_observed"
	outcomeUncertain            = "uncertain"
)

// uncertainResult is the stdout result of an unconfirmed create.
type uncertainResult struct {
	Outcome           string           `json:"outcome"`
	WebhookType       string           `json:"webhook_type"`
	WebhookIdentifier string           `json:"webhook_identifier"`
	Endpoint          *webhookResponse `json:"endpoint,omitempty"`
}

// reconcileCreate handles a create whose outcome is unknown. It never
// retries the mutation: it reads the endpoint by its known identity and
// reports what it observed. Observing the requested state does not prove
// that this request created it, that the stored credential matches, or that
// the mandatory audit event was appended, so the command still fails.
func (c *adminClient) reconcileCreate(req administration.CreateRequest, cause string) int {
	target := req.WebhookType + ":" + req.WebhookIdentifier
	fmt.Fprintf(c.env.Stderr, "hookrelay admin webhook create: outcome uncertain for %s: %s\n", target, cause)
	fmt.Fprintf(c.env.Stderr, "hookrelay admin webhook create: not retrying; reading %s to reconcile\n", target)

	result := uncertainResult{Outcome: outcomeUncertain, WebhookType: req.WebhookType, WebhookIdentifier: req.WebhookIdentifier}
	w, err := c.getWebhook(req.WebhookType, req.WebhookIdentifier)
	var ae *apiError
	switch {
	case err == nil && matchesCreate(w, req):
		result.Outcome = outcomeDesiredStateObserved
		result.Endpoint = w
		fmt.Fprintf(c.env.Stderr, "WARNING: %s exists with the requested settings, but this does NOT confirm the create: "+
			"it cannot prove that this request performed it, that the stored credential matches, or that the mandatory audit event was written. "+
			"Check the audit before any further mutation.\n", target)
	case err == nil:
		result.Endpoint = w
		fmt.Fprintf(c.env.Stderr, "WARNING: %s exists but differs from the request; operator reconciliation is required. Do not retry the create.\n", target)
	case errors.As(err, &ae) && ae.Status == http.StatusNotFound:
		fmt.Fprintf(c.env.Stderr, "WARNING: %s is not present. The create may still have partially run; operator reconciliation is required before any retry, "+
			"which must reuse --identifier %s.\n", target, req.WebhookIdentifier)
	default:
		fmt.Fprintf(c.env.Stderr, "WARNING: %s could not be read (%v); operator reconciliation is required. Do not retry the create.\n", target, err)
	}
	_ = c.printUncertain(result)
	return ExitError
}

// matchesCreate compares the safe observable state with the request. The
// credential value and generation are not observable and cannot be compared.
func matchesCreate(w *webhookResponse, req administration.CreateRequest) bool {
	return w.WebhookType == req.WebhookType &&
		w.WebhookIdentifier == req.WebhookIdentifier &&
		w.BotID == req.BotID &&
		w.Enabled == *req.Enabled &&
		w.Credential.Kind == req.Credential.Kind &&
		w.Credential.Configured
}

func (c *adminClient) printEndpoint(w *webhookResponse) error {
	if c.format == "json" {
		return writeJSONResult(c.env.Stdout, w)
	}
	tw := tabwriter.NewWriter(c.env.Stdout, 0, 0, 2, ' ', 0)
	writeEndpointRows(tw, w)
	return tw.Flush()
}

func (c *adminClient) printUncertain(r uncertainResult) error {
	if c.format == "json" {
		return writeJSONResult(c.env.Stdout, r)
	}
	tw := tabwriter.NewWriter(c.env.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "outcome\t%s\n", r.Outcome)
	if r.Endpoint != nil {
		writeEndpointRows(tw, r.Endpoint)
	} else {
		fmt.Fprintf(tw, "webhook_type\t%s\nwebhook_identifier\t%s\n", r.WebhookType, r.WebhookIdentifier)
	}
	return tw.Flush()
}

func writeEndpointRows(w io.Writer, e *webhookResponse) {
	fmt.Fprintf(w, "webhook_type\t%s\n", e.WebhookType)
	fmt.Fprintf(w, "webhook_identifier\t%s\n", e.WebhookIdentifier)
	fmt.Fprintf(w, "bot_platform\t%s\n", e.BotPlatform)
	fmt.Fprintf(w, "bot_id\t%s\n", e.BotID)
	fmt.Fprintf(w, "enabled\t%t\n", e.Enabled)
	fmt.Fprintf(w, "credential\t%s (configured: %t)\n", e.Credential.Kind, e.Credential.Configured)
	fmt.Fprintf(w, "generation_id\t%s\n", e.GenerationID)
	fmt.Fprintf(w, "config_version\t%d\n", e.ConfigVersion)
	fmt.Fprintf(w, "created_ms\t%d\n", e.CreatedMs)
	fmt.Fprintf(w, "updated_ms\t%d\n", e.UpdatedMs)
	fmt.Fprintf(w, "webhook_path\t%s\n", e.WebhookPath)
}

func writeJSONResult(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

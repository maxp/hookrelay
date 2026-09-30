package ingestion_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	vk "github.com/valkey-io/valkey-go"

	"github.com/maxp/hookrelay/internal/app"
	"github.com/maxp/hookrelay/internal/config"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/valkey"
)

const (
	secret   = "telegram-secret-001"
	identity = "telegram:42:chat:-1001"
)

// testURL maps the shared pinned Valkey to this package's database 3.
func testURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if raw == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set; start the pinned Valkey container")
	}
	return strings.TrimSuffix(strings.TrimSuffix(raw, "/0"), "/") + "/3"
}

type env struct {
	t        *testing.T
	adapter  *valkey.Adapter
	raw      vk.Client
	registry *prometheus.Registry
	public   http.Handler
	handler  *ingestion.Handler
}

func defaultLimits() valkey.AcceptLimits {
	return valkey.AcceptLimits{MaxQueuedMessages: 1000, MaxQueuedMessagesPerRecipient: 100, MaxDedupRecords: 1000, DedupRetention: time.Hour}
}

// compose builds the application public handler over real Valkey. acceptor
// overrides the Valkey acceptor when non-nil.
func compose(t *testing.T, acceptor func(*valkey.Adapter) ingestion.MessageAcceptor) *env {
	t.Helper()
	return composeLimits(t, defaultLimits(), acceptor)
}

// composeLimits is compose with explicit acceptance limits.
func composeLimits(t *testing.T, limits valkey.AcceptLimits, acceptor func(*valkey.Adapter) ingestion.MessageAcceptor) *env {
	t.Helper()
	url := testURL(t)
	opt, err := valkey.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	a, err := valkey.NewAdapter(opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	raw, err := vk.NewClient(opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close)
	ctx := context.Background()
	if err := a.FlushDB(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ValidateReadiness(ctx, false); err != nil {
		t.Fatal(err)
	}
	if _, _, r := a.CreateEndpoint(ctx, valkey.Endpoint{
		Type: "telegram", Identifier: "wh_chat", BotPlatform: "telegram", BotID: "42", Enabled: true,
		CredentialKind: "secret_token", CredentialValue: secret, GenerationID: "0195c4d8-0000-7000-8000-000000000001",
	}, "event", "webhook_endpoint_created", "req"); r != valkey.CreateOK {
		t.Fatalf("create endpoint = %s", r)
	}

	types, err := ingestion.Builtin(ingestion.BuiltinOptions{})
	if err != nil {
		t.Fatal(err)
	}
	valkeyAcceptor := valkey.NewMessageAcceptor(a, limits)
	var acc ingestion.MessageAcceptor = valkeyAcceptor
	if acceptor != nil {
		acc = acceptor(a)
	}
	reg := prometheus.NewRegistry()
	h, err := ingestion.NewHandler(ingestion.HandlerDeps{
		Registry: types, Endpoints: valkey.NewEndpointLookup(a), Acceptor: acc, Capacity: valkeyAcceptor,
		Gen: gen.Crypto{}, Clock: gen.SystemClock{}, Registerer: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(nil, func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	readiness := &app.Readiness{}
	readiness.MarkReady() // This harness bypasses App.Run after validating storage.
	application := app.New(app.Deps{
		Config: cfg, Logger: observability.NewTestLogger("error", &strings.Builder{}), Registry: reg,
		Readiness: readiness, Webhooks: h,
	})
	return &env{t: t, adapter: a, raw: raw, registry: reg, public: application.PublicHandler(), handler: h}
}

func (e *env) post(body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/webhook/telegram/wh_chat", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	r.Header.Set("X-Telegram-Bot-Api-Secret-Token", secret)
	w := httptest.NewRecorder()
	e.public.ServeHTTP(w, r)
	return w
}

func (e *env) do(args ...string) vk.ValkeyResult {
	return e.raw.Do(context.Background(), e.raw.B().Arbitrary(args...).Build())
}

func (e *env) keys(pattern string) []string {
	ks, err := e.do("KEYS", pattern).AsStrSlice()
	if err != nil {
		e.t.Fatal(err)
	}
	return ks
}

func (e *env) counter(name string) float64 {
	families, _ := e.registry.Gather()
	for _, f := range families {
		if f.GetName() == name {
			total := 0.0
			for _, m := range f.GetMetric() {
				total += m.GetCounter().GetValue()
			}
			return total
		}
	}
	return 0
}

const fixture = `{"update_id":500,"message":{"message_id":7,"date":1700000000,"chat":{"id":-1001,"type":"group"},"from":{"id":9},"text":"hello"}}`

// TestIngestionOverRealValkey drives a signed chat-scope update through the
// composed public listener and asserts every durable structure, then proves
// duplicate and conflicting-duplicate behavior creates nothing new.
func TestIngestionOverRealValkey(t *testing.T) {
	e := compose(t, nil)

	w := e.post(fixture)
	if w.Code != http.StatusOK || w.Body.Len() != 0 || w.Header().Get("X-Request-Id") == "" {
		t.Fatalf("accept = %d %q", w.Code, w.Body.String())
	}

	// Dedup record for (telegram, telegram/42, "500").
	digest := sha256.Sum256([]byte("telegram\ntelegram\n42\n500"))
	dedupKey := "hr1:d:" + hex.EncodeToString(digest[:])
	record, err := e.do("HGETALL", dedupKey).AsStrMap()
	if err != nil || record["message_id"] == "" {
		t.Fatalf("dedup record = %v (%v)", record, err)
	}
	bodySum := sha256.Sum256([]byte(fixture))
	if record["body_digest"] != hex.EncodeToString(bodySum[:]) {
		t.Errorf("body_digest = %s", record["body_digest"])
	}
	if ttl, _ := e.do("PTTL", dedupKey).AsInt64(); ttl <= 0 {
		t.Errorf("dedup TTL = %d", ttl)
	}
	messageID := record["message_id"]

	// Canonical Message blob.
	blob, err := e.do("GET", "hr1:m:"+messageID).ToString()
	if err != nil {
		t.Fatalf("blob: %v", err)
	}
	var msg model.CanonicalMessage
	if err := json.Unmarshal([]byte(blob), &msg); err != nil {
		t.Fatalf("blob decode: %v", err)
	}
	wantRecipient := model.Recipient{Scope: model.ScopeChat, BotPlatform: "telegram", BotID: "42", ChatID: "-1001"}
	if msg.MessageID != messageID || msg.Recipient != wantRecipient || msg.SourceEventID != "500" ||
		msg.PlatformEventType != "message" || msg.OccurredMs == nil || *msg.OccurredMs != 1700000000000 || msg.ReceivedMs <= 0 {
		t.Errorf("message = %+v", msg)
	}
	if !strings.Contains(string(msg.Payload), `"text":"hello"`) || !strings.Contains(string(msg.Payload), `"update_id":500`) {
		t.Errorf("payload = %s", msg.Payload)
	}

	// Queue, head state, ready index, counters.
	if q, _ := e.do("LRANGE", "hr1:r:"+identity+":q", "0", "-1").AsStrSlice(); len(q) != 1 || q[0] != messageID {
		t.Errorf("queue = %v", q)
	}
	state, _ := e.do("HGETALL", "hr1:r:"+identity+":s").AsStrMap()
	if state["status"] != "ready" || state["head_message_id"] != messageID || state["delivery_cycle"] != "1" || state["attempt"] != "1" {
		t.Errorf("state = %v", state)
	}
	if _, err := e.do("ZSCORE", "hr1:ready", identity).AsInt64(); err != nil {
		t.Errorf("ready membership: %v", err)
	}
	if n, _ := e.do("ZCARD", "hr1:dedup_age").AsInt64(); n != 1 {
		t.Errorf("dedup_age = %d", n)
	}
	if n, _ := e.do("GET", "hr1:stats:queued_messages").AsInt64(); n != 1 {
		t.Errorf("queued counter = %d", n)
	}

	// Duplicate with identical bytes, then a conflicting duplicate.
	for _, body := range []string{fixture, strings.Replace(fixture, "hello", "edited", 1)} {
		if w := e.post(body); w.Code != http.StatusOK || w.Body.Len() != 0 {
			t.Fatalf("duplicate = %d", w.Code)
		}
	}
	if blobs := e.keys("hr1:m:*"); len(blobs) != 1 {
		t.Errorf("message blobs = %v, want exactly one", blobs)
	}
	if n, _ := e.do("LLEN", "hr1:r:"+identity+":q").AsInt64(); n != 1 {
		t.Errorf("queue length = %d, want 1", n)
	}
	if n, _ := e.do("GET", "hr1:stats:queued_messages").AsInt64(); n != 1 {
		t.Errorf("queued counter = %d, want 1", n)
	}
	if got := e.counter("hookrelay_messages_duplicate_total"); got != 2 {
		t.Errorf("duplicates = %v, want 2", got)
	}
	if got := e.counter("hookrelay_dedup_conflicts_total"); got != 1 {
		t.Errorf("dedup conflicts = %v, want 1", got)
	}

	// A second message for the same chat queues behind the head.
	if w := e.post(strings.Replace(fixture, `"update_id":500`, `"update_id":501`, 1)); w.Code != http.StatusOK {
		t.Fatalf("second message = %d", w.Code)
	}
	if n, _ := e.do("LLEN", "hr1:r:"+identity+":q").AsInt64(); n != 2 {
		t.Errorf("queue length = %d, want 2", n)
	}
	if head, _ := e.do("HGET", "hr1:r:"+identity+":s", "head_message_id").ToString(); head != messageID {
		t.Errorf("head = %s, want the first message", head)
	}
}

// TestIngestionValkeyUnavailable pins a retryable 503 with no partial state
// when Valkey cannot accept.
func TestIngestionValkeyUnavailable(t *testing.T) {
	e := compose(t, func(a *valkey.Adapter) ingestion.MessageAcceptor {
		opt, _ := valkey.ParseURL(testURL(t))
		down, err := valkey.NewAdapter(opt)
		if err != nil {
			t.Fatal(err)
		}
		down.Close()
		return valkey.NewMessageAcceptor(down, valkey.AcceptLimits{
			MaxQueuedMessages: 1000, MaxQueuedMessagesPerRecipient: 100, MaxDedupRecords: 1000, DedupRetention: time.Hour,
		})
	})
	before := e.keys("*")
	w := e.post(fixture)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" || w.Body.Len() != 0 {
		t.Fatalf("unavailable = %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
	}
	if after := e.keys("*"); len(after) != len(before) {
		t.Errorf("keys changed from %v to %v", before, after)
	}
}

func update(id int, chat string) string {
	return fmt.Sprintf(`{"update_id":%d,"message":{"message_id":1,"date":1700000000,"chat":{"id":%s},"text":"x"}}`, id, chat)
}

// TestIngestionCapacityRejections pins every capacity stop through HTTP:
// 503 + Retry-After, nothing created, and the global acceptance signal
// stopping only for the global queue and deduplication caps.
func TestIngestionCapacityRejections(t *testing.T) {
	cases := []struct {
		name           string
		limits         func(*valkey.AcceptLimits)
		stopsAccepting bool
	}{
		{"per-recipient queue", func(l *valkey.AcceptLimits) { l.MaxQueuedMessagesPerRecipient = 1 }, false},
		{"global queue", func(l *valkey.AcceptLimits) { l.MaxQueuedMessages = 1 }, true},
		{"dedup records", func(l *valkey.AcceptLimits) { l.MaxDedupRecords = 1 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limits := defaultLimits()
			tc.limits(&limits)
			e := composeLimits(t, limits, nil)
			if !e.handler.AcceptingWebhooks(context.Background()) {
				t.Fatal("not accepting before any message")
			}
			if w := e.post(update(1, "-1001")); w.Code != http.StatusOK {
				t.Fatalf("first = %d", w.Code)
			}
			before := e.keys("*")
			w := e.post(update(2, "-1001"))
			if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" || w.Body.Len() != 0 {
				t.Fatalf("over capacity = %d Retry-After %q", w.Code, w.Header().Get("Retry-After"))
			}
			if after := e.keys("*"); len(after) != len(before) {
				t.Errorf("capacity rejection created keys: %d → %d", len(before), len(after))
			}
			if got := e.handler.AcceptingWebhooks(context.Background()); got == tc.stopsAccepting {
				t.Errorf("accepting = %v, want %v", got, !tc.stopsAccepting)
			}
			// A different Recipient is still accepted unless a global cap applies.
			other := e.post(update(3, "-2002")).Code
			if want := map[bool]int{true: 503, false: 200}[tc.stopsAccepting]; other != want {
				t.Errorf("other recipient = %d, want %d", other, want)
			}
		})
	}
}

// TestIngestionBlockedRecipient pins a retryable 503 for a blocked
// Recipient without affecting other Recipients.
func TestIngestionBlockedRecipient(t *testing.T) {
	e := compose(t, nil)
	e.do("HSET", "hr1:q:"+identity, "detected_ms", "1", "reason_code", "queue_head_mismatch")
	before := e.keys("*")
	w := e.post(fixture)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("blocked = %d", w.Code)
	}
	if after := e.keys("*"); len(after) != len(before) {
		t.Errorf("blocked recipient changed keys")
	}
	if w := e.post(update(9, "-3003")); w.Code != http.StatusOK {
		t.Errorf("other recipient = %d", w.Code)
	}
	if !e.handler.AcceptingWebhooks(context.Background()) {
		t.Error("a blocked Recipient must not stop global acceptance")
	}
}

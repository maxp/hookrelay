package delivery_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/app"
	"github.com/maxp/hookrelay/internal/config"
	"github.com/maxp/hookrelay/internal/delivery"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/model"
	"github.com/maxp/hookrelay/internal/observability"
	"github.com/maxp/hookrelay/internal/valkey"
)

const (
	consumerSecret = "consumer-secret-value-01"
	webhookSecret  = "telegram-secret-001"
)

// composeApp builds the public listener with ingestion and the Consumer API
// over the pinned Valkey (this package owns database 6).
func composeApp(t *testing.T) (http.Handler, *delivery.Handler, *prometheus.Registry) {
	t.Helper()
	public, consumer, reg, _ := composeStack(t)
	return public, consumer, reg
}

// composeStack is composeApp plus the delivery store for maintenance. The
// retry policy uses 20 ms nominal delays so retries are due quickly.
func composeStack(t *testing.T) (http.Handler, *delivery.Handler, *prometheus.Registry, *valkey.DeliveryStore) {
	return composeStackWithLease(t, time.Minute)
}

// testPolicy is the short-delay retry policy of the composed stack.
var testPolicy = delivery.RetryPolicy{MaxAttempts: 4, Delays: []time.Duration{20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond}, JitterMin: 0.5, JitterMax: 1}

func composeStackWithLease(t *testing.T, lease time.Duration) (http.Handler, *delivery.Handler, *prometheus.Registry, *valkey.DeliveryStore) {
	t.Helper()
	raw := os.Getenv("HOOKRELAY_TEST_VALKEY_URL")
	if raw == "" {
		t.Skip("HOOKRELAY_TEST_VALKEY_URL not set; start the pinned Valkey container")
	}
	opt, err := valkey.ParseURL(strings.TrimSuffix(strings.TrimSuffix(raw, "/0"), "/") + "/6")
	if err != nil {
		t.Fatal(err)
	}
	a, err := valkey.NewAdapter(opt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	ctx := context.Background()
	if err := a.FlushDB(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ValidateReadiness(ctx, false); err != nil {
		t.Fatal(err)
	}
	if _, _, r := a.CreateEndpoint(ctx, valkey.Endpoint{
		Type: "telegram", Identifier: "wh_d", BotPlatform: "telegram", BotID: "42", Enabled: true,
		CredentialKind: "secret_token", CredentialValue: webhookSecret, GenerationID: "0195c4d8-0000-7000-8000-000000000001",
	}, "event", "webhook_endpoint_created", "req"); r != valkey.CreateOK {
		t.Fatalf("create endpoint = %s", r)
	}

	reg := prometheus.NewRegistry()
	types, _ := ingestion.Builtin(ingestion.BuiltinOptions{})
	webhooks, err := ingestion.NewHandler(ingestion.HandlerDeps{
		Registry: types, Endpoints: valkey.NewEndpointLookup(a),
		Acceptor: valkey.NewMessageAcceptor(a, valkey.AcceptLimits{MaxQueuedMessages: 100, MaxQueuedMessagesPerRecipient: 10, MaxDedupRecords: 100, DedupRetention: time.Hour}),
		Gen:      gen.Crypto{}, Clock: gen.SystemClock{}, Registerer: reg,
	})
	if err != nil {
		t.Fatal(err)
	}
	store := valkey.NewDeliveryStore(a, valkey.ClaimLimits{MaxActiveLeases: 10, InitialLeaseDuration: lease})
	attempts, err := delivery.NewAttemptMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := delivery.NewHandler(delivery.HandlerDeps{
		Attempts: attempts,
		Claimer:  store, Acknowledger: store, NegativeAcknowledger: store, Stats: store, ConsumerSecret: consumerSecret, Gen: gen.Crypto{}, Clock: gen.SystemClock{}, Registerer: reg,
		RetryPolicy: testPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := config.Load(nil, func(string) string { return "" })
	application := app.New(app.Deps{
		Config: cfg, Logger: observability.NewTestLogger("error", &strings.Builder{}), Registry: reg,
		Readiness: &app.Readiness{}, Webhooks: webhooks, ConsumerAPI: consumer,
	})
	return application.PublicHandler(), consumer, reg, store
}

func send(h http.Handler, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

type claimBody struct {
	Delivery struct {
		DeliveryToken  string `json:"delivery_token"`
		DeliveryCycle  int64  `json:"delivery_cycle"`
		Attempt        int64  `json:"attempt"`
		ClaimedMs      int64  `json:"claimed_ms"`
		LeaseExpiresMs int64  `json:"lease_expires_ms"`
	} `json:"delivery"`
	Message json.RawMessage `json:"message"`
}

// TestClaimOverRealValkey drives webhook → claim → replay → empty through
// the composed public listener.
func TestClaimOverRealValkey(t *testing.T) {
	public, consumer, reg := composeApp(t)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret, "Consumer-Instance-Id": "worker-1"}
	claim := func(op string) *httptest.ResponseRecorder {
		return send(public, "/v1/deliveries/claim", `{"operation_id":"`+op+`","wait_ms":0}`, auth)
	}
	op1, op2 := "0195c4d8-0000-7000-8000-00000000000a", "0195c4d8-0000-7000-8000-00000000000b"

	if w := claim(op1); w.Code != http.StatusNoContent {
		t.Fatalf("claim on an empty pool = %d", w.Code)
	}
	if w := claim(op1); w.Code != http.StatusNoContent {
		t.Fatalf("replayed empty claim = %d", w.Code)
	}

	update := `{"update_id":7,"message":{"message_id":1,"date":1700000000,"chat":{"id":-77},"text":"hi"}}`
	if w := send(public, "/webhook/telegram/wh_d", update, map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}); w.Code != http.StatusOK {
		t.Fatalf("webhook = %d", w.Code)
	}

	w := claim(op2)
	if w.Code != http.StatusOK {
		t.Fatalf("claim = %d %s", w.Code, w.Body.String())
	}
	var body claimBody
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	var msg model.CanonicalMessage
	if err := json.Unmarshal(body.Message, &msg); err != nil {
		t.Fatalf("message: %v", err)
	}
	if !strings.HasPrefix(body.Delivery.DeliveryToken, "dlv_") || len(body.Delivery.DeliveryToken) != 26 ||
		body.Delivery.DeliveryCycle != 1 || body.Delivery.Attempt != 1 || body.Delivery.LeaseExpiresMs-body.Delivery.ClaimedMs != 60000 {
		t.Errorf("delivery = %+v", body.Delivery)
	}
	if msg.Recipient.ChatID != "-77" || msg.SourceEventID != "7" || msg.PlatformEventType != "message" || !strings.Contains(string(msg.Payload), `"text":"hi"`) {
		t.Errorf("message = %+v", msg)
	}
	if strings.Contains(string(body.Message), "null") || strings.Contains(string(body.Message), "routing_issue") {
		t.Errorf("absent optional fields encoded: %s", body.Message)
	}

	// The same operation replays the same attempt.
	replay := claim(op2)
	if replay.Code != http.StatusOK || replay.Body.String() != w.Body.String() {
		t.Errorf("replay = %d %s", replay.Code, replay.Body.String())
	}
	// Other arguments conflict.
	conflict := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op2+`","wait_ms":5}`, auth)
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "operation_conflict") {
		t.Errorf("conflict = %d %s", conflict.Code, conflict.Body.String())
	}
	// Nothing else is claimable while the only head is leased.
	if w := claim("0195c4d8-0000-7000-8000-00000000000c"); w.Code != http.StatusNoContent {
		t.Errorf("claim behind a lease = %d", w.Code)
	}

	consumer.RefreshGauges(context.Background())
	families, _ := reg.Gather()
	values := map[string]float64{}
	for _, f := range families {
		for _, m := range f.GetMetric() {
			if g := m.GetGauge(); g != nil {
				values[f.GetName()] = g.GetValue()
			}
		}
	}
	if values["hookrelay_active_leases"] != 1 || values["hookrelay_queue_messages"] != 1 || values["hookrelay_ready_recipients"] != 0 {
		t.Errorf("gauges = %v", values)
	}
}

// TestClaimAckCycleOverRealValkey drives claim → ack → next head → repeat
// ack → claim replay (claim_no_longer_active) → drained queue end to end.
func TestClaimAckCycleOverRealValkey(t *testing.T) {
	public, _, _ := composeApp(t)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret}
	hook := map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}
	for _, id := range []string{"1", "2"} {
		body := `{"update_id":` + id + `,"message":{"message_id":` + id + `,"date":1700000000,"chat":{"id":-88},"text":"t` + id + `"}}`
		if w := send(public, "/webhook/telegram/wh_d", body, hook); w.Code != http.StatusOK {
			t.Fatalf("webhook %s = %d", id, w.Code)
		}
	}
	claimOnce := func(op string) (claimBody, model.CanonicalMessage, int) {
		w := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op+`","wait_ms":0}`, auth)
		var b claimBody
		var m model.CanonicalMessage
		if w.Code == http.StatusOK {
			_ = json.Unmarshal(w.Body.Bytes(), &b)
			_ = json.Unmarshal(b.Message, &m)
		}
		return b, m, w.Code
	}
	ack := func(token string) *httptest.ResponseRecorder {
		return send(public, "/v1/deliveries/ack", `{"delivery_token":"`+token+`"}`, auth)
	}

	op1 := "0195c4d8-0000-7000-8000-0000000000b1"
	first, m1, code := claimOnce(op1)
	if code != 200 || m1.SourceEventID != "1" {
		t.Fatalf("first claim = %d %+v", code, m1)
	}
	w := ack(first.Delivery.DeliveryToken)
	var acked struct {
		Status         string `json:"status"`
		MessageID      string `json:"message_id"`
		AcknowledgedMs int64  `json:"acknowledged_ms"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &acked); w.Code != 200 || err != nil || acked.Status != "acknowledged" || acked.MessageID != m1.MessageID {
		t.Fatalf("ack = %d %s", w.Code, w.Body.String())
	}
	if again := ack(first.Delivery.DeliveryToken); again.Code != 200 || again.Body.String() != w.Body.String() {
		t.Errorf("repeat ack = %d %s", again.Code, again.Body.String())
	}
	replay := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op1+`","wait_ms":0}`, auth)
	if replay.Code != http.StatusConflict || !strings.Contains(replay.Body.String(), "claim_no_longer_active") || strings.Contains(replay.Body.String(), "dlv_") {
		t.Errorf("claim replay after ack = %d %s", replay.Code, replay.Body.String())
	}

	second, m2, code := claimOnce("0195c4d8-0000-7000-8000-0000000000b2")
	if code != 200 || m2.SourceEventID != "2" {
		t.Fatalf("second claim = %d %+v", code, m2)
	}
	if w := ack(second.Delivery.DeliveryToken); w.Code != 200 {
		t.Fatalf("second ack = %d", w.Code)
	}
	if _, _, code := claimOnce("0195c4d8-0000-7000-8000-0000000000b3"); code != http.StatusNoContent {
		t.Errorf("claim on a drained queue = %d", code)
	}
	if w := ack("dlv_ZZZZZZZZZZZZZZZZZZZZZZ"); w.Code != http.StatusNotFound {
		t.Errorf("unknown token = %d", w.Code)
	}
}

// TestLongPollOverRealValkey pins the waiting contract over real Valkey:
// work arriving mid-wait is claimed at once; a wait that ends empty records
// its outcome for replay; a cancelled wait records nothing, so repeating the
// same operation_id later still claims.
func TestLongPollOverRealValkey(t *testing.T) {
	public, _, _ := composeApp(t)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret}
	hook := map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}
	webhook := func(id string) {
		body := `{"update_id":` + id + `,"message":{"message_id":1,"date":1700000000,"chat":{"id":-` + id + `}}}`
		if w := send(public, "/webhook/telegram/wh_d", body, hook); w.Code != http.StatusOK {
			t.Fatalf("webhook %s = %d", id, w.Code)
		}
	}

	// Work appears mid-wait.
	result := make(chan *httptest.ResponseRecorder, 1)
	start := time.Now()
	go func() {
		result <- send(public, "/v1/deliveries/claim", `{"operation_id":"0195c4d8-0000-7000-8000-0000000000d1","wait_ms":10000}`, auth)
	}()
	time.Sleep(400 * time.Millisecond)
	webhook("31")
	select {
	case w := <-result:
		if w.Code != http.StatusOK {
			t.Fatalf("mid-wait claim = %d", w.Code)
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("claim returned after %v, want shortly after the webhook", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting claim did not pick up new work")
	}

	// A wait that ends empty replays as 204 immediately.
	op := "0195c4d8-0000-7000-8000-0000000000d2"
	if w := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op+`","wait_ms":600}`, auth); w.Code != http.StatusNoContent {
		t.Fatalf("empty wait = %d", w.Code)
	}
	webhook("32")
	start = time.Now()
	if w := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op+`","wait_ms":600}`, auth); w.Code != http.StatusNoContent || time.Since(start) > 300*time.Millisecond {
		t.Errorf("replayed empty wait = %d after %v, want an immediate recorded 204", w.Code, time.Since(start))
	}

	// A cancelled wait records nothing; the same operation later claims.
	// Drain webhook 32 first so the wait below is genuinely empty.
	if w := send(public, "/v1/deliveries/claim", `{"operation_id":"0195c4d8-0000-7000-8000-0000000000d4","wait_ms":0}`, auth); w.Code != http.StatusOK {
		t.Fatalf("drain = %d", w.Code)
	}
	op = "0195c4d8-0000-7000-8000-0000000000d3"
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodPost, "/v1/deliveries/claim", strings.NewReader(`{"operation_id":"`+op+`","wait_ms":10000}`)).WithContext(ctx)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+consumerSecret)
	done := make(chan struct{})
	cancelled := httptest.NewRecorder()
	go func() {
		public.ServeHTTP(cancelled, r)
		close(done)
	}()
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	if cancelled.Body.Len() != 0 {
		t.Fatal("the cancelled wait claimed work; the scenario is not a genuine wait")
	}
	webhook("33")
	if w := send(public, "/v1/deliveries/claim", `{"operation_id":"`+op+`","wait_ms":10000}`, auth); w.Code != http.StatusOK {
		t.Errorf("repeat after cancelled wait = %d", w.Code)
	}
}

// TestNackRetryOverRealValkey drives webhook → claim → nack → background
// maintenance → retried claim through the composed public listener: the
// retry is claimed with attempt 2 and a new Delivery Token, and acking the
// nacked token conflicts.
func TestNackRetryOverRealValkey(t *testing.T) {
	public, _, _, store := composeStack(t)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret}
	update := `{"update_id":8,"message":{"message_id":1,"date":1700000000,"chat":{"id":-78},"text":"retry"}}`
	if w := send(public, "/webhook/telegram/wh_d", update, map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}); w.Code != http.StatusOK {
		t.Fatalf("webhook = %d", w.Code)
	}
	w := send(public, "/v1/deliveries/claim", `{"operation_id":"0195c4d8-0000-7000-8000-000000000021","wait_ms":0}`, auth)
	var first claimBody
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &first) != nil {
		t.Fatalf("claim = %d %s", w.Code, w.Body.String())
	}
	token := `{"delivery_token":"` + first.Delivery.DeliveryToken + `"}`
	nack := send(public, "/v1/deliveries/nack", `{"delivery_token":"`+first.Delivery.DeliveryToken+`","reason_code":"test_failure"}`, auth)
	var nacked struct {
		Status    string `json:"status"`
		Attempt   int64  `json:"attempt"`
		RetryAtMs int64  `json:"retry_at_ms"`
	}
	if nack.Code != http.StatusOK || json.Unmarshal(nack.Body.Bytes(), &nacked) != nil || nacked.Status != "retry_scheduled" || nacked.Attempt != 1 {
		t.Fatalf("nack = %d %s", nack.Code, nack.Body.String())
	}
	if w := send(public, "/v1/deliveries/ack", token, auth); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "delivery_already_nacked") {
		t.Errorf("ack after nack = %d %s", w.Code, w.Body.String())
	}

	defer runMaintenance(t, store)()

	var retried claimBody
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; ; i++ {
		w := send(public, "/v1/deliveries/claim", `{"operation_id":"0195c4d8-0000-7000-8000-0000000001`+fmt.Sprintf("%02d", i%100)+`","wait_ms":0}`, auth)
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &retried); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry never became claimable; last claim = %d", w.Code)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if retried.Delivery.Attempt != 2 || retried.Delivery.DeliveryCycle != 1 || retried.Delivery.DeliveryToken == first.Delivery.DeliveryToken ||
		retried.Delivery.ClaimedMs < nacked.RetryAtMs {
		t.Errorf("retried delivery = %+v, want attempt 2 with a new token claimed after %d", retried.Delivery, nacked.RetryAtMs)
	}
}

// runMaintenance runs background maintenance with a 10 ms interval and
// returns its stop function.
func runMaintenance(t *testing.T, store *valkey.DeliveryStore) func() {
	t.Helper()
	attempts, err := delivery.NewAttemptMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	m, err := delivery.NewMaintenance(delivery.MaintenanceDeps{
		Retries: store, Leases: store, RetryPolicy: testPolicy, Attempts: attempts,
		Config: delivery.MaintenanceConfig{Interval: 10 * time.Millisecond, IntervalJitter: 5 * time.Millisecond, BatchSize: 100, MaxContinuousBatches: 5},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	return func() { cancel(); <-done }
}

// TestLeaseExpiryOverRealValkey drives webhook → claim with a short lease →
// background expiry → retried claim through the composed public listener:
// the stalled attempt's ack, nack, and claim replay all see it as ended,
// and the retry is claimed with attempt 2 and a new Delivery Token.
func TestLeaseExpiryOverRealValkey(t *testing.T) {
	public, _, _, store := composeStackWithLease(t, 30*time.Millisecond)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret}
	update := `{"update_id":9,"message":{"message_id":1,"date":1700000000,"chat":{"id":-79},"text":"stall"}}`
	if w := send(public, "/webhook/telegram/wh_d", update, map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}); w.Code != http.StatusOK {
		t.Fatalf("webhook = %d", w.Code)
	}
	claimBodyFor := func(op string) string { return `{"operation_id":"` + op + `","wait_ms":0}` }
	firstOp := "0195c4d8-0000-7000-8000-000000000031"
	w := send(public, "/v1/deliveries/claim", claimBodyFor(firstOp), auth)
	var first claimBody
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &first) != nil {
		t.Fatalf("claim = %d %s", w.Code, w.Body.String())
	}

	defer runMaintenance(t, store)()
	var retried claimBody
	deadline := time.Now().Add(3 * time.Second)
	for i := 0; ; i++ {
		w := send(public, "/v1/deliveries/claim", claimBodyFor("0195c4d8-0000-7000-8000-0000000002"+fmt.Sprintf("%02d", i%100)), auth)
		if w.Code == http.StatusOK {
			if err := json.Unmarshal(w.Body.Bytes(), &retried); err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired lease never retried; last claim = %d", w.Code)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if retried.Delivery.Attempt != 2 || retried.Delivery.DeliveryToken == first.Delivery.DeliveryToken {
		t.Errorf("retried delivery = %+v, want attempt 2 with a new token", retried.Delivery)
	}

	token := `{"delivery_token":"` + first.Delivery.DeliveryToken + `"}`
	for _, path := range []string{"/v1/deliveries/ack", "/v1/deliveries/nack"} {
		if w := send(public, path, token, auth); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "stale_delivery_token") {
			t.Errorf("%s with the expired token = %d %s", path, w.Code, w.Body.String())
		}
	}
	if w := send(public, "/v1/deliveries/claim", claimBodyFor(firstOp), auth); w.Code != http.StatusConflict || !strings.Contains(w.Body.String(), "claim_no_longer_active") {
		t.Errorf("claim replay after expiry = %d %s", w.Code, w.Body.String())
	}
}

// TestDeadLetterOverRealValkey drives four nacks of one message through
// the composed public listener and background maintenance: the fourth
// dead-letters it (repeatable result) and the Recipient's next message is
// claimed as attempt 1.
func TestDeadLetterOverRealValkey(t *testing.T) {
	public, _, _, store := composeStack(t)
	auth := map[string]string{"Authorization": "Bearer " + consumerSecret}
	for _, id := range []string{"10", "11"} {
		update := `{"update_id":` + id + `,"message":{"message_id":` + id + `,"date":1700000000,"chat":{"id":-80},"text":"poison"}}`
		if w := send(public, "/webhook/telegram/wh_d", update, map[string]string{"X-Telegram-Bot-Api-Secret-Token": webhookSecret}); w.Code != http.StatusOK {
			t.Fatalf("webhook %s = %d", id, w.Code)
		}
	}
	defer runMaintenance(t, store)()

	op := 0
	claimNext := func() claimBody {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			op++
			w := send(public, "/v1/deliveries/claim", `{"operation_id":"0195c4d8-0000-7000-8000-0000000003`+fmt.Sprintf("%02d", op)+`","wait_ms":0}`, auth)
			if w.Code == http.StatusOK {
				var b claimBody
				if err := json.Unmarshal(w.Body.Bytes(), &b); err != nil {
					t.Fatal(err)
				}
				return b
			}
			if time.Now().After(deadline) {
				t.Fatalf("nothing claimable; last claim = %d", w.Code)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	var first string
	var final *httptest.ResponseRecorder
	var token string
	for attempt := int64(1); attempt <= 4; attempt++ {
		c := claimNext()
		var msg model.CanonicalMessage
		if err := json.Unmarshal(c.Message, &msg); err != nil {
			t.Fatal(err)
		}
		if first == "" {
			first = msg.MessageID
		}
		if msg.MessageID != first || c.Delivery.Attempt != attempt {
			t.Fatalf("claim %d = message %s attempt %d, want %s attempt %d", attempt, msg.MessageID, c.Delivery.Attempt, first, attempt)
		}
		token = `{"delivery_token":"` + c.Delivery.DeliveryToken + `"}`
		final = send(public, "/v1/deliveries/nack", token, auth)
		if final.Code != http.StatusOK {
			t.Fatalf("nack %d = %d %s", attempt, final.Code, final.Body.String())
		}
	}
	var dead struct {
		Status         string `json:"status"`
		MessageID      string `json:"message_id"`
		DeliveryCycle  int64  `json:"delivery_cycle"`
		DeadLetteredMs int64  `json:"dead_lettered_ms"`
	}
	if err := json.Unmarshal(final.Body.Bytes(), &dead); err != nil || dead.Status != "dead_lettered" || dead.MessageID != first ||
		dead.DeliveryCycle != 1 || dead.DeadLetteredMs <= 0 {
		t.Fatalf("fourth nack = %s", final.Body.String())
	}
	if w := send(public, "/v1/deliveries/nack", token, auth); w.Code != http.StatusOK || w.Body.String() != final.Body.String() {
		t.Errorf("repeat = %d %s, want the recorded dead_lettered result", w.Code, w.Body.String())
	}

	next := claimNext()
	var msg model.CanonicalMessage
	if err := json.Unmarshal(next.Message, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.MessageID == first || msg.SourceEventID != "11" || next.Delivery.Attempt != 1 || next.Delivery.DeliveryCycle != 1 {
		t.Errorf("next claim = %+v %s, want update 11 attempt 1", next.Delivery, msg.SourceEventID)
	}
}

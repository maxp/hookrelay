package valkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maxp/hookrelay/internal/administration"
	"github.com/maxp/hookrelay/internal/gen"
	"github.com/maxp/hookrelay/internal/ingestion"
	"github.com/maxp/hookrelay/internal/model"
	vk "github.com/valkey-io/valkey-go"
)

type reconciliationPopulation struct {
	endpoints, recipients, queued, dedup, deadLetters, sessions, payloadBytes int
}

// seedReconciliationPopulation uses real acceptance/endpoint/session transitions
// and schema-shaped retained dedup/DLQ fixtures. No consumers or maintenance run:
// the measurement is a quiescent startup pass, not a serving-time benchmark.
func seedReconciliationPopulation(t *testing.T, a *Adapter, p reconciliationPopulation) {
	t.Helper()
	ctx := context.Background()
	for i := range p.endpoints {
		e := endpointFixture()
		e.Identifier, e.BotID = fmt.Sprintf("wh_cost_%d", i), strconv.Itoa(1000+i/50)
		if _, _, outcome := a.CreateEndpoint(ctx, e, gen.Crypto{}.UUIDv7(), "webhook_endpoint_created", "cost"); outcome != CreateOK {
			t.Fatalf("seed endpoint: %s", outcome)
		}
	}
	if _, err := a.EnsureAdminAuth(ctx, testAdminSecret, gen.Crypto{}); err != nil {
		t.Fatal(err)
	}
	sessions := NewSessionStore(a)
	for i := range p.sessions {
		if out := createSession(t, sessions, digestOf(i)); out.Result != administration.SessionCreated {
			t.Fatalf("seed session: %s", out.Result)
		}
	}
	limits := AcceptLimits{MaxQueuedMessages: p.queued + 1, MaxQueuedMessagesPerRecipient: 1000,
		MaxDedupRecords: int64(p.dedup + p.queued + 1), DedupRetention: 168 * time.Hour}
	acceptor := NewMessageAcceptor(a, limits)
	payload, err := json.Marshal(map[string]string{"text": strings.Repeat("x", p.payloadBytes)})
	if err != nil {
		t.Fatal(err)
	}
	for i := range p.queued {
		id := gen.Crypto{}.UUIDv7()
		recipient := model.Recipient{BotPlatform: "telegram", BotID: "42", Scope: "chat", ChatID: strconv.Itoa(-1 - i%p.recipients)}
		blob, err := (model.CanonicalMessage{MessageID: id, ReceivedMs: 1740000000123,
			Recipient: recipient, PlatformEventType: "message", Payload: payload}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256([]byte(id))
		digest := hex.EncodeToString(sum[:])
		req := acceptReq(id, digest, digest)
		req.RecipientIdentity, req.MessageJSON = recipient.Identity(), blob
		if out := acceptor.Accept(ctx, req); out.Outcome != ingestion.AcceptAccepted {
			t.Fatalf("seed acceptance %d: %s", i, out.Outcome)
		}
	}

	// Retained dedup records for already-drained messages dominate key counts
	// at the default capacity. Bound fixture pipelining to 300 commands.
	now, err := a.serverTimeMs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var batch []vk.Completed
	flush := func() {
		for _, out := range a.client.DoMulti(ctx, batch...) {
			if err := out.Error(); err != nil {
				t.Fatal(err)
			}
		}
		batch = batch[:0]
	}
	for i := p.queued; i < p.dedup; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("retained-%d", i)))
		digest := hex.EncodeToString(sum[:])
		key := "hr1:d:" + digest
		batch = append(batch,
			a.client.B().Arbitrary("HSET", key, "message_id", fmt.Sprintf("drained-%d", i), "body_digest", digest,
				"accepted_ms", itoa64(now), "expires_ms", itoa64(now+limits.DedupRetention.Milliseconds())).Build(),
			a.client.B().Arbitrary("PEXPIRE", key, itoa64(limits.DedupRetention.Milliseconds())).Build(),
			a.client.B().Arbitrary("ZADD", "hr1:dedup_age", itoa64(now), digest).Build())
		if len(batch) >= 300 {
			flush()
		}
	}
	flush()
	for i := range p.deadLetters {
		id := gen.Crypto{}.UUIDv7()
		recipient := model.Recipient{BotPlatform: "telegram", BotID: "42", Scope: "chat", ChatID: strconv.Itoa(-100000 - i)}
		blob, err := (model.CanonicalMessage{MessageID: id, ReceivedMs: now,
			Recipient: recipient, PlatformEventType: "message", Payload: payload}).Encode()
		if err != nil {
			t.Fatal(err)
		}
		a.testDo(t, "SET", "hr1:m:"+id, string(blob))
		a.testDo(t, "HSET", "hr1:dl:"+id, "bot_platform", "telegram", "bot_id", "42", "recipient_scope", "chat",
			"chat_id", recipient.ChatID, "recipient_identity", recipient.Identity(), "dead_lettered_ms", itoa64(now),
			"dead_letter_reason", "nack_exhausted", "delivery_cycle", "1")
		a.testDo(t, "ZADD", "hr1:dlq", itoa64(now), id)
	}
}

func TestReconciliationCostFixture(t *testing.T) {
	a, _ := claimSetup(t)
	seedReconciliationPopulation(t, a, reconciliationPopulation{5, 3, 12, 30, 4, 2, 1024})
	rep, err := a.Reconcile(context.Background(), ReconcileOptions{Full: true, BatchSize: 3, AdminSecret: testAdminSecret})
	if err != nil || rep.Hold() != "" || rep.Recipients != 3 {
		t.Fatalf("fixture reconciliation = %+v, %v", rep, err)
	}
	for kind, n := range rep.Findings {
		if n != 0 {
			t.Errorf("fixture finding %s = %d", kind, n)
		}
	}
}

// TestReconciliationCost is opt-in because it flushes the integration database
// and can populate more than a million keys. Use a dedicated disposable Valkey.
// It prints counts/costs only, never fixture secrets or Canonical Messages.
func TestReconciliationCost(t *testing.T) {
	profile := os.Getenv("HOOKRELAY_RECONCILIATION_COST_PROFILE")
	if profile == "" {
		t.Skip("set HOOKRELAY_RECONCILIATION_COST_PROFILE=medium|capacity|deep|large-payload on a disposable Valkey")
	}
	populations := map[string]reconciliationPopulation{
		"medium":        {1000, 1000, 10000, 100000, 100, 100, 1024},
		"capacity":      {1000, 10000, 100000, 1000000, 100, 100, 1024},
		"deep":          {1000, 100, 100000, 100000, 100, 100, 1024},
		"large-payload": {1000, 1000, 10000, 100000, 100, 100, 65536},
	}
	p, ok := populations[profile]
	if !ok {
		t.Fatalf("unknown reconciliation cost profile %q", profile)
	}
	a, _ := claimSetup(t)
	probe := testAdapter(t, false)
	start := time.Now()
	seedReconciliationPopulation(t, a, p)
	t.Logf("profile=%s population=%+v setup=%s", profile, p, time.Since(start))
	for pass := range 3 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		var latencies []time.Duration
		var probeErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					start := time.Now()
					if err := probe.client.Do(ctx, probe.client.B().Ping().Build()).Error(); err != nil {
						if ctx.Err() == nil {
							probeErr = err
						}
						return
					}
					latencies = append(latencies, time.Since(start))
				}
			}
		})
		start := time.Now()
		rep, err := a.Reconcile(ctx, ReconcileOptions{Full: true, BatchSize: 100, MessageCheckBound: 1000, AdminSecret: testAdminSecret})
		elapsed := time.Since(start)
		cancel()
		wg.Wait()
		runtime.ReadMemStats(&after)
		if err != nil || rep.Hold() != "" || rep.Recipients != p.recipients || probeErr != nil {
			t.Fatalf("cost pass=%d report=%+v error=%v probe=%v", pass+1, rep, err, probeErr)
		}
		for kind, n := range rep.Findings {
			if n != 0 {
				t.Fatalf("unexpected finding %s=%d", kind, n)
			}
		}
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		percentile := func(percent int) time.Duration {
			if len(latencies) == 0 {
				return 0
			}
			return latencies[(len(latencies)-1)*percent/100]
		}
		t.Logf("profile=%s pass=%d duration=%s allocated_bytes=%d heap_after_bytes=%d ping_samples=%d ping_p95=%s ping_p99=%s ping_max=%s",
			profile, pass+1, elapsed, after.TotalAlloc-before.TotalAlloc, after.HeapAlloc,
			len(latencies), percentile(95), percentile(99), percentile(100))
	}
}

package valkey

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/ingestion"
)

func evictionLimits(maxRecords int64) AcceptLimits {
	l := testLimits()
	l.MaxDedupRecords = maxRecords
	l.DedupMinRetention = 10 * time.Minute
	return l
}

// backdate moves a live dedup record's acceptance time (record and index
// member) to ago before Valkey now.
func backdate(t *testing.T, a *Adapter, digest string, ago time.Duration) int64 {
	t.Helper()
	now, err := a.serverTimeMs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	at := now - ago.Milliseconds()
	a.testDo(t, "HSET", "hr1:d:"+digest, "accepted_ms", itoa64(at))
	a.testDo(t, "ZADD", "hr1:dedup_age", itoa64(at), digest)
	return at
}

func acceptWith(a *Adapter, limits AcceptLimits, id, rid string) ingestion.AcceptResult {
	r := acceptReq(id, "d-"+id, "b-"+id)
	r.RecipientIdentity = rid
	r.MessageJSON = []byte(`{"message_id":"` + id + `"}`)
	return NewMessageAcceptor(a, limits).Accept(context.Background(), r)
}

// TestAcceptEvictsTheOldestAtCapacity pins accept_v3's acceptance-time
// eviction: at the cap an eligible oldest record is evicted (record and
// member) in the same operation and early_evicted is reported; a too-young
// oldest record refuses with dedup_capacity and no writes; duplicates are
// still proven at the cap.
func TestAcceptEvictsTheOldestAtCapacity(t *testing.T) {
	a, _ := claimSetup(t)
	limits := evictionLimits(2)
	for _, id := range []string{"m1", "m2"} {
		if r := acceptWith(a, limits, id, ridA); r.Outcome != ingestion.AcceptAccepted || r.EarlyEvicted != 0 {
			t.Fatalf("accept %s = %+v", id, r)
		}
	}
	backdate(t, a, "d-m1", 20*time.Minute)

	r := acceptWith(a, limits, "m3", ridB)
	if r.Outcome != ingestion.AcceptAccepted || r.EarlyEvicted != 1 {
		t.Fatalf("accept at cap = %+v", r)
	}
	if exists(t, a, "hr1:d:d-m1") {
		t.Error("evicted record kept")
	}
	if _, ok := score(t, a, "hr1:dedup_age", "d-m1"); ok {
		t.Error("evicted index member kept")
	}
	if !exists(t, a, "hr1:d:d-m3") || cardinality(t, a, "hr1:dedup_age") != 2 {
		t.Error("new record missing or cap exceeded")
	}
	if !exists(t, a, "hr1:m:m1") {
		t.Error("eviction touched the message of the evicted record")
	}

	before := snapshot(t, a)
	if r := acceptWith(a, limits, "m4", ridB); r.Outcome != ingestion.AcceptDedupCapacity {
		t.Errorf("too-young oldest = %+v", r)
	}
	assertUnchanged(t, a, before, "dedup_capacity")
	if r := acceptWith(a, limits, "m3", ridB); r.Outcome != ingestion.AcceptDuplicate {
		t.Errorf("duplicate at cap = %+v", r)
	}
}

// TestAcceptRefusesInconsistentEvictionCandidate pins wrong_type without
// writes for a missing, mistyped, or mismatched oldest record.
func TestAcceptRefusesInconsistentEvictionCandidate(t *testing.T) {
	for name, setup := range map[string]func(t *testing.T, a *Adapter){
		"record missing": func(t *testing.T, a *Adapter) { a.testDo(t, "DEL", "hr1:d:d-m1") },
		"record not a hash": func(t *testing.T, a *Adapter) {
			a.testDo(t, "DEL", "hr1:d:d-m1")
			a.testDo(t, "SET", "hr1:d:d-m1", "x")
		},
		"accepted_ms mismatch":  func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:d:d-m1", "accepted_ms", "123") },
		"accepted_ms malformed": func(t *testing.T, a *Adapter) { a.testDo(t, "HSET", "hr1:d:d-m1", "accepted_ms", "x") },
	} {
		t.Run(name, func(t *testing.T) {
			a, _ := claimSetup(t)
			limits := evictionLimits(2)
			acceptWith(a, limits, "m1", ridA)
			acceptWith(a, limits, "m2", ridA)
			backdate(t, a, "d-m1", 20*time.Minute)
			setup(t, a)
			before := snapshot(t, a)
			if r := acceptWith(a, limits, "m3", ridB); r.Outcome != ingestion.AcceptInternalFailure {
				t.Errorf("accept = %+v", r)
			}
			assertUnchanged(t, a, before, name)
		})
	}
}

// TestConcurrentAcceptanceAtCapacity pins that concurrent acceptances at
// the cap each evict at most one eligible record, never exceed the cap,
// and refuse once only young records remain.
func TestConcurrentAcceptanceAtCapacity(t *testing.T) {
	a, _ := claimSetup(t)
	limits := evictionLimits(5)
	limits.MaxQueuedMessagesPerRecipient = 100
	for i := 0; i < 5; i++ {
		id := "old" + itoa(i)
		acceptWith(a, limits, id, ridA)
		backdate(t, a, "d-"+id, time.Duration(20+i)*time.Minute)
	}
	var wg sync.WaitGroup
	results := make([]ingestion.AcceptResult, 10)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = acceptWith(a, limits, "new"+itoa(i), ridB)
		}(i)
	}
	wg.Wait()
	accepted, evicted, refused := 0, 0, 0
	for _, r := range results {
		switch r.Outcome {
		case ingestion.AcceptAccepted:
			accepted++
			evicted += r.EarlyEvicted
		case ingestion.AcceptDedupCapacity:
			refused++
		default:
			t.Errorf("unexpected result %+v", r)
		}
	}
	if accepted != 5 || evicted != 5 || refused != 5 || cardinality(t, a, "hr1:dedup_age") != 5 {
		t.Errorf("accepted %d evicted %d refused %d live %d", accepted, evicted, refused, cardinality(t, a, "hr1:dedup_age"))
	}
}

// TestEvictDedup pins evict_dedup_v1: the oldest eligible records are
// removed while the cap is reached, stopping below the cap, at a record
// younger than the minimum retention, at the batch bound, or at an
// inconsistent candidate (reported, never evicted).
func TestEvictDedup(t *testing.T) {
	ctx := context.Background()
	seed := func(t *testing.T, a *Adapter, limits AcceptLimits, ages ...time.Duration) {
		for i, age := range ages {
			id := "e" + itoa(i)
			if r := acceptWith(a, limits, id, ridA); r.Outcome != ingestion.AcceptAccepted {
				t.Fatalf("seed %s = %+v", id, r)
			}
			backdate(t, a, "d-"+id, age)
		}
	}
	old, young := 30*time.Minute, time.Minute

	a, _ := claimSetup(t)
	limits := evictionLimits(100)
	seed(t, a, limits, old+3*time.Minute, old+2*time.Minute, old+time.Minute, young, young)
	limits.MaxDedupRecords = 3
	m := NewMessageAcceptor(a, limits)
	if n, err := m.EvictDedup(ctx); err != nil || n != 3 {
		t.Fatalf("evict = %d, %v; want the three old records (down below the cap)", n, err)
	}
	if exists(t, a, "hr1:d:d-e0") || exists(t, a, "hr1:d:d-e2") || !exists(t, a, "hr1:d:d-e3") || cardinality(t, a, "hr1:dedup_age") != 2 {
		t.Error("wrong records evicted")
	}
	if n, err := m.EvictDedup(ctx); err != nil || n != 0 {
		t.Errorf("below cap = %d, %v", n, err)
	}

	a, _ = claimSetup(t)
	limits = evictionLimits(100)
	seed(t, a, limits, old, young, young)
	limits.MaxDedupRecords = 2
	if n, err := NewMessageAcceptor(a, limits).EvictDedup(ctx); err != nil || n != 1 {
		t.Errorf("min retention stop = %d, %v", n, err)
	}
	before := snapshot(t, a)
	if n, _ := NewMessageAcceptor(a, limits).EvictDedup(ctx); n != 0 {
		t.Errorf("only young records left, evicted %d", n)
	}
	assertUnchanged(t, a, before, "min retention")

	a, _ = claimSetup(t)
	limits = evictionLimits(100)
	seed(t, a, limits, old+time.Minute, old, old)
	limits.MaxDedupRecords, limits.EvictionBatch = 1, 1
	if n, err := NewMessageAcceptor(a, limits).EvictDedup(ctx); err != nil || n != 1 {
		t.Errorf("batch bound = %d, %v", n, err)
	}

	a, _ = claimSetup(t)
	limits = evictionLimits(100)
	seed(t, a, limits, old+time.Minute, old)
	a.testDo(t, "HSET", "hr1:d:d-e1", "accepted_ms", "1")
	a.testDo(t, "HSET", "hr1:d:d-e0", "accepted_ms", "1")
	limits.MaxDedupRecords = 1
	before = snapshot(t, a)
	if n, err := NewMessageAcceptor(a, limits).EvictDedup(ctx); err == nil || n != 0 {
		t.Errorf("inconsistent candidate = %d, %v", n, err)
	}
	assertUnchanged(t, a, before, "candidate_invalid")

	a.testDo(t, "DEL", "hr1:dedup_age")
	a.testDo(t, "SET", "hr1:dedup_age", "x")
	if _, err := NewMessageAcceptor(a, limits).EvictDedup(ctx); err == nil {
		t.Error("wrong-typed index accepted")
	}
}

// TestEvictDedupArguments pins argument rejection.
func TestEvictDedupArguments(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	valid := []string{"10", "60000", "3600000", "100", "hr1"}
	for i := 0; i < 4; i++ {
		args := append([]string(nil), valid...)
		args[i] = "0"
		if _, err := a.RunScript(ctx, "evict_dedup_v1", []string{"hr1:dedup_age"}, args); err == nil || errors.Is(err, ErrNotDispatched) {
			t.Errorf("argument %d = 0 accepted", i+1)
		}
	}
	if _, err := a.RunScript(ctx, "evict_dedup_v1", []string{"other:dedup_age"}, valid); err == nil {
		t.Error("foreign key accepted")
	}
	if _, err := a.RunScript(ctx, "evict_dedup_v1", []string{"hr1:dedup_age"}, valid); err != nil {
		t.Errorf("valid call: %v", err)
	}
}

// TestCapacityAndServerSample pins the capacity snapshot (Valkey time,
// memory, oldest live record) and the Valkey memory/AOF metrics.
func TestCapacityAndServerSample(t *testing.T) {
	a, _ := claimSetup(t)
	ctx := context.Background()
	limits := evictionLimits(100)
	acceptWith(a, limits, "m1", ridA)
	acceptWith(a, limits, "m2", ridA)
	at := backdate(t, a, "d-m1", 20*time.Minute)
	c, err := NewMessageAcceptor(a, limits).Capacity(ctx)
	if err != nil || c.NowMs <= at || c.DedupRecords != 2 || c.OldestDedupAcceptedMs != at || c.UsedMemoryBytes <= 0 || c.MaxMemoryBytes < 0 {
		t.Fatalf("capacity = %+v, %v", c, err)
	}

	reg := prometheus.NewRegistry()
	if err := a.Instrument(reg); err != nil {
		t.Fatal(err)
	}
	if err := a.SampleServer(ctx); err != nil {
		t.Fatal(err)
	}
	p, _ := a.info(ctx, "persistence")
	if got := value(t, reg, "hookrelay_valkey_memory_used_bytes"); got <= 0 {
		t.Errorf("memory used = %v", got)
	}
	if got := value(t, reg, "hookrelay_valkey_memory_max_bytes"); got != float64(c.MaxMemoryBytes) {
		t.Errorf("memory max = %v, want %d", got, c.MaxMemoryBytes)
	}
	if got, want := value(t, reg, "hookrelay_valkey_aof_enabled"), map[string]float64{"0": 0, "1": 1}[p["aof_enabled"]]; got != want {
		t.Errorf("aof enabled = %v, want %v", got, want)
	}
	if got := value(t, reg, "hookrelay_valkey_aof_delayed_fsync_total"); got < 0 {
		t.Errorf("delayed fsync = %v", got)
	}
}

// TestOldestReadyMessageAge pins the stats source of
// hookrelay_oldest_ready_message_age_seconds.
func TestOldestReadyMessageAge(t *testing.T) {
	a, s := claimSetup(t)
	ctx := context.Background()
	if st, err := s.Stats(ctx); err != nil || st.OldestReadyAgeMs != 0 {
		t.Fatalf("empty = %+v, %v", st, err)
	}
	enqueueJSON(t, a, "m1", ridA) // received_ms 1740000000123
	st, err := s.Stats(ctx)
	now, _ := a.serverTimeMs(ctx)
	if err != nil || st.OldestReadyAgeMs <= 0 || st.OldestReadyAgeMs > now-1740000000123 {
		t.Errorf("age = %d (now %d), %v", st.OldestReadyAgeMs, now, err)
	}
	enqueue(t, a, "m2", ridB) // no received_ms in the blob
	claimNext(t, s, "op-1", "dlv_1")
	if st, _ := s.Stats(ctx); st.OldestReadyAgeMs != 0 {
		t.Errorf("age without received_ms = %d", st.OldestReadyAgeMs)
	}
}

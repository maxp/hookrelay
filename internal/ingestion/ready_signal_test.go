package ingestion

import (
	"slices"
	"testing"
)

type recordingReady struct{ sources []string }

func (r *recordingReady) Signal(source string) { r.sources = append(r.sources, source) }

// TestAcceptanceSignalsReady pins the ingestion source: an accepted message
// signals once; a duplicate or a rejected acceptance does not.
func TestAcceptanceSignalsReady(t *testing.T) {
	hs := newHarness(t)
	ready := &recordingReady{}
	hs.h.d.Ready = ready
	if w := hs.post("/webhook/telegram/wh_on", chatUpdate); w.Code != 200 {
		t.Fatalf("accept = %d", w.Code)
	}
	for _, outcome := range []AcceptOutcome{AcceptDuplicate, AcceptRecipientBlocked, AcceptGlobalCapacity} {
		hs.acceptor.result = AcceptResult{Outcome: outcome, MessageID: "m"}
		hs.post("/webhook/telegram/wh_on", chatUpdate)
	}
	if want := []string{"accept"}; !slices.Equal(ready.sources, want) {
		t.Errorf("signals = %v, want %v", ready.sources, want)
	}
}

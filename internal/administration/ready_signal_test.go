package administration

import (
	"net/http"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type recordingReady struct{ sources []string }

func (r *recordingReady) Signal(source string) { r.sources = append(r.sources, source) }

// TestReadySignals pins the administrative ready-signal sources: a replay
// that becomes the head and a clear that restores the ready index signal;
// a replay queued behind the head and a clear restoring another index do
// not.
func TestReadySignals(t *testing.T) {
	ready := &recordingReady{}
	dl := &fakeDeadLetters{}
	rc := &fakeRecipients{clear: ClearCleared}
	svc, err := NewService(ServiceDeps{
		Repo: newFakeRepo(), DeadLetters: dl, Recipients: rc, Catalog: fakeCatalog{}, Audit: &fakeAudit{},
		AdminSecret: "admin-secret-value-016", Gen: fixedGen{}, Ready: ready, Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	h := Handler(svc)
	for _, position := range []string{"after_active_head", "head", "after_pending_replay"} {
		dl.replay = Replay{Result: ReplayReplayed, DeliveryCycle: 2, QueuePosition: position, DeduplicationResolution: "not_conflicting", RecipientIdentity: "telegram:42:chat:-100"}
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/dead-letters/"+dlqID+"/replay", "admin-secret-value-016", ""); rec.Code != http.StatusOK {
			t.Fatalf("replay %s = %d", position, rec.Code)
		}
	}
	body := `{"recipient":` + chatRecipient + `,"expected_detected_ms":55,"expected_reason_code":"head_message_missing"}`
	for _, restored := range []string{"leases", "ready", "retries", "none"} {
		rc.clearInfo = restored
		if rec := doJSON(t, h, http.MethodPost, "/admin/v1/recipient-blocks/clear", "admin-secret-value-016", body); rec.Code != http.StatusNoContent {
			t.Fatalf("clear %s = %d", restored, rec.Code)
		}
	}
	if want := []string{"replay", "block_clear"}; !slices.Equal(ready.sources, want) {
		t.Errorf("signals = %v, want %v", ready.sources, want)
	}
}

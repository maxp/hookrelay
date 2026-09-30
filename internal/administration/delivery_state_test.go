package administration

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

type fakeMessages struct {
	state DeliveryState
	err   error
	reads []string
}

func (f *fakeMessages) DeliveryState(_ context.Context, messageID string) (DeliveryState, error) {
	f.reads = append(f.reads, messageID)
	return f.state, f.err
}

func deliveryStateHandler(t *testing.T, f *fakeMessages) http.Handler {
	t.Helper()
	svc, err := NewService(ServiceDeps{
		Repo: newFakeRepo(), Messages: f, Catalog: fakeCatalog{}, Audit: &fakeAudit{},
		AdminSecret: "admin-secret-value-016", Gen: fixedGen{}, Registerer: prometheus.NewRegistry(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return Handler(svc)
}

// TestDeliveryStateContract pins the bounded response, the omitted queue
// position outside the queue, 404 message_not_found, 409 for inconsistent
// state, 503 for an unavailable store, and identifier validation.
func TestDeliveryStateContract(t *testing.T) {
	path := "/admin/v1/messages/" + dlqID + "/delivery-state"
	for name, tc := range map[string]struct {
		state  DeliveryState
		err    error
		status int
		body   string
	}{
		"queued behind head": {DeliveryState{Found: true, State: "queued", DeliveryCycle: 2, QueuePosition: "behind_head"}, nil, 200,
			`{"message_id":"` + dlqID + `","delivery_cycle":2,"state":"queued","queue_position":"behind_head"}`},
		"acknowledged": {DeliveryState{Found: true, State: "acknowledged", DeliveryCycle: 3}, nil, 200,
			`{"message_id":"` + dlqID + `","delivery_cycle":3,"state":"acknowledged"}`},
		"not found":    {DeliveryState{}, nil, 404, `"code":"message_not_found"`},
		"inconsistent": {DeliveryState{Inconsistent: "queued_delivery_state_missing"}, nil, 409, `queued_delivery_state_missing`},
		"unavailable":  {DeliveryState{}, errors.New("down"), 503, `"code":"dependency_unavailable"`},
	} {
		f := &fakeMessages{state: tc.state, err: tc.err}
		rec := doJSON(t, deliveryStateHandler(t, f), http.MethodGet, path, "admin-secret-value-016", "")
		if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.body) {
			t.Errorf("%s: %d %s", name, rec.Code, rec.Body.String())
		}
	}
	f := &fakeMessages{}
	h := deliveryStateHandler(t, f)
	if rec := doJSON(t, h, http.MethodGet, "/admin/v1/messages/not-a-uuid/delivery-state", "admin-secret-value-016", ""); rec.Code != 404 || len(f.reads) != 0 {
		t.Errorf("malformed id = %d, reads %d", rec.Code, len(f.reads))
	}
	if rec := doJSON(t, h, http.MethodGet, path, "wrong", ""); rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated = %d", rec.Code)
	}
}

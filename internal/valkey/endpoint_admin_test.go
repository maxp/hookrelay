package valkey

import (
	"context"
	"testing"

	"github.com/maxp/hookrelay/internal/administration"
)

// createAt creates an endpoint and pins its listing score and created_ms,
// so tests control the list order.
func createAt(t *testing.T, a *Adapter, id string, createdMs string) {
	t.Helper()
	e := endpointFixture()
	e.Identifier = id
	if _, _, r := a.CreateEndpoint(context.Background(), e, "event-"+id, "webhook_endpoint_created", "req"); r != CreateOK {
		t.Fatalf("create %s = %s", id, r)
	}
	a.testDo(t, "ZADD", "hr1:webhooks", createdMs, "telegram:"+id)
	a.testDo(t, "HSET", "hr1:wh:telegram:"+id, "created_ms", createdMs)
}

// TestListEndpoints pins the listing read: descending created_ms, equal
// scores by descending member, strict-after cursors across the tie, and
// orphan members reported (missing, wrong type, malformed) rather than
// failing the page.
func TestListEndpoints(t *testing.T) {
	a := testAdapter(t, false)
	ctx := context.Background()
	flushAll(t, a)
	gate(t, a, false)
	createAt(t, a, "wh_a", "100")
	createAt(t, a, "wh_b1", "200")
	createAt(t, a, "wh_b2", "200")
	createAt(t, a, "wh_c", "300")
	a.testDo(t, "ZADD", "hr1:webhooks", "400", "telegram:wh_gone")
	a.testDo(t, "ZADD", "hr1:webhooks", "350", "telegram:wh_str")
	a.testDo(t, "SET", "hr1:wh:telegram:wh_str", "x")
	createAt(t, a, "wh_bad", "50")
	a.testDo(t, "HDEL", "hr1:wh:telegram:wh_bad", "generation_id")
	store := NewEndpointStore(a)

	page, err := store.ListEndpoints(ctx, 4, nil)
	if err != nil || len(page) != 4 {
		t.Fatalf("page 1 = %+v, %v", page, err)
	}
	want := []struct{ member, orphan string }{
		{"telegram:wh_gone", administration.OrphanMissing},
		{"telegram:wh_str", administration.OrphanWrongType},
		{"telegram:wh_c", ""},
		{"telegram:wh_b2", ""},
	}
	for i, w := range want {
		if page[i].Member != w.member || page[i].Orphan != w.orphan || (w.orphan == "") != (page[i].Endpoint != nil) {
			t.Errorf("item %d = %+v, want %+v", i, page[i], w)
		}
	}
	if e := page[2].Endpoint; e.Identifier != "wh_c" || e.CreatedMs != 300 || e.BotID != "123456789" || e.ConfigVersion != 1 {
		t.Errorf("endpoint = %+v", e)
	}

	page, err = store.ListEndpoints(ctx, 4, &administration.EndpointCursor{CreatedMs: 200, ID: "telegram:wh_b2"})
	if err != nil || len(page) != 3 || page[0].Member != "telegram:wh_b1" || page[1].Member != "telegram:wh_a" ||
		page[2].Member != "telegram:wh_bad" || page[2].Orphan != administration.OrphanMalformed {
		t.Fatalf("page 2 = %+v, %v", page, err)
	}

	flushAll(t, a)
	if page, err := store.ListEndpoints(ctx, 10, nil); err != nil || len(page) != 0 {
		t.Errorf("empty listing = %+v, %v", page, err)
	}
}

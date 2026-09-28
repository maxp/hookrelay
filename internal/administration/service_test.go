package administration

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// --- fakes ------------------------------------------------------------------

type fakeRepo struct {
	endpoints     map[string]*Endpoint
	createCalls   int
	failWrongType bool
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{endpoints: map[string]*Endpoint{}}
}

func key(t, id string) string { return t + ":" + id }

func (f *fakeRepo) CreateEndpoint(_ context.Context, e Endpoint, _, _, _ string) (int64, int64, CreateEndpointResult) {
	f.createCalls++
	if f.failWrongType {
		return 0, 0, CreateWrongType
	}
	if _, exists := f.endpoints[key(e.Type, e.Identifier)]; exists {
		return 0, 0, CreateConflict
	}
	if len(f.endpoints) >= 100 {
		return 0, 0, CreateBotLimit
	}
	e.CreatedMs, e.UpdatedMs, e.ConfigVersion = 1740000000000, 1740000000000, 1
	f.endpoints[key(e.Type, e.Identifier)] = &e
	return e.CreatedMs, e.UpdatedMs, CreateOK
}

func (f *fakeRepo) GetEndpoint(_ context.Context, webhookType, identifier string) (*Endpoint, error) {
	if f.failWrongType {
		return nil, ErrStoredWrongType
	}
	if e, ok := f.endpoints[key(webhookType, identifier)]; ok {
		return e, nil
	}
	return nil, nil
}

type fakeCatalog struct{}

func (fakeCatalog) Lookup(webhookType string) (string, []string, bool) {
	if webhookType == "telegram" {
		return "telegram", []string{"secret_token"}, true
	}
	return "", nil, false
}

type fakeAudit struct {
	rejected int
}

func (f *fakeAudit) AppendRejectedAuth(context.Context, string, string, string) { f.rejected++ }

type fixedGen struct{ uuid string }

func (fixedGen) UUIDv7() string { return "0195-uuid" }
func (fixedGen) Base64URL(int) (string, error) {
	return "AAAAAAAAAAAAAAAAAAAAAA", nil
}

func testService(t *testing.T, repo *fakeRepo) (*Service, *fakeAudit) {
	t.Helper()
	audit := &fakeAudit{}
	return NewService(repo, fakeCatalog{}, audit, "admin-secret-value-016", fixedGen{}), audit
}

func doJSON(t *testing.T, h http.Handler, method, path, secret, body string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if secret != "" {
		req.Header.Set("Authorization", "Bearer "+secret)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

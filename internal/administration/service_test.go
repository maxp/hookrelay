package administration

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/maxp/hookrelay/internal/observability"
)

// --- fakes ------------------------------------------------------------------

type fakeRepo struct {
	endpoints     map[string]*Endpoint
	createCalls   int
	failWrongType bool
	createResult  CreateEndpointResult // forced create outcome when set
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{endpoints: map[string]*Endpoint{}}
}

func key(t, id string) string { return t + ":" + id }

func (f *fakeRepo) CreateEndpoint(_ context.Context, e Endpoint, _, _, _ string) (int64, int64, CreateEndpointResult) {
	f.createCalls++
	if f.createResult != "" {
		return 0, 0, f.createResult
	}
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
	// Like the endpoint Hash, the fake does not persist the Bot Platform.
	e.BotPlatform = ""
	f.endpoints[key(e.Type, e.Identifier)] = &e
	return e.CreatedMs, e.UpdatedMs, CreateOK
}

func (f *fakeRepo) GetEndpoint(_ context.Context, webhookType, identifier string) (*Endpoint, error) {
	if f.failWrongType {
		return nil, ErrStoredWrongType
	}
	if e, ok := f.endpoints[key(webhookType, identifier)]; ok {
		stored := *e
		return &stored, nil
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
	fail     bool
}

func (f *fakeAudit) AppendRejectedAuth(context.Context, string, string, string) error {
	f.rejected++
	if f.fail {
		return errors.New("audit unavailable")
	}
	return nil
}

type fixedGen struct{ uuid string }

func (fixedGen) UUIDv7() string { return "0195-uuid" }
func (fixedGen) Base64URL(int) (string, error) {
	return "AAAAAAAAAAAAAAAAAAAAAA", nil
}

func testService(t *testing.T, repo *fakeRepo) (*Service, *fakeAudit) {
	t.Helper()
	svc, audit, _, _ := observedService(t, repo)
	return svc, audit
}

// observedService also returns the captured log output and the metrics
// registry so tests can assert feature events and audit metrics.
func observedService(t *testing.T, repo *fakeRepo) (*Service, *fakeAudit, *bytes.Buffer, *prometheus.Registry) {
	t.Helper()
	audit := &fakeAudit{}
	logs := &bytes.Buffer{}
	reg := prometheus.NewRegistry()
	svc, err := NewService(ServiceDeps{
		Repo:        repo,
		Catalog:     fakeCatalog{},
		Audit:       audit,
		AdminSecret: "admin-secret-value-016",
		Gen:         fixedGen{},
		Logger:      observability.NewTestLogger("info", logs),
		Registerer:  reg,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc, audit, logs, reg
}

// counterValue reads one labelled counter series from the registry.
func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	metric:
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if labels[lp.GetName()] != lp.GetValue() {
					continue metric
				}
			}
			return m.GetCounter().GetValue()
		}
	}
	t.Fatalf("metric %s%v not found", name, labels)
	return 0
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

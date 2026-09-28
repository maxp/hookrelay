package cli

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestVersionHumanAndJSON pins both output forms.
func TestVersionHumanAndJSON(t *testing.T) {
	var buf bytes.Buffer
	if code := Version([]string{}, &buf); code != ExitOK {
		t.Fatalf("human version exit = %d", code)
	}
	if !strings.Contains(buf.String(), "hookrelay dev") || !strings.Contains(buf.String(), "commit:") {
		t.Errorf("human output = %q", buf.String())
	}

	buf.Reset()
	if code := Version([]string{"--json"}, &buf); code != ExitOK {
		t.Fatalf("json version exit = %d", code)
	}
	var decoded map[string]string
	if err := json.Unmarshal(buf.Bytes(), &decoded); err != nil {
		t.Fatalf("json output not valid JSON: %v\n%s", err, buf.String())
	}
	for _, key := range []string{"version", "commit", "build_time", "dirty"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("missing key %q in %v", key, decoded)
		}
	}
}

// TestGenerateKindsAndFileOutput pins the generator contract: base64url
// secrets with 256-bit entropy, the wh_ identifier form, stdout-vs-file
// behavior, and no overwriting without --force.
func TestGenerateKindsAndFileOutput(t *testing.T) {
	// Generators write to os.Stdout directly; route through file mode for
	// deterministic capture, then verify shapes.
	dir := t.TempDir()
	path := filepath.Join(dir, "consumer")

	if code := Generate([]string{"consumer-secret", "--output-file", path}); code != ExitOK {
		t.Fatalf("consumer-secret exit = %d", code)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	value := strings.TrimRight(string(data), "\n")
	if len(value) != 43 { // 32 bytes → 43 base64url chars without padding
		t.Errorf("consumer secret length = %d, want 43", len(value))
	}
	if strings.ContainsAny(value, "+/=") {
		t.Errorf("consumer secret not base64url-unpadded: %q", value)
	}

	if code := Generate([]string{"webhook-id", "--output-file", path}); code != ExitUsage {
		t.Errorf("overwrite without --force exit = %d, want usage error", code)
	}
	if code := Generate([]string{"webhook-id", "--output-file", path, "--force"}); code != ExitOK {
		t.Fatalf("webhook-id --force exit = %d", code)
	}
	data, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimRight(string(data), "\n")
	if !strings.HasPrefix(id, "wh_") || len(id) != 3+22 {
		t.Errorf("webhook id = %q, want wh_<22 base64url chars>", id)
	}
}

// TestHealthcheckContract pins silent success on 2xx and failure otherwise.
func TestHealthcheckContract(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	if code := Healthcheck([]string{"--url", ok.URL}); code != ExitOK {
		t.Errorf("healthy endpoint exit = %d, want 0", code)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer bad.Close()
	if code := Healthcheck([]string{"--url", bad.URL}); code != ExitError {
		t.Errorf("unhealthy endpoint exit = %d, want 1", code)
	}

	if code := Healthcheck([]string{"--url", "http://127.0.0.1:1"}); code != ExitError {
		t.Errorf("unreachable endpoint exit = %d, want 1", code)
	}
}

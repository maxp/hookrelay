package observability

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

// TestLogEnvelope pins the structured-log contract: snake_case fields,
// integer timestamp_ms, service hookrelay, version present, bounded level
// names, and message instead of msg.
func TestLogEnvelope(t *testing.T) {
	SetBuildVersion("test-1")
	var buf bytes.Buffer
	log := NewTestLogger("info", &buf)

	LogEvent(log, slog.LevelInfo, "webhook_accepted", "accepted a webhook request", "request_id", "0195", "bot_platform", "telegram")
	line := buf.String()
	if strings.Count(line, "\n") != 1 {
		t.Fatalf("expected one log line, got %q", line)
	}

	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("log line is not JSON: %v\n%s", err, line)
	}
	for _, key := range []string{"timestamp_ms", "level", "service", "version", "event", "message", "request_id"} {
		if _, ok := record[key]; !ok {
			t.Errorf("missing required field %q in %v", key, record)
		}
	}
	if record["service"] != "hookrelay" {
		t.Errorf("service = %v", record["service"])
	}
	if record["version"] != "test-1" {
		t.Errorf("version = %v", record["version"])
	}
	if record["event"] != "webhook_accepted" {
		t.Errorf("event = %v", record["event"])
	}
	if record["message"] != "accepted a webhook request" {
		t.Errorf("message should carry the human-readable description, got %v", record["message"])
	}
	if record["level"] != "info" {
		t.Errorf("level = %v, want lowercase bounded name", record["level"])
	}
	if ts, ok := record["timestamp_ms"].(float64); !ok || ts <= 0 || ts != float64(int64(ts)) {
		t.Errorf("timestamp_ms must be an integer epoch millis, got %v", record["timestamp_ms"])
	}
	if _, ok := record["time"]; ok {
		t.Error("default time key leaked into output")
	}
	if _, ok := record["msg"]; ok {
		t.Error("default msg key leaked into output")
	}
}

// TestLogLevelFiltering pins startup-level selection with no runtime API.
func TestLogLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	log := NewTestLogger("warn", &buf)
	log.Debug("debug_event")
	if buf.Len() != 0 {
		t.Errorf("debug record emitted at warn level: %s", buf.String())
	}
	log.Warn("warn_event")
	if !strings.Contains(buf.String(), `"level":"warn"`) {
		t.Errorf("warn record missing: %s", buf.String())
	}
}

// TestLevelNames pins the bounded level vocabulary.
func TestLevelNames(t *testing.T) {
	for tc, want := range map[slog.Level]string{
		slog.LevelDebug: "debug",
		slog.LevelInfo:  "info",
		slog.LevelWarn:  "warn",
		slog.LevelError: "error",
	} {
		if got := levelName(tc); got != want {
			t.Errorf("levelName(%v) = %q, want %q", tc, got, want)
		}
	}
}

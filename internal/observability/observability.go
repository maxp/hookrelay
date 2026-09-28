// Package observability constructs structured logging, the Prometheus
// registry, and shared redaction conventions. It does not absorb feature
// behavior into a generic facade.
package observability

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// service and version are stamped onto every log record by the handler.
var (
	serviceName  = "hookrelay"
	buildVersion = "dev"
)

// SetBuildVersion records the embedded build version for log records. Serve
// calls it once at startup before any logging.
func SetBuildVersion(version string) {
	if version != "" {
		buildVersion = version
	}
}

// NewLogger builds the application logger: structured JSON to standard
// output with the bounded envelope (timestamp_ms, level, service, version,
// event, message). Centralized attribute handling emits timestamp_ms and
// message rather than the default time and msg keys. The level is selected
// at startup; there is no runtime level API.
func NewLogger(level string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.Int64("timestamp_ms", t.UnixMilli())
				}
				return slog.Attr{}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String("level", levelName(l))
				}
				return a
			case slog.MessageKey:
				return slog.String("message", a.Value.String())
			}
			return a
		},
	})
	return slog.New(&envelopeHandler{inner: h})
}

// LogEvent records a feature event with the required bounded event name and
// a short human-readable message. Feature modules use this instead of calling
// slog directly so the envelope stays uniform.
func LogEvent(l *slog.Logger, level slog.Level, event, message string, args ...any) {
	if !l.Enabled(context.Background(), level) {
		return
	}
	l.Log(context.Background(), level, message, append([]any{"event", event}, args...)...)
}

func parseLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func levelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return "error"
	case l >= slog.LevelWarn:
		return "warn"
	case l >= slog.LevelInfo:
		return "info"
	default:
		return "debug"
	}
}

// envelopeHandler stamps the required service and version fields onto every
// record and drops them from per-call duplication.
type envelopeHandler struct {
	inner slog.Handler
}

func (h *envelopeHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return h.inner.Enabled(ctx, l)
}

func (h *envelopeHandler) Handle(ctx context.Context, r slog.Record) error {
	r.AddAttrs(
		slog.String("service", serviceName),
		slog.String("version", buildVersion),
	)
	return h.inner.Handle(ctx, r)
}

func (h *envelopeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &envelopeHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *envelopeHandler) WithGroup(name string) slog.Handler {
	return &envelopeHandler{inner: h.inner.WithGroup(name)}
}

// NewTestLogger writes envelope-compliant JSON into the provided buffer —
// used by tests to assert the log contract.
func NewTestLogger(level string, w writer) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: parseLevel(level),
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch a.Key {
			case slog.TimeKey:
				if t, ok := a.Value.Any().(time.Time); ok {
					return slog.Int64("timestamp_ms", t.UnixMilli())
				}
				return slog.Attr{}
			case slog.LevelKey:
				if l, ok := a.Value.Any().(slog.Level); ok {
					return slog.String("level", levelName(l))
				}
				return a
			case slog.MessageKey:
				return slog.String("message", a.Value.String())
			}
			return a
		},
	})
	return slog.New(&envelopeHandler{inner: h})
}

type writer interface {
	Write(p []byte) (int, error)
}

// NewMetricsRegistry returns a private Prometheus registry. hookrelay never
// registers collectors on global defaults.
func NewMetricsRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

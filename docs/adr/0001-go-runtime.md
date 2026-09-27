# Use Go for the application runtime

hookrelay will be implemented in Go. Go was selected over TypeScript on Node.js or Bun and Clojure/JVM because it provides a mature standard HTTP server, straightforward bounded-concurrency primitives, a small deployable artifact, and mature Valkey, Prometheus, and OpenTelemetry support with comparatively low operational complexity. The first implementation will use one codebase and one main process, with boundaries that allow maintenance work to be split into another entry point later.

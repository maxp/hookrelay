# Serve the operational UI as embedded static files without a build step

Milestone 4 ships the operational panel as plain HTML, CSS, and JavaScript ES modules embedded in the Go binary with `//go:embed` and served on the administrative listener. There is no bundler, no Node toolchain, no frontend framework, and no server-side templating: the pages are static and fetch everything through the same JSON Admin API the CLI uses, authenticated by the browser session cookie.

The panel is small (a handful of polled read views and three actions), so a build pipeline would add a second toolchain, a supply-chain surface, and CI steps for little benefit. Static assets keep the single distroless binary, let one strict Content Security Policy (`script-src 'self'`, no inline script or style) apply to every page, and keep all authorization decisions in the API rather than in rendered HTML. Because webhook payloads are attacker-influenced, the UI renders data only through DOM APIs and `textContent`, never `innerHTML`.

The trade-off is hand-written DOM code and no JavaScript unit-test runner; UI behavior is kept thin, the API contract carries the logic and its tests, and Go tests enforce the asset and header rules. If the panel grows into a control plane, revisit this decision rather than accreting a framework piecemeal.

package administration

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

// uiFiles is the operational panel: static HTML, CSS, and JavaScript
// modules without a build step (ADR 0009). The assets carry no data; every
// view reads the JSON Admin API with the browser session.
//
//go:embed ui
var uiFiles embed.FS

// uiContentSecurityPolicy allows only same-origin scripts, styles, and API
// calls, with no inline script or style, no framing, and no form posts
// elsewhere.
const uiContentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; " +
	"img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'"

// uiHandler serves the embedded panel under /ui/. Directories other than
// the root are not listed.
func uiHandler() http.Handler {
	root, err := fs.Sub(uiFiles, "ui")
	if err != nil {
		panic(err) // the embedded tree is fixed at build time
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", uiContentSecurityPolicy)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cache-Control", "no-cache")
		name := strings.TrimPrefix(r.URL.Path, "/ui/")
		if name == "" {
			name = "index.html"
		}
		info, err := fs.Stat(root, name)
		if err != nil || info.IsDir() {
			http.NotFound(w, r)
			return
		}
		http.ServeFileFS(w, r, root, name)
	})
}

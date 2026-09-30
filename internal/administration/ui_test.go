package administration

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path"
	"regexp"
	"strings"
	"testing"
)

func uiService(t *testing.T) http.Handler {
	t.Helper()
	svc, err := NewService(ServiceDeps{Repo: newFakeRepo(), Sessions: &fakeSessions{}, AdminOrigin: testOrigin,
		Catalog: fakeCatalog{}, Audit: &fakeAudit{}, AdminSecret: "admin-secret-value-016", Gen: fixedGen{}})
	if err != nil {
		t.Fatal(err)
	}
	return Handler(svc)
}

// TestUIServing pins asset serving: the index at /ui/, the security
// headers on every asset, JavaScript served as a module type, no directory
// listing, 404 for unknown files, and / redirecting to the panel.
func TestUIServing(t *testing.T) {
	h := uiService(t)
	get := func(p string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, p, nil))
		return rec
	}
	for _, p := range []string{"/ui/", "/ui/app.js", "/ui/app.css", "/ui/views/overview.js"} {
		rec := get(p)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d", p, rec.Code)
		}
		want := map[string]string{
			"Content-Security-Policy": uiContentSecurityPolicy, "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
			"X-Frame-Options": "DENY", "Cache-Control": "no-cache",
		}
		for k, v := range want {
			if rec.Header().Get(k) != v {
				t.Errorf("%s %s = %q", p, k, rec.Header().Get(k))
			}
		}
	}
	if rec := get("/ui/"); !strings.Contains(rec.Body.String(), `<script type="module" src="app.js">`) ||
		!strings.HasPrefix(rec.Header().Get("Content-Type"), "text/html") {
		t.Errorf("index = %q %s", rec.Header().Get("Content-Type"), rec.Body.String())
	}
	if ct := get("/ui/app.js").Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Errorf("app.js Content-Type = %q", ct)
	}
	for _, p := range []string{"/ui/views/", "/ui/missing.js", "/ui/../http.go"} {
		if rec := get(p); rec.Code == http.StatusOK {
			t.Errorf("%s = %d %s", p, rec.Code, rec.Body.String())
		}
	}
	if rec := get("/"); rec.Code != http.StatusFound || rec.Header().Get("Location") != "/ui/" {
		t.Errorf("/ = %d %q", rec.Code, rec.Header().Get("Location"))
	}
	if rec := get("/ui"); rec.Code/100 != 3 || rec.Header().Get("Location") != "/ui/" {
		t.Errorf("/ui = %d %q", rec.Code, rec.Header().Get("Location"))
	}
}

var (
	inlineScript  = regexp.MustCompile(`<script\b[^>]*>\s*[^<\s]`)
	scriptTag     = regexp.MustCompile(`<script\b[^>]*>`)
	handlerAttr   = regexp.MustCompile(`(?i)\son[a-z]+\s*=`)
	styleAttr     = regexp.MustCompile(`(?i)\sstyle\s*=|<style\b`)
	assetRef      = regexp.MustCompile(`(?:src|href)="([^"#:]+)"`)
	moduleImport  = regexp.MustCompile(`(?m)^\s*import\b[^"]*"([^"]+)"`)
	htmlSinks     = regexp.MustCompile(`innerHTML|outerHTML|insertAdjacentHTML|document\.write|\beval\(|new Function|setAttribute\(\s*["'](?:style|on)`)
	externalFetch = regexp.MustCompile(`fetch\(\s*["'\x60]https?:`)
)

// TestUIAssetRules enforces ADR 0009 statically: no inline script, style,
// or event-handler markup (the CSP would block it), every referenced asset
// and module import embedded, and no HTML-string sinks or external fetches
// in the JavaScript, so data is only ever rendered as text.
func TestUIAssetRules(t *testing.T) {
	root, err := fs.Sub(uiFiles, "ui")
	if err != nil {
		t.Fatal(err)
	}
	var files int
	err = fs.WalkDir(root, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files++
		data, err := fs.ReadFile(root, name)
		if err != nil {
			return err
		}
		src := string(data)
		dir := path.Dir(name)
		exists := func(ref string) bool {
			_, err := fs.Stat(root, path.Join(dir, ref))
			return err == nil
		}
		switch path.Ext(name) {
		case ".html":
			for _, re := range []*regexp.Regexp{inlineScript, handlerAttr, styleAttr} {
				if m := re.FindString(src); m != "" {
					t.Errorf("%s: forbidden markup %q", name, m)
				}
			}
			for _, m := range scriptTag.FindAllString(src, -1) {
				if !strings.Contains(m, " src=") {
					t.Errorf("%s: script without src %q", name, m)
				}
			}
			for _, m := range assetRef.FindAllStringSubmatch(src, -1) {
				if !exists(m[1]) {
					t.Errorf("%s: missing asset %q", name, m[1])
				}
			}
		case ".js":
			if m := htmlSinks.FindString(src); m != "" {
				t.Errorf("%s: forbidden sink %q", name, m)
			}
			if m := externalFetch.FindString(src); m != "" {
				t.Errorf("%s: external fetch %q", name, m)
			}
			for _, m := range moduleImport.FindAllStringSubmatch(src, -1) {
				if !strings.HasPrefix(m[1], ".") || !exists(m[1]) {
					t.Errorf("%s: unresolvable import %q", name, m[1])
				}
			}
		case ".css":
			if strings.Contains(src, "@import") || strings.Contains(src, "url(http") {
				t.Errorf("%s: external stylesheet reference", name)
			}
		default:
			t.Errorf("unexpected embedded file %s", name)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 5 {
		t.Errorf("only %d embedded files", files)
	}
}

// TestUIRulePatterns proves the static rules catch what they forbid.
func TestUIRulePatterns(t *testing.T) {
	for re, bad := range map[*regexp.Regexp]string{
		inlineScript:  `<script>alert(1)</script>`,
		handlerAttr:   `<button onclick="x()">`,
		styleAttr:     `<div style="color:red">`,
		htmlSinks:     `node.innerHTML = data`,
		externalFetch: `fetch("https://evil.example/x")`,
	} {
		if !re.MatchString(bad) {
			t.Errorf("%v does not match %q", re, bad)
		}
	}
	for _, good := range []string{`<script type="module" src="app.js"></script>`, `node.textContent = data`} {
		if inlineScript.MatchString(good) || htmlSinks.MatchString(good) {
			t.Errorf("rule matches allowed %q", good)
		}
	}
}

// Mount matrix (KTD13): the SAME admin handler instance registered behind
// gin, echo, chi, and the stdlib mux — at the /admin prefix and at the root —
// and driven with real HTTP through each router, plus the browser-hardening
// response headers (KTD11). The router libraries are test-only dependencies
// of this file; library code imports none of them (R13).
package admin_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"
	"github.com/labstack/echo/v5"
)

// newTestHandler (admin_test.go) builds the fake-backed handler the matrix
// registers in each router; serve wraps one in an httptest server.

func serve(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts
}

// The four mount recipes exactly as the README documents them. No router
// wraps http.StripPrefix — the handler self-strips (KTD3).

func ginMount(h http.Handler) http.Handler {
	gin.SetMode(gin.ReleaseMode) // suppress [GIN-debug] route print
	r := gin.New()               // no Logger/Recovery middleware noise
	r.Any("/admin/*any", gin.WrapH(h))
	return r
}

func echoMount(h http.Handler) http.Handler {
	// echo v5 prints no banner unless Start is called (it never is here),
	// so nothing to suppress.
	e := echo.New()
	e.Any("/admin/*", echo.WrapHandler(h))
	e.Any("/admin", echo.WrapHandler(h)) // echo needs the bare route too
	return e
}

func chiMount(h http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Mount("/admin", h)
	return r
}

func stdlibMount(h http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/admin/", h)
	return mux
}

// --- AE7: prefix mount under all four routers ---

func TestMountMatrixPrefix(t *testing.T) {
	routers := []struct {
		name  string
		mount func(http.Handler) http.Handler
	}{
		{"gin", ginMount},
		{"echo", echoMount},
		{"chi", chiMount},
		{"stdlib", stdlibMount},
	}
	for _, rt := range routers {
		t.Run(rt.name, func(t *testing.T) {
			h, _ := newTestHandler(t, "/admin")
			ts := serve(t, rt.mount(h))

			// Dashboard serves behind the router, links carrying the prefix.
			res, body := doGet(t, ts.Client(), ts.URL+"/admin/")
			if res.StatusCode != http.StatusOK {
				t.Fatalf("GET /admin/: status = %d, want 200", res.StatusCode)
			}
			if !strings.Contains(body, `href="/admin/topics/1"`) {
				t.Fatal("dashboard links lack the /admin prefix")
			}

			// The bare mount path redirects to the prefixed root — the
			// generated-redirect-preserves-prefix proof (AE7).
			res, _ = doGet(t, noRedirect(), ts.URL+"/admin")
			if res.StatusCode < 300 || res.StatusCode >= 400 {
				t.Fatalf("GET /admin: status = %d, want 3xx", res.StatusCode)
			}
			if loc := res.Header.Get("Location"); loc != "/admin/" {
				t.Fatalf("GET /admin: Location = %q, want /admin/", loc)
			}
		})
	}
}

// TestGinMountCreateTopic drives a full mutation through gin's Any+WrapH
// recipe: the POST reaches the mux post-strip, the store creates the topic,
// and the redirect Location carries the prefix.
func TestGinMountCreateTopic(t *testing.T) {
	h, f := newTestHandler(t, "/admin")
	ts := serve(t, ginMount(h))

	res, _ := doPost(t, noRedirect(), ts.URL+"/admin/topics", url.Values{"name": {"payments"}})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /admin/topics: status = %d, want 303", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/topics/4" {
		t.Fatalf("POST /admin/topics: Location = %q, want /admin/topics/4", loc)
	}
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/4")
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "payments") {
		t.Fatalf("GET /admin/topics/4: status = %d, want 200 with the new topic", res.StatusCode)
	}
	if got := len(f.topicNames()); got != 2 {
		t.Fatalf("topics after create = %d, want 2 (orders + payments)", got)
	}
}

// TestEchoMountDeadListPage exercises the dead-letter browse through echo —
// the payload-carrying surface (R4/R12) — verifying rows and prefix-aware
// links behind the echo recipe.
func TestEchoMountDeadListPage(t *testing.T) {
	h, f := newTestHandler(t, "/admin")
	ts := serve(t, echoMount(h))
	id := f.addDead(2, []byte("poison-through-echo"), 5, time.Now().Add(time.Hour))

	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /admin/channels/2/dead: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "poison-through-echo") {
		t.Fatal("dead list through echo: body preview missing")
	}
	if !strings.Contains(body, `href="/admin/channels/2/dead/`+strconv.FormatInt(id, 10)+`"`) {
		t.Fatal("dead list through echo: row link lacks the /admin prefix")
	}
}

// TestChiMountDeleteTopicRedirectKeepsPrefix proves a delete POST through
// chi's Mount lands post-strip and its redirect Location keeps the prefix.
func TestChiMountDeleteTopicRedirectKeepsPrefix(t *testing.T) {
	h, _ := newTestHandler(t, "/admin")
	ts := serve(t, chiMount(h))

	res, _ := doPost(t, noRedirect(), ts.URL+"/admin/topics/1/delete", nil)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("POST /admin/topics/1/delete: status = %d, want 303", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/" {
		t.Fatalf("POST delete: Location = %q, want /admin/ (prefix preserved)", loc)
	}
}

// --- AE7: root mount (stdlib + one framework router) ---

func TestMountMatrixRoot(t *testing.T) {
	routers := []struct {
		name  string
		mount func(http.Handler) http.Handler
	}{
		{"stdlib", func(h http.Handler) http.Handler {
			mux := http.NewServeMux()
			mux.Handle("/", h)
			return mux
		}},
		{"chi", func(h http.Handler) http.Handler {
			r := chi.NewRouter()
			r.Mount("/", h)
			return r
		}},
	}
	for _, rt := range routers {
		t.Run(rt.name, func(t *testing.T) {
			h, _ := newTestHandler(t, "/")
			ts := serve(t, rt.mount(h))

			// Pages serve at / with root-relative links.
			res, body := doGet(t, ts.Client(), ts.URL+"/")
			if res.StatusCode != http.StatusOK {
				t.Fatalf("GET /: status = %d, want 200", res.StatusCode)
			}
			if !strings.Contains(body, `href="/topics/1"`) {
				t.Fatal("root mount: dashboard links are not root-relative")
			}
			if strings.Contains(body, `"/admin`) {
				t.Fatal("root mount: leaked /admin prefix in body")
			}

			// Generated redirects stay root-relative.
			res, _ = doPost(t, noRedirect(), ts.URL+"/topics/1/delete", nil)
			if res.StatusCode != http.StatusSeeOther {
				t.Fatalf("POST /topics/1/delete: status = %d, want 303", res.StatusCode)
			}
			if loc := res.Header.Get("Location"); loc != "/" {
				t.Fatalf("POST delete: Location = %q, want / (root-relative)", loc)
			}
		})
	}
}

// --- KTD11: asset MIME/cache and browser-hardening headers ---

func TestAssetAndSecurityHeaders(t *testing.T) {
	h, f := newTestHandler(t, "/admin")
	ts := serve(t, h)
	id := f.addDead(2, []byte("header-payload"), 5, time.Now().Add(time.Hour))

	// Static JS: explicit MIME map (http.DetectContentType never returns
	// application/javascript) plus an hour of shared caching.
	res, _ := doGet(t, ts.Client(), ts.URL+"/admin/static/admin.js")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("admin.js: status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/javascript") {
		t.Errorf("admin.js Content-Type = %q, want application/javascript", ct)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "public, max-age=3600" {
		t.Errorf("admin.js Cache-Control = %q, want public, max-age=3600", cc)
	}
	if xo := res.Header.Get("X-Content-Type-Options"); xo != "nosniff" {
		t.Errorf("admin.js X-Content-Type-Options = %q, want nosniff", xo)
	}

	// Static CSS: same cache policy, text/css from the map.
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/static/admin.css")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("admin.css: status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/css") {
		t.Errorf("admin.css Content-Type = %q, want text/css", ct)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "public, max-age=3600" {
		t.Errorf("admin.css Cache-Control = %q, want public, max-age=3600", cc)
	}

	// HTML pages: always revalidate, never frameable, no referrer leak.
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dashboard: status = %d, want 200", res.StatusCode)
	}
	if cc := res.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Errorf("page Cache-Control = %q, want no-cache", cc)
	}
	if xo := res.Header.Get("X-Frame-Options"); xo != "DENY" {
		t.Errorf("page X-Frame-Options = %q, want DENY", xo)
	}
	if rp := res.Header.Get("Referrer-Policy"); rp != "same-origin" {
		t.Errorf("page Referrer-Policy = %q, want same-origin", rp)
	}
	if xo := res.Header.Get("X-Content-Type-Options"); xo != "nosniff" {
		t.Errorf("page X-Content-Type-Options = %q, want nosniff", xo)
	}

	// JSON is never stored — including the dead-letter body endpoints, the
	// only APIs carrying payloads (R12).
	for _, path := range []string{
		"/admin/api/summary",
		"/admin/api/channels/2/dead",
		"/admin/api/channels/2/dead/" + strconv.FormatInt(id, 10),
	} {
		res, _ = doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status = %d, want 200", path, res.StatusCode)
		}
		if cc := res.Header.Get("Cache-Control"); cc != "no-store" {
			t.Errorf("GET %s Cache-Control = %q, want no-store", path, cc)
		}
		if xo := res.Header.Get("X-Content-Type-Options"); xo != "nosniff" {
			t.Errorf("GET %s X-Content-Type-Options = %q, want nosniff", path, xo)
		}
	}

	// The static directory itself answers 404, not a file listing.
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/static/")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /admin/static/: status = %d, want 404 (no directory listing)", res.StatusCode)
	}
}

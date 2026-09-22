package admin

import (
	"net/http"
	"path"
	"strings"
)

// Asset MIME and browser-hardening headers (KTD11). The middleware runs at
// the innermost, post-strip layer of New's chain, so its path tests see
// handler-relative paths (/static/…, /api/…, pages) regardless of the mount
// prefix:
//
//   - /static/*: an explicit MIME map for .js/.css — http.DetectContentType
//     never returns application/javascript, so the type is set from the
//     extension before the embedded file server can sniff — plus an hour of
//     shared caching (the asset set changes only with the binary).
//   - /api/*: Cache-Control: no-store — JSON is live data, and the
//     dead-letter endpoints are the only APIs carrying message bodies, which
//     must never sit in a shared cache (R12).
//   - every other response (the HTML pages): Cache-Control: no-store as
//     well (review #11) — every page renders live backlog/dead state, and
//     the dead-letter HTML pages carry message payloads, so no page is safe
//     to store even with revalidation; X-Frame-Options: DENY so admin pages
//     cannot be framed, and Referrer-Policy: same-origin so dead-body URLs
//     never leak out via a referrer.
//   - every response: X-Content-Type-Options: nosniff.
//
// The middleware also suppresses the plain directory listing the embedded
// file server would serve at exactly /static/ (a 404 instead).

// staticMIME holds the asset types set from the extension: the embedded
// files carry no MIME metadata, and sniffing is wrong for JavaScript.
var staticMIME = map[string]string{
	".js":  "application/javascript; charset=utf-8",
	".css": "text/css; charset=utf-8",
}

// isStaticPath reports whether the post-strip path addresses the static
// asset subtree (including its bare "/static" form, which the internal mux
// redirects to "/static/").
func isStaticPath(p string) bool {
	return p == "/static" || strings.HasPrefix(p, "/static/")
}

// securityHeaders wraps next with the asset/cache/security policy above.
func (h *handler) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		w.Header().Set("X-Content-Type-Options", "nosniff")
		switch {
		case isStaticPath(p):
			w.Header().Set("Cache-Control", "public, max-age=3600")
			if p == "/static/" {
				http.NotFound(w, r) // no directory listing
				return
			}
			if ct, ok := staticMIME[path.Ext(p)]; ok {
				w.Header().Set("Content-Type", ct)
			}
		case strings.HasPrefix(p, "/api/"):
			w.Header().Set("Cache-Control", "no-store")
		default:
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("X-Frame-Options", "DENY")
			w.Header().Set("Referrer-Policy", "same-origin")
		}
		next.ServeHTTP(w, r)
	})
}

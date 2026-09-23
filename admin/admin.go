// Package admin provides a mountable, zero-build admin UI and JSON API for a
// novaque topology: a dashboard with live per-channel backlog, topic/channel
// detail pages with day-bucket trends, and create/delete flows.
//
// New returns a single stdlib http.Handler. Mount it under gin, echo, chi, or
// net/http at any path prefix (Options.Prefix, default /admin; "/" mounts at
// the root). The handler strips its own prefix at the edge and re-applies it
// to every generated URL, so hosts never wrap http.StripPrefix.
//
// The admin never migrates and never creates rows on read paths: entities
// are addressed by the numeric IDs surfaced by the list endpoints (KTD6),
// and unknown IDs answer 404.
package admin

import (
	"bytes"
	"context"
	"crypto/subtle"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/usual2970/novaque"
)

// Options configure the admin handler.
type Options struct {
	// Prefix is the mount path the handler strips from incoming paths and
	// re-applies to every generated URL and redirect (KTD3). The zero value
	// mounts at the default /admin. Pass "/" to mount at the root:
	// normalization yields a leading slash and no trailing slash, and the
	// normalized empty prefix means root.
	Prefix string
	// BasicAuthUser and BasicAuthPass are an optional single basic-auth
	// pair (KTD5) — the only auth the package provides; real authentication
	// belongs in host middleware. Setting exactly one of the two is a
	// construction error. Both use constant-time compares when enabled.
	BasicAuthUser string
	BasicAuthPass string
}

const (
	// defaultPrefix is the mount path when Options.Prefix is unset.
	defaultPrefix = "/admin"
	// trendDays caps the trend/detail window: min(StatsRetentionDays, 30)
	// with the Client's default retention of 30. The Client does not export
	// its configured retention, so 30 — the retention default and the cap —
	// is requested from the store; days pruned by a shorter retention
	// simply zero-fill (R3).
	trendDays = 30
	// dateFormat renders UTC day buckets in pages and JSON.
	dateFormat = "2006-01-02"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static
var staticFS embed.FS

// errNotFound marks an unknown entity id; read paths map it to 404.
var errNotFound = errors.New("novaque/admin: id not found")

// handler is the constructed admin surface. New wires its middleware chain
// and returns it as an http.Handler.
type handler struct {
	client *novaque.Client
	prefix string
	pages  map[string]*template.Template
	static http.Handler
	mux    *http.ServeMux
}

// New builds the admin handler for client under opts (KTD1: the admin
// depends on the concrete Client — never a raw store — so cache eviction and
// stats flushes act on the app's own instance). It errors on a nil client
// and on a half-configured basic-auth pair (AE8).
func New(client *novaque.Client, opts Options) (http.Handler, error) {
	if client == nil {
		return nil, errors.New("novaque/admin: client is nil")
	}
	if (opts.BasicAuthUser == "") != (opts.BasicAuthPass == "") {
		return nil, errors.New("novaque/admin: basic auth requires both BasicAuthUser and BasicAuthPass")
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	h := &handler{client: client, prefix: normalizePrefix(prefix)}

	// Templates: Funcs before Parse, one set per page so each page file can
	// define its own "content" block against the shared layout (KTD10).
	// Every URL in markup flows through the single "url" helper (KTD3).
	fm := template.FuncMap{
		"url":     h.path,
		"heatmap": buildContributionGraph,
	}
	h.pages = make(map[string]*template.Template, 5)
	for _, page := range []string{"dashboard.html", "topic.html", "channel.html", "confirm.html", "dead.html"} {
		t, err := template.New("layout.html").Funcs(fm).ParseFS(templateFS, "templates/layout.html", "templates/"+page)
		if err != nil {
			return nil, fmt.Errorf("novaque/admin: parse %s: %w", page, err)
		}
		h.pages[page] = t
	}
	staticSub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("novaque/admin: static subdir: %w", err)
	}
	h.static = http.StripPrefix("/static/", http.FileServerFS(staticSub))

	h.mux = http.NewServeMux()
	h.routes()

	// KTD4 chain: prefix strip → basic auth → CrossOriginProtection → mux,
	// with the KTD11 header policy at the innermost (post-strip) layer.
	// All mutations are POST; the cross-origin protection rejects browser
	// cross-origin posts while same-origin forms and curl pass.
	var chain http.Handler = h.preservePrefixRedirects(h.securityHeaders(h.mux))
	var cop http.CrossOriginProtection
	chain = cop.Handler(chain)
	if opts.BasicAuthUser != "" {
		chain = h.basicAuth(chain, opts.BasicAuthUser, opts.BasicAuthPass)
	}
	chain = h.stripPrefix(chain)
	return chain, nil
}

// routes registers the internal (post-strip) route surface: pages, forms,
// JSON data endpoints, dead-letter shapes (U5), and static assets.
func (h *handler) routes() {
	m := h.mux
	// Pages.
	m.HandleFunc("GET /{$}", h.pageDashboard)
	m.HandleFunc("GET /topics/{id}", h.pageTopic)
	m.HandleFunc("GET /channels/{id}", h.pageChannel)
	m.HandleFunc("GET /topics/{id}/delete", h.pageConfirmDeleteTopic)
	m.HandleFunc("GET /channels/{id}/delete", h.pageConfirmDeleteChannel)
	// Dead-letter surface (U5): paginated browse with truncated bodies, the
	// full-body view, and the requeue/delete actions — the only surfaces
	// carrying message payloads (R12).
	m.HandleFunc("GET /channels/{id}/dead", h.pageDead)
	m.HandleFunc("GET /channels/{id}/dead/{deliveryID}", h.pageDeadDelivery)
	m.HandleFunc("GET /api/channels/{id}/dead", h.apiDeadList)
	m.HandleFunc("GET /api/channels/{id}/dead/{deliveryID}", h.apiDeadDelivery)
	m.HandleFunc("POST /channels/{id}/dead/{deliveryID}/requeue", h.formRequeueDead)
	m.HandleFunc("POST /channels/{id}/dead/{deliveryID}/delete", h.formDeleteDead)
	// Forms — every mutation is a POST (KTD4).
	m.HandleFunc("POST /topics", h.formCreateTopic)
	m.HandleFunc("POST /channels", h.formCreateChannel)
	m.HandleFunc("POST /topics/{id}/delete", h.formDeleteTopic)
	m.HandleFunc("POST /channels/{id}/delete", h.formDeleteChannel)
	// JSON data endpoints mirroring page data for the poller.
	m.HandleFunc("GET /api/summary", h.apiSummary)
	m.HandleFunc("GET /api/topics/{id}", h.apiTopic)
	m.HandleFunc("GET /api/channels/{id}", h.apiChannel)
	// Static assets (templates are a separate embed and never served).
	m.Handle("GET /static/", h.static)
}

// normalizePrefix canonicalizes a mount path: leading slash, no trailing
// slash. "/" collapses to "" — the normalized empty prefix means root mount
// (KTD3).
func normalizePrefix(p string) string {
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	for len(p) > 1 && strings.HasSuffix(p, "/") {
		p = strings.TrimSuffix(p, "/")
	}
	if p == "/" {
		return ""
	}
	return p
}

// path joins the mount prefix with the given segments — the single URL
// source for templates (the "url" FuncMap) and handlers alike (KTD3).
func (h *handler) path(parts ...any) string {
	var b strings.Builder
	b.WriteString(h.prefix)
	for _, p := range parts {
		switch v := p.(type) {
		case string:
			b.WriteString(v)
		case int:
			b.WriteString(strconv.Itoa(v))
		case int64:
			b.WriteString(strconv.FormatInt(v, 10))
		default:
			b.WriteString(fmt.Sprint(v))
		}
	}
	return b.String()
}

// stripPrefix removes the configured mount prefix with the empty-path guard
// http.StripPrefix lacks: a request to exactly the prefix redirects to the
// prefixed root (bare /admin → /admin/) instead of 404ing on an empty path
// (KTD3). Paths that merely share leading text with the prefix are 404s.
func (h *handler) stripPrefix(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.prefix == "" {
			next.ServeHTTP(w, r)
			return
		}
		switch {
		case r.URL.Path == h.prefix:
			target := h.prefix + "/"
			if r.URL.RawQuery != "" {
				target += "?" + r.URL.RawQuery
			}
			http.Redirect(w, r, target, http.StatusMovedPermanently)
		case strings.HasPrefix(r.URL.Path, h.prefix+"/"):
			r2 := r.Clone(r.Context())
			r2.URL.Path = r.URL.Path[len(h.prefix):]
			next.ServeHTTP(w, r2)
		default:
			http.NotFound(w, r)
		}
	})
}

// preservePrefixRedirects wraps next in a ResponseWriter that re-applies the
// mount prefix to redirect Locations the internal mux issues from
// already-stripped paths (stdlib ServeMux path-cleaning redirects lose the
// prefix). Locations this package generates itself already carry the prefix
// and pass through untouched (KTD3).
func (h *handler) preservePrefixRedirects(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.prefix == "" {
			next.ServeHTTP(w, r)
			return
		}
		next.ServeHTTP(&prefixRedirectWriter{ResponseWriter: w, prefix: h.prefix}, r)
	})
}

type prefixRedirectWriter struct {
	http.ResponseWriter
	prefix string
}

func (w *prefixRedirectWriter) WriteHeader(code int) {
	if code/100 == 3 {
		if loc := w.Header().Get("Location"); strings.HasPrefix(loc, "/") && !locHasPrefix(loc, w.prefix) {
			w.Header().Set("Location", w.prefix+loc)
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

// locHasPrefix reports whether loc already carries the mount prefix.
func locHasPrefix(loc, prefix string) bool {
	return loc == prefix || strings.HasPrefix(loc, prefix+"/")
}

// basicAuth enforces the optional single pair with constant-time compares
// (KTD5). U6 wires host-facing auth posture; the mechanics land here.
func (h *handler) basicAuth(next http.Handler, user, pass string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		userOK := ok && subtle.ConstantTimeCompare([]byte(u), []byte(user)) == 1
		passOK := ok && subtle.ConstantTimeCompare([]byte(p), []byte(pass)) == 1
		if !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="novaque admin", charset="UTF-8"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// --- view models ---

// baseView carries the fields every layout render needs. Poll is the
// page's un-prefixed JSON endpoint (e.g. /api/summary): the poller builds
// the full URL as BASE + Poll, with BASE the injected prefix constant.
type baseView struct {
	Prefix string
	Title  string
	Poll   string
}

// channelView is one channel row with its live backlog (zero-filled).
type channelView struct {
	ID      int64
	Name    string
	Backlog novaque.BacklogRow
}

// topicGroupView is one topic with its channels and summed backlog totals.
type topicGroupView struct {
	ID       int64
	Name     string
	Totals   novaque.BacklogRow
	Channels []channelView
}

// dayView is one zero-filled UTC day bucket for detail pages and heatmaps.
type dayView struct {
	novaque.DailyCounters
}

type dashboardView struct {
	baseView
	Groups      []topicGroupView
	CreateName  string
	CreateError string
}

type topicView struct {
	baseView
	ID           int64
	Name         string
	Totals       novaque.BacklogRow
	Channels     []channelView
	Days         []dayView
	ChannelName  string
	ChannelError string
}

type channelDetailView struct {
	baseView
	ID        int64
	Name      string
	TopicID   int64
	TopicName string
	Backlog   novaque.BacklogRow
	Days      []dayView
}

type confirmView struct {
	baseView
	Heading     string
	Lines       []string
	Warnings    []string
	ActionURL   string
	ActionLabel string
	CancelURL   string
}

// --- data assembly (bounded Client calls; no Ensure on read, KTD6) ---

// backlogsByChannel indexes one Backlogs batch for per-channel lookup;
// channels without a row zero-fill from the map's zero value.
func backlogsByChannel(backlogs []novaque.BacklogRow) map[int64]novaque.BacklogRow {
	byChannel := make(map[int64]novaque.BacklogRow, len(backlogs))
	for _, b := range backlogs {
		byChannel[b.ChannelID] = b
	}
	return byChannel
}

// channelsWithBacklog builds one topic's channel rows (in ListChannels
// order) with zero-filled backlogs, plus the topic's summed totals.
func channelsWithBacklog(channels []novaque.ChannelInfo, topicID int64, byChannel map[int64]novaque.BacklogRow) ([]channelView, novaque.BacklogRow) {
	var totals novaque.BacklogRow
	rows := []channelView{}
	for _, c := range channels {
		if c.TopicID != topicID {
			continue
		}
		b := byChannel[c.ID] // zero value zero-fills
		rows = append(rows, channelView{ID: c.ID, Name: c.Name, Backlog: b})
		totals.Pending += b.Pending
		totals.Ready += b.Ready
		totals.InFlight += b.InFlight
		totals.Dead += b.Dead
	}
	return rows, totals
}

// loadGroups assembles the dashboard/summary model from three Client calls
// — ListTopics, ListChannels, Backlogs — grouped in Go (F1). The query count
// is constant no matter how many channels exist (R2); Backlog rows appear
// only for channels with deliveries, so the rest zero-fill.
func (h *handler) loadGroups(ctx context.Context) ([]topicGroupView, error) {
	topics, err := h.client.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	channels, err := h.client.ListChannels(ctx)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	backlogs, err := h.client.Backlogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("backlogs: %w", err)
	}
	byChannel := backlogsByChannel(backlogs)
	groups := make([]topicGroupView, 0, len(topics))
	for _, t := range topics {
		chans, totals := channelsWithBacklog(channels, t.ID, byChannel)
		groups = append(groups, topicGroupView{ID: t.ID, Name: t.Name, Totals: totals, Channels: chans})
	}
	return groups, nil
}

// loadTopicBase resolves the topic through the list endpoint and assembles
// its channels + backlogs — everything but the day-bucket counters, so the
// delete-confirmation path can skip the counter reads. An unknown id returns
// errNotFound — no row is ever created.
func (h *handler) loadTopicBase(ctx context.Context, id int64) (topicView, error) {
	var v topicView
	topics, err := h.client.ListTopics(ctx)
	if err != nil {
		return v, fmt.Errorf("list topics: %w", err)
	}
	for _, t := range topics {
		if t.ID == id {
			v.ID, v.Name = t.ID, t.Name
			break
		}
	}
	if v.ID == 0 {
		return v, errNotFound
	}
	channels, err := h.client.ListChannels(ctx)
	if err != nil {
		return v, fmt.Errorf("list channels: %w", err)
	}
	backlogs, err := h.client.BacklogsForTopic(ctx, id)
	if err != nil {
		return v, fmt.Errorf("backlogs: %w", err)
	}
	v.Channels, v.Totals = channelsWithBacklog(channels, id, backlogsByChannel(backlogs))
	return v, nil
}

// loadTopic adds the zero-filled daily counters on top of loadTopicBase.
func (h *handler) loadTopic(ctx context.Context, id int64) (topicView, error) {
	v, err := h.loadTopicBase(ctx, id)
	if err != nil {
		return v, err
	}
	rows, err := h.client.TopicDailyCounters(ctx, id, trendDays)
	if err != nil {
		return v, fmt.Errorf("topic daily counters: %w", err)
	}
	v.Days = zeroFillDaily(rows, trendDays, time.Now())
	return v, nil
}

// loadChannelBase resolves the channel — names plus live backlog — without
// the day-bucket counters; the delete-confirmation path needs no counters.
// An unknown id returns errNotFound.
func (h *handler) loadChannelBase(ctx context.Context, id int64) (channelDetailView, error) {
	m, err := h.resolveChannel(ctx, id)
	if err != nil {
		return channelDetailView{}, err
	}
	v := channelDetailView{
		ID:        m.ChannelID,
		Name:      m.ChannelName,
		TopicID:   m.TopicID,
		TopicName: m.TopicName,
	}
	// Point read (R2): one channel's box must not pay a batched aggregate.
	b, err := h.client.ChannelBacklogByID(ctx, id)
	if err != nil {
		return v, fmt.Errorf("backlog: %w", err)
	}
	v.Backlog = novaque.BacklogRow{ChannelID: id, Pending: b.Pending, Ready: b.Ready, InFlight: b.InFlight, Dead: b.Dead}
	return v, nil
}

// loadChannel adds the zero-filled daily counters on top of loadChannelBase.
func (h *handler) loadChannel(ctx context.Context, id int64) (channelDetailView, error) {
	v, err := h.loadChannelBase(ctx, id)
	if err != nil {
		return v, err
	}
	rows, err := h.client.ChannelDailyCounters(ctx, id, trendDays)
	if err != nil {
		return v, fmt.Errorf("channel daily counters: %w", err)
	}
	v.Days = zeroFillDaily(rows, trendDays, time.Now())
	return v, nil
}

// zeroFillDaily expands the store's existing-day rows over the full trailing
// window ending today UTC, zero-filling missing days (R3). Rows are
// newest-first (today first). The store returns only days with recorded rows;
// the trend renders a bar for every window day, so the fill happens here in Go.
func zeroFillDaily(rows []novaque.DailyCounters, days int, now time.Time) []dayView {
	byDay := make(map[string]novaque.DailyCounters, len(rows))
	for _, row := range rows {
		byDay[row.Day.UTC().Format(dateFormat)] = row
	}
	today := now.UTC().Truncate(24 * time.Hour)
	out := make([]dayView, 0, days)
	for i := 0; i < days; i++ {
		day := today.AddDate(0, 0, -i)
		row := byDay[day.Format(dateFormat)] // zero value zero-fills
		row.Day = day
		out = append(out, dayView{DailyCounters: row})
	}
	return out
}

// --- rendering helpers ---

// render executes a page template into a buffer so a template failure can
// still answer a clean 500 instead of a half-written response.
func (h *handler) render(w http.ResponseWriter, r *http.Request, page string, status int, data any) {
	t, ok := h.pages[page]
	if !ok {
		h.renderError(w, r, http.StatusInternalServerError, "unknown page template "+page)
		return
	}
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		h.renderError(w, r, http.StatusInternalServerError, "template error: "+err.Error())
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = buf.WriteTo(w)
}

// renderError answers a plain standalone error page — no partial dashboards
// (failure-propagation contract). The message is HTML-escaped; the dashboard
// link is generated through the prefix-aware helper.
func (h *handler) renderError(w http.ResponseWriter, r *http.Request, code int, msg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	fmt.Fprintf(w, `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>%03d — novaque admin</title></head>
<body>
<main class="error-page">
<h1>%d</h1>
<p>%s</p>
<p><a href="%s">Back to the dashboard</a></p>
</main>
</body>
</html>
`, code, code, template.HTMLEscapeString(msg), h.path("/"))
}

func (h *handler) notFound(w http.ResponseWriter, r *http.Request) {
	h.renderError(w, r, http.StatusNotFound, "Not found.")
}

// pathID parses a positive numeric path value; false answers a 404 (page or
// JSON flavor chosen by the caller).
func (h *handler) pathID(r *http.Request, name string) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || id <= 0 {
		return 0, false
	}
	return id, true
}

// --- page handlers ---

func (h *handler) pageDashboard(w http.ResponseWriter, r *http.Request) {
	h.renderDashboardForm(w, r, "", "")
}

// renderDashboardForm renders the dashboard — the plain page (empty form
// state) and the re-render carrying the create-topic form state (submitted
// name + error) after a failed create.
func (h *handler) renderDashboardForm(w http.ResponseWriter, r *http.Request, name, errMsg string) {
	groups, err := h.loadGroups(r.Context())
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	h.render(w, r, "dashboard.html", http.StatusOK, dashboardView{
		baseView:    baseView{Prefix: h.prefix, Title: "Dashboard", Poll: "/api/summary"},
		Groups:      groups,
		CreateName:  name,
		CreateError: errMsg,
	})
}

func (h *handler) pageTopic(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	h.renderTopicForm(w, r, id, "", "")
}

// renderTopicForm renders the topic page — the plain detail view (empty
// form state) and the re-render carrying the create-channel form state
// (submitted name + error) after a failed create.
func (h *handler) renderTopicForm(w http.ResponseWriter, r *http.Request, id int64, name, errMsg string) {
	v, err := h.loadTopic(r.Context(), id)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	v.baseView = baseView{Prefix: h.prefix, Title: "Topic " + v.Name, Poll: fmt.Sprintf("/api/topics/%d", id)}
	v.ChannelName, v.ChannelError = name, errMsg
	h.render(w, r, "topic.html", http.StatusOK, v)
}

func (h *handler) pageChannel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	v, err := h.loadChannel(r.Context(), id)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	v.baseView = baseView{Prefix: h.prefix, Title: "Channel " + v.TopicName + "/" + v.Name, Poll: fmt.Sprintf("/api/channels/%d", id)}
	h.render(w, r, "channel.html", http.StatusOK, v)
}

// --- delete confirmations (R6) ---

func (h *handler) pageConfirmDeleteTopic(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	v, err := h.loadTopicBase(r.Context(), id)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	// Backlog rows are per channel, so totals have no double count; a
	// zero-channel topic sums to zero, which is correct.
	h.render(w, r, "confirm.html", http.StatusOK, confirmView{
		baseView: baseView{Prefix: h.prefix, Title: "Delete topic " + v.Name + "?"},
		Heading:  "Delete topic “" + v.Name + "”?",
		Lines: []string{
			fmt.Sprintf("Channels deleted: %d", len(v.Channels)),
			fmt.Sprintf("Deliveries deleted: %d (pending %d · in-flight %d · dead %d)",
				v.Totals.Pending+v.Totals.InFlight+v.Totals.Dead, v.Totals.Pending, v.Totals.InFlight, v.Totals.Dead),
			"Messages deleted: every retained message under this topic.",
			fmt.Sprintf("Stats deleted: retained day-bucket rows for every channel plus the zero-channel sentinel rows (%d day buckets in window).", trendDays),
		},
		Warnings: []string{
			"This permanently removes the topic and everything under it in one transaction.",
			"Publishers self-heal: the next publish to this topic name re-creates it (create-on-publish semantics).",
		},
		ActionURL:   h.path("/topics/", id, "/delete"),
		ActionLabel: "Delete topic",
		CancelURL:   h.path("/topics/", id),
	})
}

func (h *handler) pageConfirmDeleteChannel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	v, err := h.loadChannelBase(r.Context(), id)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	h.render(w, r, "confirm.html", http.StatusOK, confirmView{
		baseView: baseView{Prefix: h.prefix, Title: "Delete channel " + v.Name + "?"},
		Heading:  "Delete channel “" + v.Name + "” (topic " + v.TopicName + ")?",
		Lines: []string{
			fmt.Sprintf("Deliveries deleted: %d (pending %d · in-flight %d · dead %d)",
				v.Backlog.Pending+v.Backlog.InFlight+v.Backlog.Dead, v.Backlog.Pending, v.Backlog.InFlight, v.Backlog.Dead),
			fmt.Sprintf("Stats deleted: this channel's day-bucket rows (%d day buckets in window).", trendDays),
			"Messages kept: shared message rows survive, so sibling channels keep their deliveries.",
		},
		Warnings: []string{
			// R11/KTD9 wording: removing the channel from consumer config
			// must happen BEFORE restarting consumers — a restarted
			// consumer that still subscribes re-creates the channel.
			"Remove this channel from your consumer configuration before restarting consumers: a running consumer left subscribed idles forever until restarted, and a consumer restarted while still subscribed re-creates the channel.",
		},
		ActionURL:   h.path("/channels/", id, "/delete"),
		ActionLabel: "Delete channel",
		CancelURL:   h.path("/channels/", id),
	})
}

// --- form handlers (all mutations POST, KTD4) ---

func (h *handler) formCreateTopic(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.renderDashboardForm(w, r, name, "Name is required.")
		return
	}
	// Ensure-backed and idempotent (R5): a duplicate resolves the existing
	// id, so the redirect lands on the existing entity's page.
	id, err := h.client.CreateTopic(r.Context(), name)
	if err != nil {
		h.renderDashboardForm(w, r, name, err.Error())
		return
	}
	http.Redirect(w, r, h.path("/topics/", id), http.StatusSeeOther)
}

func (h *handler) formCreateChannel(w http.ResponseWriter, r *http.Request) {
	topicID, err := strconv.ParseInt(r.FormValue("topic_id"), 10, 64)
	if err != nil || topicID <= 0 {
		h.notFound(w, r)
		return
	}
	// The form posts the topic id; the list endpoint is the only id→name
	// source (KTD6), and CreateChannel takes names.
	topics, err := h.client.ListTopics(r.Context())
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	var topicName string
	for _, t := range topics {
		if t.ID == topicID {
			topicName = t.Name
			break
		}
	}
	if topicName == "" {
		h.notFound(w, r)
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.renderTopicForm(w, r, topicID, name, "Name is required.")
		return
	}
	id, err := h.client.CreateChannel(r.Context(), topicName, name)
	if err != nil {
		h.renderTopicForm(w, r, topicID, name, err.Error())
		return
	}
	http.Redirect(w, r, h.path("/channels/", id), http.StatusSeeOther)
}

func (h *handler) formDeleteTopic(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	if err := h.client.DeleteTopic(r.Context(), id); err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	// Back to the dashboard (prefix-preserving), not the deleted entity.
	http.Redirect(w, r, h.path("/"), http.StatusSeeOther)
}

func (h *handler) formDeleteChannel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	if err := h.client.DeleteChannel(r.Context(), id); err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	http.Redirect(w, r, h.path("/"), http.StatusSeeOther)
}

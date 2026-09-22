// Package admin_test exercises the mountable admin UI over a fake store: the
// prefix flow (KTD3), server-rendered pages (R1-R3, R5, R6), the JSON data
// endpoints, and the no-Ensure-on-read invariant (R10). Tests drive the
// handler with a bare http.Client — no JS execution — so every assertion is
// also a no-JS usability proof.
package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/usual2970/novaque"
	"github.com/usual2970/novaque/admin"
	"github.com/usual2970/novaque/store"
)

// fakeStore implements only the store methods the admin surface exercises.
// The embedded nil store.Store panics on any unimplemented call, so a read
// path reaching for Ensure* (create-on-read) fails the test loudly instead
// of passing vacuously. Backlog reads are counted (backlogsN /
// backlogsForTopicN / channelBacklogN) so tests can pin which shape a page
// used: dashboards and topic pages must take the batched reads (R2), never
// a per-channel N+1; the channel page takes exactly one point read.
type fakeStore struct {
	store.Store

	mu       sync.Mutex
	nextID   int64
	topics   map[string]int64
	channels map[string]map[string]int64 // topic name -> channel name -> id

	backlogs     []store.BacklogRow
	topicDaily   map[int64][]store.DailyCounters
	channelDaily map[int64][]store.DailyCounters
	dead         map[int64][]store.DeadDelivery // channel id -> dead rows

	deletedTopics   []int64
	deletedChannels []int64
	ensureTopics    []string
	ensureChannels  [][2]string

	listTopicsN   int
	listChannelsN int
	backlogsN     int

	backlogsForTopicN int
	channelBacklogN   int

	requeuedDead []int64
	deletedDead  []int64
	lastDeadCall [4]int64 // channelID, before, limit, bodyPrefix of the latest ListDead

	failBacklogs    bool
	failEnsureTopic error
	failEnsureChan  error
	failListDead    error
	failRequeueDead error
	failDeleteDead  error
}

func newFake() *fakeStore {
	return &fakeStore{
		nextID:       1,
		topics:       map[string]int64{},
		channels:     map[string]map[string]int64{},
		topicDaily:   map[int64][]store.DailyCounters{},
		channelDaily: map[int64][]store.DailyCounters{},
		dead:         map[int64][]store.DeadDelivery{},
	}
}

// seedOrders installs topic "orders" (id 1) with channels "emails" (2) and
// "billing" (3) plus live backlog rows, without going through Ensure.
func (f *fakeStore) seedOrders() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics["orders"] = 1
	f.channels["orders"] = map[string]int64{"emails": 2, "billing": 3}
	f.nextID = 4
	f.backlogs = []store.BacklogRow{
		{ChannelID: 2, Pending: 3, Ready: 2, InFlight: 1, Dead: 1},
		{ChannelID: 3, Pending: 5, Ready: 4, InFlight: 2, Dead: 2},
	}
}

// seedDaily installs counters on only some days of the trailing window, so
// zero-fill over every window day is observable.
func (f *fakeStore) seedDaily() {
	f.mu.Lock()
	defer f.mu.Unlock()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	f.topicDaily[1] = []store.DailyCounters{
		{Day: today, Publish: 5, Ack: 4},
		{Day: today.AddDate(0, 0, -2), Publish: 3},
		{Day: today.AddDate(0, 0, -5), Publish: 8, Dead: 1},
	}
	f.channelDaily[2] = []store.DailyCounters{
		{Day: today, Publish: 5, Ack: 4},
		{Day: today.AddDate(0, 0, -3), Publish: 2},
	}
}

func (f *fakeStore) addChannels(topic string, names ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, n := range names {
		f.channels[topic][n] = f.nextID
		f.nextID++
	}
}

// addDead seeds one dead delivery under channelID with the given body,
// attempts, and expiry, assigning the next id; it returns the delivery id.
// The seeded topology is always "orders", so Topic/Channel resolve from the
// channel map.
func (f *fakeStore) addDead(channelID int64, body []byte, attempts int, expiresAt time.Time) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextID
	f.nextID++
	f.dead[channelID] = append(f.dead[channelID], store.DeadDelivery{
		ID:          id,
		MessageID:   id + 10000,
		ChannelID:   channelID,
		Topic:       "orders",
		Channel:     f.channelNameLocked(channelID),
		Body:        body,
		Status:      store.StatusDead,
		Attempts:    attempts,
		MaxAttempts: 5,
		AvailableAt: time.Now().Add(-time.Hour),
		ExpiresAt:   expiresAt,
	})
	return id
}

// removeDeadLocked drops a dead row by id under channelID — the scoped
// guard (review #10): a row dead under a different channel is not this
// call's to move. Status-guarded semantics hold (the row is only here while
// dead); false means no dead row of that channel matched — the driver's
// 0-rows case. Callers hold f.mu.
func (f *fakeStore) removeDeadLocked(channelID, deliveryID int64) bool {
	rows := f.dead[channelID]
	for i, d := range rows {
		if d.ID == deliveryID {
			f.dead[channelID] = append(rows[:i:i], rows[i+1:]...)
			return true
		}
	}
	return false
}

func (f *fakeStore) channelNameLocked(channelID int64) string {
	for _, chans := range f.channels {
		for name, id := range chans {
			if id == channelID {
				return name
			}
		}
	}
	return ""
}

func (f *fakeStore) counts() (topics, chans, backlogs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listTopicsN, f.listChannelsN, f.backlogsN
}

func (f *fakeStore) ensureCalls() (topics []string, chans [][2]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string{}, f.ensureTopics...), append([][2]string{}, f.ensureChannels...)
}

func (f *fakeStore) deletes() (topics, chans []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64{}, f.deletedTopics...), append([]int64{}, f.deletedChannels...)
}

func (f *fakeStore) deadOps() (requeued, deleted []int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64{}, f.requeuedDead...), append([]int64{}, f.deletedDead...)
}

func (f *fakeStore) lastDeadListCall() (channelID, before, limit, bodyPrefix int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastDeadCall[0], f.lastDeadCall[1], f.lastDeadCall[2], f.lastDeadCall[3]
}

func (f *fakeStore) topicNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for name := range f.topics {
		out = append(out, name)
	}
	return out
}

func (f *fakeStore) FlushStats(context.Context) error { return nil }

func (f *fakeStore) EnsureTopic(_ context.Context, name string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureTopics = append(f.ensureTopics, name)
	if f.failEnsureTopic != nil {
		return 0, f.failEnsureTopic
	}
	if id, ok := f.topics[name]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.topics[name] = id
	f.channels[name] = map[string]int64{}
	return id, nil
}

func (f *fakeStore) EnsureChannel(_ context.Context, topic, channel string) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureChannels = append(f.ensureChannels, [2]string{topic, channel})
	if f.failEnsureChan != nil {
		return 0, f.failEnsureChan
	}
	if _, ok := f.topics[topic]; !ok {
		f.topics[topic] = f.nextID
		f.nextID++
		f.channels[topic] = map[string]int64{}
	}
	if id, ok := f.channels[topic][channel]; ok {
		return id, nil
	}
	id := f.nextID
	f.nextID++
	f.channels[topic][channel] = id
	return id, nil
}

func (f *fakeStore) ListTopics(_ context.Context) ([]store.TopicInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listTopicsN++
	var out []store.TopicInfo
	for name, id := range f.topics {
		out = append(out, store.TopicInfo{ID: id, Name: name})
	}
	// Insertion sort by name; listings are tiny in tests.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Name < out[j-1].Name; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

func (f *fakeStore) ListChannels(_ context.Context) ([]store.ChannelInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listChannelsN++
	var out []store.ChannelInfo
	for tName, chans := range f.channels {
		topicID := f.topics[tName]
		for cName, id := range chans {
			out = append(out, store.ChannelInfo{ID: id, TopicID: topicID, Name: cName})
		}
	}
	// Topic ids ascend with creation, so (TopicID, Name) is a stable
	// stand-in for the contract's topic-name-then-channel-name order.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && (out[j].TopicID < out[j-1].TopicID ||
			(out[j].TopicID == out[j-1].TopicID && out[j].Name < out[j-1].Name)); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

func (f *fakeStore) Backlogs(_ context.Context) ([]store.BacklogRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backlogsN++
	if f.failBacklogs {
		return nil, errors.New("boom-backlogs")
	}
	return f.backlogs, nil
}

// BacklogsForTopic mirrors the scoped contract: only the seeded rows of
// channels under topicID; failBacklogs fails it like Backlogs.
func (f *fakeStore) BacklogsForTopic(_ context.Context, topicID int64) ([]store.BacklogRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.backlogsForTopicN++
	if f.failBacklogs {
		return nil, errors.New("boom-backlogs")
	}
	under := map[int64]bool{}
	for tName, tID := range f.topics {
		if tID == topicID {
			for _, cID := range f.channels[tName] {
				under[cID] = true
			}
		}
	}
	var out []store.BacklogRow
	for _, r := range f.backlogs {
		if under[r.ChannelID] {
			out = append(out, r)
		}
	}
	return out, nil
}

// ChannelBacklog point-reads one channel's counts out of the seeded batch; a
// channel with no row zero-fills. failBacklogs fails it like Backlogs.
func (f *fakeStore) ChannelBacklog(_ context.Context, channelID int64) (store.ChannelBacklog, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.channelBacklogN++
	if f.failBacklogs {
		return store.ChannelBacklog{}, errors.New("boom-backlogs")
	}
	var b store.ChannelBacklog
	for _, r := range f.backlogs {
		if r.ChannelID == channelID {
			b = store.ChannelBacklog{Pending: r.Pending, Ready: r.Ready, InFlight: r.InFlight, Dead: r.Dead}
		}
	}
	return b, nil
}

func (f *fakeStore) TopicDailyCounters(_ context.Context, topicID int64, days int) ([]store.DailyCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topicDaily[topicID], nil
}

func (f *fakeStore) ChannelDailyCounters(_ context.Context, channelID int64, days int) ([]store.DailyCounters, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.channelDaily[channelID], nil
}

func (f *fakeStore) DeleteTopic(_ context.Context, topicID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedTopics = append(f.deletedTopics, topicID)
	for name, id := range f.topics {
		if id == topicID {
			delete(f.topics, name)
			delete(f.channels, name)
		}
	}
	return nil
}

func (f *fakeStore) DeleteChannel(_ context.Context, channelID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedChannels = append(f.deletedChannels, channelID)
	for _, chans := range f.channels {
		for cName, id := range chans {
			if id == channelID {
				delete(chans, cName)
			}
		}
	}
	return nil
}

// ListDead mirrors the store contract: newest-first (id DESC), before > 0
// keeps only ids strictly below it, limit bounds the page, and bodyPrefix > 0
// caps each returned Body at that many bytes while BodyLen keeps the true
// full length (review #13 — the handler derives the truncation marker from
// the pair, so the fake must not re-truncate or leave BodyLen zero).
func (f *fakeStore) ListDead(_ context.Context, channelID int64, before int64, limit int, bodyPrefix int) ([]store.DeadDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeadCall = [4]int64{channelID, before, int64(limit), int64(bodyPrefix)}
	if f.failListDead != nil {
		return nil, f.failListDead
	}
	rows := make([]store.DeadDelivery, 0, len(f.dead[channelID]))
	for _, d := range f.dead[channelID] {
		if before > 0 && d.ID >= before {
			continue
		}
		d.BodyLen = int64(len(d.Body))
		if bodyPrefix > 0 && len(d.Body) > bodyPrefix {
			d.Body = d.Body[:bodyPrefix]
		}
		rows = append(rows, d)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID > rows[j].ID })
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// RequeueDead follows the channel-scoped guarded-WHERE contract (review
// #10): the call is captured, an injected failure surfaces, and no dead row
// under channelID is 0 affected rows → store.ErrDeadGone (the R11
// idempotent-success input — requeued or deleted already, or another
// channel's delivery).
func (f *fakeStore) RequeueDead(_ context.Context, deliveryID, channelID int64, freshTTL time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeuedDead = append(f.requeuedDead, deliveryID)
	if f.failRequeueDead != nil {
		return f.failRequeueDead
	}
	if !f.removeDeadLocked(channelID, deliveryID) {
		return store.ErrDeadGone
	}
	return nil
}

// DeleteDead mirrors the same channel-scoped guard as RequeueDead.
func (f *fakeStore) DeleteDead(_ context.Context, deliveryID, channelID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedDead = append(f.deletedDead, deliveryID)
	if f.failDeleteDead != nil {
		return f.failDeleteDead
	}
	if !f.removeDeadLocked(channelID, deliveryID) {
		return store.ErrDeadGone
	}
	return nil
}

// --- harness helpers ---

// newTestHandler builds the fake-backed admin handler at prefix — the shared
// construction behind every test server here and in the mount matrix.
func newTestHandler(t *testing.T, prefix string) (http.Handler, *fakeStore) {
	t.Helper()
	f := newFake()
	f.seedOrders()
	f.seedDaily()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatalf("novaque.Open: %v", err)
	}
	h, err := admin.New(c, admin.Options{Prefix: prefix})
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}
	return h, f
}

// newTestServer serves newTestHandler over httptest. prefix "/admin" is the
// explicit default mount.
func newTestServer(t *testing.T, prefix string) (*httptest.Server, *fakeStore) {
	t.Helper()
	h, f := newTestHandler(t, prefix)
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	return ts, f
}

func noRedirect() *http.Client {
	return &http.Client{CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}}
}

func doGet(t *testing.T, c *http.Client, rawURL string) (*http.Response, string) {
	t.Helper()
	res, err := c.Get(rawURL)
	if err != nil {
		t.Fatalf("GET %s: %v", rawURL, err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("read body %s: %v", rawURL, err)
	}
	return res, string(body)
}

func doPost(t *testing.T, c *http.Client, rawURL string, form url.Values) (*http.Response, string) {
	t.Helper()
	res, err := c.PostForm(rawURL, form)
	if err != nil {
		t.Fatalf("POST %s: %v", rawURL, err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		t.Fatalf("read body %s: %v", rawURL, err)
	}
	return res, string(body)
}

// --- prefix flow (KTD3) ---

func TestPrefixRedirectsPreservePrefix(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	c := noRedirect()

	// Bare prefix redirects to the prefixed root, not "/".
	res, _ := doGet(t, c, ts.URL+"/admin")
	if res.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("bare /admin: status = %d, want 301", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/" {
		t.Fatalf("bare /admin: Location = %q, want /admin/", loc)
	}

	// The internal mux redirects "/static" → "/static/" for its subtree
	// route; the Location-rewriting writer must re-apply the prefix the
	// mux never sees.
	res, _ = doGet(t, c, ts.URL+"/admin/static")
	if res.StatusCode < 300 || res.StatusCode >= 400 {
		t.Fatalf("static redirect: status = %d, want 3xx", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/static/" {
		t.Fatalf("static redirect: Location = %q, want /admin/static/", loc)
	}

	// Trailing-slash page paths are not registered and answer 404 (the
	// mux keeps trailing slashes; no prefix-losing clean redirect fires).
	res, _ = doGet(t, c, ts.URL+"/admin/topics/1/")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("trailing-slash page: status = %d, want 404", res.StatusCode)
	}

	// Generated page links carry the prefix.
	_, body := doGet(t, ts.Client(), ts.URL+"/admin/")
	if !strings.Contains(body, `href="/admin/topics/1"`) {
		t.Fatal("dashboard links lack prefix; want href=/admin/topics/1")
	}

	// A path that merely starts with the prefix text is not ours.
	res, _ = doGet(t, c, ts.URL+"/administry")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("/administry: status = %d, want 404", res.StatusCode)
	}
}

// TestPrefixNormalizationInNew pins normalizePrefix's input handling
// (review #6): a prefix given without its leading slash, or with trailing
// slashes, mounts identically to "/admin", and the bare-prefix redirect
// preserves the request's query string.
func TestPrefixNormalizationInNew(t *testing.T) {
	for _, prefix := range []string{"admin", "/admin/", "/admin///"} {
		ts, _ := newTestServer(t, prefix)
		c := noRedirect()

		// The normalized mount serves the dashboard and generated links
		// use /admin.
		res, body := doGet(t, ts.Client(), ts.URL+"/admin/")
		if res.StatusCode != http.StatusOK {
			t.Fatalf("prefix %q: GET /admin/ = %d, want 200", prefix, res.StatusCode)
		}
		if !strings.Contains(body, `href="/admin/topics/1"`) {
			t.Fatalf("prefix %q: links not prefixed with /admin", prefix)
		}

		// Bare prefix: 301 to the slashed root.
		res, _ = doGet(t, c, ts.URL+"/admin")
		if res.StatusCode != http.StatusMovedPermanently || res.Header.Get("Location") != "/admin/" {
			t.Fatalf("prefix %q: bare redirect = %d %q, want 301 /admin/", prefix, res.StatusCode, res.Header.Get("Location"))
		}
	}

	// The bare-prefix redirect carries the original query string through.
	ts, _ := newTestServer(t, "/admin")
	res, _ := doGet(t, noRedirect(), ts.URL+"/admin?keep=me&z=1")
	if res.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("bare with query: status = %d, want 301", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/?keep=me&z=1" {
		t.Fatalf("bare with query: Location = %q, want /admin/?keep=me&z=1", loc)
	}
}

func TestRootMountServesEveryPage(t *testing.T) {
	ts, _ := newTestServer(t, "/")
	for _, path := range []string{"/", "/topics/1", "/channels/2", "/channels/2/dead", "/api/summary", "/static/admin.css"} {
		res, body := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("root mount GET %s: status = %d, want 200", path, res.StatusCode)
		}
		if strings.Contains(body, `"/admin`) {
			t.Fatalf("root mount GET %s leaked default /admin prefix in body", path)
		}
	}
}

func TestDefaultPrefixIsAdmin(t *testing.T) {
	f := newFake()
	f.seedOrders()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := admin.New(c, admin.Options{})
	if err != nil {
		t.Fatalf("admin.New: %v", err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	res, _ := doGet(t, noRedirect(), ts.URL+"/admin")
	if res.StatusCode != http.StatusMovedPermanently || res.Header.Get("Location") != "/admin/" {
		t.Fatalf("zero Options: /admin → %d %q, want 301 /admin/", res.StatusCode, res.Header.Get("Location"))
	}
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("zero Options: /admin/ status = %d, want 200", res.StatusCode)
	}
}

// --- dashboard (R2) ---

func TestDashboardRendersBacklogGroupedByTopic(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	for _, want := range []string{"orders", "emails", "billing", `data-b="2-pending">3<`, `data-b="3-dead">2<`} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	// Topic totals sum the channel backlogs (8 pending = 3 + 5).
	if !strings.Contains(body, `data-t="1-pending">8<`) {
		t.Error("dashboard topic totals not summed: want pending total 8")
	}
	// The poller hook names the un-prefixed endpoint; JS combines it with
	// the injected BASE prefix constant.
	if !strings.Contains(body, `data-poll="/api/summary"`) {
		t.Error("dashboard lacks data-poll=/api/summary")
	}
	// html/template JS-escapes the prefix's slash ("\/admin"), which is
	// semantically identical in JS — accept either spelling.
	if !strings.Contains(body, `const BASE = "/admin";`) && !strings.Contains(body, `const BASE = "\/admin";`) {
		t.Error("dashboard lacks injected BASE constant")
	}
}

func TestDashboardQueryCountBounded(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	res, _ := doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	lt, lc, lb := f.counts()
	if lt != 1 || lc != 1 || lb != 1 {
		t.Fatalf("first render: ListTopics=%d ListChannels=%d Backlogs=%d, want 1/1/1", lt, lc, lb)
	}
	// Grow the channel count; the dashboard must not add queries.
	f.addChannels("orders", "webhooks", "slack", "audit", "sms")
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	lt, lc, lb = f.counts()
	if lt != 2 || lc != 2 || lb != 2 {
		t.Fatalf("after growth: ListTopics=%d ListChannels=%d Backlogs=%d, want 2/2/2 (constant per render)", lt, lc, lb)
	}
}

// TestDetailPagesScopeBacklogReads pins the detail-page query shape (review
// fix for the R2 aggregate): topic and channel pages must not pay the
// all-channels Backlogs GROUP BY — the topic page takes the topic-scoped
// batch, the channel page exactly one point read — and the dashboard keeps
// the unfiltered batch (loadGroups above), never the scoped variants.
func TestDetailPagesScopeBacklogReads(t *testing.T) {
	ts, f := newTestServer(t, "/admin")

	res, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/1")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("topic page status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, `data-b="2-pending">3<`) {
		t.Fatal("topic page: scoped read lost the channel's backlog row")
	}
	if f.backlogsN != 0 {
		t.Fatalf("topic page ran the all-channels Backlogs %d times, want 0", f.backlogsN)
	}
	if f.backlogsForTopicN != 1 {
		t.Fatalf("topic page BacklogsForTopic calls = %d, want 1", f.backlogsForTopicN)
	}
	if f.channelBacklogN != 0 {
		t.Fatalf("topic page point reads = %d, want 0 (a list page must batch)", f.channelBacklogN)
	}

	res, body = doGet(t, ts.Client(), ts.URL+"/admin/channels/2")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("channel page status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, `data-b="2-pending">3<`) {
		t.Fatal("channel page: point read lost the backlog box")
	}
	if f.backlogsN != 0 {
		t.Fatalf("channel page ran the all-channels Backlogs %d times, want 0", f.backlogsN)
	}
	if f.channelBacklogN != 1 {
		t.Fatalf("channel page point reads = %d, want exactly 1", f.channelBacklogN)
	}

	// The JSON detail endpoints ride the same loaders, so they inherit the
	// scoped shape.
	for _, path := range []string{"/admin/api/topics/1", "/admin/api/channels/2"} {
		res, _ = doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status = %d, want 200", path, res.StatusCode)
		}
	}
	if f.backlogsN != 0 {
		t.Fatalf("JSON detail endpoints ran the all-channels Backlogs %d times, want 0", f.backlogsN)
	}

	// The dashboard stays on the unfiltered batch — never the scoped reads.
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200", res.StatusCode)
	}
	if f.backlogsN != 1 {
		t.Fatalf("dashboard Backlogs calls = %d, want 1", f.backlogsN)
	}
	if f.backlogsForTopicN != 2 || f.channelBacklogN != 2 {
		t.Fatalf("dashboard used scoped reads: BacklogsForTopic=%d channelBacklog=%d, want 2/2 (unchanged by the dashboard)", f.backlogsForTopicN, f.channelBacklogN)
	}
}

// --- detail pages + trends (R3) ---

func TestTopicPageLateChannelNote(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/1")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	lower := strings.ToLower(body)
	// R5: the UI states that a channel created after a publish receives only
	// future publishes.
	if !strings.Contains(lower, "only future publishes") {
		t.Fatal("topic page: late-channel note (only future publishes) missing")
	}
	if !strings.Contains(lower, "retroactively") {
		t.Fatal("topic page: note should say messages are not retroactively delivered")
	}
	// The create-channel form posts the topic id.
	if !strings.Contains(body, `name="topic_id" value="1"`) {
		t.Fatal("topic page: create-channel form lacks hidden topic_id")
	}
}

func TestTrendZeroFillsEveryDay(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	_, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/1")
	// 30-day window (min(retention, 30)); the fake recorded only 3 days, so
	// 27 heatmap cells must be zero-filled — every day in the window renders.
	if got := strings.Count(body, ` Publish"`); got != 30 {
		t.Fatalf("publish heatmap tooltips = %d, want 30 (zero-filled window)", got)
	}
	if got := strings.Count(body, `class="contrib-cell contrib-cell--l`); got < 180 {
		t.Fatalf("heatmap cells = %d, want at least 180 (30 days × 6 metrics)", got)
	}
	if !strings.Contains(body, `class="contrib-legend"`) {
		t.Fatal("heatmap missing legend")
	}
	if !strings.Contains(body, `class="contrib-strip"`) {
		t.Fatal("heatmap missing day strip")
	}
	if got := strings.Count(body, `contrib-strip__row`); got < 7 {
		t.Fatalf("heatmap rows = %d, want 1 month + 6 metric rows", got)
	}
	if !strings.Contains(body, `>Publish</span>`) || !strings.Contains(body, `>Dead</span>`) {
		t.Fatal("heatmap missing metric row labels")
	}
	if !strings.Contains(body, `class="contrib-scroll"`) {
		t.Fatal("heatmap missing scroll wrapper")
	}
	if !strings.Contains(body, `contrib-cell--l0`) {
		t.Fatal("heatmap lacks level-0 cells for days without counters")
	}
	if !strings.Contains(body, `contrib-cell--l`) {
		t.Fatal("heatmap missing intensity level classes")
	}
	// Seeded publish max 8 → today (5) should not be level 0.
	if !strings.Contains(body, `contrib-cell--l2`) && !strings.Contains(body, `contrib-cell--l3`) {
		t.Fatal("heatmap should show non-zero publish intensity for seeded counters")
	}
	if got := strings.Count(body, `class="day-row"`); got != 30 {
		t.Fatalf("counter table rows = %d, want 30 (zero-filled window)", got)
	}
	// Seeded values render in table and summary total (5+3+8).
	if !strings.Contains(body, ">5<") {
		t.Fatal("trend missing seeded publish count 5")
	}
	if !strings.Contains(body, `(16 publishes total)`) {
		t.Fatal("heatmap summary missing total publish count 16")
	}
}

// --- unknown ID → 404, no row created (R10 / AE5) ---

func TestUnknownID404AndNoCreate(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	for _, path := range []string{
		"/admin/topics/424242",
		"/admin/channels/424242",
		"/admin/topics/424242/delete",
		"/admin/channels/424242/delete",
		"/admin/channels/424242/dead",
		"/admin/channels/424242/dead/9",
		"/admin/channels/2/dead/424242",
	} {
		res, _ := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, res.StatusCode)
		}
	}
	for _, path := range []string{
		"/admin/api/channels/424242/dead",
		"/admin/api/channels/2/dead/424242",
	} {
		res, _ := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, res.StatusCode)
		}
		if ct := res.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("api 404 Content-Type = %q, want application/json", ct)
		}
	}
	res, _ := doGet(t, ts.Client(), ts.URL+"/admin/api/topics/424242")
	if res.StatusCode != http.StatusNotFound {
		t.Errorf("GET /admin/api/topics/424242: status = %d, want 404", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("api 404 Content-Type = %q, want application/json", ct)
	}
	// Read paths must never create rows: no Ensure on read.
	topics, chans := f.ensureCalls()
	if len(topics) != 0 || len(chans) != 0 {
		t.Fatalf("read paths called Ensure: topics=%v channels=%v", topics, chans)
	}
}

// --- create flows (R5) ---

func TestCreateTopicIdempotentRedirectsToExisting(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	c := noRedirect()

	// Duplicate create resolves the existing topic's page.
	res, _ := doPost(t, c, ts.URL+"/admin/topics", url.Values{"name": {"orders"}})
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("duplicate create: status = %d, want 303", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/topics/1" {
		t.Fatalf("duplicate create: Location = %q, want /admin/topics/1", loc)
	}
	if got := len(f.topicNames()); got != 1 {
		t.Fatalf("topics after duplicate create = %d, want 1 (idempotent)", got)
	}

	// Fresh create redirects to the new entity's page.
	res, _ = doPost(t, c, ts.URL+"/admin/topics", url.Values{"name": {"payments"}})
	if loc := res.Header.Get("Location"); loc != "/admin/topics/4" {
		t.Fatalf("fresh create: Location = %q, want /admin/topics/4", loc)
	}
	// And the target page serves.
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/4")
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "payments") {
		t.Fatalf("fresh create target page: status %d", res.StatusCode)
	}
}

func TestCreateTopicFormErrors(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")

	// Empty name re-renders the form with an error.
	res, body := doPost(t, ts.Client(), ts.URL+"/admin/topics", url.Values{"name": {"   "}})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("empty name: status = %d, want 200 (form re-render)", res.StatusCode)
	}
	if !strings.Contains(body, "Name is required") {
		t.Fatal("empty name: error message missing")
	}

	// Driver failure re-renders the form with the driver error.
	ts2, f2 := newTestServer(t, "/admin")
	f2.mu.Lock()
	f2.failEnsureTopic = errors.New("mysql: invalid topic name q!")
	f2.mu.Unlock()
	res, body = doPost(t, ts2.Client(), ts2.URL+"/admin/topics", url.Values{"name": {"q!"}})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("driver error: status = %d, want 200 (form re-render)", res.StatusCode)
	}
	if !strings.Contains(body, "invalid topic name") {
		t.Fatal("driver error: error text missing from re-rendered form")
	}
}

func TestCreateChannelFromTopicForm(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	c := noRedirect()

	// Duplicate: resolves the existing channel (id 2 = orders/emails).
	res, _ := doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {"1"}, "name": {"emails"}})
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/channels/2" {
		t.Fatalf("duplicate channel create: %d %q, want 303 /admin/channels/2", res.StatusCode, res.Header.Get("Location"))
	}

	// Fresh: new id, Ensure saw the topic name. The duplicate above also
	// went through Ensure — that is the idempotency mechanism — so expect
	// both calls with the fresh pair last.
	res, _ = doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {"1"}, "name": {"webhooks"}})
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/channels/4" {
		t.Fatalf("fresh channel create: %d %q, want 303 /admin/channels/4", res.StatusCode, res.Header.Get("Location"))
	}
	_, chans := f.ensureCalls()
	if len(chans) != 2 || chans[0] != [2]string{"orders", "emails"} || chans[1] != [2]string{"orders", "webhooks"} {
		t.Fatalf("EnsureChannel calls = %v, want [[orders emails] [orders webhooks]]", chans)
	}

	// Unknown topic id → 404, nothing created.
	res, _ = doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {"999"}, "name": {"x"}})
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown topic: status = %d, want 404", res.StatusCode)
	}
}

// TestCreateChannelFromTopicFormErrorBranches pins the branches the happy
// path test skips (review #6): malformed topic ids 404, a whitespace-only
// name re-renders the topic page with the form error and never Ensures, and
// a failing EnsureChannel (failEnsureChan — the fake hook no test
// previously set) re-renders the driver error instead of redirecting.
func TestCreateChannelFromTopicFormErrorBranches(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	c := noRedirect()

	// topic_id parse failures (non-numeric or non-positive) are 404s.
	for _, bad := range []string{"abc", "-1", "0"} {
		res, _ := doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {bad}, "name": {"x"}})
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("topic_id=%q: status = %d, want 404", bad, res.StatusCode)
		}
	}

	// Empty name: the topic page re-renders (200) with the form error and
	// no Ensure happens.
	res, body := doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {"1"}, "name": {"   "}})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("empty name: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "Name is required.") || !strings.Contains(body, "orders") {
		t.Fatal("empty name: form error or topic page missing")
	}
	if _, chans := f.ensureCalls(); len(chans) != 0 {
		t.Fatalf("empty name must not reach EnsureChannel, saw %v", chans)
	}

	// Driver failure: failEnsureChan (honored in the fake's EnsureChannel)
	// surfaces on the re-rendered form rather than a redirect.
	f.mu.Lock()
	f.failEnsureChan = errors.New("boom-ensure-channel")
	f.mu.Unlock()
	res, body = doPost(t, c, ts.URL+"/admin/channels", url.Values{"topic_id": {"1"}, "name": {"retries"}})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("failing EnsureChannel: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "boom-ensure-channel") {
		t.Fatal("failing EnsureChannel: driver error missing from re-rendered form")
	}
}

// --- delete confirmations + deletes (R6, R11 wording fix) ---

func TestDeleteTopicConfirmShowsBlastRadius(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/topics/1/delete")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	for _, want := range []string{"orders", "2", "sentinel"} {
		if !strings.Contains(body, want) {
			t.Errorf("topic confirm missing %q", want)
		}
	}
	// Confirm via plain POST form (no JS dependency).
	if !strings.Contains(body, `method="post" action="/admin/topics/1/delete"`) {
		t.Fatal("topic confirm lacks POST form to /admin/topics/1/delete")
	}
}

func TestDeleteChannelConfirmWarnsConsumerConfig(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/delete")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	lower := strings.ToLower(body)
	// Adopted fix: the warning must say to REMOVE THE CHANNEL FROM CONSUMER
	// CONFIG BEFORE RESTART (R11/KTD9), not merely "restart consumers".
	if !strings.Contains(lower, "remove this channel from your consumer configuration") {
		t.Fatal("channel confirm: missing 'remove this channel from your consumer configuration' warning")
	}
	if !strings.Contains(lower, "before restarting") {
		t.Fatal("channel confirm: missing 'before restarting' wording")
	}
	if !strings.Contains(lower, "sibling") {
		t.Fatal("channel confirm: missing note that messages survive for sibling channels")
	}
	if !strings.Contains(body, "5") {
		t.Fatal("channel confirm: missing backlog blast radius numbers")
	}
}

func TestDeletePostsRedirectToDashboard(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	c := noRedirect()

	res, _ := doPost(t, c, ts.URL+"/admin/topics/1/delete", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/" {
		t.Fatalf("topic delete: %d %q, want 303 /admin/", res.StatusCode, res.Header.Get("Location"))
	}
	res, _ = doPost(t, c, ts.URL+"/admin/channels/2/delete", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/" {
		t.Fatalf("channel delete: %d %q, want 303 /admin/", res.StatusCode, res.Header.Get("Location"))
	}
	topics, chans := f.deletes()
	if len(topics) != 1 || topics[0] != 1 {
		t.Fatalf("DeleteTopic calls = %v, want [1]", topics)
	}
	if len(chans) != 1 || chans[0] != 2 {
		t.Fatalf("DeleteChannel calls = %v, want [2]", chans)
	}
}

func TestChannelDeleteLeavesSiblingChannels(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, _ := doPost(t, noRedirect(), ts.URL+"/admin/channels/2/delete", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/" {
		t.Fatalf("channel delete: %d %q, want 303 /admin/", res.StatusCode, res.Header.Get("Location"))
	}
	_, body := doGet(t, ts.Client(), ts.URL+"/admin/")
	if !strings.Contains(body, "billing") {
		t.Fatal("sibling channel billing vanished after channel delete")
	}
	if strings.Contains(body, `href="/admin/channels/2"`) {
		t.Fatal("deleted channel emails still linked from dashboard")
	}
}

// --- JSON endpoints ---

func TestAPISummaryShape(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/summary")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var got struct {
		Topics []struct {
			ID     int64  `json:"id"`
			Name   string `json:"name"`
			Totals struct {
				Pending int64 `json:"pending"`
			} `json:"totals"`
			Channels []struct {
				ID      int64  `json:"id"`
				TopicID int64  `json:"topic_id"`
				Name    string `json:"name"`
				Backlog struct {
					Pending  int64 `json:"pending"`
					Ready    int64 `json:"ready"`
					InFlight int64 `json:"in_flight"`
					Dead     int64 `json:"dead"`
				} `json:"backlog"`
			} `json:"channels"`
		} `json:"topics"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode summary: %v\nbody: %s", err, raw)
	}
	if len(got.Topics) != 1 {
		t.Fatalf("topics = %d, want 1", len(got.Topics))
	}
	topic := got.Topics[0]
	if topic.ID != 1 || topic.Name != "orders" || topic.Totals.Pending != 8 {
		t.Fatalf("topic summary = %+v", topic)
	}
	if len(topic.Channels) != 2 {
		t.Fatalf("channels = %d, want 2", len(topic.Channels))
	}
	// ListChannels orders by topic name then channel name: billing first.
	first, second := topic.Channels[0], topic.Channels[1]
	if first.ID != 3 || first.Name != "billing" ||
		first.Backlog.Pending != 5 || first.Backlog.Ready != 4 || first.Backlog.InFlight != 2 || first.Backlog.Dead != 2 {
		t.Fatalf("channel summary [0] = %+v, want billing/3", first)
	}
	if second.ID != 2 || second.TopicID != 1 || second.Name != "emails" ||
		second.Backlog.Pending != 3 || second.Backlog.Ready != 2 || second.Backlog.InFlight != 1 || second.Backlog.Dead != 1 {
		t.Fatalf("channel summary [1] = %+v, want emails/2", second)
	}
}

func TestAPITopicAndChannelDetailZeroFilled(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	today := time.Now().UTC().Format("2006-01-02")

	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/topics/1")
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("topic detail: %d %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	var topic struct {
		ID   int64 `json:"id"`
		Days []struct {
			Day     string `json:"day"`
			Publish int64  `json:"publish"`
		} `json:"days"`
	}
	if err := json.Unmarshal([]byte(raw), &topic); err != nil {
		t.Fatalf("decode topic detail: %v", err)
	}
	if len(topic.Days) != 30 {
		t.Fatalf("topic days = %d, want 30 (zero-filled)", len(topic.Days))
	}
	last := topic.Days[len(topic.Days)-1]
	if last.Day != today || last.Publish != 5 {
		t.Fatalf("last day = %+v, want %s publish=5", last, today)
	}

	res, raw = doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("channel detail status = %d", res.StatusCode)
	}
	var channel struct {
		ID        int64  `json:"id"`
		TopicName string `json:"topic_name"`
		Backlog   struct {
			Dead int64 `json:"dead"`
		} `json:"backlog"`
		Days []struct {
			Day string `json:"day"`
		} `json:"days"`
	}
	if err := json.Unmarshal([]byte(raw), &channel); err != nil {
		t.Fatalf("decode channel detail: %v", err)
	}
	if channel.ID != 2 || channel.TopicName != "orders" || channel.Backlog.Dead != 1 || len(channel.Days) != 30 {
		t.Fatalf("channel detail = %+v (days %d)", channel, len(channel.Days))
	}
}

// --- static assets ---

func TestStaticAssetsServed(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/static/admin.css")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("css status = %d", res.StatusCode)
	}
	if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/css") {
		t.Fatalf("css Content-Type = %q", res.Header.Get("Content-Type"))
	}
	if !strings.Contains(body, "contrib-cell") {
		t.Fatal("css missing contrib-cell rules")
	}
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/static/admin.js")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("js status = %d", res.StatusCode)
	}
	// Templates are never served as static files.
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/templates/layout.html")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("templates as static: status = %d, want 404", res.StatusCode)
	}
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/static/missing.css")
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("missing static: status = %d, want 404", res.StatusCode)
	}
}

// --- error pages ---

func TestStoreErrorRendersPlain500(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	f.mu.Lock()
	f.failBacklogs = true
	f.mu.Unlock()
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", res.StatusCode)
	}
	if !strings.Contains(body, "boom-backlogs") {
		t.Fatal("error page missing driver error text")
	}
	if strings.Contains(body, "emails") {
		t.Fatal("error page leaked partial dashboard content")
	}
}

// TestStoreErrorRendersJSON500 pins the JSON error branches the plain-500
// test skips (review #6): with the store failing, /api/summary and the two
// detail endpoints answer 500 with a JSON error object — content-type
// included — never a partial feed.
func TestStoreErrorRendersJSON500(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	f.mu.Lock()
	f.failBacklogs = true
	f.mu.Unlock()
	for _, path := range []string{"/admin/api/summary", "/admin/api/topics/1", "/admin/api/channels/2"} {
		res, raw := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusInternalServerError {
			t.Fatalf("GET %s: status = %d, want 500", path, res.StatusCode)
		}
		if ct := res.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
			t.Fatalf("GET %s: Content-Type = %q, want application/json", path, ct)
		}
		var out map[string]string
		if err := json.Unmarshal([]byte(raw), &out); err != nil {
			t.Fatalf("GET %s: decode error body: %v (%s)", path, err, raw)
		}
		if !strings.Contains(out["error"], "boom-backlogs") {
			t.Fatalf("GET %s: error = %q, want the driver error text", path, out["error"])
		}
	}
}

// --- basic auth options (AE8) ---

func TestBasicAuthOptionsValidation(t *testing.T) {
	f := newFake()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.New(c, admin.Options{BasicAuthUser: "u"}); err == nil {
		t.Fatal("user without pass: want construction error")
	}
	if _, err := admin.New(c, admin.Options{BasicAuthPass: "p"}); err == nil {
		t.Fatal("pass without user: want construction error")
	}
	if _, err := admin.New(c, admin.Options{BasicAuthUser: "", BasicAuthPass: ""}); err != nil {
		t.Fatalf("auth disabled: unexpected error %v", err)
	}
	if _, err := admin.New(nil, admin.Options{}); err == nil {
		t.Fatal("nil client: want construction error")
	}
}

func TestBasicAuthProtectsWhenConfigured(t *testing.T) {
	f := newFake()
	f.seedOrders()
	c, err := novaque.Open(f, novaque.Options{})
	if err != nil {
		t.Fatal(err)
	}
	h, err := admin.New(c, admin.Options{Prefix: "/admin", BasicAuthUser: "ops", BasicAuthPass: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	res, _ := doGet(t, ts.Client(), ts.URL+"/admin/")
	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no creds: status = %d, want 401", res.StatusCode)
	}
	if res.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 lacks WWW-Authenticate")
	}

	// Wrong credentials answer the same 401 + challenge.
	reqBad, err := http.NewRequest(http.MethodGet, ts.URL+"/admin/", nil)
	if err != nil {
		t.Fatal(err)
	}
	reqBad.SetBasicAuth("ops", "not-the-pass")
	resBad, err := ts.Client().Do(reqBad)
	if err != nil {
		t.Fatal(err)
	}
	resBad.Body.Close()
	if resBad.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong creds: status = %d, want 401", resBad.StatusCode)
	}
	if resBad.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("wrong-creds 401 lacks WWW-Authenticate")
	}

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/admin/", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth("ops", "secret")
	res2, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res2.Body.Close()
	if res2.StatusCode != http.StatusOK {
		t.Fatalf("with creds: status = %d, want 200", res2.StatusCode)
	}
}

// --- cross-origin protection (AE6 / KTD4) ---

// TestCrossOriginPostRejected proves the CrossOriginProtection wired into
// New's chain: a browser POST with a mismatched Origin is rejected before
// any mutation runs, while the same POST without Origin (curl-style) and a
// same-origin browser POST both pass.
func TestCrossOriginPostRejected(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")

	post := func(origin string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, ts.URL+"/admin/topics",
			strings.NewReader(url.Values{"name": {"csrf-probe"}}.Encode()))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		res, err := noRedirect().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res
	}

	// Cross-origin browser POST (mismatched Origin) → 403, no mutation.
	if res := post("https://evil.example"); res.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin POST: status = %d, want 403", res.StatusCode)
	}

	// The same POST without Origin (curl-style) → 303 to the topic page.
	res := post("")
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("curl-style POST: status = %d, want 303", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/topics/4" {
		t.Fatalf("curl-style POST: Location = %q, want /admin/topics/4", loc)
	}

	// Same-origin browser POST (Origin matching the request host) passes —
	// the admin's own forms must work in a real browser.
	if res := post(ts.URL); res.StatusCode != http.StatusSeeOther {
		t.Fatalf("same-origin POST: status = %d, want 303", res.StatusCode)
	}
}

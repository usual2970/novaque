// Package admin_test exercises the mountable admin UI over a fake store: the
// prefix flow (KTD3), server-rendered pages (R1-R3, R5, R6), the JSON data
// endpoints, and the no-Ensure-on-read invariant (R10). Tests drive the
// handler with a bare http.Client — no JS execution — so every assertion is
// also a no-JS usability proof.
package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sort"
	"strconv"
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
// path reaching for Ensure* (create-on-read) or a per-channel backlog read
// (the N+1 the batched Backlogs query exists to avoid) fails the test
// loudly instead of passing vacuously.
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

	requeuedDead []int64
	deletedDead  []int64
	lastDeadCall [3]int64 // channelID, before, limit of the latest ListDead

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

// removeDeadLocked drops a dead row by id (status-guarded semantics: the
// row is only here while dead); false means no dead row matched — the
// driver's 0-rows case. Callers hold f.mu.
func (f *fakeStore) removeDeadLocked(deliveryID int64) bool {
	for cid, rows := range f.dead {
		for i, d := range rows {
			if d.ID == deliveryID {
				f.dead[cid] = append(rows[:i:i], rows[i+1:]...)
				return true
			}
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

func (f *fakeStore) lastDeadListCall() (channelID, before, limit int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastDeadCall[0], f.lastDeadCall[1], f.lastDeadCall[2]
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
// keeps only ids strictly below it, limit bounds the page.
func (f *fakeStore) ListDead(_ context.Context, channelID int64, before int64, limit int) ([]store.DeadDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastDeadCall = [3]int64{channelID, before, int64(limit)}
	if f.failListDead != nil {
		return nil, f.failListDead
	}
	rows := make([]store.DeadDelivery, 0, len(f.dead[channelID]))
	for _, d := range f.dead[channelID] {
		if before > 0 && d.ID >= before {
			continue
		}
		rows = append(rows, d)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID > rows[j].ID })
	if limit > 0 && len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

// RequeueDead follows the guarded-WHERE contract: the call is captured, an
// injected failure surfaces, and a missing dead row is 0 affected rows →
// store.ErrDeadGone (the R11 idempotent-success input).
func (f *fakeStore) RequeueDead(_ context.Context, deliveryID int64, freshTTL time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requeuedDead = append(f.requeuedDead, deliveryID)
	if f.failRequeueDead != nil {
		return f.failRequeueDead
	}
	if !f.removeDeadLocked(deliveryID) {
		return store.ErrDeadGone
	}
	return nil
}

func (f *fakeStore) DeleteDead(_ context.Context, deliveryID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedDead = append(f.deletedDead, deliveryID)
	if f.failDeleteDead != nil {
		return f.failDeleteDead
	}
	if !f.removeDeadLocked(deliveryID) {
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
	// 27 bars must be zero-filled — every day in the window renders a bar.
	if got := strings.Count(body, `class="trend-bar"`); got != 30 {
		t.Fatalf("trend bars = %d, want 30 (zero-filled window)", got)
	}
	if !strings.Contains(body, `height: 0%`) {
		t.Fatal("trend lacks zero-height bars for days without counters")
	}
	if got := strings.Count(body, `class="day-row"`); got != 30 {
		t.Fatalf("counter table rows = %d, want 30 (zero-filled window)", got)
	}
	// Seeded values render.
	if !strings.Contains(body, ">5<") {
		t.Fatal("trend missing seeded publish count 5")
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
	if !strings.Contains(body, "trend-bar") {
		t.Fatal("css missing trend-bar rules")
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

// --- dead-letter surface (U5: R4, R7, R11, R12) ---

func TestDeadListPageRendersRows(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	newest := f.addDead(2, []byte("poison payload"), 5, time.Now().Add(2*time.Hour))
	f.addDead(2, []byte("already past"), 5, time.Now().Add(-time.Minute))

	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead list: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "Dead letters") {
		t.Fatal("dead list: heading missing")
	}
	if !strings.Contains(body, "orders/emails") {
		t.Fatal("dead list: channel heading missing")
	}
	if !strings.Contains(body, "poison payload") {
		t.Fatal("dead list: body preview missing")
	}
	if !strings.Contains(body, "5/5") {
		t.Fatal("dead list: attempts/max missing")
	}
	// Remaining TTL renders as a human duration (2h seeded, so 1h59m..2h0m),
	// not a raw timestamp; the past-expiry row renders "expired".
	if regexp.MustCompile(`\d+h\d+m\d+s`).FindString(body) == "" {
		t.Fatal("dead list: remaining TTL not rendered as a human duration")
	}
	if !strings.Contains(body, "expired") {
		t.Fatal("dead list: expired row lacks the expired marker")
	}
	// Rows link to the full-body page through the prefix-aware helper, and
	// the poller hook names the channel's dead JSON.
	if !strings.Contains(body, `href="/admin/channels/2/dead/`+strconv.FormatInt(newest, 10)+`"`) {
		t.Fatalf("dead list: link to delivery %d missing", newest)
	}
	if !strings.Contains(body, `data-poll="/api/channels/2/dead"`) {
		t.Fatal("dead list: data-poll missing")
	}
}

func TestDeadListEmptyState(t *testing.T) {
	ts, _ := newTestServer(t, "/admin")
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/3/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("empty dead list: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "No dead deliveries") {
		t.Fatal("empty dead list: empty-state text missing")
	}
}

func TestDeadListTruncatesBody(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	big := append(bytes.Repeat([]byte("A"), 4096), bytes.Repeat([]byte("B"), 904)...)
	id := f.addDead(2, big, 5, time.Now().Add(time.Hour))

	// List page: byte-truncated preview plus a visible marker; the tail
	// beyond the cap must not leak into the list.
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead list: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "truncated") || !strings.Contains(body, "5000 bytes total") {
		t.Fatal("dead list: truncation marker missing")
	}
	if !strings.Contains(body, strings.Repeat("A", 4096)) {
		t.Fatal("dead list: 4096-byte preview missing")
	}
	if strings.Contains(body, strings.Repeat("B", 904)) {
		t.Fatal("dead list: full body leaked past the truncation cap")
	}

	// The per-delivery JSON carries the full body.
	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2/dead/"+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead json: status = %d, want 200", res.StatusCode)
	}
	var dj struct {
		Body      []byte `json:"body"`
		BodyBytes int    `json:"body_bytes"`
	}
	if err := json.Unmarshal([]byte(raw), &dj); err != nil {
		t.Fatalf("decode dead json: %v", err)
	}
	if len(dj.Body) != 5000 || dj.BodyBytes != 5000 || !bytes.HasSuffix(dj.Body, bytes.Repeat([]byte("B"), 904)) {
		t.Fatalf("dead json body = %d bytes (body_bytes %d), want the full 5000 with the B tail", len(dj.Body), dj.BodyBytes)
	}

	// The full-body HTML page carries the full body too.
	res, body = doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead page: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, strings.Repeat("B", 904)) {
		t.Fatal("full-body page: complete body missing")
	}
	if !strings.Contains(body, "5000 bytes") {
		t.Fatal("full-body page: byte count missing")
	}
}

func TestDeadPaginationKeysetWalk(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	var maxID int64
	for i := 0; i < 120; i++ {
		maxID = f.addDead(2, []byte(fmt.Sprintf("dead-%03d", i)), 5, time.Now().Add(time.Hour))
	}
	minID := maxID - 119 // ids are contiguous

	rowLink := regexp.MustCompile(`href="/admin/channels/2/dead/(\d+)"`)
	olderLink := regexp.MustCompile(`href="(/admin/channels/2/dead\?before=\d+)"`)

	// First page: exactly 50 rows, newest first.
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("page 1: status = %d, want 200", res.StatusCode)
	}
	if got := strings.Count(body, `class="dead-row"`); got != 50 {
		t.Fatalf("page 1 rows = %d, want 50", got)
	}
	if !strings.Contains(body, `href="/admin/channels/2/dead/`+strconv.FormatInt(maxID, 10)+`"`) {
		t.Fatal("page 1: newest delivery missing")
	}
	if strings.Contains(body, `href="/admin/channels/2/dead/`+strconv.FormatInt(minID, 10)+`"`) {
		t.Fatal("page 1: oldest delivery leaked into the first page")
	}
	if _, before, limit := f.lastDeadListCall(); before != 0 || limit != 51 {
		t.Fatalf("page 1 store call: before=%d limit=%d, want 0/51 (page probe)", before, limit)
	}

	// Walk Older links to the end: no skips, no dupes, cursor carried.
	seen := map[int64]bool{}
	pages := 0
	next := "/admin/channels/2/dead"
	for {
		res, body := doGet(t, ts.Client(), ts.URL+next)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("walk page %d: status = %d, want 200", pages+1, res.StatusCode)
		}
		for _, m := range rowLink.FindAllStringSubmatch(body, -1) {
			id, err := strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				t.Fatalf("walk link %q: %v", m[1], err)
			}
			if seen[id] {
				t.Fatalf("walk: delivery %d appears twice", id)
			}
			seen[id] = true
		}
		pages++
		older := olderLink.FindStringSubmatch(body)
		if older == nil {
			break
		}
		next = older[1]
		if pages > 10 {
			t.Fatal("pagination did not terminate")
		}
	}
	if pages != 3 || len(seen) != 120 {
		t.Fatalf("walk covered %d pages / %d unique ids, want 3 pages / 120 ids (no skips or dupes)", pages, len(seen))
	}

	// Page 2 was fetched with page 1's last row as the cursor, and its
	// poller hook carries the cursor too.
	res, body = doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead?before="+strconv.FormatInt(maxID-49, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("page 2: status = %d, want 200", res.StatusCode)
	}
	if _, before, limit := f.lastDeadListCall(); before != maxID-49 || limit != 51 {
		t.Fatalf("page 2 store call: before=%d limit=%d, want before=%d limit=51", before, limit, maxID-49)
	}
	if !strings.Contains(body, `data-poll="/api/channels/2/dead?before=`+strconv.FormatInt(maxID-49, 10)+`"`) {
		t.Fatal("page 2: data-poll lacks the before cursor")
	}
	if !strings.Contains(body, "Back to first page") {
		t.Fatal("page 2: back-to-first-page link missing")
	}
}

func TestDeadBodiesAutoEscaped(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("<script>alert(1)</script>"), 5, time.Now().Add(time.Hour))

	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead list: status = %d", res.StatusCode)
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatal("dead list: body not rendered in escaped form")
	}
	if strings.Contains(body, "<script>alert") {
		t.Fatal("dead list: raw script markup leaked into the page")
	}

	res, body = doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("full-body page: status = %d", res.StatusCode)
	}
	if !strings.Contains(body, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatal("full-body page: body not rendered in escaped form")
	}
	if strings.Contains(body, "<script>alert") {
		t.Fatal("full-body page: raw script markup leaked into the page")
	}

	// JSON round-trips the bytes untouched (it is not an HTML surface).
	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2/dead/"+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead json: status = %d", res.StatusCode)
	}
	var dj struct {
		Body []byte `json:"body"`
	}
	if err := json.Unmarshal([]byte(raw), &dj); err != nil {
		t.Fatalf("decode dead json: %v", err)
	}
	if string(dj.Body) != "<script>alert(1)</script>" {
		t.Fatalf("dead json body = %q, want byte-exact round-trip", dj.Body)
	}
}

func TestDeadDeliveryFullBodyPage(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("full-body-view-payload"), 3, time.Now().Add(90*time.Minute))

	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("full-body page: status = %d, want 200", res.StatusCode)
	}
	for _, want := range []string{
		"full-body-view-payload",
		"3/5",
		strconv.FormatInt(id+10000, 10), // message id
		`method="post" action="/admin/channels/2/dead/` + strconv.FormatInt(id, 10) + `/requeue"`,
		`method="post" action="/admin/channels/2/dead/` + strconv.FormatInt(id, 10) + `/delete"`,
		"Requeue (reset attempts, fresh TTL)",
		"Delete permanently",
		`href="/admin/channels/2"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("full-body page missing %q", want)
		}
	}
	// The detail page does not poll (a static view of one delivery).
	if strings.Contains(body, "data-poll") {
		t.Fatal("full-body page: unexpected data-poll")
	}
}

func TestDeadListJSONOmitsBodies(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("secret-payload"), 1, time.Now().Add(time.Hour))

	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2/dead")
	if res.StatusCode != http.StatusOK || res.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("dead list json: %d %q", res.StatusCode, res.Header.Get("Content-Type"))
	}
	// R12: the list JSON carries metadata only — no body bytes at all.
	if strings.Contains(raw, "secret-payload") || strings.Contains(raw, `"body"`) {
		t.Fatalf("dead list json leaked payload: %s", raw)
	}
	var got struct {
		ChannelID int64 `json:"channel_id"`
		Before    int64 `json:"before"`
		HasOlder  bool  `json:"has_older"`
		Dead      []struct {
			ID           int64  `json:"id"`
			Attempts     int    `json:"attempts"`
			RemainingTTL string `json:"remaining_ttl"`
			HasBody      bool   `json:"has_body"`
			BodyBytes    int    `json:"body_bytes"`
		} `json:"dead"`
	}
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("decode dead list json: %v\nbody: %s", err, raw)
	}
	if got.ChannelID != 2 || got.Before != 0 || got.HasOlder {
		t.Fatalf("dead list envelope = %+v", got)
	}
	if len(got.Dead) != 1 {
		t.Fatalf("dead rows = %d, want 1", len(got.Dead))
	}
	row := got.Dead[0]
	if row.ID != id || row.Attempts != 1 || row.RemainingTTL == "" || !row.HasBody || row.BodyBytes != len("secret-payload") {
		t.Fatalf("dead row = %+v", row)
	}

	// The cursor is respected.
	res, raw = doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2/dead?before="+strconv.FormatInt(id, 10))
	if res.StatusCode != http.StatusOK {
		t.Fatalf("dead list json (paged): status = %d", res.StatusCode)
	}
	if _, before, _ := f.lastDeadListCall(); before != id {
		t.Fatalf("paged json store call: before=%d, want %d", before, id)
	}
	if !strings.Contains(raw, `"dead":[]`) {
		t.Fatalf("paged json should be empty past the only row: %s", raw)
	}
}

func TestDeadRequeueIdempotentWhenGone(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("poison"), 5, time.Now().Add(time.Hour))
	c := noRedirect()

	res, _ := doPost(t, c, ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/requeue", nil)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("requeue: status = %d, want 303", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/channels/2/dead?done=requeued" {
		t.Fatalf("requeue: Location = %q, want /admin/channels/2/dead?done=requeued", loc)
	}
	requeued, _ := f.deadOps()
	if len(requeued) != 1 || requeued[0] != id {
		t.Fatalf("RequeueDead calls = %v, want [%d]", requeued, id)
	}

	// The row left the dead set: a second POST is a 0-rows op and must be
	// idempotent success (R11), not an error.
	res, _ = doPost(t, c, ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/requeue", nil)
	if res.StatusCode != http.StatusSeeOther {
		t.Fatalf("requeue after gone: status = %d, want 303 (idempotent success)", res.StatusCode)
	}
	if loc := res.Header.Get("Location"); loc != "/admin/channels/2/dead?done=already-gone" {
		t.Fatalf("requeue after gone: Location = %q, want done=already-gone", loc)
	}
	// The notice renders on the list the redirect lands on.
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead?done=already-gone")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("list after gone: status = %d, want 200", res.StatusCode)
	}
	if !strings.Contains(body, "no longer dead") {
		t.Fatal("list after gone: idempotent-success notice missing")
	}
}

func TestDeadDeleteAction(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("poison"), 5, time.Now().Add(time.Hour))

	res, _ := doPost(t, noRedirect(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/delete", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/channels/2/dead?done=deleted" {
		t.Fatalf("delete: %d %q, want 303 done=deleted", res.StatusCode, res.Header.Get("Location"))
	}
	_, deleted := f.deadOps()
	if len(deleted) != 1 || deleted[0] != id {
		t.Fatalf("DeleteDead calls = %v, want [%d]", deleted, id)
	}

	// Following the redirect lands on an empty list with the notice.
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead?done=deleted")
	if res.StatusCode != http.StatusOK {
		t.Fatalf("list after delete: status = %d", res.StatusCode)
	}
	if !strings.Contains(body, "deleted") || !strings.Contains(body, "No dead deliveries") {
		t.Fatal("list after delete: notice or empty state missing")
	}

	// Deleting the now-gone row is idempotent success too.
	res, _ = doPost(t, noRedirect(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/delete", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/channels/2/dead?done=already-gone" {
		t.Fatalf("delete after gone: %d %q, want 303 done=already-gone", res.StatusCode, res.Header.Get("Location"))
	}
}

func TestDeadActionErrors(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	id := f.addDead(2, []byte("poison"), 5, time.Now().Add(time.Hour))

	f.mu.Lock()
	f.failRequeueDead = errors.New("mysql: lost connection")
	f.mu.Unlock()
	res, body := doPost(t, ts.Client(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/requeue", nil)
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("requeue failure: status = %d, want 500", res.StatusCode)
	}
	if !strings.Contains(body, "mysql: lost connection") {
		t.Fatal("requeue failure: driver error text missing")
	}

	f.mu.Lock()
	f.failRequeueDead = nil
	f.failDeleteDead = errors.New("mysql: deadlock")
	f.mu.Unlock()
	res, body = doPost(t, ts.Client(), ts.URL+"/admin/channels/2/dead/"+strconv.FormatInt(id, 10)+"/delete", nil)
	if res.StatusCode != http.StatusInternalServerError || !strings.Contains(body, "deadlock") {
		t.Fatalf("delete failure: %d, want 500 with driver text", res.StatusCode)
	}

	// Read-side store failures answer clean 500s, page and JSON flavors.
	f.mu.Lock()
	f.failListDead = errors.New("boom-listdead")
	f.mu.Unlock()
	res, _ = doGet(t, ts.Client(), ts.URL+"/admin/channels/2/dead")
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("list failure: status = %d, want 500", res.StatusCode)
	}
	res, raw := doGet(t, ts.Client(), ts.URL+"/admin/api/channels/2/dead")
	if res.StatusCode != http.StatusInternalServerError || !strings.Contains(raw, "boom-listdead") {
		t.Fatalf("list json failure: %d %s, want 500 with error", res.StatusCode, raw)
	}
}

func TestDeadDeliveryScopedToChannel(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	in3 := f.addDead(3, []byte("billing poison"), 5, time.Now().Add(time.Hour))

	// Dead in channel 3 (billing); the channel 2 URL must 404 both flavors.
	for _, path := range []string{
		"/admin/channels/2/dead/" + strconv.FormatInt(in3, 10),
		"/admin/api/channels/2/dead/" + strconv.FormatInt(in3, 10),
	} {
		res, _ := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404 (delivery is dead in another channel)", path, res.StatusCode)
		}
	}
	res, body := doGet(t, ts.Client(), ts.URL+"/admin/channels/3/dead/"+strconv.FormatInt(in3, 10))
	if res.StatusCode != http.StatusOK || !strings.Contains(body, "billing poison") {
		t.Fatalf("own-channel view: %d, want 200 with the body", res.StatusCode)
	}
}

func TestDeadUnknownIDs404(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	f.addDead(2, []byte("poison"), 5, time.Now().Add(time.Hour))
	for _, path := range []string{
		"/admin/channels/999/dead",
		"/admin/channels/999/dead/9",
		"/admin/channels/2/dead/999999",
		"/admin/channels/2/dead/abc",
		"/admin/channels/2/dead/9x",
		"/admin/api/channels/999/dead",
		"/admin/api/channels/2/dead/999999",
		"/admin/api/channels/2/dead/abc",
	} {
		res, _ := doGet(t, ts.Client(), ts.URL+path)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, res.StatusCode)
		}
	}
	// Actions on an unknown channel or with malformed path values 404.
	// (A GET on the POST-only action URLs is the mux's 405, like every
	// other form route in the package.)
	for _, path := range []string{
		"/admin/channels/999/dead/1/requeue",
		"/admin/channels/2/dead/abc/requeue",
		"/admin/channels/2/dead/9x/delete",
	} {
		res, _ := doPost(t, noRedirect(), ts.URL+path, nil)
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("POST %s: status = %d, want 404", path, res.StatusCode)
		}
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

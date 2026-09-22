// Dead-letter surface tests (U5: R4, R7, R11, R12), split from admin_test.go
// (review finding: the 1600-line test file mixed every surface). Shares the
// fakeStore/harness helpers defined in admin_test.go.
package admin_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestDeadActionsCannotCrossChannels pins review #10: the write actions are
// scoped to the channel in the URL. A dead delivery owned by another channel
// is never requeued or deleted through this channel's URL — the scoped store
// guard matches 0 rows, which collapses into the same idempotent
// already-gone notice as any other ErrDeadGone (R11) rather than a distinct
// error.
func TestDeadActionsCannotCrossChannels(t *testing.T) {
	ts, f := newTestServer(t, "/admin")
	in3 := f.addDead(3, []byte("billing poison"), 5, time.Now().Add(time.Hour))

	for _, path := range []string{
		"/admin/channels/2/dead/" + strconv.FormatInt(in3, 10) + "/requeue",
		"/admin/channels/2/dead/" + strconv.FormatInt(in3, 10) + "/delete",
	} {
		res, _ := doPost(t, noRedirect(), ts.URL+path, nil)
		if res.StatusCode != http.StatusSeeOther {
			t.Errorf("POST %s: status = %d, want 303 (scoped guard, idempotent collapse)", path, res.StatusCode)
		}
		if loc := res.Header.Get("Location"); loc != "/admin/channels/2/dead?done=already-gone" {
			t.Errorf("POST %s: Location = %q, want done=already-gone", path, loc)
		}
	}

	// No mutation ran: the row is still dead under its owning channel.
	f.mu.Lock()
	rows := len(f.dead[3])
	f.mu.Unlock()
	if rows != 1 {
		t.Fatalf("dead rows under owning channel 3 = %d, want 1 (cross-channel action mutated state)", rows)
	}

	// The same actions through the owning channel still land as real
	// mutations.
	res, _ := doPost(t, noRedirect(), ts.URL+"/admin/channels/3/dead/"+strconv.FormatInt(in3, 10)+"/requeue", nil)
	if res.StatusCode != http.StatusSeeOther || res.Header.Get("Location") != "/admin/channels/3/dead?done=requeued" {
		t.Fatalf("own-channel requeue: %d %q, want 303 done=requeued", res.StatusCode, res.Header.Get("Location"))
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
	// Review #9: the dead browse no longer arms a poller (the feed matched
	// no apply() branch in admin.js and its responses were discarded).
	if strings.Contains(body, "data-poll") {
		t.Fatal("dead list: unexpected data-poll — dead surface is static")
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
	if _, before, limit, prefix := f.lastDeadListCall(); before != 0 || limit != 51 || prefix != 4096 {
		t.Fatalf("page 1 store call: before=%d limit=%d prefix=%d, want 0/51/4096 (page probe, preview-bounded read)", before, limit, prefix)
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
	if _, before, limit, prefix := f.lastDeadListCall(); before != maxID-49 || limit != 51 || prefix != 4096 {
		t.Fatalf("page 2 store call: before=%d limit=%d prefix=%d, want before=%d limit=51 prefix=4096", before, limit, prefix, maxID-49)
	}
	if strings.Contains(body, "data-poll") {
		t.Fatal("page 2: unexpected data-poll — dead surface is static (review #9)")
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
	// The per-delivery read requests whole bodies (prefix 0); only list
	// reads pass a prefix (review #13).
	if _, _, _, prefix := f.lastDeadListCall(); prefix != 0 {
		t.Fatalf("delivery store call: prefix=%d, want 0 (full body)", prefix)
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
	if _, before, _, prefix := f.lastDeadListCall(); before != id || prefix != 4096 {
		t.Fatalf("paged json store call: before=%d prefix=%d, want %d/4096 (list reads are preview-bounded)", before, prefix, id)
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
		t.Fatalf("list after delete: status = %d, want 200", res.StatusCode)
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

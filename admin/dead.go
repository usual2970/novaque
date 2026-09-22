package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/usual2970/novaque"
)

// The dead-letter surface (U5): a paginated browse with truncated bodies and
// remaining TTL (R4), a full-body view, and the requeue/delete actions (R7).
// The dead pages and the per-delivery JSON endpoint are the ONLY surfaces in
// this package that carry message payloads (R12): every other page stays
// payload-free, and even the dead list JSON serves metadata plus a size hint
// only — the full body lives behind the per-delivery endpoints.

const (
	// deadPageSize is the fixed dead-list page size (R4).
	deadPageSize = 50
	// deadBodyTruncate bounds the body preview in the list. Byte truncation
	// on purpose: payloads are opaque bytes, not guaranteed UTF-8, and the
	// template's contextual escaping handles whatever the slice contains.
	deadBodyTruncate = 4096
)

// --- view models ---

// deadRowView is one list row: truncated body preview, attempts, remaining
// TTL rendered from the DB-clock expiry against the wall clock.
type deadRowView struct {
	ID          int64
	MessageID   int64
	Body        string
	Truncated   bool
	BodyBytes   int
	Attempts    int
	MaxAttempts int
	Remaining   string
	Expired     bool
	AvailableAt time.Time
	ExpiresAt   time.Time
}

// deadDeliveryView carries the full body plus metadata for the per-delivery
// page, whose forms double as the requeue/delete confirmation step.
type deadDeliveryView struct {
	ID          int64
	MessageID   int64
	Topic       string
	Channel     string
	Body        string
	BodyBytes   int
	Attempts    int
	MaxAttempts int
	Remaining   string
	Expired     bool
	AvailableAt time.Time
	ExpiresAt   time.Time
}

// deadView backs both dead.html branches: the list renders while Delivery is
// nil; a non-nil Delivery renders the full-body view (adopted review fix:
// payload viewing must be usable with JS disabled, and it stays inside the
// dead-letter surface, so R12 holds).
type deadView struct {
	baseView
	ChannelID   int64
	ChannelName string
	TopicID     int64
	TopicName   string
	Rows        []deadRowView
	Before      int64
	HasOlder    bool
	OlderBefore int64
	Notice      string
	Delivery    *deadDeliveryView
}

// deadChannelMeta resolves a channel id to its names through the list
// endpoints (KTD6: reads never Ensure); unknown ids return errNotFound.
type deadChannelMeta struct {
	ChannelID   int64
	ChannelName string
	TopicID     int64
	TopicName   string
}

func (h *handler) deadChannelMeta(ctx context.Context, channelID int64) (deadChannelMeta, error) {
	var m deadChannelMeta
	channels, err := h.client.ListChannels(ctx)
	if err != nil {
		return m, fmt.Errorf("list channels: %w", err)
	}
	for _, c := range channels {
		if c.ID == channelID {
			m.ChannelID, m.ChannelName, m.TopicID = c.ID, c.Name, c.TopicID
			break
		}
	}
	if m.ChannelID == 0 {
		return m, errNotFound
	}
	topics, err := h.client.ListTopics(ctx)
	if err != nil {
		return m, fmt.Errorf("list topics: %w", err)
	}
	for _, t := range topics {
		if t.ID == m.TopicID {
			m.TopicName = t.Name
			break
		}
	}
	return m, nil
}

// --- data assembly ---

// deadRemaining renders remaining TTL from the DB-clock expiry against
// time.Now(); a non-positive remainder is "expired" — the row stays dead
// until the purge reclaims it.
func deadRemaining(expiresAt time.Time) (remaining string, expired bool) {
	left := expiresAt.Sub(time.Now())
	if left <= 0 {
		return "expired", true
	}
	return left.Round(time.Second).String(), false
}

func newDeadRowView(d novaque.DeadDelivery) deadRowView {
	remaining, expired := deadRemaining(d.ExpiresAt)
	v := deadRowView{
		ID:          d.ID,
		MessageID:   d.MessageID,
		Body:        string(d.Body),
		BodyBytes:   len(d.Body),
		Attempts:    d.Attempts,
		MaxAttempts: d.MaxAttempts,
		Remaining:   remaining,
		Expired:     expired,
		AvailableAt: d.AvailableAt,
		ExpiresAt:   d.ExpiresAt,
	}
	if len(d.Body) > deadBodyTruncate {
		v.Body = string(d.Body[:deadBodyTruncate])
		v.Truncated = true
	}
	return v
}

// loadDeadList assembles the list model: channel names plus one page of dead
// rows. One row past the page is probed so the Older link is exact without a
// second query; the probe row is dropped.
func (h *handler) loadDeadList(ctx context.Context, channelID, before int64) (deadView, error) {
	meta, err := h.deadChannelMeta(ctx, channelID)
	if err != nil {
		return deadView{}, err
	}
	rows, err := h.client.ListDead(ctx, channelID, before, deadPageSize+1)
	if err != nil {
		return deadView{}, fmt.Errorf("list dead: %w", err)
	}
	hasOlder := len(rows) > deadPageSize
	if hasOlder {
		rows = rows[:deadPageSize]
	}
	v := deadView{
		ChannelID:   meta.ChannelID,
		ChannelName: meta.ChannelName,
		TopicID:     meta.TopicID,
		TopicName:   meta.TopicName,
		Rows:        make([]deadRowView, 0, len(rows)),
		Before:      before,
		HasOlder:    hasOlder,
	}
	for _, d := range rows {
		v.Rows = append(v.Rows, newDeadRowView(d))
	}
	if hasOlder {
		v.OlderBefore = rows[len(rows)-1].ID
	}
	return v, nil
}

// loadDeadDelivery fetches one dead delivery with the pagination query the
// store already exports — before = deliveryID+1 with limit 1 returns the
// highest dead id <= deliveryID in this channel, so an empty or mismatched
// row means the delivery is not dead in this channel → errNotFound.
// (deliveryID+1 at math.MaxInt64 wraps negative, which reads as "no bound":
// the newest row is still returned, so the trick holds at the boundary.)
func (h *handler) loadDeadDelivery(ctx context.Context, channelID, deliveryID int64) (deadChannelMeta, novaque.DeadDelivery, error) {
	meta, err := h.deadChannelMeta(ctx, channelID)
	if err != nil {
		return meta, novaque.DeadDelivery{}, err
	}
	rows, err := h.client.ListDead(ctx, channelID, deliveryID+1, 1)
	if err != nil {
		return meta, novaque.DeadDelivery{}, fmt.Errorf("load dead delivery: %w", err)
	}
	if len(rows) == 0 || rows[0].ID != deliveryID {
		return meta, novaque.DeadDelivery{}, errNotFound
	}
	return meta, rows[0], nil
}

// --- request helpers ---

// deadBefore parses the ?before= keyset cursor; absent, malformed, or
// non-positive means the first page.
func deadBefore(r *http.Request) int64 {
	before, err := strconv.ParseInt(r.URL.Query().Get("before"), 10, 64)
	if err != nil || before <= 0 {
		return 0
	}
	return before
}

// deadNotice maps the ?done= flag an action redirect carries into a visible
// notice; unknown flags render none.
func deadNotice(done string) string {
	switch done {
	case "requeued":
		return "Delivery requeued — attempts reset and a fresh TTL applied."
	case "deleted":
		return "Dead delivery deleted."
	case "already-gone":
		return "That delivery was no longer dead — already requeued or deleted elsewhere. Nothing was changed."
	}
	return ""
}

// --- page handlers ---

// pageDead answers GET /channels/{id}/dead — the paginated dead-letter
// browse (R4): truncated bodies, attempts, remaining TTL, Older pagination.
func (h *handler) pageDead(w http.ResponseWriter, r *http.Request) {
	channelID, ok := h.pathID(r, "id")
	if !ok {
		h.notFound(w, r)
		return
	}
	before := deadBefore(r)
	v, err := h.loadDeadList(r.Context(), channelID, before)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	v.baseView = baseView{
		Prefix: h.prefix,
		Title:  "Dead letters — " + v.TopicName + "/" + v.ChannelName,
		Poll:   fmt.Sprintf("/api/channels/%d/dead", channelID),
	}
	if before > 0 {
		v.Poll = fmt.Sprintf("%s?before=%d", v.Poll, before)
	}
	v.Notice = deadNotice(r.URL.Query().Get("done"))
	h.render(w, r, "dead.html", http.StatusOK, v)
}

// pageDeadDelivery answers GET /channels/{id}/dead/{deliveryID} — the
// server-rendered full-body view plus the requeue/delete confirm forms (the
// actions themselves are POST-only; no mutation happens on a GET). Rendering
// the full payload here keeps payload viewing usable with JS disabled
// (KTD10) while staying inside the dead-letter surface (R12).
func (h *handler) pageDeadDelivery(w http.ResponseWriter, r *http.Request) {
	channelID, ok := h.pathID(r, "id")
	deliveryID, okDelivery := h.pathID(r, "deliveryID")
	if !ok || !okDelivery {
		h.notFound(w, r)
		return
	}
	meta, d, err := h.loadDeadDelivery(r.Context(), channelID, deliveryID)
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	if err != nil {
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}
	remaining, expired := deadRemaining(d.ExpiresAt)
	v := deadView{
		ChannelID:   meta.ChannelID,
		ChannelName: meta.ChannelName,
		TopicID:     meta.TopicID,
		TopicName:   meta.TopicName,
		Delivery: &deadDeliveryView{
			ID:          d.ID,
			MessageID:   d.MessageID,
			Topic:       d.Topic,
			Channel:     d.Channel,
			Body:        string(d.Body),
			BodyBytes:   len(d.Body),
			Attempts:    d.Attempts,
			MaxAttempts: d.MaxAttempts,
			Remaining:   remaining,
			Expired:     expired,
			AvailableAt: d.AvailableAt,
			ExpiresAt:   d.ExpiresAt,
		},
	}
	v.baseView = baseView{Prefix: h.prefix, Title: fmt.Sprintf("Dead delivery #%d", deliveryID)}
	h.render(w, r, "dead.html", http.StatusOK, v)
}

// --- action handlers (mutations are POST-only, KTD4) ---

// formRequeueDead answers POST /channels/{id}/dead/{deliveryID}/requeue
// (R7): attempts reset and a fresh TTL via the Client. A wrapped
// ErrDeadGone — someone else requeued or deleted first — is idempotent
// success (R11): a notice back on the list, never an error page.
func (h *handler) formRequeueDead(w http.ResponseWriter, r *http.Request) {
	channelID, deliveryID, ok := h.deadPathIDs(r)
	if !ok {
		h.notFound(w, r)
		return
	}
	if _, err := h.deadChannelMeta(r.Context(), channelID); err != nil {
		h.deadChannelError(w, r, err)
		return
	}
	h.deadActionDone(w, r, channelID, h.client.RequeueDead(r.Context(), deliveryID), "requeued")
}

// formDeleteDead answers POST /channels/{id}/dead/{deliveryID}/delete (R7):
// the dead delivery row is removed; the shared message row is reclaimed by
// the orphan purge once its siblings are gone. ErrDeadGone is idempotent
// success exactly as for requeue (R11).
func (h *handler) formDeleteDead(w http.ResponseWriter, r *http.Request) {
	channelID, deliveryID, ok := h.deadPathIDs(r)
	if !ok {
		h.notFound(w, r)
		return
	}
	if _, err := h.deadChannelMeta(r.Context(), channelID); err != nil {
		h.deadChannelError(w, r, err)
		return
	}
	h.deadActionDone(w, r, channelID, h.client.DeleteDead(r.Context(), deliveryID), "deleted")
}

// deadPathIDs parses the {id} and {deliveryID} path values; false is a 404.
func (h *handler) deadPathIDs(r *http.Request) (channelID, deliveryID int64, ok bool) {
	channelID, ok = h.pathID(r, "id")
	if !ok {
		return 0, 0, false
	}
	deliveryID, ok = h.pathID(r, "deliveryID")
	if !ok {
		return 0, 0, false
	}
	return channelID, deliveryID, true
}

// deadActionDone maps an action outcome to the redirect back to the list:
// success carries done=<flag>; a wrapped ErrDeadGone is idempotent success
// carrying done=already-gone (R11); anything else is a clean 500.
func (h *handler) deadActionDone(w http.ResponseWriter, r *http.Request, channelID int64, err error, done string) {
	switch {
	case err == nil:
		http.Redirect(w, r, h.path("/channels/", channelID, "/dead?done=", done), http.StatusSeeOther)
	case errors.Is(err, novaque.ErrDeadGone):
		http.Redirect(w, r, h.path("/channels/", channelID, "/dead?done=already-gone"), http.StatusSeeOther)
	default:
		h.renderError(w, r, http.StatusInternalServerError, err.Error())
	}
}

// deadChannelError answers the pre-action channel resolution failure.
func (h *handler) deadChannelError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, errNotFound) {
		h.notFound(w, r)
		return
	}
	h.renderError(w, r, http.StatusInternalServerError, err.Error())
}

// --- JSON endpoints ---

// deadRowJSON is one list row WITHOUT the body (R12): metadata, remaining
// TTL, and a size hint only — the payload stays behind the per-delivery
// endpoint.
type deadRowJSON struct {
	ID           int64  `json:"id"`
	MessageID    int64  `json:"message_id"`
	Attempts     int    `json:"attempts"`
	MaxAttempts  int    `json:"max_attempts"`
	RemainingTTL string `json:"remaining_ttl"`
	Expired      bool   `json:"expired"`
	AvailableAt  string `json:"available_at"`
	ExpiresAt    string `json:"expires_at"`
	HasBody      bool   `json:"has_body"`
	BodyBytes    int    `json:"body_bytes"`
}

type deadListJSON struct {
	ChannelID int64         `json:"channel_id"`
	Before    int64         `json:"before"`
	HasOlder  bool          `json:"has_older"`
	Dead      []deadRowJSON `json:"dead"`
}

// deadDeliveryJSON carries the full payload: encoding/json base64-encodes
// the []byte body, so arbitrary (non-UTF-8) bytes round-trip byte-exact.
type deadDeliveryJSON struct {
	ID           int64  `json:"id"`
	MessageID    int64  `json:"message_id"`
	ChannelID    int64  `json:"channel_id"`
	Topic        string `json:"topic"`
	Channel      string `json:"channel"`
	Body         []byte `json:"body"`
	BodyBytes    int    `json:"body_bytes"`
	Attempts     int    `json:"attempts"`
	MaxAttempts  int    `json:"max_attempts"`
	AvailableAt  string `json:"available_at"`
	ExpiresAt    string `json:"expires_at"`
	RemainingTTL string `json:"remaining_ttl"`
	Expired      bool   `json:"expired"`
}

// apiDeadList answers GET /api/channels/{id}/dead — the dead page's poller
// feed, honoring the ?before= cursor. Metadata only: bodies are omitted
// (R12).
func (h *handler) apiDeadList(w http.ResponseWriter, r *http.Request) {
	channelID, ok := h.pathID(r, "id")
	if !ok {
		jsonError(w, http.StatusNotFound, "channel not found")
		return
	}
	before := deadBefore(r)
	v, err := h.loadDeadList(r.Context(), channelID, before)
	if errors.Is(err, errNotFound) {
		jsonError(w, http.StatusNotFound, "channel not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := deadListJSON{
		ChannelID: v.ChannelID,
		Before:    before,
		HasOlder:  v.HasOlder,
		Dead:      make([]deadRowJSON, 0, len(v.Rows)),
	}
	for _, row := range v.Rows {
		out.Dead = append(out.Dead, deadRowJSON{
			ID:           row.ID,
			MessageID:    row.MessageID,
			Attempts:     row.Attempts,
			MaxAttempts:  row.MaxAttempts,
			RemainingTTL: row.Remaining,
			Expired:      row.Expired,
			AvailableAt:  row.AvailableAt.Format(time.RFC3339),
			ExpiresAt:    row.ExpiresAt.Format(time.RFC3339),
			HasBody:      row.BodyBytes > 0,
			BodyBytes:    row.BodyBytes,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// apiDeadDelivery answers GET /api/channels/{id}/dead/{deliveryID} — the
// full body plus metadata. Together with the HTML full-body page, one of the
// only two payload-bearing surfaces in the admin (R12).
func (h *handler) apiDeadDelivery(w http.ResponseWriter, r *http.Request) {
	channelID, ok := h.pathID(r, "id")
	deliveryID, okDelivery := h.pathID(r, "deliveryID")
	if !ok || !okDelivery {
		jsonError(w, http.StatusNotFound, "dead delivery not found")
		return
	}
	_, d, err := h.loadDeadDelivery(r.Context(), channelID, deliveryID)
	if errors.Is(err, errNotFound) {
		jsonError(w, http.StatusNotFound, "dead delivery not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	remaining, expired := deadRemaining(d.ExpiresAt)
	writeJSON(w, http.StatusOK, deadDeliveryJSON{
		ID:           d.ID,
		MessageID:    d.MessageID,
		ChannelID:    d.ChannelID,
		Topic:        d.Topic,
		Channel:      d.Channel,
		Body:         d.Body,
		BodyBytes:    len(d.Body),
		Attempts:     d.Attempts,
		MaxAttempts:  d.MaxAttempts,
		AvailableAt:  d.AvailableAt.Format(time.RFC3339),
		ExpiresAt:    d.ExpiresAt.Format(time.RFC3339),
		RemainingTTL: remaining,
		Expired:      expired,
	})
}

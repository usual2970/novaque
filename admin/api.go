package admin

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/usual2970/novaque"
)

// The JSON endpoints mirror page data for the fetch poller (KTD10): the
// summary carries what the dashboard needs (topics + channels + backlogs),
// the detail endpoints carry the zero-filled day buckets. Responses are
// ids and counts only — never message payloads (R12).

type backlogJSON struct {
	Pending  int64 `json:"pending"`
	Ready    int64 `json:"ready"`
	InFlight int64 `json:"in_flight"`
	Dead     int64 `json:"dead"`
}

type channelJSON struct {
	ID      int64       `json:"id"`
	TopicID int64       `json:"topic_id"`
	Name    string      `json:"name"`
	Backlog backlogJSON `json:"backlog"`
}

type topicJSON struct {
	ID       int64         `json:"id"`
	Name     string        `json:"name"`
	Totals   backlogJSON   `json:"totals"`
	Channels []channelJSON `json:"channels"`
}

type dayJSON struct {
	Day     string `json:"day"`
	Publish int64  `json:"publish"`
	Claim   int64  `json:"claim"`
	Ack     int64  `json:"ack"`
	Requeue int64  `json:"requeue"`
	Dead    int64  `json:"dead"`
	Purge   int64  `json:"purge"`
}

type summaryJSON struct {
	Topics []topicJSON `json:"topics"`
}

type topicDetailJSON struct {
	topicJSON
	Days []dayJSON `json:"days"`
}

type channelDetailJSON struct {
	channelJSON
	TopicName string    `json:"topic_name"`
	Days      []dayJSON `json:"days"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func backlogToJSON(b novaque.BacklogRow) backlogJSON {
	return backlogJSON{Pending: b.Pending, Ready: b.Ready, InFlight: b.InFlight, Dead: b.Dead}
}

func channelsToJSON(topicID int64, chans []channelView) []channelJSON {
	out := make([]channelJSON, 0, len(chans))
	for _, c := range chans {
		out = append(out, channelJSON{
			ID:      c.ID,
			TopicID: topicID,
			Name:    c.Name,
			Backlog: backlogToJSON(c.Backlog),
		})
	}
	return out
}

func groupsToJSON(groups []topicGroupView) []topicJSON {
	out := make([]topicJSON, 0, len(groups))
	for _, g := range groups {
		out = append(out, topicJSON{
			ID:       g.ID,
			Name:     g.Name,
			Totals:   backlogToJSON(g.Totals),
			Channels: channelsToJSON(g.ID, g.Channels),
		})
	}
	return out
}

func daysToJSON(days []dayView) []dayJSON {
	out := make([]dayJSON, 0, len(days))
	for _, d := range days {
		out = append(out, dayJSON{
			Day:     d.Day.Format(dateFormat),
			Publish: d.Publish,
			Claim:   d.Claim,
			Ack:     d.Ack,
			Requeue: d.Requeue,
			Dead:    d.Dead,
			Purge:   d.Purge,
		})
	}
	return out
}

// apiSummary answers GET /api/summary — the dashboard poller's feed. Same
// three bounded Client calls as the page (F1, R2).
func (h *handler) apiSummary(w http.ResponseWriter, r *http.Request) {
	groups, err := h.loadGroups(r.Context())
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, summaryJSON{Topics: groupsToJSON(groups)})
}

// apiTopic answers GET /api/topics/{id} — topic detail incl. zero-filled
// day buckets. Unknown ids answer 404 without creating rows (R10).
func (h *handler) apiTopic(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		jsonError(w, http.StatusNotFound, "topic not found")
		return
	}
	v, err := h.loadTopic(r.Context(), id)
	if errors.Is(err, errNotFound) {
		jsonError(w, http.StatusNotFound, "topic not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, topicDetailJSON{
		topicJSON: topicJSON{
			ID:       v.ID,
			Name:     v.Name,
			Totals:   backlogToJSON(v.Totals),
			Channels: channelsToJSON(v.ID, v.Channels),
		},
		Days: daysToJSON(v.Days),
	})
}

// apiChannel answers GET /api/channels/{id} — channel detail incl.
// zero-filled day buckets. Unknown ids answer 404 (R10).
func (h *handler) apiChannel(w http.ResponseWriter, r *http.Request) {
	id, ok := h.pathID(r, "id")
	if !ok {
		jsonError(w, http.StatusNotFound, "channel not found")
		return
	}
	v, err := h.loadChannel(r.Context(), id)
	if errors.Is(err, errNotFound) {
		jsonError(w, http.StatusNotFound, "channel not found")
		return
	}
	if err != nil {
		jsonError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, channelDetailJSON{
		channelJSON: channelJSON{
			ID:      v.ID,
			TopicID: v.TopicID,
			Name:    v.Name,
			Backlog: backlogToJSON(v.Backlog),
		},
		TopicName: v.TopicName,
		Days:      daysToJSON(v.Days),
	})
}

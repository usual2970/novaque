package admin

import (
	"encoding/json"
	"errors"
	"net/http"
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

func groupsToJSON(groups []topicGroupView) []topicJSON {
	out := make([]topicJSON, 0, len(groups))
	for _, g := range groups {
		tj := topicJSON{
			ID:       g.ID,
			Name:     g.Name,
			Totals:   backlogJSON{Pending: g.Totals.Pending, Ready: g.Totals.Ready, InFlight: g.Totals.InFlight, Dead: g.Totals.Dead},
			Channels: make([]channelJSON, 0, len(g.Channels)),
		}
		for _, c := range g.Channels {
			tj.Channels = append(tj.Channels, channelJSON{
				ID:      c.ID,
				TopicID: g.ID,
				Name:    c.Name,
				Backlog: backlogJSON{Pending: c.Backlog.Pending, Ready: c.Backlog.Ready, InFlight: c.Backlog.InFlight, Dead: c.Backlog.Dead},
			})
		}
		out = append(out, tj)
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
			ID:     v.ID,
			Name:   v.Name,
			Totals: backlogJSON{Pending: v.Totals.Pending, Ready: v.Totals.Ready, InFlight: v.Totals.InFlight, Dead: v.Totals.Dead},
			Channels: func() []channelJSON {
				out := make([]channelJSON, 0, len(v.Channels))
				for _, c := range v.Channels {
					out = append(out, channelJSON{
						ID:      c.ID,
						TopicID: v.ID,
						Name:    c.Name,
						Backlog: backlogJSON{Pending: c.Backlog.Pending, Ready: c.Backlog.Ready, InFlight: c.Backlog.InFlight, Dead: c.Backlog.Dead},
					})
				}
				return out
			}(),
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
			Backlog: backlogJSON{Pending: v.Backlog.Pending, Ready: v.Backlog.Ready, InFlight: v.Backlog.InFlight, Dead: v.Backlog.Dead},
		},
		TopicName: v.TopicName,
		Days:      daysToJSON(v.Days),
	})
}

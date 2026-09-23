package admin

import "time"

// contributionGraph is a multi-row heatmap: one strip per counter kind over the
// trailing UTC day window (typically 30 days × 6 metrics).
type contributionGraph struct {
	Total        int64
	WindowDays   int
	MonthColumns []monthColumn
	Metrics      []metricRow
}

type monthColumn struct {
	MonthLabel string
}

type metricRow struct {
	ID    string
	Label string
	Cells []metricCell
}

type metricCell struct {
	Day   time.Time
	Value int64
	Level int
}

type metricDef struct {
	id    string
	label string
	value func(dayView) int64
}

// heatmapScaleFloor avoids mapping a tiny row max (e.g. 2) to l4 when that day
// is only the busiest among quiet days. Rows whose 30-day max meets or exceeds
// the floor use pure relative quartiles against that max.
const heatmapScaleFloor int64 = 1000

var heatmapMetrics = []metricDef{
	{"publish", "Publish", func(d dayView) int64 { return d.Publish }},
	{"claim", "Claim", func(d dayView) int64 { return d.Claim }},
	{"ack", "Ack", func(d dayView) int64 { return d.Ack }},
	{"requeue", "Requeue", func(d dayView) int64 { return d.Requeue }},
	{"purge", "Purge", func(d dayView) int64 { return d.Purge }},
	{"dead", "Dead", func(d dayView) int64 { return d.Dead }},
}

// counterLevel maps a count to heat intensity 0–4 (quartiles). Uses the row
// max when it is at least heatmapScaleFloor; otherwise scales against the
// floor so low-volume rows stay subdued.
func counterLevel(value, max int64) int {
	if value <= 0 || max <= 0 {
		return 0
	}
	scale := max
	if scale < heatmapScaleFloor {
		scale = heatmapScaleFloor
	}
	lvl := int((value*4 + scale - 1) / scale)
	if lvl < 1 {
		return 1
	}
	if lvl > 4 {
		return 4
	}
	return lvl
}

// deadCounterLevel scales dead deliveries within the row only (no volume
// floor). Any dead count is at least l2 so a rare dead day stays visible.
func deadCounterLevel(value, max int64) int {
	if value <= 0 || max <= 0 {
		return 0
	}
	lvl := int((value*4 + max - 1) / max)
	if lvl < 1 {
		lvl = 1
	}
	if lvl > 4 {
		lvl = 4
	}
	if lvl < 2 {
		lvl = 2
	}
	return lvl
}

// chronologicalDayViews returns days oldest-first. zeroFillDaily emits
// newest-first for tables and JSON; heatmaps keep chronological left-to-right.
func chronologicalDayViews(days []dayView) []dayView {
	if len(days) < 2 {
		return days
	}
	first, last := days[0].Day.UTC(), days[len(days)-1].Day.UTC()
	if !first.After(last) {
		return days
	}
	out := make([]dayView, len(days))
	for i := range days {
		out[i] = days[len(days)-1-i]
	}
	return out
}

func buildContributionGraph(days []dayView) contributionGraph {
	if len(days) == 0 {
		return contributionGraph{}
	}
	// Detail pages list days newest-first; the strip reads left-to-right as time.
	chrono := chronologicalDayViews(days)
	months := make([]monthColumn, len(chrono))
	var total int64
	for i, d := range chrono {
		day := d.Day.UTC().Truncate(24 * time.Hour)
		if i == 0 || day.Month() != chrono[i-1].Day.UTC().Truncate(24*time.Hour).Month() {
			months[i].MonthLabel = day.Format("Jan")
		}
		total += d.Publish
	}

	metrics := make([]metricRow, 0, len(heatmapMetrics))
	for _, def := range heatmapMetrics {
		var max int64
		vals := make([]int64, len(chrono))
		for i, d := range chrono {
			v := def.value(d)
			vals[i] = v
			if v > max {
				max = v
			}
		}
		levelFn := counterLevel
		if def.id == "dead" {
			levelFn = deadCounterLevel
		}
		cells := make([]metricCell, len(chrono))
		for i, d := range chrono {
			day := d.Day.UTC().Truncate(24 * time.Hour)
			cells[i] = metricCell{
				Day:   day,
				Value: vals[i],
				Level: levelFn(vals[i], max),
			}
		}
		metrics = append(metrics, metricRow{
			ID:    def.id,
			Label: def.label,
			Cells: cells,
		})
	}

	return contributionGraph{
		Total:        total,
		WindowDays:   len(days),
		MonthColumns: months,
		Metrics:      metrics,
	}
}

// publishLevel is kept for tests naming compatibility.
func publishLevel(publish, max int64) int { return counterLevel(publish, max) }

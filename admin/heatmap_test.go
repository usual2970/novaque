package admin

import (
	"testing"
	"time"

	"github.com/usual2970/novaque"
)

func TestPublishLevel(t *testing.T) {
	tests := []struct {
		publish, max int64
		want         int
	}{
		{0, 10, 0},
		{1, 1000, 1},
		{2, 2, 1}, // quiet row: scaled against floor, not l4
		{500, 500, 2},
		{500, 5000, 1},
		{2500, 5000, 2},
		{5000, 5000, 4},
		{10_000, 10_000, 4},
		{100_000, 100_000, 4},
	}
	for _, tc := range tests {
		if got := publishLevel(tc.publish, tc.max); got != tc.want {
			t.Fatalf("publishLevel(%d, %d) = %d, want %d", tc.publish, tc.max, got, tc.want)
		}
	}
}

func TestBuildMultiMetricStrip(t *testing.T) {
	start := time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC)
	days := []dayView{
		{DailyCounters: dailyRow(start, 2, 1, 1, 0, 0, 0)},
		{DailyCounters: dailyRow(start.AddDate(0, 0, 1), 0, 0, 0, 0, 0, 0)},
	}
	g := buildContributionGraph(days)
	if len(g.Metrics) != 6 {
		t.Fatalf("metrics = %d, want 6", len(g.Metrics))
	}
	if g.Metrics[0].Label != "Publish" || len(g.Metrics[0].Cells) != 2 {
		t.Fatalf("publish row = %+v", g.Metrics[0])
	}
	if g.Metrics[0].Cells[0].Level == 0 || g.Metrics[0].Cells[1].Level != 0 {
		t.Fatalf("publish levels = %d, %d", g.Metrics[0].Cells[0].Level, g.Metrics[0].Cells[1].Level)
	}
	if g.Metrics[1].Cells[0].Value != 1 {
		t.Fatalf("claim[0] = %d, want 1", g.Metrics[1].Cells[0].Value)
	}
}

func TestDeadCounterLevel(t *testing.T) {
	// Dead row: no scale floor; single dead day in a quiet window stays visible.
	if got := deadCounterLevel(1, 1); got < 2 {
		t.Fatalf("deadCounterLevel(1,1) = %d, want >= 2", got)
	}
	if got := deadCounterLevel(2, 2); got != 4 {
		t.Fatalf("deadCounterLevel(2,2) = %d, want 4 (row peak)", got)
	}
	if got := deadCounterLevel(1, 100); got != 2 {
		t.Fatalf("deadCounterLevel(1,100) = %d, want 2 (minimum visibility)", got)
	}
	if got := counterLevel(1, 100); got != 1 {
		t.Fatalf("counterLevel(1,100) = %d, want 1 (publish uses floor)", got)
	}
}

func TestMonthLabelsInStrip(t *testing.T) {
	aug31 := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
	sep1 := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	g := buildContributionGraph([]dayView{
		{DailyCounters: dailyRow(aug31, 1, 0, 0, 0, 0, 0)},
		{DailyCounters: dailyRow(sep1, 1, 0, 0, 0, 0, 0)},
	})
	if g.MonthColumns[0].MonthLabel != "Aug" || g.MonthColumns[1].MonthLabel != "Sep" {
		t.Fatalf("labels = %q, %q", g.MonthColumns[0].MonthLabel, g.MonthColumns[1].MonthLabel)
	}
}

func dailyRow(day time.Time, publish, claim, ack, requeue, dead, purge int64) novaque.DailyCounters {
	return novaque.DailyCounters{
		Day: day, Publish: publish, Claim: claim, Ack: ack,
		Requeue: requeue, Dead: dead, Purge: purge,
	}
}

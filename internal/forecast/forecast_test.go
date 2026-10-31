package forecast

import (
	"math"
	"testing"
	"time"
)

func TestTimeToClearAndFull(t *testing.T) {
	if got := TimeToClear(1000, 100, 300); got != 5 {
		t.Fatalf("clear = %v, want 5", got)
	}
	if !math.IsInf(TimeToClear(1000, 300, 100), 1) {
		t.Fatal("growing backlog should never clear")
	}
	if got := TimeToFull(60, 100, 4); got != 10 {
		t.Fatalf("full = %v, want 10", got)
	}
}

func TestTrackerEstimates(t *testing.T) {
	var tr Tracker
	now := time.Now()
	end, pos := uint64(10000), uint64(0)
	// Arrivals 100/s, drain 600/s: net 500/s.
	for i := 0; i < 20; i++ {
		tr.Observe(now.Add(time.Duration(i)*time.Second), end, pos)
		end += 100
		pos += 600
	}
	e := tr.Estimate()
	if !e.Shrinking || math.Abs(e.ArrivalRate-100) > 5 || math.Abs(e.DrainRate-600) > 5 {
		t.Fatalf("estimate %+v", e)
	}
	want := float64(e.Backlog) / 500
	if math.Abs(e.ClearSeconds-want) > 0.5 || e.ClearLow > e.ClearSeconds || (e.ClearHigh >= 0 && e.ClearHigh < e.ClearSeconds) {
		t.Fatalf("clear %+v, want ≈%.1f with low ≤ clear ≤ high", e, want)
	}
}

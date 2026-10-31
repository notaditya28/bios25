// Package forecast turns backlog measurements into forward-looking estimates: how fast a backlog
// grows or drains, when it will clear (with a confidence range), and when storage will fill.
// The math is deliberately simple and explainable — exponentially weighted moving averages of
// arrival and drain rates with their variance — and is shared by the live brokers and the
// scenario simulator.
package forecast

import (
	"math"
	"sync"
	"time"
)

const alpha = 0.2 // EWMA weight of the newest rate sample

// Rate is an EWMA of a rate together with its variance (for confidence ranges).
type Rate struct {
	Mean, Var float64
	init      bool
}

func (r *Rate) Add(x float64) {
	if !r.init {
		r.Mean, r.init = x, true
		return
	}
	d := x - r.Mean
	r.Mean += alpha * d
	r.Var = (1 - alpha) * (r.Var + alpha*d*d)
}

func (r Rate) Std() float64 { return math.Sqrt(r.Var) }

// TimeToClear: seconds until a backlog clears if arrivals and drain continue at these rates
// (+Inf if it isn't shrinking).
func TimeToClear(backlog, arrival, drain float64) float64 {
	if backlog <= 0 {
		return 0
	}
	if net := drain - arrival; net > 0 {
		return backlog / net
	}
	return math.Inf(1)
}

// TimeToFull: seconds until used reaches budget at growth bytes/s (+Inf if not growing).
func TimeToFull(used, budget, growth float64) float64 {
	if budget <= 0 || growth <= 0 {
		return math.Inf(1)
	}
	if used >= budget {
		return 0
	}
	return (budget - used) / growth
}

// Estimate is a forecast for one backlog (records unless stated).
type Estimate struct {
	Backlog      uint64  `json:"backlog"`
	ArrivalRate  float64 `json:"arrival_per_s"`
	DrainRate    float64 `json:"drain_per_s"`
	Shrinking    bool    `json:"shrinking"`
	ClearSeconds float64 `json:"clear_s"`      // -1 = not shrinking
	ClearLow     float64 `json:"clear_low_s"`  // optimistic (drain one std faster)
	ClearHigh    float64 `json:"clear_high_s"` // pessimistic (drain one std slower; -1 = maybe never)
}

func finite(x float64) float64 {
	if math.IsInf(x, 0) || math.IsNaN(x) {
		return -1
	}
	return math.Round(x*10) / 10
}

// Tracker follows one backlog: a source end position and a sink position (e.g. leader log end
// and a follower's match offset, or a partition end and a consumer group's low watermark).
type Tracker struct {
	mu      sync.Mutex
	lastAt  time.Time
	lastEnd uint64
	lastPos uint64
	arrival Rate
	drain   Rate
	backlog uint64
}

// Observe records a sample.
func (t *Tracker) Observe(now time.Time, end, pos uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if pos > end {
		pos = end
	}
	if !t.lastAt.IsZero() {
		dt := now.Sub(t.lastAt).Seconds()
		if dt <= 0 {
			return
		}
		t.arrival.Add(float64(end-min(end, t.lastEnd)) / dt)
		t.drain.Add(float64(pos-min(pos, t.lastPos)) / dt)
	}
	t.lastAt, t.lastEnd, t.lastPos, t.backlog = now, end, pos, end-pos
}

func (t *Tracker) Estimate() Estimate {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := float64(t.backlog)
	e := Estimate{Backlog: t.backlog, ArrivalRate: math.Round(t.arrival.Mean*10) / 10, DrainRate: math.Round(t.drain.Mean*10) / 10}
	e.ClearSeconds = finite(TimeToClear(b, t.arrival.Mean, t.drain.Mean))
	sd := math.Sqrt(t.arrival.Var + t.drain.Var)
	e.ClearLow = finite(TimeToClear(b, t.arrival.Mean, t.drain.Mean+sd))
	e.ClearHigh = finite(TimeToClear(b, t.arrival.Mean, t.drain.Mean-sd))
	e.Shrinking = t.backlog == 0 || t.drain.Mean > t.arrival.Mean
	return e
}

// Growth tracks a single growing quantity (e.g. bytes on disk).
type Growth struct {
	mu     sync.Mutex
	lastAt time.Time
	last   float64
	rate   Rate
}

func (g *Growth) Observe(now time.Time, v float64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.lastAt.IsZero() {
		if dt := now.Sub(g.lastAt).Seconds(); dt > 0 {
			g.rate.Add(math.Max(0, v-g.last) / dt)
		}
	}
	g.lastAt, g.last = now, v
}

func (g *Growth) Rate() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rate.Mean
}

// HumanDuration renders seconds for recommendations.
func HumanDuration(s float64) string {
	switch {
	case s < 0 || math.IsInf(s, 0):
		return "never (not shrinking)"
	case s < 90:
		return time.Duration(s * float64(time.Second)).Round(time.Second).String()
	case s < 2*3600:
		return time.Duration(s * float64(time.Second)).Round(time.Minute).String()
	}
	return time.Duration(s * float64(time.Second)).Round(time.Hour).String()
}

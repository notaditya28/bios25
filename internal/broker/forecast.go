package broker

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/forecast"
	"github.com/notaditya28/hydra-stream/internal/meta"
)

// forecaster samples replication and consumer backlogs on this broker and turns them into
// forecasts (time to clear, with a range) and recommendations.
type forecaster struct {
	b *Broker

	mu      sync.Mutex
	repl    map[string]*forecast.Tracker // topic/partition->follower
	groups  map[string]*forecast.Tracker // group|topic|partition
	disk    forecast.Growth
	used    int64
	diskAt  time.Time
	snapAt  time.Time
	meta    map[string]replMeta
	avgSize map[meta.PartKey]float64
}

type replMeta struct {
	key      meta.PartKey
	follower string
	up       bool
}

func newForecaster(b *Broker) *forecaster {
	return &forecaster{b: b, repl: map[string]*forecast.Tracker{}, groups: map[string]*forecast.Tracker{},
		meta: map[string]replMeta{}, avgSize: map[meta.PartKey]float64{}}
}

const sampleEvery = 500 * time.Millisecond

func (f *forecaster) run(ctx context.Context) {
	t := time.NewTicker(sampleEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		f.sample(time.Now())
	}
}

func (f *forecaster) sample(now time.Time) {
	b := f.b
	seen := map[string]bool{}
	for _, p := range b.parts.All() {
		fs := p.Followers()
		if len(fs) > 0 {
			l := p.Log()
			if n := l.EndOffset() - l.StartOffset(); n > 0 {
				f.mu.Lock()
				f.avgSize[p.Key] = float64(l.Bytes()) / float64(n)
				f.mu.Unlock()
			}
		}
		for _, fl := range fs {
			k := fmt.Sprintf("%s/%d->%s", p.Key.Topic, p.Key.Partition, fl.ID)
			seen[k] = true
			f.mu.Lock()
			tr := f.repl[k]
			if tr == nil {
				tr = &forecast.Tracker{}
				f.repl[k] = tr
			}
			f.meta[k] = replMeta{key: p.Key, follower: fl.ID, up: time.Since(fl.LastAck) < 2*time.Second}
			f.mu.Unlock()
			tr.Observe(now, fl.End, fl.Match)
		}
	}
	for _, cp := range b.cp.Groups() {
		p := b.parts.Get(meta.PartKey{Topic: cp.Topic, Partition: cp.Partition})
		if p == nil {
			continue
		}
		k := cpKey(cp.Group, cp.Topic, cp.Partition)
		f.mu.Lock()
		tr := f.groups[k]
		if tr == nil {
			tr = &forecast.Tracker{}
			f.groups[k] = tr
		}
		f.mu.Unlock()
		tr.Observe(now, p.Log().EndOffset(), cp.Low)
	}
	f.mu.Lock()
	for k := range f.repl {
		if !seen[k] {
			delete(f.repl, k) // no longer leader: someone else forecasts it
			delete(f.meta, k)
		}
	}
	diskDue := now.Sub(f.diskAt) > 2*time.Second
	f.mu.Unlock()
	if diskDue {
		var used int64
		_ = filepath.WalkDir(b.cfg.DataDir, func(_ string, d fs.DirEntry, err error) error {
			if err == nil && !d.IsDir() {
				if info, err := d.Info(); err == nil {
					used += info.Size()
				}
			}
			return nil
		})
		f.disk.Observe(now, float64(used))
		f.mu.Lock()
		f.used, f.diskAt = used, now
		f.mu.Unlock()
	}
}

type ReplicationForecast struct {
	Topic          string  `json:"topic"`
	Partition      int     `json:"partition"`
	Follower       string  `json:"follower"`
	FollowerRegion string  `json:"follower_region"`
	FollowerUp     bool    `json:"follower_up"`
	BacklogBytes   float64 `json:"backlog_bytes"`
	forecast.Estimate
}

type GroupForecast struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	forecast.Estimate
}

type DiskForecast struct {
	UsedBytes    int64   `json:"used_bytes"`
	BudgetBytes  int64   `json:"budget_bytes"`
	GrowthPerSec float64 `json:"growth_bytes_per_s"`
	FullSeconds  float64 `json:"time_to_full_s"` // -1 = not growing / no budget
}

type Forecast struct {
	Broker          string                `json:"broker"`
	At              time.Time             `json:"at"`
	Replication     []ReplicationForecast `json:"replication"`
	Groups          []GroupForecast       `json:"groups"`
	Disk            DiskForecast          `json:"disk"`
	Recommendations []string              `json:"recommendations"`
}

func (f *forecaster) snapshot() Forecast {
	b := f.b
	out := Forecast{Broker: b.cfg.Info.ID, At: time.Now(), Replication: []ReplicationForecast{}, Groups: []GroupForecast{}, Recommendations: []string{}}
	f.mu.Lock()
	for k, tr := range f.repl {
		m := f.meta[k]
		e := tr.Estimate()
		out.Replication = append(out.Replication, ReplicationForecast{Topic: m.key.Topic, Partition: m.key.Partition,
			Follower: m.follower, FollowerRegion: b.guard.RegionOf(m.follower), FollowerUp: m.up,
			BacklogBytes: float64(e.Backlog) * f.avgSize[m.key], Estimate: e})
	}
	for k, tr := range f.groups {
		var g GroupForecast
		parts := splitKey(k)
		g.Group, g.Topic, g.Partition = parts.group, parts.topic, parts.part
		g.Estimate = tr.Estimate()
		out.Groups = append(out.Groups, g)
	}
	used := f.used
	f.mu.Unlock()
	growth := f.disk.Rate()
	full := forecast.TimeToFull(float64(used), float64(b.cfg.DiskBudget), growth)
	out.Disk = DiskForecast{UsedBytes: used, BudgetBytes: b.cfg.DiskBudget, GrowthPerSec: growth, FullSeconds: -1}
	if full >= 0 && full < 1e12 {
		out.Disk.FullSeconds = full
	}
	sort.Slice(out.Replication, func(i, j int) bool { return out.Replication[i].Backlog > out.Replication[j].Backlog })
	out.Recommendations = recommend(out)
	return out
}

type keyParts struct {
	group, topic string
	part         int
}

func splitKey(k string) keyParts {
	var kp keyParts
	parts := make([]string, 0, 3)
	start := 0
	for i := 0; i < len(k); i++ {
		if k[i] == '|' {
			parts = append(parts, k[start:i])
			start = i + 1
		}
	}
	parts = append(parts, k[start:])
	if len(parts) == 3 {
		kp.group, kp.topic = parts[0], parts[1]
		fmt.Sscan(parts[2], &kp.part)
	}
	return kp
}

// recommend turns forecasts into actions an operator can take. Plain rules, stated as such.
func recommend(fc Forecast) []string {
	var out []string
	worstClear := 0.0
	for _, r := range fc.Replication {
		if r.Backlog < 1000 {
			continue
		}
		name := fmt.Sprintf("%s/%d → %s (%s)", r.Topic, r.Partition, r.Follower, r.FollowerRegion)
		switch {
		case !r.FollowerUp:
			out = append(out, fmt.Sprintf("%s is unreachable; its backlog is %d records (%.1f MB) and grows %.0f/s. Expect a catch-up burst when it returns — keep the recovery share adaptive so live traffic stays fast.",
				name, r.Backlog, r.BacklogBytes/1e6, r.ArrivalRate))
		case !r.Shrinking:
			out = append(out, fmt.Sprintf("%s is falling behind (%.0f/s in, %.0f/s drained). Raise the link's capacity or the recovery share, or apply producer backpressure on non-urgent topics.",
				name, r.ArrivalRate, r.DrainRate))
		default:
			worstClear = max(worstClear, r.ClearSeconds)
			out = append(out, fmt.Sprintf("%s catching up: %d records left, clears in ~%s (range %s–%s).",
				name, r.Backlog, forecast.HumanDuration(r.ClearSeconds), forecast.HumanDuration(r.ClearLow), forecast.HumanDuration(r.ClearHigh)))
		}
	}
	for _, g := range fc.Groups {
		if g.Backlog > 1000 && !g.Shrinking {
			out = append(out, fmt.Sprintf("consumer group %q on %s/%d is falling behind (lag %d, %.0f/s in vs %.0f/s processed): add consumers or slow producers.",
				g.Group, g.Topic, g.Partition, g.Backlog, g.ArrivalRate, g.DrainRate))
		}
	}
	if d := fc.Disk; d.FullSeconds >= 0 {
		if worstClear > 0 && d.FullSeconds < worstClear {
			out = append(out, fmt.Sprintf("storage fills in ~%s, before the replication backlog clears (~%s): prioritise recovery bandwidth and throttle non-urgent producers now.",
				forecast.HumanDuration(d.FullSeconds), forecast.HumanDuration(worstClear)))
		} else if d.FullSeconds < 3600 {
			out = append(out, fmt.Sprintf("storage fills in ~%s at %.1f MB/s: apply producer backpressure or add capacity.",
				forecast.HumanDuration(d.FullSeconds), d.GrowthPerSec/1e6))
		}
	}
	return out
}

// Command hydra-bench measures Hydra Stream's features against baselines run on the SAME code
// paths, so the comparison is about the algorithm, not the implementation:
//
//	bandwidth   adaptive scheduler vs FIFO vs a fixed recovery share, on flow.Link, while a link
//	            loses 40% of its capacity mid-run with a recovery backlog queued
//	checkpoint  granular (offset ranges) vs offset-only checkpoints, crash trials with
//	            heavy-tailed processing times, on client.RangeSet
//	policy      cost of a per-hop jurisdiction check (residency.Guard) per batch
//
// Each benchmark repeats and reports the median (and p95 where it matters). Results go to stdout
// as Markdown and, with -out, to a file.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"math"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/flow"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/route"
	"github.com/notaditya28/hydra-stream/internal/trail"
)

func main() {
	which := flag.String("run", "bandwidth,checkpoint,policy", "benchmarks to run")
	runs := flag.Int("runs", 3, "repetitions per configuration (median reported)")
	out := flag.String("out", "", "also write the Markdown report here")
	flag.Parse()

	var w io.Writer = os.Stdout
	var f *os.File
	if *out != "" {
		var err error
		if f, err = os.Create(*out); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer f.Close()
		w = io.MultiWriter(os.Stdout, f)
	}
	fmt.Fprintf(w, "# Hydra Stream benchmarks\n\n%s · %d runs per configuration · medians unless noted\n", time.Now().Format("2006-01-02 15:04"), *runs)
	for _, name := range strings.Split(*which, ",") {
		switch strings.TrimSpace(name) {
		case "bandwidth":
			bandwidth(w, *runs)
		case "checkpoint":
			checkpoint(w)
		case "policy":
			policyCost(w)
		default:
			fmt.Fprintln(os.Stderr, "unknown benchmark", name)
			os.Exit(2)
		}
	}
}

// ---- bandwidth ----

type bwResult struct {
	liveP50, liveP95, liveP99 float64 // ms
	urgentMiss                float64 // fraction of urgent sends over their deadline
	clearSecs                 float64 // time to drain the recovery backlog
	util                      float64 // bytes moved / capacity offered
}

const (
	bwCap       = 4 << 20 // 4 MB/s
	bwDropAt    = 2 * time.Second
	bwDropTo    = 0.6 // capacity falls to 60%
	bwBacklog   = 10 << 20
	bwChunk     = 128 << 10
	bwLive      = 16 << 10 // every 10 ms: 1.6 MB/s
	bwUrgent    = 1 << 10  // every 50 ms
	bwDeadline  = 25 * time.Millisecond
	bwMaxSecond = 40
)

func bwRun(mode flow.Mode) bwResult {
	l := flow.NewLink(flow.Config{Mode: mode})
	defer l.Close()
	l.SetCapacity(bwCap)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), bwMaxSecond*time.Second)
	defer cancel()

	var mu sync.Mutex
	var live []float64
	var urgent, missed int
	var moved int64
	done := make(chan struct{})
	var clear time.Duration

	// Recovery backlog: 4 senders draining a fixed queue.
	var remaining int64 = bwBacklog
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mu.Lock()
				if remaining <= 0 {
					mu.Unlock()
					return
				}
				remaining -= bwChunk
				mu.Unlock()
				if l.Acquire(ctx, route.Recovery, bwChunk) != nil {
					return
				}
				mu.Lock()
				moved += bwChunk
				mu.Unlock()
			}
		}()
	}
	go func() { wg.Wait(); clear = time.Since(start); close(done) }()
	stop := make(chan struct{})
	tick := func(every time.Duration, fn func()) {
		go func() {
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					go fn()
				}
			}
		}()
	}
	tick(10*time.Millisecond, func() {
		t0 := time.Now()
		if l.Acquire(ctx, route.Live, bwLive) == nil {
			mu.Lock()
			live = append(live, float64(time.Since(t0).Microseconds())/1000)
			moved += bwLive
			mu.Unlock()
		}
	})
	tick(50*time.Millisecond, func() {
		t0 := time.Now()
		if l.Acquire(ctx, route.Urgent, bwUrgent) == nil {
			mu.Lock()
			urgent++
			if time.Since(t0) > bwDeadline {
				missed++
			}
			moved += bwUrgent
			mu.Unlock()
		}
	})
	time.AfterFunc(bwDropAt, func() { l.SetCapacity(bwCap * bwDropTo) })
	select {
	case <-done:
	case <-ctx.Done():
		clear = time.Since(start)
	}
	close(stop)
	elapsed := time.Since(start).Seconds()
	cancel()
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	offered := bwCap*math.Min(elapsed, bwDropAt.Seconds()) + bwCap*bwDropTo*math.Max(0, elapsed-bwDropAt.Seconds())
	r := bwResult{liveP50: pct(live, 50), liveP95: pct(live, 95), liveP99: pct(live, 99), clearSecs: clear.Seconds(), util: float64(moved) / offered}
	if urgent > 0 {
		r.urgentMiss = float64(missed) / float64(urgent)
	}
	return r
}

func bandwidth(w io.Writer, runs int) {
	fmt.Fprintf(w, "\n## 1. Replication bandwidth under a degrading link\n\n")
	fmt.Fprintf(w, "One directed link, %d MB/s, falls to %.0f%% after %v. Live traffic 1.6 MB/s (16 KB / 10 ms), urgent control 1 KB / 50 ms with a %v deadline, and a %d MB recovery backlog sent by 4 concurrent senders. Same scheduler code; only the policy differs.\n\n",
		bwCap>>20, bwDropTo*100, bwDropAt, bwDeadline, bwBacklog>>20)
	fmt.Fprintln(w, "| mode | live p50 ms | live p95 ms | live p99 ms | urgent over deadline | backlog cleared in | link utilization |")
	fmt.Fprintln(w, "|---|---:|---:|---:|---:|---:|---:|")
	for _, mode := range []flow.Mode{flow.FIFO, flow.Fixed, flow.Adaptive} {
		var rs []bwResult
		for range runs {
			rs = append(rs, bwRun(mode))
		}
		med := func(f func(bwResult) float64) float64 {
			xs := make([]float64, len(rs))
			for i, r := range rs {
				xs[i] = f(r)
			}
			return pct(xs, 50)
		}
		name := map[flow.Mode]string{flow.FIFO: "FIFO (one shared queue)", flow.Fixed: "fixed 50% recovery share", flow.Adaptive: "**adaptive (Hydra)**"}[mode]
		fmt.Fprintf(w, "| %s | %.1f | %.1f | %.1f | %.1f%% | %.1f s | %.0f%% |\n", name,
			med(func(r bwResult) float64 { return r.liveP50 }), med(func(r bwResult) float64 { return r.liveP95 }),
			med(func(r bwResult) float64 { return r.liveP99 }), 100*med(func(r bwResult) float64 { return r.urgentMiss }),
			med(func(r bwResult) float64 { return r.clearSecs }), 100*med(func(r bwResult) float64 { return r.util }))
	}
	fmt.Fprintln(w, "\nLive delay is the time a send waits for link budget (queueing), not network latency.")
}

// ---- checkpoint ----

func checkpoint(w io.Writer) {
	const (
		msgs     = 20000
		workers  = 16
		commit   = 100 * time.Millisecond
		trials   = 400
		medianMs = 5.0
		tailP    = 0.02 // 2% of messages are 50x slower (e.g. a slow downstream call)
		tailX    = 50.0
	)
	fmt.Fprintf(w, "\n## 2. Consumer failover: what gets processed twice\n\n")
	fmt.Fprintf(w, "%d messages, %d parallel workers, processing time log-normal (median %.0f ms) with %.0f%% of messages %.0fx slower; checkpoint every %v; the consumer crashes at a random moment (%d trials). Both modes commit at the same instants; the only difference is what a checkpoint can say. Granular uses the real client.RangeSet.\n\n",
		msgs, workers, medianMs, tailP*100, tailX, commit, trials)
	rng := mrand.New(mrand.NewPCG(1, 2))
	// Processing schedule: each worker takes the next offset when free.
	finish := make([]float64, msgs) // ms
	free := make([]float64, workers)
	for off := 0; off < msgs; off++ {
		wk := 0
		for i := range free {
			if free[i] < free[wk] {
				wk = i
			}
		}
		d := medianMs * math.Exp(0.5*rng.NormFloat64())
		if rng.Float64() < tailP {
			d *= tailX
		}
		free[wk] += d
		finish[off] = free[wk]
	}
	end := 0.0
	for _, f := range free {
		end = math.Max(end, f)
	}
	var dupOff, dupGran, sizeGran []float64
	cm := float64(commit.Milliseconds())
	for range trials {
		crash := rng.Float64() * end
		last := math.Floor(crash/cm) * cm
		var acked client.RangeSet
		low := -1
		for off := 0; off < msgs; off++ {
			if finish[off] <= last {
				acked.Add(uint64(off))
			} else if low < 0 {
				low = off
			}
		}
		if low < 0 {
			low = msgs
		}
		acked.TrimBelow(uint64(low))
		off1, gran := 0, 0
		for off := 0; off < msgs; off++ {
			if finish[off] > crash {
				continue // not finished: reprocessed in both modes, not a duplicate
			}
			if off >= low {
				off1++ // offset-only resumes at low: everything done after it repeats
			}
			if off >= low && !acked.Contains(uint64(off)) {
				gran++ // granular: only work finished after the last checkpoint repeats
			}
		}
		dupOff = append(dupOff, float64(off1))
		dupGran = append(dupGran, float64(gran))
		sizeGran = append(sizeGran, float64(acked.Len()))
	}
	fmt.Fprintln(w, "| checkpoint | duplicates after crash (median) | p95 | checkpoint size (ranges, median) |")
	fmt.Fprintln(w, "|---|---:|---:|---:|")
	fmt.Fprintf(w, "| offset-only (Kafka-style committed offset) | %.0f | %.0f | 1 offset |\n", pct(dupOff, 50), pct(dupOff, 95))
	fmt.Fprintf(w, "| **granular ranges (Hydra)** | %.0f | %.0f | %.0f |\n", pct(dupGran, 50), pct(dupGran, 95), pct(sizeGran, 50))
	fmt.Fprintln(w, "\nPulsar can track individual acks on the broker (ack holes) for shared subscriptions; the comparison here is against a single committed offset, the Kafka default. The end-to-end cluster test (TestGranularCheckpointsAndFencing) shows the same effect on the real system: 1 redelivery vs 100.")
}

// ---- policy ----

func policyCost(w io.Writer) {
	fmt.Fprintf(w, "\n## 3. Cost of a per-hop jurisdiction check\n\n")
	pub, key, _ := ed25519.GenerateKey(rand.Reader)
	doc := policy.Document{Version: 1, IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), Policies: []policy.Policy{
		{Class: "financial", StorageRegions: []string{"IN", "EU"}, MinAcks: "all"},
		{Class: "eu-personal", StorageRegions: []string{"EU"}, TransitRegions: []string{"US"}},
	}}
	s, _ := policy.Sign(doc, key)
	eng := policy.NewEngine(pub, []string{"IN", "EU", "US"})
	if err := eng.Install(s); err != nil {
		panic(err)
	}
	dir, _ := os.MkdirTemp("", "hydra-bench")
	defer os.RemoveAll(dir)
	tl, _ := trail.Open(filepath.Join(dir, "decisions.log"))
	defer tl.Close()
	regionOf := func(id string) string { return strings.ToUpper(id[:2]) }
	g := residency.NewGuard("in-1", "IN", regionOf, eng, tl)
	narrow, _ := eng.Class("eu-personal")
	cases := []struct {
		name string
		c    residency.Check
	}{
		{"store check, built-in residency (hot path, allow not logged)", residency.Check{Topic: "t", Residency: "IN", Hop: "store", Need: residency.Store, Quiet: true}},
		{"store check, signed policy class (allow not logged)", residency.Check{Topic: "t", Residency: "policy:financial", Hop: "store", Need: residency.Store, Quiet: true}},
		{"peer check with a per-message class narrowing", residency.Check{Topic: "t", Residency: "policy:financial", Narrow: &narrow, Hop: "replicate-send", Peer: "eu-1", Need: residency.Transit, Quiet: true}},
		{"entry check with an audit record written", residency.Check{Topic: "t", Residency: "policy:financial", Hop: "entry", Need: residency.Transit, Batch: "b1", Records: 100}},
	}
	fmt.Fprintln(w, "| check | ns per batch | allocs |")
	fmt.Fprintln(w, "|---|---:|---:|")
	for _, c := range cases {
		r := testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = g.Do(c.c)
			}
		})
		fmt.Fprintf(w, "| %s | %d | %d |\n", c.name, r.NsPerOp(), r.AllocsPerOp())
	}
	fmt.Fprintln(w, "\nChecks run once per batch per hop, not per message. A produce batch crosses 2-4 hops, so the whole policy path costs a few microseconds against a cross-region round trip of 80-220 ms.")
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Ceil(p/100*float64(len(s)))) - 1
	return s[max(0, min(i, len(s)-1))]
}

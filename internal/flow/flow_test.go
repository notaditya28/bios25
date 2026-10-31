package flow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/route"
)

// run drives a link with a steady live stream and a saturating recovery backlog for d.
func run(t *testing.T, mode Mode, d time.Duration) Stats {
	l := NewLink(Config{Mode: mode})
	defer l.Close()
	const capacity = 4 << 20 // 4 MB/s
	l.SetCapacity(capacity)
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	var wg sync.WaitGroup
	// Recovery: 4 concurrent senders, 128 KB chunks, always busy.
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = l.Acquire(ctx, route.Recovery, 128<<10)
			}
		}()
	}
	// Live: 16 KB every 10 ms (1.6 MB/s, 40% of the link).
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(10 * time.Millisecond)
		defer tk.Stop()
		for ctx.Err() == nil {
			<-tk.C
			_ = l.Acquire(ctx, route.Live, 16<<10)
		}
	}()
	wg.Wait()
	return l.Stats()
}

func TestAdaptiveKeepsLiveFastUnderBacklog(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	fifo := run(t, FIFO, 3*time.Second)
	adapt := run(t, Adaptive, 3*time.Second)
	fl, al := fifo.Classes["live"], adapt.Classes["live"]
	t.Logf("live p95: fifo %.1f ms, adaptive %.1f ms; recovery bytes fifo %d, adaptive %d (recovery weight %.2f)",
		fl.DelayP95, al.DelayP95, fifo.Classes["recovery"].Bytes, adapt.Classes["recovery"].Bytes, adapt.RecoveryWeight)
	if al.DelayP95 >= fl.DelayP95/2 {
		t.Fatalf("adaptive live p95 %.1f ms not clearly better than fifo %.1f ms", al.DelayP95, fl.DelayP95)
	}
	// Work-conserving: recovery still gets most of what live doesn't use.
	total := float64(adapt.Classes["recovery"].Bytes + adapt.Classes["live"].Bytes)
	if total < 0.8*3*(4<<20) {
		t.Fatalf("link under-used: %.0f bytes in 3 s at 4 MB/s", total)
	}
}

func TestUnlimitedLinkNeverWaits(t *testing.T) {
	l := NewLink(Config{})
	defer l.Close()
	start := time.Now()
	for range 1000 {
		if err := l.Acquire(context.Background(), route.Recovery, 1<<20); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > time.Second {
		t.Fatalf("unlimited link throttled: %v", time.Since(start))
	}
}

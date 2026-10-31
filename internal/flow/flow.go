// Package flow schedules traffic on a capacity-limited inter-region link.
//
// Every send acquires its size in bytes from the Link for its destination region before it
// goes out. A token bucket enforces the link's capacity; when several classes wait, the next one
// is picked by start-time fair queuing (each class advances a virtual clock by bytes/weight; the
// lowest clock goes next). The scheduler is work-conserving: capacity a class doesn't use flows
// to the others. Control traffic (votes, heartbeats, metadata) is always served first.
//
// Modes:
//
//	adaptive (default) – an AIMD controller tunes the recovery class's weight from the smoothed
//	                     queueing delay of live traffic: when live traffic waits longer than the
//	                     target, recovery's share is cut multiplicatively; while live is healthy
//	                     and a backlog exists, it grows additively. Bounded so neither starves.
//	fifo               – one queue in arrival order (no classes): a plain replicator.
//	fixed              – recovery/catch-up traffic is rate-limited to a fixed share of the link
//	                     (a static replication rate limit); everything else FIFO.
package flow

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/route"
)

type Mode string

const (
	Adaptive Mode = "adaptive"
	FIFO     Mode = "fifo"
	Fixed    Mode = "fixed"
)

type Config struct {
	Mode               Mode
	TargetLiveDelay    time.Duration // adaptive: live queueing-delay target (default 20ms)
	FixedRecoveryShare float64       // fixed: share of capacity for recovery (default 0.5)
}

func (c Config) withDefaults() Config {
	if c.Mode == "" {
		c.Mode = Adaptive
	}
	if c.TargetLiveDelay == 0 {
		c.TargetLiveDelay = 20 * time.Millisecond
	}
	if c.FixedRecoveryShare == 0 {
		c.FixedRecoveryShare = 0.5
	}
	return c
}

const (
	wUrgent      = 8.0
	wLive        = 4.0
	wRecoveryMin = 0.25 // recovery never gets less than ~5% of a saturated link
	wRecoveryMax = 6.0
	burstSeconds = 0.02
	tick         = 100 * time.Millisecond
	keepSamples  = 2048
)

type waiter struct {
	bytes float64
	class route.Class
	enq   time.Time
	ready chan struct{}
}

// ClassStats summarises one class's traffic since the link was created (or Reset).
type ClassStats struct {
	Bytes    int64   `json:"bytes"`
	Sends    int64   `json:"sends"`
	Queued   int     `json:"queued"`
	DelayP50 float64 `json:"delay_p50_ms"`
	DelayP95 float64 `json:"delay_p95_ms"`
	DelayP99 float64 `json:"delay_p99_ms"`
	DelayMax float64 `json:"delay_max_ms"`
}

type Stats struct {
	Mode            Mode                  `json:"mode"`
	CapacityBps     float64               `json:"capacity_bytes_per_s"`
	RecoveryWeight  float64               `json:"recovery_weight"`
	LiveDelayEWMAms float64               `json:"live_delay_ewma_ms"`
	Classes         map[string]ClassStats `json:"classes"`
}

// Link schedules one directed link.
type Link struct {
	cfg Config

	mu        sync.Mutex
	capacity  float64 // bytes/s; 0 = unlimited
	tokens    float64
	last      time.Time
	recTokens float64 // fixed mode: recovery's own bucket
	queues    [route.NumClasses][]*waiter
	fifo      []*waiter
	pass      [route.NumClasses]float64
	weights   [route.NumClasses]float64
	liveEWMA  float64 // seconds
	liveTick  []float64
	bytes     [route.NumClasses]int64
	sends     [route.NumClasses]int64
	delays    [route.NumClasses][]float64 // seconds, ring
	delayNext [route.NumClasses]int
	wake      chan struct{}
	stop      chan struct{}
}

func NewLink(cfg Config) *Link {
	l := &Link{cfg: cfg.withDefaults(), last: time.Now(), wake: make(chan struct{}, 1), stop: make(chan struct{})}
	l.weights = [route.NumClasses]float64{route.Control: 1000, route.Urgent: wUrgent, route.Live: wLive, route.Recovery: 1}
	go l.dispatch()
	go l.control()
	return l
}

func (l *Link) Close() { close(l.stop) }

// SetCapacity sets the link capacity in bytes per second (0 = unlimited).
func (l *Link) SetCapacity(bps float64) {
	l.mu.Lock()
	l.capacity = bps
	l.tokens = 0
	l.mu.Unlock()
	l.kick()
}

func (l *Link) Capacity() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.capacity
}

func (l *Link) kick() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// Acquire blocks until bytes of class may be sent on the link.
func (l *Link) Acquire(ctx context.Context, class route.Class, bytes int) error {
	l.mu.Lock()
	if l.capacity <= 0 {
		l.recordLocked(class, float64(bytes), 0)
		l.mu.Unlock()
		return nil
	}
	w := &waiter{bytes: float64(bytes), class: class, enq: time.Now(), ready: make(chan struct{})}
	if l.cfg.Mode == FIFO {
		l.fifo = append(l.fifo, w)
	} else {
		if len(l.queues[class]) == 0 {
			// A class waking up starts at the current virtual time (no saved-up credit).
			l.pass[class] = math.Max(l.pass[class], l.minPassLocked())
		}
		l.queues[class] = append(l.queues[class], w)
	}
	l.mu.Unlock()
	l.kick()
	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
		l.mu.Lock()
		l.removeLocked(w)
		l.mu.Unlock()
		select {
		case <-w.ready: // granted concurrently: fine, the bytes were accounted
			return nil
		default:
		}
		return ctx.Err()
	}
}

func (l *Link) removeLocked(w *waiter) {
	del := func(q []*waiter) []*waiter {
		for i, x := range q {
			if x == w {
				return append(q[:i], q[i+1:]...)
			}
		}
		return q
	}
	l.fifo = del(l.fifo)
	l.queues[w.class] = del(l.queues[w.class])
}

func (l *Link) minPassLocked() float64 {
	m := math.Inf(1)
	for c := range l.queues {
		if len(l.queues[c]) > 0 && l.pass[c] < m {
			m = l.pass[c]
		}
	}
	if math.IsInf(m, 1) {
		return 0
	}
	return m
}

// pickLocked chooses the next waiter (nil if none may go now).
func (l *Link) pickLocked() *waiter {
	switch l.cfg.Mode {
	case FIFO:
		if len(l.fifo) == 0 {
			return nil
		}
		w := l.fifo[0]
		l.fifo = l.fifo[1:]
		return w
	case Fixed:
		// Recovery has its own fixed-rate bucket; everything else goes in arrival order.
		var best *waiter
		for c := range l.queues {
			if len(l.queues[c]) == 0 {
				continue
			}
			w := l.queues[c][0]
			if route.Class(c) == route.Recovery && l.recTokens <= 0 {
				continue
			}
			if best == nil || w.enq.Before(best.enq) || route.Class(c) == route.Control {
				best = w
				if route.Class(c) == route.Control {
					break
				}
			}
		}
		if best != nil {
			l.queues[best.class] = l.queues[best.class][1:]
			if best.class == route.Recovery {
				l.recTokens -= best.bytes
			}
		}
		return best
	}
	if len(l.queues[route.Control]) > 0 {
		w := l.queues[route.Control][0]
		l.queues[route.Control] = l.queues[route.Control][1:]
		return w
	}
	best := -1
	for c := range l.queues {
		if len(l.queues[c]) > 0 && (best < 0 || l.pass[c] < l.pass[best]) {
			best = c
		}
	}
	if best < 0 {
		return nil
	}
	w := l.queues[best][0]
	l.queues[best] = l.queues[best][1:]
	l.pass[best] += w.bytes / l.weights[best]
	return w
}

func (l *Link) dispatch() {
	for {
		l.mu.Lock()
		now := time.Now()
		if l.capacity > 0 {
			el := now.Sub(l.last).Seconds()
			burst := l.capacity * burstSeconds
			l.tokens = math.Min(burst, l.tokens+el*l.capacity)
			if l.cfg.Mode == Fixed {
				l.recTokens = math.Min(burst*l.cfg.FixedRecoveryShare, l.recTokens+el*l.capacity*l.cfg.FixedRecoveryShare)
			}
		}
		l.last = now
		var wait time.Duration = -1
		for l.capacity > 0 && l.tokens > 0 {
			w := l.pickLocked()
			if w == nil {
				break
			}
			// Bytes beyond the balance become debt: large sends are allowed, then paid back.
			l.tokens -= w.bytes
			l.recordLocked(w.class, w.bytes, now.Sub(w.enq).Seconds()+w.bytes/l.capacity)
			close(w.ready)
		}
		if l.capacity > 0 && l.pendingLocked() {
			need := -l.tokens
			if l.cfg.Mode == Fixed && l.onlyRecoveryLocked() && l.recTokens <= 0 {
				need = math.Max(need, -l.recTokens/l.cfg.FixedRecoveryShare)
			}
			wait = time.Duration(math.Max(need, 1)/l.capacity*float64(time.Second)) + time.Millisecond/2
		}
		if l.capacity <= 0 { // unlimited: release everyone
			for c := range l.queues {
				for _, w := range l.queues[c] {
					close(w.ready)
				}
				l.queues[c] = nil
			}
			for _, w := range l.fifo {
				close(w.ready)
			}
			l.fifo = nil
		}
		l.mu.Unlock()
		var timer <-chan time.Time
		if wait >= 0 {
			timer = time.After(wait)
		}
		select {
		case <-l.stop:
			return
		case <-l.wake:
		case <-timer:
		}
	}
}

func (l *Link) pendingLocked() bool {
	if len(l.fifo) > 0 {
		return true
	}
	for c := range l.queues {
		if len(l.queues[c]) > 0 {
			return true
		}
	}
	return false
}

func (l *Link) onlyRecoveryLocked() bool {
	for c := range l.queues {
		if route.Class(c) != route.Recovery && len(l.queues[c]) > 0 {
			return false
		}
	}
	return true
}

// recordLocked accounts a granted send; delay = queueing + transmission time (seconds).
func (l *Link) recordLocked(c route.Class, bytes, delay float64) {
	l.bytes[c] += int64(bytes)
	l.sends[c]++
	if len(l.delays[c]) < keepSamples {
		l.delays[c] = append(l.delays[c], delay)
	} else {
		l.delays[c][l.delayNext[c]] = delay
		l.delayNext[c] = (l.delayNext[c] + 1) % keepSamples
	}
	if c == route.Live {
		l.liveTick = append(l.liveTick, delay)
	}
}

// control is the AIMD loop (adaptive mode only).
func (l *Link) control() {
	t := time.NewTicker(tick)
	defer t.Stop()
	l.mu.Lock()
	l.weights[route.Recovery] = 1
	l.mu.Unlock()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
		}
		l.mu.Lock()
		if l.cfg.Mode != Adaptive {
			l.mu.Unlock()
			continue
		}
		// Smoothed live delay: p90 of this tick's samples feeds an EWMA (decays when idle).
		sample := 0.0
		if n := len(l.liveTick); n > 0 {
			sort.Float64s(l.liveTick)
			sample = l.liveTick[(n*9)/10]
		}
		l.liveTick = l.liveTick[:0]
		l.liveEWMA = 0.3*sample + 0.7*l.liveEWMA
		target := l.cfg.TargetLiveDelay.Seconds()
		w := l.weights[route.Recovery]
		switch {
		case l.liveEWMA > target:
			w *= 0.7 // live traffic is suffering: back off catch-up quickly
		case l.liveEWMA < target/2 && len(l.queues[route.Recovery]) > 0:
			w += 0.25 // live is healthy and there is backlog: give catch-up more, slowly
		}
		l.weights[route.Recovery] = math.Max(wRecoveryMin, math.Min(wRecoveryMax, w))
		l.mu.Unlock()
	}
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	return s[min(len(s)-1, int(float64(len(s))*p))] * 1000
}

func (l *Link) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	st := Stats{Mode: l.cfg.Mode, CapacityBps: l.capacity, RecoveryWeight: l.weights[route.Recovery],
		LiveDelayEWMAms: l.liveEWMA * 1000, Classes: map[string]ClassStats{}}
	for c := route.Class(0); c < route.NumClasses; c++ {
		q := len(l.queues[c])
		if l.cfg.Mode == FIFO {
			q = 0
			for _, w := range l.fifo {
				if w.class == c {
					q++
				}
			}
		}
		mx := 0.0
		for _, d := range l.delays[c] {
			mx = math.Max(mx, d)
		}
		st.Classes[c.String()] = ClassStats{Bytes: l.bytes[c], Sends: l.sends[c], Queued: q,
			DelayP50: pct(l.delays[c], 0.5), DelayP95: pct(l.delays[c], 0.95), DelayP99: pct(l.delays[c], 0.99), DelayMax: mx * 1000}
	}
	return st
}

// Reset clears the statistics (not the controller state).
func (l *Link) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for c := range l.delays {
		l.delays[c], l.delayNext[c], l.bytes[c], l.sends[c] = nil, 0, 0, 0
	}
}

package chaos

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
)

// Stream is one producer writing to one partition at a steady rate. Every acknowledged
// message is recorded so the checker can verify it later.
type Stream struct {
	Name      string // also the message prefix: "<name>-<n>"
	Topic     string
	Partition int
	Residency string // topic residency
	Home      string // region whose brokers the client prefers
	Addrs     []string

	mu    sync.Mutex
	acks  []Ack
	fails []Fail
	start time.Time
	stop  time.Time
}

type Ack struct {
	Offset  uint64
	Value   string
	At      time.Time
	Latency time.Duration
}

type Fail struct {
	At     time.Time
	Took   time.Duration
	Code   string
	Reason string
}

func errCode(err error) (string, string) {
	var e *api.Error
	if errors.As(err, &e) {
		return string(e.Code), e.Reason
	}
	return "TRANSPORT", ""
}

// Run produces batches every interval until ctx ends. Failed batches are retried with the same
// sequence numbers by the idempotent producer, so the stream never skips or duplicates.
func (s *Stream) Run(ctx context.Context, batch int, interval time.Duration) {
	prod := client.NewProducer(s.Addrs...)
	s.mu.Lock()
	s.start = time.Now()
	s.mu.Unlock()
	t := time.NewTicker(interval)
	defer t.Stop()
	for n := 0; ctx.Err() == nil; {
		vals := make([]string, batch)
		for i := range vals {
			vals[i] = fmt.Sprintf("%s-%d", s.Name, n+i)
		}
		start := time.Now()
		resp, err := prod.Send(ctx, s.Topic, s.Partition, vals, func(a client.Attempt) {
			if ctx.Err() != nil {
				return // cancelled because the scenario ended, not a cluster error
			}
			code, reason := errCode(a.Err)
			s.mu.Lock()
			s.fails = append(s.fails, Fail{At: time.Now(), Took: a.Took, Code: code, Reason: reason})
			s.mu.Unlock()
		})
		if err != nil {
			break // ctx ended mid-batch; the batch may or may not have committed (it's not counted)
		}
		now := time.Now()
		s.mu.Lock()
		for i, o := range resp.Offsets {
			s.acks = append(s.acks, Ack{Offset: o.Offset, Value: vals[i], At: now, Latency: now.Sub(start)})
		}
		s.mu.Unlock()
		n += batch
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
	s.mu.Lock()
	s.stop = time.Now()
	s.mu.Unlock()
}

func (s *Stream) Acks() []Ack {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Ack(nil), s.acks...)
}

func (s *Stream) Fails() []Fail {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Fail(nil), s.fails...)
}

func (s *Stream) LastAck() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.acks) == 0 {
		return time.Time{}
	}
	return s.acks[len(s.acks)-1].At
}

// MaxStall is the longest time the stream went without an acknowledgement, from its start to
// its stop (a stream that never recovered stalls until the end).
func (s *Stream) MaxStall() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	last, worst := s.start, time.Duration(0)
	for _, a := range s.acks {
		worst = max(worst, a.At.Sub(last))
		last = a.At
	}
	return max(worst, s.stop.Sub(last))
}

func (s *Stream) seqOf(v string) (int, bool) {
	rest, ok := strings.CutPrefix(v, s.Name+"-")
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(rest)
	return n, err == nil
}

// Probe repeatedly asks brokers in other regions for a region's resident topic and records
// how fast an explicit answer (WRONG_REGION / REGION_UNAVAILABLE) comes back.
type Probe struct {
	Topic  string
	Region string   // where the topic is resident
	Via    []string // brokers in other regions to ask

	mu      sync.Mutex
	answers []Fail // reuse: At, Took, Code
}

func (p *Probe) Run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for ctx.Err() == nil {
		for _, addr := range p.Via {
			start := time.Now()
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			_, err := client.New(addr).Fetch(cctx, api.FetchRequest{Topic: p.Topic, MaxRecords: 1})
			cancel()
			code, reason := errCode(err)
			if err == nil {
				code = "OK"
			}
			if code == "TRANSPORT" {
				continue // that broker is down; ask the next one
			}
			p.mu.Lock()
			p.answers = append(p.answers, Fail{At: time.Now(), Took: time.Since(start), Code: code, Reason: reason})
			p.mu.Unlock()
			break
		}
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
}

func (p *Probe) Answers() []Fail {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Fail(nil), p.answers...)
}

// LatencyProbe samples fetch latency of one global partition from one broker (used to show
// rerouting around a slow link).
type LatencyProbe struct {
	Topic     string
	Partition int
	Addr      string

	mu      sync.Mutex
	samples []sample
}

type sample struct {
	at time.Time
	d  time.Duration
}

func (l *LatencyProbe) Run(ctx context.Context, interval time.Duration) {
	c := client.New(l.Addr)
	t := time.NewTicker(interval)
	defer t.Stop()
	for ctx.Err() == nil {
		start := time.Now()
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := c.Fetch(cctx, api.FetchRequest{Topic: l.Topic, Partition: l.Partition, MaxRecords: 1})
		cancel()
		if err == nil {
			l.mu.Lock()
			l.samples = append(l.samples, sample{start, time.Since(start)})
			l.mu.Unlock()
		}
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
}

// P50 returns the median latency of samples taken in [from, to).
func (l *LatencyProbe) P50(from, to time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	var ds []time.Duration
	for _, s := range l.samples {
		if !s.at.Before(from) && s.at.Before(to) {
			ds = append(ds, s.d)
		}
	}
	if len(ds) == 0 {
		return 0
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })
	return ds[len(ds)/2]
}

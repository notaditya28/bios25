package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

// Checkpoint mirrors the broker's checkpoint record.
type Checkpoint struct {
	Group     string      `json:"group"`
	Topic     string      `json:"topic"`
	Partition int         `json:"partition"`
	Member    string      `json:"member"`
	Epoch     uint64      `json:"epoch"`
	Low       uint64      `json:"low"`
	Acked     [][2]uint64 `json:"acked,omitempty"`
	InFlight  []uint64    `json:"in_flight,omitempty"`
	Commits   int64       `json:"commits"`
}

type cpRequest struct {
	Topic     string      `json:"topic"`
	Partition int         `json:"partition"`
	Member    string      `json:"member"`
	Epoch     uint64      `json:"epoch,omitempty"`
	Low       uint64      `json:"low"`
	Acked     [][2]uint64 `json:"acked,omitempty"`
	InFlight  []uint64    `json:"in_flight,omitempty"`
}

func (c *Client) Claim(ctx context.Context, group, topic string, part int, member string) (Checkpoint, error) {
	var out Checkpoint
	return out, c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/claim", cpRequest{Topic: topic, Partition: part, Member: member}, &out)
}

func (c *Client) CommitCheckpoint(ctx context.Context, group string, cp Checkpoint) (Checkpoint, error) {
	var out Checkpoint
	req := cpRequest{Topic: cp.Topic, Partition: cp.Partition, Member: cp.Member, Epoch: cp.Epoch, Low: cp.Low, Acked: cp.Acked, InFlight: cp.InFlight}
	return out, c.do(ctx, http.MethodPost, "/v1/groups/"+url.PathEscape(group)+"/commit", req, &out)
}

// RangeSet is a set of offsets stored as sorted, disjoint [from, to) ranges: compact when
// acknowledgements are mostly contiguous, exact when they are not.
type RangeSet struct{ r [][2]uint64 }

func (s *RangeSet) Add(off uint64) {
	i := sort.Search(len(s.r), func(i int) bool { return s.r[i][1] >= off })
	switch {
	case i < len(s.r) && s.r[i][0] <= off && off < s.r[i][1]:
		return // already present
	case i < len(s.r) && s.r[i][1] == off:
		s.r[i][1]++
		if i+1 < len(s.r) && s.r[i+1][0] == s.r[i][1] {
			s.r[i][1] = s.r[i+1][1]
			s.r = append(s.r[:i+1], s.r[i+2:]...)
		}
	case i < len(s.r) && s.r[i][0] == off+1:
		s.r[i][0] = off
	default:
		s.r = append(s.r, [2]uint64{})
		copy(s.r[i+1:], s.r[i:])
		s.r[i] = [2]uint64{off, off + 1}
	}
}

func (s *RangeSet) Contains(off uint64) bool {
	i := sort.Search(len(s.r), func(i int) bool { return s.r[i][1] > off })
	return i < len(s.r) && s.r[i][0] <= off
}

// TrimBelow drops everything below low.
func (s *RangeSet) TrimBelow(low uint64) {
	out := s.r[:0]
	for _, r := range s.r {
		if r[1] <= low {
			continue
		}
		if r[0] < low {
			r[0] = low
		}
		out = append(out, r)
	}
	s.r = out
}

func (s *RangeSet) Ranges() [][2]uint64 { return append([][2]uint64(nil), s.r...) }
func (s *RangeSet) Len() int            { return len(s.r) }

// GroupConsumer consumes one partition as a member of a group with granular checkpoints:
// out-of-order acknowledgements are remembered exactly, so a new owner after a failover skips
// everything already done. With Granular=false it behaves like a classic consumer that commits a
// single offset (the baseline: everything above the oldest unfinished message is redelivered).
type GroupConsumer struct {
	Group, Topic, Member string
	Partition            int
	Granular             bool

	clients []*Client
	next    int

	mu       sync.Mutex
	epoch    uint64
	low      uint64   // every offset below is done
	acked    RangeSet // done offsets above low
	inflight map[uint64]bool
	pos      uint64 // next offset to fetch
	skipped  int64  // records skipped on resume because the checkpoint said they were done
}

func NewGroupConsumer(group, member, topic string, part int, granular bool, addrs ...string) *GroupConsumer {
	g := &GroupConsumer{Group: group, Member: member, Topic: topic, Partition: part, Granular: granular, inflight: map[uint64]bool{}}
	for _, a := range addrs {
		g.clients = append(g.clients, New(a))
	}
	return g
}

func (g *GroupConsumer) client() *Client { return g.clients[g.next%len(g.clients)] }

// retry runs f against the brokers until it succeeds, rotating on failure.
func (g *GroupConsumer) retry(ctx context.Context, f func(*Client) error) error {
	var err error
	for ctx.Err() == nil {
		if err = f(g.client()); err == nil {
			return nil
		}
		if code := api.CodeOf(err); code == api.CodeStaleEpoch || code == api.CodeInvalidRequest {
			return err
		}
		g.next++
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	return errors.Join(ctx.Err(), err)
}

// Claim takes ownership (fencing any previous owner) and loads the checkpoint.
func (g *GroupConsumer) Claim(ctx context.Context) error {
	var cp Checkpoint
	if err := g.retry(ctx, func(c *Client) error {
		var err error
		cp, err = c.Claim(ctx, g.Group, g.Topic, g.Partition, g.Member)
		return err
	}); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.epoch, g.low, g.pos = cp.Epoch, cp.Low, cp.Low
	g.acked = RangeSet{}
	for _, r := range cp.Acked {
		for o := r[0]; o < r[1]; o++ {
			g.acked.Add(o)
		}
	}
	g.inflight = map[uint64]bool{}
	return nil
}

func (g *GroupConsumer) Epoch() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.epoch
}

// Poll fetches the next records, skipping those the checkpoint says are already done.
func (g *GroupConsumer) Poll(ctx context.Context, max int, wait time.Duration) ([]api.FetchedRecord, error) {
	g.mu.Lock()
	pos := g.pos
	g.mu.Unlock()
	var resp api.FetchResponse
	if err := g.retry(ctx, func(c *Client) error {
		var err error
		resp, err = c.Fetch(ctx, api.FetchRequest{Topic: g.Topic, Partition: g.Partition, Offset: pos, MaxRecords: max, Wait: wait})
		return err
	}); err != nil {
		return nil, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	out := resp.Records[:0]
	for _, r := range resp.Records {
		if r.Offset < g.low || g.acked.Contains(r.Offset) {
			g.skipped++
			continue
		}
		g.inflight[r.Offset] = true
		out = append(out, r)
	}
	if resp.NextOffset > g.pos {
		g.pos = resp.NextOffset
	}
	return out, nil
}

// Ack marks an offset as processed (in any order).
func (g *GroupConsumer) Ack(off uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.inflight, off)
	if off < g.low {
		return
	}
	g.acked.Add(off)
	// Advance the low watermark over the contiguous done prefix.
	for g.acked.Len() > 0 && g.acked.r[0][0] == g.low {
		g.low = g.acked.r[0][1]
		g.acked.TrimBelow(g.low)
	}
}

// Snapshot returns the checkpoint this consumer would commit now.
func (g *GroupConsumer) Snapshot() Checkpoint {
	g.mu.Lock()
	defer g.mu.Unlock()
	cp := Checkpoint{Group: g.Group, Topic: g.Topic, Partition: g.Partition, Member: g.Member, Epoch: g.epoch, Low: g.low}
	if g.Granular {
		cp.Acked = g.acked.Ranges()
		for o := range g.inflight {
			cp.InFlight = append(cp.InFlight, o)
		}
		sort.Slice(cp.InFlight, func(i, j int) bool { return cp.InFlight[i] < cp.InFlight[j] })
	}
	return cp
}

// Commit stores the checkpoint (fenced by the ownership epoch).
func (g *GroupConsumer) Commit(ctx context.Context) error {
	cp := g.Snapshot()
	return g.retry(ctx, func(c *Client) error {
		_, err := c.CommitCheckpoint(ctx, g.Group, cp)
		return err
	})
}

// Skipped counts records skipped on resume because they were already done.
func (g *GroupConsumer) Skipped() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.skipped
}

func (g *GroupConsumer) String() string {
	return fmt.Sprintf("%s/%s on %s/%d epoch %d", g.Group, g.Member, g.Topic, g.Partition, g.Epoch())
}

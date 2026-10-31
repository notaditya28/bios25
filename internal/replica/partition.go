// Package replica implements partition replication inside a region: one leader, followers,
// writes acknowledged once a majority of replicas (2 of 3) has them, and leader-epoch fencing.
package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

type Role int

const (
	RoleNone Role = iota // not serving (offline, fenced, or waiting for metadata)
	RoleFollower
	RoleLeader
)

func (r Role) String() string { return [...]string{"none", "follower", "leader"}[r] }

type Timing struct {
	Heartbeat       time.Duration // leader sends an (empty) append at least this often
	FollowerTimeout time.Duration // a follower silent this long doesn't count towards the quorum
	ProduceTimeout  time.Duration // max wait for a write to reach a majority
	RPCTimeout      time.Duration
	MaxBatch        int // records per replication request
}

func DefaultTiming() Timing {
	return Timing{
		Heartbeat:       100 * time.Millisecond,
		FollowerTimeout: 1000 * time.Millisecond,
		ProduceTimeout:  2000 * time.Millisecond,
		RPCTimeout:      1000 * time.Millisecond,
		MaxBatch:        5000,
	}
}

type partState struct {
	Epoch uint64 `json:"epoch"`
}

type follower struct {
	match   uint64 // follower's log matches ours up to here (exclusive)
	lastAck time.Time
}

// Partition is one replica of a partition.
type Partition struct {
	Key       meta.PartKey
	residency string
	guard     *residency.Guard
	self      string
	log       *seglog.Log
	state     string
	rpc       *rpc.Client
	timing    Timing

	mu          sync.Mutex
	epoch       uint64 // highest leader epoch accepted (persisted): fences anything older
	role        Role
	leader      string
	leaderEpoch uint64
	replicas    []string
	hw          uint64 // high watermark: offsets below are committed on a majority
	epochStart  uint64 // leader: offset of the marker record written on election
	fencedAt    time.Time
	changed     chan struct{} // closed and replaced when hw or role changes
	followers   map[string]*follower
	stopRepl    context.CancelFunc
}

func openPartition(dir string, key meta.PartKey, res, self string, logCfg seglog.Config, client *rpc.Client, timing Timing, guard *residency.Guard) (*Partition, error) {
	l, err := seglog.Open(dir, logCfg)
	if err != nil {
		return nil, err
	}
	p := &Partition{
		Key: key, residency: res, guard: guard, self: self, log: l, state: filepath.Join(dir, "state.json"),
		rpc: client, timing: timing, changed: make(chan struct{}),
	}
	var st partState
	if err := fsutil.ReadJSON(p.state, &st); err != nil && !errors.Is(err, os.ErrNotExist) {
		l.Close()
		return nil, err
	}
	p.epoch = st.Epoch
	return p, nil
}

func (p *Partition) Log() *seglog.Log { return p.log }

func (p *Partition) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Partition) setEpochLocked(e uint64) {
	if e == p.epoch {
		return
	}
	p.epoch = e
	if err := fsutil.WriteJSONAtomic(p.state, partState{Epoch: e}); err != nil {
		slog.Error("persist partition epoch", "partition", p.Key, "err", err)
	}
}

func (p *Partition) stopReplicationLocked() {
	if p.stopRepl != nil {
		p.stopRepl()
		p.stopRepl = nil
	}
	p.followers = nil
}

// Status is a snapshot for metadata APIs and tests.
type Status struct {
	Role        Role
	Leader      string
	Epoch       uint64
	LeaderEpoch uint64
	HW          uint64
	EndOffset   uint64
}

func (p *Partition) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Status{p.role, p.leader, p.epoch, p.leaderEpoch, p.hw, p.log.EndOffset()}
}

// becomeLeader is applied from committed metadata.
func (p *Partition) becomeLeader(epoch uint64, replicas []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch < p.epoch || (p.role == RoleLeader && p.leaderEpoch == epoch) {
		return
	}
	p.stopReplicationLocked()
	p.setEpochLocked(epoch)
	p.role, p.leader, p.leaderEpoch, p.replicas = RoleLeader, p.self, epoch, replicas
	// A marker record in our epoch: the high watermark only advances once a majority has it,
	// so entries from older epochs are committed only together with one of ours.
	recs := []seglog.Record{{Epoch: epoch, Flags: seglog.FlagControl}}
	base, err := p.log.Append(recs)
	if err != nil {
		slog.Error("write leader marker", "partition", p.Key, "err", err)
		p.role = RoleNone
		p.notifyLocked()
		return
	}
	p.epochStart = base
	ctx, cancel := context.WithCancel(context.Background())
	p.stopRepl = cancel
	p.followers = map[string]*follower{}
	now := time.Now()
	for _, r := range replicas {
		if r == p.self {
			continue
		}
		p.followers[r] = &follower{lastAck: now} // grace period before counting it as down
		go p.replicate(ctx, r, epoch)
	}
	p.advanceHWLocked()
	slog.Info("became partition leader", "partition", p.Key, "epoch", epoch, "end_offset", base+1, "hw", p.hw)
	p.notifyLocked()
}

func (p *Partition) becomeFollower(leader string, epoch uint64, replicas []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch < p.epoch || (p.role == RoleFollower && p.leaderEpoch == epoch && p.leader == leader) {
		return
	}
	p.stopReplicationLocked()
	p.setEpochLocked(epoch)
	p.role, p.leader, p.leaderEpoch, p.replicas = RoleFollower, leader, epoch, replicas
	p.notifyLocked()
}

func (p *Partition) becomeNone() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.role == RoleNone {
		return
	}
	p.stopReplicationLocked()
	p.role, p.leader = RoleNone, ""
	p.notifyLocked()
}

// stuckSince reports whether metadata assigns us leadership at an epoch we've been fenced past.
func (p *Partition) stuck(pm *meta.PartitionMeta, olderThan time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return pm.Leader == p.self && p.epoch > pm.LeaderEpoch && !p.fencedAt.IsZero() && time.Since(p.fencedAt) > olderThan
}

// FenceResponse reports our log so the controller can pick the most up-to-date replica.
type FenceResponse struct {
	OK        bool   `json:"ok"`
	Epoch     uint64 `json:"epoch"`
	LastEpoch uint64 `json:"last_epoch"`
	EndOffset uint64 `json:"end_offset"`
}

// fence raises our epoch so nothing from an older leader is accepted any more. Called by the
// controller before it picks a new leader.
func (p *Partition) fence(epoch uint64) *FenceResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch <= p.epoch {
		return &FenceResponse{Epoch: p.epoch}
	}
	if p.role == RoleLeader {
		slog.Warn("leader fenced by controller", "partition", p.Key, "old_epoch", p.leaderEpoch, "new_epoch", epoch)
		p.stopReplicationLocked()
		p.role = RoleNone
		p.notifyLocked()
	}
	p.setEpochLocked(epoch)
	p.fencedAt = time.Now()
	return &FenceResponse{OK: true, Epoch: epoch, LastEpoch: p.log.LastEpoch(), EndOffset: p.log.EndOffset()}
}

func (p *Partition) stepDown(newer uint64, from string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if newer <= p.epoch {
		return
	}
	slog.Warn("fenced: stale leader stepping down", "partition", p.Key, "my_epoch", p.leaderEpoch, "current_epoch", newer, "told_by", from)
	p.stopReplicationLocked()
	p.setEpochLocked(newer)
	p.fencedAt = time.Now()
	p.role, p.leader = RoleNone, ""
	p.notifyLocked()
}

func (p *Partition) notLeaderLocked() error {
	e := api.Errorf(api.CodeNotLeader, "%s is not the leader of %s", p.self, p.Key)
	e.Leader = p.leader
	if p.role == RoleLeader {
		e.Leader = ""
	}
	return e
}

func (p *Partition) quorumAliveLocked() bool {
	alive := 1
	cutoff := time.Now().Add(-p.timing.FollowerTimeout)
	for _, f := range p.followers {
		if f.lastAck.After(cutoff) {
			alive++
		}
	}
	return alive >= len(p.replicas)/2+1
}

// Produce appends records as leader and waits until a majority of replicas has them.
func (p *Partition) Produce(ctx context.Context, recs []seglog.Record) (uint64, error) {
	p.mu.Lock()
	if p.role != RoleLeader {
		err := p.notLeaderLocked()
		p.mu.Unlock()
		return 0, err
	}
	if !p.quorumAliveLocked() {
		p.mu.Unlock()
		e := api.Errorf(api.CodeNotEnoughReplicas, "%s: fewer than %d of %d replicas reachable", p.Key, len(p.replicas)/2+1, len(p.replicas))
		e.Reason = "replicas_unreachable"
		return 0, e
	}
	epoch := p.leaderEpoch
	for i := range recs {
		recs[i].Epoch = epoch
	}
	base, err := p.log.Append(recs)
	if err == nil {
		p.advanceHWLocked() // a single-replica partition commits immediately
	}
	p.mu.Unlock()
	if err != nil {
		if errors.Is(err, seglog.ErrRecordTooLarge) {
			return 0, api.Errorf(api.CodeInvalidRequest, "record larger than %d bytes", seglog.MaxRecordSize)
		}
		return 0, api.Errorf(api.CodeInternal, "append %s: %v", p.Key, err)
	}

	last := base + uint64(len(recs)) - 1
	timer := time.NewTimer(p.timing.ProduceTimeout)
	defer timer.Stop()
	for {
		p.mu.Lock()
		hw, role, le, ch := p.hw, p.role, p.leaderEpoch, p.changed
		p.mu.Unlock()
		if hw > last {
			return base, nil
		}
		if role != RoleLeader || le != epoch {
			e := api.Errorf(api.CodeNotLeader, "%s: leadership changed before the write was committed; it may or may not survive", p.Key)
			e.Reason = "leadership_changed"
			return 0, e
		}
		select {
		case <-ch:
		case <-timer.C:
			e := api.Errorf(api.CodeNotEnoughReplicas, "%s: write not acknowledged by a majority within %v", p.Key, p.timing.ProduceTimeout)
			e.Reason = "ack_timeout"
			return 0, e
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Fetch serves committed records (below the high watermark) as leader. Control records are skipped.
func (p *Partition) Fetch(ctx context.Context, offset uint64, maxRecords, maxBytes int, wait time.Duration) (api.FetchResponse, error) {
	deadline := time.Now().Add(wait)
	for {
		p.mu.Lock()
		if p.role != RoleLeader {
			err := p.notLeaderLocked()
			p.mu.Unlock()
			return api.FetchResponse{}, err
		}
		hw, ch := p.hw, p.changed
		p.mu.Unlock()
		start, end := p.log.StartOffset(), p.log.EndOffset()
		if offset < start || offset > end {
			return api.FetchResponse{}, api.Errorf(api.CodeOffsetOutOfRange, "offset %d outside [%d, %d] for %s", offset, start, hw, p.Key)
		}
		resp := api.FetchResponse{
			Topic: p.Key.Topic, Partition: p.Key.Partition, Records: []api.FetchedRecord{},
			NextOffset: offset, HighWatermark: hw, LogStart: start,
		}
		if offset < hw {
			recs, err := p.log.Read(offset, hw, maxRecords, maxBytes)
			if err != nil {
				return api.FetchResponse{}, api.Errorf(api.CodeInternal, "read %s: %v", p.Key, err)
			}
			for _, r := range recs {
				resp.NextOffset = r.Offset + 1
				if r.IsControl() {
					continue
				}
				resp.Records = append(resp.Records, api.FetchedRecord{Offset: r.Offset, Timestamp: r.Timestamp, Key: string(r.Key), Value: string(r.Value)})
			}
			if len(resp.Records) > 0 || time.Now().After(deadline) {
				return resp, nil
			}
			offset = resp.NextOffset // only control records: keep going
			continue
		}
		left := time.Until(deadline)
		if left <= 0 {
			return resp, nil
		}
		t := time.NewTimer(left)
		select {
		case <-ch:
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return resp, nil
		}
		t.Stop()
	}
}

// AppendRequest is what a leader sends to each follower.
type AppendRequest struct {
	Topic       string          `json:"topic"`
	Partition   int             `json:"partition"`
	Residency   string          `json:"residency"`
	Leader      string          `json:"leader"`
	LeaderEpoch uint64          `json:"leader_epoch"`
	PrevOffset  uint64          `json:"prev_offset"` // offset of the first record (= follower end offset if in sync)
	PrevEpoch   uint64          `json:"prev_epoch"`  // epoch of the record at PrevOffset-1
	HW          uint64          `json:"hw"`
	Records     []seglog.Record `json:"records"`
}

type AppendResponse struct {
	OK        bool   `json:"ok"`
	Epoch     uint64 `json:"epoch"`      // follower's epoch; > LeaderEpoch means the leader is stale
	EndOffset uint64 `json:"end_offset"` // on success: matched up to here; on mismatch: retry from here
}

// handleAppend runs on a follower.
func (p *Partition) handleAppend(req *AppendRequest) (*AppendResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if req.LeaderEpoch < p.epoch {
		slog.Warn("fenced stale leader", "partition", p.Key, "from", req.Leader, "epoch", req.LeaderEpoch, "current", p.epoch)
		return &AppendResponse{Epoch: p.epoch}, nil
	}
	if req.LeaderEpoch > p.epoch || p.role != RoleFollower || p.leader != req.Leader {
		p.stopReplicationLocked()
		p.setEpochLocked(req.LeaderEpoch)
		p.role, p.leader, p.leaderEpoch = RoleFollower, req.Leader, req.LeaderEpoch
		p.notifyLocked()
	}

	leo := p.log.EndOffset()
	if req.PrevOffset > leo {
		return &AppendResponse{Epoch: p.epoch, EndOffset: leo}, nil
	}
	if req.PrevOffset > 0 {
		e, err := p.log.EpochAt(req.PrevOffset - 1)
		if err != nil || e != req.PrevEpoch {
			// Our log diverged from the leader's. Everything below our high watermark is
			// committed and therefore identical on the leader; drop the rest and re-fetch.
			to := min(p.hw, req.PrevOffset-1)
			if err := p.log.TruncateTo(to); err != nil {
				return nil, api.Errorf(api.CodeInternal, "truncate %s: %v", p.Key, err)
			}
			slog.Info("truncated divergent log tail", "partition", p.Key, "to", to, "was", leo)
			return &AppendResponse{Epoch: p.epoch, EndOffset: p.log.EndOffset()}, nil
		}
	}

	recs := req.Records
	for i := range recs {
		if recs[i].Offset != req.PrevOffset+uint64(i) {
			return nil, api.Errorf(api.CodeInvalidRequest, "non-contiguous append for %s", p.Key)
		}
	}
	// Skip records we already have; truncate at the first conflict (Raft's log matching rule).
	i, conflict := 0, false
	for i < len(recs) && recs[i].Offset < leo && !conflict {
		have, err := p.log.Read(recs[i].Offset, min(leo, req.PrevOffset+uint64(len(recs))), len(recs)-i, 1<<30)
		if err != nil || len(have) == 0 {
			return nil, api.Errorf(api.CodeInternal, "read %s: %v", p.Key, err)
		}
		for _, h := range have {
			if h.Epoch != recs[i].Epoch {
				conflict = true
				break
			}
			i++
		}
	}
	if conflict {
		if err := p.log.TruncateTo(recs[i].Offset); err != nil {
			return nil, api.Errorf(api.CodeInternal, "truncate %s: %v", p.Key, err)
		}
		p.hw = min(p.hw, recs[i].Offset)
	}
	recs = recs[i:]
	if len(recs) > 0 {
		if err := p.log.AppendWithOffsets(recs); err != nil {
			return nil, api.Errorf(api.CodeInternal, "append %s: %v", p.Key, err)
		}
	}
	match := req.PrevOffset + uint64(len(req.Records))
	if hw := min(req.HW, match); hw > p.hw {
		p.hw = hw
		p.notifyLocked()
	}
	return &AppendResponse{OK: true, Epoch: p.epoch, EndOffset: match}, nil
}

// replicate streams our log to one follower while we lead at epoch.
func (p *Partition) replicate(ctx context.Context, to string, epoch uint64) {
	if err := p.guard.CheckPeer(p.Key.Topic, p.residency, to, "replicate to"); err != nil {
		return // never ship resident data out of its region, whatever the metadata says
	}
	next := p.log.EndOffset()
	backoff := 20 * time.Millisecond
	var lastSent time.Time
	for ctx.Err() == nil {
		end := p.log.EndOffset()
		next = min(next, end)
		if next == end {
			if wait := p.timing.Heartbeat - time.Since(lastSent); wait > 0 {
				wctx, cancel := context.WithTimeout(ctx, wait)
				_ = p.log.Wait(wctx, end)
				cancel()
				continue
			}
		}
		req := AppendRequest{Topic: p.Key.Topic, Partition: p.Key.Partition, Residency: p.residency, Leader: p.self, LeaderEpoch: epoch, PrevOffset: next}
		if next > 0 {
			e, err := p.log.EpochAt(next - 1)
			if err != nil {
				next = p.log.EndOffset()
				continue
			}
			req.PrevEpoch = e
		}
		if next < end {
			recs, err := p.log.Read(next, end, p.timing.MaxBatch, 4<<20)
			if err != nil {
				slog.Error("read for replication", "partition", p.Key, "err", err)
				time.Sleep(backoff)
				continue
			}
			req.Records = recs
		}
		p.mu.Lock()
		req.HW = p.hw
		p.mu.Unlock()

		lastSent = time.Now()
		rctx, cancel := context.WithTimeout(ctx, p.timing.RPCTimeout)
		var resp AppendResponse
		err := p.rpc.Call(rctx, to, PathAppend, req, &resp)
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				time.Sleep(backoff)
				backoff = min(backoff*2, 200*time.Millisecond)
			}
			continue
		}
		backoff = 20 * time.Millisecond
		if !resp.OK && resp.Epoch > epoch {
			p.stepDown(resp.Epoch, to)
			return
		}
		if !resp.OK {
			next = resp.EndOffset
			continue
		}
		next = resp.EndOffset
		p.ackFollower(to, epoch, resp.EndOffset)
	}
}

func (p *Partition) ackFollower(id string, epoch, match uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f, ok := p.followers[id]
	if p.role != RoleLeader || p.leaderEpoch != epoch || !ok {
		return
	}
	f.match, f.lastAck = match, time.Now()
	p.advanceHWLocked()
}

// advanceHWLocked moves the high watermark to the highest offset a majority of replicas has.
func (p *Partition) advanceHWLocked() {
	matches := []uint64{p.log.EndOffset()}
	for _, f := range p.followers {
		matches = append(matches, f.match)
	}
	slices.Sort(matches)
	slices.Reverse(matches)
	quorum := matches[len(p.replicas)/2]
	if quorum > p.epochStart && quorum > p.hw {
		p.hw = quorum
		p.notifyLocked()
	}
}

func (p *Partition) close() error {
	p.mu.Lock()
	p.stopReplicationLocked()
	p.role = RoleNone
	p.notifyLocked()
	p.mu.Unlock()
	return p.log.Close()
}

func (p *Partition) String() string { return fmt.Sprintf("partition %s", p.Key) }

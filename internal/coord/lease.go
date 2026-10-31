package coord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/rpc"
)

// Lease is a Coordinator using majority-granted, epoch-numbered leases:
//
//   - A candidate bumps the epoch and asks every broker in the group for a lease. It becomes
//     controller only with grants from a majority (2 of 3).
//   - A broker grants at most one candidate per epoch, refuses lower epochs, and refuses newer
//     epochs from other candidates while a lease it granted is still running.
//   - Metadata changes are accepted by a majority before they count as committed, and brokers
//     reject metadata from a controller epoch lower than the one they promised (fencing).
//     A new controller adopts the newest metadata seen in its grant quorum, which always
//     includes every committed version because any two majorities overlap.
//
// Leases rely on clocks advancing at the same rate: safe on one machine; across machines this
// needs bounded clock drift or a real consensus protocol.
type Lease struct {
	cfg    LeaseConfig
	quorum int

	mu         sync.Mutex
	st         persisted
	grantUntil time.Time // the lease we granted (to anyone, maybe ourselves) runs until here

	isCtrl     bool
	ctrlEpoch  uint64
	leaseUntil time.Time
	lastAck    map[string]time.Time
	stuck      []meta.PartKey

	ctrlID        string // controller we last heard from
	ctrlEpochSeen uint64
	nextCampaign  time.Time
	synced        bool
	maxSeen       uint64 // highest epoch seen in rejections

	proposeMu sync.Mutex
	applyMu   sync.Mutex
	applied   *meta.Metadata
	pulling   atomic.Bool
}

type LeaseConfig struct {
	Self      string
	Peers     []string // every broker in the group, including Self
	RPC       *rpc.Client
	StateFile string
	Timing    Timing
	// OnCommit is called, serialised and in version order, for each committed metadata version
	// once this broker is synced.
	OnCommit func(*meta.Metadata)
	// Stuck returns this broker's partitions that are fenced past their assigned leader epoch.
	Stuck func(*meta.Metadata) []meta.PartKey
}

type persisted struct {
	PromisedEpoch uint64         `json:"promised_epoch"`
	PromisedTo    string         `json:"promised_to"`
	Accepted      *meta.Metadata `json:"accepted"`
	Committed     *meta.Metadata `json:"committed"`
}

const (
	pathLease  = "/internal/v1/coord/lease"
	pathPut    = "/internal/v1/coord/put"
	pathCommit = "/internal/v1/coord/commit"
	pathGet    = "/internal/v1/coord/get"
)

type LeaseRequest struct {
	Epoch            uint64 `json:"epoch"`
	Candidate        string `json:"candidate"`
	DurationMs       int64  `json:"duration_ms"`
	CommittedEpoch   uint64 `json:"committed_epoch"`
	CommittedVersion uint64 `json:"committed_version"`
}

type LeaseResponse struct {
	Granted       bool           `json:"granted"`
	PromisedEpoch uint64         `json:"promised_epoch"`
	PromisedTo    string         `json:"promised_to"`
	Accepted      *meta.Metadata `json:"accepted,omitempty"` // sent with a fresh grant
	Stuck         []meta.PartKey `json:"stuck,omitempty"`
}

type PutRequest struct {
	CtrlEpoch  uint64         `json:"ctrl_epoch"`
	Controller string         `json:"controller"`
	Meta       *meta.Metadata `json:"meta"`
}

type PutResponse struct {
	OK            bool   `json:"ok"`
	PromisedEpoch uint64 `json:"promised_epoch"`
}

type CommitRequest struct {
	Controller string `json:"controller"`
	Epoch      uint64 `json:"epoch"`
	Version    uint64 `json:"version"`
}

type GetResponse struct {
	Meta *meta.Metadata `json:"meta"`
}

type empty struct{}

func NewLease(cfg LeaseConfig) (*Lease, error) {
	l := &Lease{cfg: cfg, quorum: len(cfg.Peers)/2 + 1, lastAck: map[string]time.Time{}}
	if err := fsutil.ReadJSON(cfg.StateFile, &l.st); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("load %s: %w", cfg.StateFile, err)
	}
	if l.st.Accepted == nil {
		l.st.Accepted = meta.Empty()
	}
	if l.st.Committed == nil {
		l.st.Committed = meta.Empty()
	}
	now := time.Now()
	// We may have granted a lease just before restarting; honour it for a full duration.
	l.grantUntil = now.Add(cfg.Timing.LeaseDuration)
	l.nextCampaign = now.Add(cfg.Timing.LeaseDuration + l.jitter())
	return l, nil
}

func (l *Lease) jitter() time.Duration { return rand.N(l.cfg.Timing.LeaseDuration/2 + 1) }

func (l *Lease) Register(mux *http.ServeMux) {
	rpc.Handle(mux, pathLease, func(_ context.Context, req *LeaseRequest) (*LeaseResponse, error) {
		return l.handleLease(req), nil
	})
	rpc.Handle(mux, pathPut, func(_ context.Context, req *PutRequest) (*PutResponse, error) {
		return l.handlePut(req), nil
	})
	rpc.Handle(mux, pathCommit, func(_ context.Context, req *CommitRequest) (*empty, error) {
		l.handleCommit(req)
		return &empty{}, nil
	})
	rpc.Handle(mux, pathGet, func(_ context.Context, _ *empty) (*GetResponse, error) {
		return &GetResponse{Meta: l.Committed()}, nil
	})
}

func (l *Lease) Run(ctx context.Context) {
	t := time.NewTicker(l.cfg.Timing.RenewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		l.mu.Lock()
		isCtrl, due := l.isCtrl, time.Now().After(l.nextCampaign)
		l.mu.Unlock()
		switch {
		case isCtrl:
			l.renew(ctx)
		case due:
			l.campaign(ctx)
		}
	}
}

func (l *Lease) IsController() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.isCtrl && time.Now().Before(l.leaseUntil)
}

func (l *Lease) Controller() (string, uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ctrlID, l.ctrlEpochSeen
}

func (l *Lease) Synced() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.synced || l.isCtrl
}

func (l *Lease) Committed() *meta.Metadata {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.st.Committed
}

func (l *Lease) Live() map[string]bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isCtrl {
		return nil
	}
	live := map[string]bool{l.cfg.Self: true}
	cutoff := time.Now().Add(-l.cfg.Timing.FailTimeout)
	for id, t := range l.lastAck {
		if t.After(cutoff) {
			live[id] = true
		}
	}
	return live
}

func (l *Lease) Stuck() []meta.PartKey {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.stuck
}

func (l *Lease) persistLocked() {
	if err := fsutil.WriteJSONAtomic(l.cfg.StateFile, &l.st); err != nil {
		slog.Error("persist coordinator state", "err", err)
	}
}

func (l *Lease) stepDownLocked(reason string) {
	if l.isCtrl {
		slog.Warn("controller stepping down", "epoch", l.ctrlEpoch, "reason", reason)
	}
	l.isCtrl = false
	l.releaseSelfGrantLocked()
	l.nextCampaign = time.Now().Add(l.cfg.Timing.LeaseDuration + l.jitter())
}

// releaseSelfGrantLocked drops the lease we granted ourselves when we are not (or no longer)
// controller. Nobody relies on it, and keeping it would block other candidates for a full
// lease period, which can livelock two candidates that keep blocking each other.
func (l *Lease) releaseSelfGrantLocked() {
	if l.st.PromisedTo == l.cfg.Self {
		l.grantUntil = time.Time{}
	}
}

func (l *Lease) markSyncedLocked() {
	if l.synced {
		return
	}
	l.synced = true
	m := l.st.Committed
	go l.apply(m)
}

func (l *Lease) handleLease(req *LeaseRequest) *LeaseResponse {
	l.mu.Lock()
	now := time.Now()
	resp := &LeaseResponse{}
	granted := false
	switch {
	case req.Epoch < l.st.PromisedEpoch:
	case req.Epoch == l.st.PromisedEpoch:
		granted = l.st.PromisedTo == req.Candidate
	case l.st.PromisedTo != req.Candidate && now.Before(l.grantUntil):
		// Newer epoch, but the lease we granted someone else is still running.
	default:
		l.st.PromisedEpoch, l.st.PromisedTo = req.Epoch, req.Candidate
		l.persistLocked()
		granted = true
		resp.Accepted = l.st.Accepted
	}
	if granted {
		l.grantUntil = now.Add(time.Duration(req.DurationMs) * time.Millisecond)
		if req.Candidate != l.cfg.Self {
			if l.isCtrl {
				l.stepDownLocked(fmt.Sprintf("granted epoch %d to %s", req.Epoch, req.Candidate))
			}
			l.ctrlID, l.ctrlEpochSeen = req.Candidate, req.Epoch
			l.nextCampaign = now.Add(l.cfg.Timing.LeaseDuration + l.jitter())
			theirs := &meta.Metadata{Epoch: req.CommittedEpoch, Version: req.CommittedVersion}
			if meta.Newer(theirs, l.st.Committed) {
				go l.pull(req.Candidate)
			} else {
				l.markSyncedLocked()
			}
		}
	}
	resp.Granted = granted
	resp.PromisedEpoch, resp.PromisedTo = l.st.PromisedEpoch, l.st.PromisedTo
	committed := l.st.Committed
	l.mu.Unlock()

	if granted && req.Candidate != l.cfg.Self && l.cfg.Stuck != nil {
		resp.Stuck = l.cfg.Stuck(committed)
	}
	return resp
}

func (l *Lease) handlePut(req *PutRequest) *PutResponse {
	l.mu.Lock()
	defer l.mu.Unlock()
	if req.CtrlEpoch < l.st.PromisedEpoch {
		slog.Warn("fenced stale controller", "from", req.Controller, "epoch", req.CtrlEpoch, "current", l.st.PromisedEpoch)
		return &PutResponse{PromisedEpoch: l.st.PromisedEpoch}
	}
	if req.CtrlEpoch > l.st.PromisedEpoch {
		l.st.PromisedEpoch, l.st.PromisedTo = req.CtrlEpoch, req.Controller
	}
	if meta.Newer(req.Meta, l.st.Accepted) {
		l.st.Accepted = req.Meta
	}
	l.persistLocked()
	return &PutResponse{OK: true, PromisedEpoch: l.st.PromisedEpoch}
}

func (l *Lease) handleCommit(req *CommitRequest) {
	l.mu.Lock()
	a := l.st.Accepted
	match := a.Epoch == req.Epoch && a.Version == req.Version
	l.mu.Unlock()
	if !match {
		go l.pull(req.Controller)
		return
	}
	l.commit(a)
	l.mu.Lock()
	l.markSyncedLocked()
	l.mu.Unlock()
}

func (l *Lease) commit(m *meta.Metadata) {
	l.mu.Lock()
	if !meta.Newer(m, l.st.Committed) {
		l.mu.Unlock()
		return
	}
	l.st.Committed = m
	if meta.Newer(m, l.st.Accepted) {
		l.st.Accepted = m
	}
	l.persistLocked()
	synced := l.synced || l.isCtrl
	l.mu.Unlock()
	if synced {
		l.apply(m)
	}
}

func (l *Lease) apply(m *meta.Metadata) {
	l.applyMu.Lock()
	defer l.applyMu.Unlock()
	if l.applied != nil && !meta.Newer(m, l.applied) {
		return
	}
	l.applied = m
	if l.cfg.OnCommit != nil {
		l.cfg.OnCommit(m)
	}
}

func (l *Lease) pull(from string) {
	if !l.pulling.CompareAndSwap(false, true) {
		return
	}
	defer l.pulling.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.Timing.RPCTimeout)
	defer cancel()
	var resp GetResponse
	if err := l.cfg.RPC.Call(ctx, from, pathGet, empty{}, &resp); err != nil || resp.Meta == nil {
		return
	}
	l.commit(resp.Meta)
	l.mu.Lock()
	l.markSyncedLocked()
	l.mu.Unlock()
}

// requestLeases asks every peer (and ourselves) for a lease; nil entries are unreachable peers.
func (l *Lease) requestLeases(ctx context.Context, req LeaseRequest) map[string]*LeaseResponse {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.Timing.RPCTimeout)
	defer cancel()
	out := make(map[string]*LeaseResponse, len(l.cfg.Peers))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, peer := range l.cfg.Peers {
		if peer == l.cfg.Self {
			r := l.handleLease(&req)
			mu.Lock()
			out[peer] = r
			mu.Unlock()
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var r LeaseResponse
			if err := l.cfg.RPC.Call(ctx, peer, pathLease, req, &r); err != nil {
				return
			}
			mu.Lock()
			out[peer] = &r
			mu.Unlock()
		}()
	}
	wg.Wait()
	return out
}

func (l *Lease) campaign(ctx context.Context) {
	l.mu.Lock()
	epoch := max(l.st.PromisedEpoch, l.maxSeen) + 1
	l.mu.Unlock()

	sent := time.Now()
	resps := l.requestLeases(ctx, LeaseRequest{Epoch: epoch, Candidate: l.cfg.Self, DurationMs: l.cfg.Timing.LeaseDuration.Milliseconds()})
	grants := 0
	var best *meta.Metadata
	l.mu.Lock()
	for _, r := range resps {
		if r == nil {
			continue
		}
		if r.Granted {
			grants++
			if meta.Newer(r.Accepted, best) {
				best = r.Accepted
			}
		} else {
			l.maxSeen = max(l.maxSeen, r.PromisedEpoch)
		}
	}
	if grants < l.quorum || l.st.PromisedEpoch != epoch || l.st.PromisedTo != l.cfg.Self {
		l.releaseSelfGrantLocked()
		l.nextCampaign = time.Now().Add(l.cfg.Timing.RenewInterval + l.jitter())
		l.mu.Unlock()
		return
	}
	if meta.Newer(best, l.st.Accepted) {
		l.st.Accepted = best
		l.persistLocked()
	}
	now := time.Now()
	l.isCtrl, l.ctrlEpoch, l.leaseUntil = true, epoch, sent.Add(l.cfg.Timing.LeaseDuration)
	l.ctrlID, l.ctrlEpochSeen = l.cfg.Self, epoch
	// Only brokers that answered count as live; silent ones are treated as down immediately.
	l.lastAck = map[string]time.Time{}
	for id, r := range resps {
		if r != nil {
			l.lastAck[id] = now
		}
	}
	l.mu.Unlock()
	slog.Info("became controller", "epoch", epoch, "grants", grants)

	// Commit the metadata our quorum had accepted under our own epoch before acting on it.
	// A slow peer (e.g. a busy disk) can time out once; retry while our lease is valid.
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		if _, err = l.Propose(ctx, func(*meta.Metadata) error { return nil }); err == nil || errors.Is(err, ErrNotController) {
			break
		}
		time.Sleep(l.cfg.Timing.RenewInterval / 2)
	}
	if err != nil {
		l.mu.Lock()
		l.stepDownLocked("initial commit failed: " + err.Error())
		l.mu.Unlock()
	}
}

func (l *Lease) renew(ctx context.Context) {
	l.mu.Lock()
	epoch, c := l.ctrlEpoch, l.st.Committed
	l.mu.Unlock()

	sent := time.Now()
	resps := l.requestLeases(ctx, LeaseRequest{
		Epoch: epoch, Candidate: l.cfg.Self, DurationMs: l.cfg.Timing.LeaseDuration.Milliseconds(),
		CommittedEpoch: c.Epoch, CommittedVersion: c.Version,
	})

	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.isCtrl || l.ctrlEpoch != epoch {
		return
	}
	now := time.Now()
	grants, higher := 0, uint64(0)
	var stuck []meta.PartKey
	for id, r := range resps {
		if r == nil {
			continue
		}
		l.lastAck[id] = now
		if r.Granted {
			grants++
			stuck = append(stuck, r.Stuck...)
		} else if r.PromisedEpoch > epoch {
			higher = max(higher, r.PromisedEpoch)
		}
	}
	l.stuck = stuck
	switch {
	case higher > 0:
		l.maxSeen = max(l.maxSeen, higher)
		l.stepDownLocked(fmt.Sprintf("fenced by epoch %d", higher))
	case grants >= l.quorum:
		l.leaseUntil = sent.Add(l.cfg.Timing.LeaseDuration)
	case now.After(l.leaseUntil):
		l.stepDownLocked("lease expired: no majority")
	}
}

func (l *Lease) Propose(ctx context.Context, mutate func(*meta.Metadata) error) (*meta.Metadata, error) {
	l.proposeMu.Lock()
	defer l.proposeMu.Unlock()

	l.mu.Lock()
	if !l.isCtrl || !time.Now().Before(l.leaseUntil) {
		l.mu.Unlock()
		return nil, ErrNotController
	}
	epoch := l.ctrlEpoch
	next := l.st.Accepted.Clone()
	l.mu.Unlock()

	if err := mutate(next); err != nil {
		return nil, err
	}
	next.Epoch = epoch
	next.Version++

	put := PutRequest{CtrlEpoch: epoch, Controller: l.cfg.Self, Meta: next}
	rctx, cancel := context.WithTimeout(ctx, l.cfg.Timing.RPCTimeout)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	oks, higher := 0, uint64(0)
	for _, peer := range l.cfg.Peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var r *PutResponse
			if peer == l.cfg.Self {
				r = l.handlePut(&put)
			} else {
				r = &PutResponse{}
				if err := l.cfg.RPC.Call(rctx, peer, pathPut, put, r); err != nil {
					return
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if r.OK {
				oks++
			} else {
				higher = max(higher, r.PromisedEpoch)
			}
		}()
	}
	wg.Wait()
	if higher > epoch {
		l.mu.Lock()
		l.maxSeen = max(l.maxSeen, higher)
		l.stepDownLocked(fmt.Sprintf("metadata fenced by epoch %d", higher))
		l.mu.Unlock()
		return nil, ErrNotController
	}
	if oks < l.quorum {
		return nil, api.Errorf(api.CodeNotEnoughReplicas, "metadata accepted by %d of %d brokers, need %d", oks, len(l.cfg.Peers), l.quorum)
	}

	l.commit(next)
	creq := CommitRequest{Controller: l.cfg.Self, Epoch: next.Epoch, Version: next.Version}
	for _, peer := range l.cfg.Peers {
		if peer == l.cfg.Self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = l.cfg.RPC.Call(rctx, peer, pathCommit, creq, nil)
		}()
	}
	wg.Wait() // peers apply before answering, so callers see the change in effect
	return next, nil
}

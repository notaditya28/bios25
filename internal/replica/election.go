package replica

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Leader election for self-elected partitions (global topics: one replica per region, no
// controller above them). This is Raft's election with the usual production extensions:
//
//   - Votes: one per epoch (persisted with the epoch), only for a candidate whose log is at
//     least as up to date as ours (last epoch, then end offset). Commits need a majority and the
//     new leader's marker record, so a new leader always holds every committed record.
//   - Pre-vote: a candidate first asks "would you vote for me?" without bumping its epoch;
//     replicas say no while they hear from a live leader. An isolated region therefore never
//     inflates the epoch and can't knock out a healthy leader when it reconnects.
//   - Check-quorum: a leader that hasn't heard from a majority for FollowerTimeout steps down,
//     so a leader cut off in an isolated region stops serving instead of hanging writes.

const PathVote = "/internal/v1/replica/vote"

type VoteRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Epoch     uint64 `json:"epoch"`
	Candidate string `json:"candidate"`
	LastEpoch uint64 `json:"last_epoch"`
	EndOffset uint64 `json:"end_offset"`
	Pre       bool   `json:"pre"`
}

type VoteResponse struct {
	Granted bool   `json:"granted"`
	Epoch   uint64 `json:"epoch"`
}

// startSelfElected makes this replica take part in its partition's own elections.
func (p *Partition) startSelfElected(replicas []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.replicas = replicas
	if p.selfElected {
		return
	}
	p.selfElected = true
	if p.role == RoleNone {
		p.role = RoleFollower
	}
	// A new (or restarted) replica campaigns after a short jitter rather than a full timeout;
	// pre-vote stops this from disturbing a leader that is already running.
	p.lastHeard = time.Now().Add(-p.timing.ElectionTimeout)
	ctx, cancel := context.WithCancel(context.Background())
	p.stopElect = cancel
	go p.electionLoop(ctx)
}

func (p *Partition) electionTimeout() time.Duration {
	base := p.timing.ElectionTimeout
	return base + rand.N(base/2+1)
}

func (p *Partition) electionLoop(ctx context.Context) {
	timeout := p.electionTimeout()
	t := time.NewTicker(max(p.timing.ElectionTimeout/12, 10*time.Millisecond))
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p.mu.Lock()
		if p.role == RoleLeader {
			if time.Since(p.leaderSince) > p.timing.FollowerTimeout && !p.quorumAliveLocked() {
				slog.Warn("global partition leader lost contact with a majority of regions, stepping down",
					"partition", p.Key, "epoch", p.leaderEpoch)
				p.stopReplicationLocked()
				p.role, p.leader = RoleFollower, ""
				p.lastHeard = time.Now()
				p.notifyLocked()
			}
			p.mu.Unlock()
			continue
		}
		due := time.Since(p.lastHeard) > timeout
		p.mu.Unlock()
		if due {
			p.campaign(ctx)
			timeout = p.electionTimeout()
		}
	}
}

func (p *Partition) logPositionLocked() (uint64, uint64) {
	return p.log.LastEpoch(), p.log.EndOffset()
}

func (p *Partition) campaign(ctx context.Context) {
	p.mu.Lock()
	next := p.epoch + 1
	lastEpoch, end := p.logPositionLocked()
	replicas := p.replicas
	p.lastHeard = time.Now() // restart our timer whatever the outcome
	p.mu.Unlock()

	req := VoteRequest{Topic: p.Key.Topic, Partition: p.Key.Partition, Epoch: next, Candidate: p.self,
		LastEpoch: lastEpoch, EndOffset: end, Pre: true}
	quorum := len(replicas)/2 + 1
	if p.collectVotes(ctx, replicas, req) < quorum {
		return
	}

	p.mu.Lock()
	if p.epoch >= next || p.role == RoleLeader {
		p.mu.Unlock()
		return
	}
	p.setEpochLocked(next)
	p.votedFor = p.self
	p.persistLocked()
	p.role, p.leader = RoleFollower, ""
	p.mu.Unlock()

	req.Pre = false
	votes := p.collectVotes(ctx, replicas, req)
	if votes < quorum {
		return
	}
	slog.Info("won global partition election", "partition", p.Key, "epoch", next, "votes", votes)
	p.becomeLeader(next, replicas)
}

// collectVotes asks every other replica for a vote; our own counts as one.
func (p *Partition) collectVotes(ctx context.Context, replicas []string, req VoteRequest) int {
	ctx, cancel := context.WithTimeout(ctx, p.timing.RPCTimeout)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	votes := 1
	for _, r := range replicas {
		if r == p.self {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp VoteResponse
			if err := p.send.Send(ctx, r, PathVote, p.Key.Topic, p.residency, req, &resp); err != nil {
				return
			}
			if !req.Pre && resp.Epoch > req.Epoch {
				p.stepDown(resp.Epoch, r)
			}
			if resp.Granted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return votes
}

func (p *Partition) handleVote(req *VoteRequest) *VoteResponse {
	p.mu.Lock()
	defer p.mu.Unlock()
	lastEpoch, end := p.logPositionLocked()
	upToDate := req.LastEpoch > lastEpoch || (req.LastEpoch == lastEpoch && req.EndOffset >= end)

	if req.Pre {
		leaderAlive := p.role == RoleLeader || (p.leader != "" && time.Since(p.lastHeard) < p.timing.ElectionTimeout)
		return &VoteResponse{Granted: req.Epoch > p.epoch && upToDate && !leaderAlive, Epoch: p.epoch}
	}
	if req.Epoch < p.epoch {
		return &VoteResponse{Epoch: p.epoch}
	}
	if req.Epoch > p.epoch {
		if p.role == RoleLeader {
			slog.Warn("global partition leader stepping down for a newer election", "partition", p.Key, "epoch", p.epoch, "new_epoch", req.Epoch)
			p.stopReplicationLocked()
		}
		p.setEpochLocked(req.Epoch)
		p.role, p.leader = RoleFollower, ""
		p.notifyLocked()
	}
	if (p.votedFor == "" || p.votedFor == req.Candidate) && upToDate {
		p.votedFor = req.Candidate
		p.persistLocked()
		p.lastHeard = time.Now()
		return &VoteResponse{Granted: true, Epoch: p.epoch}
	}
	return &VoteResponse{Epoch: p.epoch}
}

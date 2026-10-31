package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/replica"
)

// runController elects partition leaders whenever this broker holds the controller lease.
func (b *Broker) runController(ctx context.Context) {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if b.coord.IsController() {
			b.electLeaders(ctx)
		}
	}
}

type decision struct {
	key    meta.PartKey
	leader string
	epoch  uint64
}

// electLeaders finds partitions whose leader is down (or stuck) and elects new ones:
//  1. fence the live replicas with a new epoch, so the old leader can no longer commit;
//  2. pick the fenced replica with the most up-to-date log (highest last epoch, then offset),
//     which holds every committed record because commits need a majority;
//  3. commit the new assignment to the region's metadata, which brokers then apply.
func (b *Broker) electLeaders(ctx context.Context) {
	md := b.coord.Committed()
	live := b.coord.Live()
	stuck := map[meta.PartKey]bool{}
	for _, k := range append(b.coord.Stuck(), b.parts.Stuck(md)...) {
		stuck[k] = true
	}

	var mu sync.Mutex
	var decisions []decision
	var wg sync.WaitGroup
	for name, t := range md.Topics {
		for i, pm := range t.Partitions {
			key := meta.PartKey{Topic: name, Partition: i}
			if pm.Leader != "" && live[pm.Leader] && !stuck[key] {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				if d := b.elect(ctx, key, pm, live); d != nil {
					mu.Lock()
					decisions = append(decisions, *d)
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if len(decisions) == 0 {
		return
	}
	_, err := b.coord.Propose(ctx, func(m *meta.Metadata) error {
		for _, d := range decisions {
			if pm := m.Partition(d.key); pm != nil && d.epoch > pm.LeaderEpoch {
				pm.Leader, pm.LeaderEpoch = d.leader, d.epoch
			}
		}
		return nil
	})
	if err != nil {
		slog.Warn("commit leader elections failed", "count", len(decisions), "err", err)
		return
	}
	for _, d := range decisions {
		slog.Info("partition leader elected", "partition", d.key, "leader", d.leader, "epoch", d.epoch)
	}
}

func (b *Broker) elect(ctx context.Context, key meta.PartKey, pm meta.PartitionMeta, live map[string]bool) *decision {
	quorum := len(pm.Replicas)/2 + 1
	var cands []string
	for _, r := range pm.Replicas {
		if live[r] {
			cands = append(cands, r)
		}
	}
	if len(cands) < quorum {
		return nil
	}
	b.ctrlMu.Lock()
	epoch := max(pm.LeaderEpoch, b.maxEpoch[key]) + 1
	b.maxEpoch[key] = epoch
	b.ctrlMu.Unlock()

	fctx, cancel := context.WithTimeout(ctx, b.cfg.Coord.RPCTimeout)
	defer cancel()
	req := replica.FenceRequest{Topic: key.Topic, Partition: key.Partition, Epoch: epoch}
	resps := make([]*replica.FenceResponse, len(cands))
	var wg sync.WaitGroup
	for i, c := range cands {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var r *replica.FenceResponse
			if c == b.cfg.Info.ID {
				r, _ = b.parts.Fence(&req)
			} else {
				r = &replica.FenceResponse{}
				if err := b.rpc.Call(fctx, c, replica.PathFence, req, r); err != nil {
					r = nil
				}
			}
			resps[i] = r
		}()
	}
	wg.Wait()

	best, oks := -1, 0
	for i, r := range resps {
		if r == nil {
			continue
		}
		if !r.OK {
			b.ctrlMu.Lock()
			b.maxEpoch[key] = max(b.maxEpoch[key], r.Epoch)
			b.ctrlMu.Unlock()
			continue
		}
		oks++
		if best < 0 || r.LastEpoch > resps[best].LastEpoch ||
			(r.LastEpoch == resps[best].LastEpoch && r.EndOffset > resps[best].EndOffset) {
			best = i
		}
	}
	if oks < quorum {
		return nil
	}
	// Ties go to the earliest replica in preferred-leader order (cands follows replica order).
	return &decision{key: key, leader: cands[best], epoch: epoch}
}

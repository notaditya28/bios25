package broker

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/route"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

func batchLabel(pid, seq uint64, n int) string {
	if pid != 0 {
		return fmt.Sprintf("producer %d seq %d+%d", pid, seq, n)
	}
	return fmt.Sprintf("%d records", n)
}

func (b *Broker) Produce(ctx context.Context, req api.ProduceRequest) (api.ProduceResponse, error) {
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		if _, target, ok := b.proxyTarget(req.Topic); ok {
			return b.proxyProduce(ctx, target, req)
		}
		return api.ProduceResponse{}, err
	}
	if len(req.Records) == 0 || len(req.Records) > api.MaxProduceRecords {
		return api.ProduceResponse{}, api.Errorf(api.CodeInvalidRequest, "a produce request needs 1..%d records", api.MaxProduceRecords)
	}
	s, err := b.resolveSpec(t, req.ProducerID, req.BaseSeq, req.Class, req.Acks, req.Via)
	if err != nil {
		b.stats.policyDenied.Add(1)
		return api.ProduceResponse{}, err
	}
	// Entry hop: the data is passing through this broker.
	if err := b.guard.Do(residency.Check{Topic: req.Topic, Partition: -1, Residency: t.Config.Residency, Narrow: s.narrow,
		Hop: "entry", Need: residency.Transit, Batch: batchLabel(req.ProducerID, req.BaseSeq, len(req.Records)), Records: len(req.Records)}); err != nil {
		return api.ProduceResponse{}, err
	}
	n := len(t.Partitions)
	resp := api.ProduceResponse{Offsets: make([]api.RecordOffset, len(req.Records)), Acks: s.acks,
		Policy: fmt.Sprintf("%s v%d", s.eff.Name, s.eff.Version)}
	if req.ProducerID != 0 {
		// Idempotent batches target exactly one explicit partition (sequence numbers are per partition).
		p := req.Records[0].Partition
		for _, r := range req.Records {
			if r.Partition == nil || p == nil || *r.Partition != *p {
				return api.ProduceResponse{}, api.Errorf(api.CodeInvalidRequest, "an idempotent batch must set the same explicit partition on every record")
			}
		}
		if *p < 0 || *p >= n {
			return api.ProduceResponse{}, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, *p)
		}
		if err := b.checkPlacement(t, *p, s); err != nil {
			return api.ProduceResponse{}, err
		}
		base, err := b.producePartition(ctx, t, *p, req.Records, s, 0)
		if err != nil {
			return api.ProduceResponse{}, err
		}
		for i := range resp.Offsets {
			resp.Offsets[i] = api.RecordOffset{Partition: *p, Offset: base + uint64(i)}
		}
		b.stats.produced.Add(int64(len(req.Records)))
		return resp, nil
	}
	sticky := b.sticky(req.Topic, n)
	groups := map[int][]int{}
	var order []int
	for i, r := range req.Records {
		var p int
		switch {
		case r.Partition != nil:
			p = *r.Partition
			if p < 0 || p >= n {
				return api.ProduceResponse{}, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, p)
			}
		case r.Key != "":
			h := fnv.New32a()
			h.Write([]byte(r.Key))
			p = int(h.Sum32() % uint32(n))
		default:
			p = sticky
		}
		if _, ok := groups[p]; !ok {
			order = append(order, p)
			if err := b.checkPlacement(t, p, s); err != nil {
				return api.ProduceResponse{}, err
			}
		}
		groups[p] = append(groups[p], i)
	}

	errs := make([]error, len(order))
	var wg sync.WaitGroup
	for gi, p := range order {
		wg.Add(1)
		go func() {
			defer wg.Done()
			idx := groups[p]
			recs := make([]api.ProduceRecord, len(idx))
			for j, i := range idx {
				recs[j] = req.Records[i]
			}
			base, err := b.producePartition(ctx, t, p, recs, s, 0)
			if err != nil {
				errs[gi] = err
				return
			}
			for j, i := range idx {
				resp.Offsets[i] = api.RecordOffset{Partition: p, Offset: base + uint64(j)}
			}
		}()
	}
	wg.Wait()
	// Partitions are independent: if one failed, others may have committed (see NOTES.md).
	for _, err := range errs {
		if err != nil {
			return api.ProduceResponse{}, err
		}
	}
	b.stats.produced.Add(int64(len(req.Records)))
	return resp, nil
}

// proxyProduce serves a client in a transit-only region by passing the request, unchanged and
// unstored, to a broker in a region allowed to store it.
func (b *Broker) proxyProduce(ctx context.Context, target string, req api.ProduceRequest) (api.ProduceResponse, error) {
	ts, _, _ := b.proxyTarget(req.Topic)
	var classes []string
	if req.Class != "" {
		classes = []string{req.Class}
	}
	n, err := b.geo.NarrowFor(classes)
	if err != nil {
		return api.ProduceResponse{}, err
	}
	if err := b.guard.Do(residency.Check{Topic: req.Topic, Partition: -1, Residency: ts.Residency, Narrow: n,
		Hop: "proxy", Need: residency.Transit, Peer: "", Batch: batchLabel(req.ProducerID, req.BaseSeq, len(req.Records)), Records: len(req.Records)}); err != nil {
		return api.ProduceResponse{}, err
	}
	var out api.ProduceResponse
	cctx, cancel := context.WithTimeout(ctx, b.cfg.Replica.ProduceTimeout+3*b.cfg.Replica.RPCTimeout)
	defer cancel()
	err = b.geo.Send(cctx, target, "/v1/topics/"+req.Topic+"/records",
		route.Opts{Topic: req.Topic, Residency: ts.Residency, Narrow: n, Classes: classes, Class: route.Live, Via: req.Via}, req, &out)
	if err != nil {
		return api.ProduceResponse{}, asUnavailable(err, target)
	}
	b.stats.proxied.Add(int64(len(req.Records)))
	return out, nil
}

type partitionProduce struct {
	Topic      string              `json:"topic"`
	Partition  int                 `json:"partition"`
	Records    []api.ProduceRecord `json:"records"`
	ProducerID uint64              `json:"producer_id,omitempty"`
	BaseSeq    uint64              `json:"base_seq,omitempty"`
	Class      string              `json:"class,omitempty"`
	Acks       string              `json:"acks,omitempty"`
	Via        string              `json:"via,omitempty"`
	Hops       int                 `json:"hops"`
}

type partitionProduceResp struct {
	Base uint64 `json:"base"`
}

func (b *Broker) producePartition(ctx context.Context, t *meta.TopicMeta, part int, recs []api.ProduceRecord, s spec, hops int) (uint64, error) {
	local, cands, err := b.targets(t, part, hops)
	if err != nil {
		return 0, err
	}
	if local {
		return b.produceLocal(ctx, t.Config.Name, part, recs, s)
	}
	var out partitionProduceResp
	in := partitionProduce{t.Config.Name, part, recs, s.pid, s.seq, s.class, s.acks, s.via, hops + 1}
	if err := b.forward(ctx, t, part, s, cands, pathProduce, b.cfg.Replica.ProduceTimeout+2*b.cfg.Replica.RPCTimeout, in, &out); err != nil {
		return 0, err
	}
	return out.Base, nil
}

func (b *Broker) produceLocal(ctx context.Context, topic string, part int, recs []api.ProduceRecord, s spec) (uint64, error) {
	p := b.parts.Get(meta.PartKey{Topic: topic, Partition: part})
	if p == nil {
		return 0, api.Errorf(api.CodeNotLeader, "%s has no replica of %s/%d", b.cfg.Info.ID, topic, part)
	}
	hdr := headersFor(s.class)
	lrecs := make([]seglog.Record, len(recs))
	for i, r := range recs {
		if r.Key != "" {
			lrecs[i].Key = []byte(r.Key)
		}
		lrecs[i].Value = []byte(r.Value)
		lrecs[i].Headers = hdr
	}
	return p.Produce(ctx, lrecs, s.pid, s.seq, s.acks)
}

// handleProduce is the internal forward target. It re-resolves the policy with this broker's own
// (authoritative) copy instead of trusting the sender's evaluation.
func (b *Broker) handleProduce(ctx context.Context, req *partitionProduce) (*partitionProduceResp, error) {
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		return nil, err
	}
	if req.Partition < 0 || req.Partition >= len(t.Partitions) {
		return nil, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	s, err := b.resolveSpec(t, req.ProducerID, req.BaseSeq, req.Class, req.Acks, req.Via)
	if err != nil {
		return nil, err
	}
	if err := b.checkPlacement(t, req.Partition, s); err != nil {
		return nil, err
	}
	base, err := b.producePartition(ctx, t, req.Partition, req.Records, s, req.Hops)
	if err != nil {
		return nil, err
	}
	return &partitionProduceResp{Base: base}, nil
}

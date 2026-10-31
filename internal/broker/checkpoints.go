package broker

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

// Granular consumer checkpoints.
//
// A consumer group's progress on a partition is stored as a low watermark (everything below is
// done) plus the exact acknowledged ranges above it, the in-flight offsets, and an ownership
// epoch. After a failover the new owner skips everything already acknowledged, even when
// processing completed out of order — a single committed offset would force redelivery of every
// message above the oldest unfinished one.
//
// Checkpoints are records in an internal topic per residency ("__cp.<residency>") with the SAME
// residency/policy as the data topic: checkpoints are replicated (2-of-3, or across regions for
// cross-region data) and never stored anywhere the data itself couldn't be. A replicated state
// machine (replica.Applier) on every replica folds the records into the current checkpoint per
// (group, topic, partition); the partition leader fences writes from stale owners with a
// compare-and-append, so a zombie consumer can't move a checkpoint after someone else took over.

const (
	cpPartitions = 4
	pathCP       = "/internal/v1/broker/checkpoint"
	maxCPRanges  = 4096
)

type Checkpoint struct {
	Group     string      `json:"group"`
	Topic     string      `json:"topic"`
	Partition int         `json:"partition"`
	Member    string      `json:"member"`
	Epoch     uint64      `json:"epoch"`
	Low       uint64      `json:"low"`             // every offset below is done
	Acked     [][2]uint64 `json:"acked,omitempty"` // [from, to) ranges above Low that are done
	InFlight  []uint64    `json:"in_flight,omitempty"`
	UpdatedAt int64       `json:"updated_at"`
	Commits   int64       `json:"commits"`
}

type cpRecord struct {
	Kind string `json:"kind"` // claim | commit
	Checkpoint
}

func cpKey(group, topic string, part int) string {
	return group + "|" + topic + "|" + strconv.Itoa(part)
}

func cpTopicFor(residency string) string {
	return "__cp." + strings.NewReplacer(":", ".", "_", "-").Replace(strings.ToLower(residency))
}

func cpPartition(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % cpPartitions)
}

// cpState is the replicated state machine of one checkpoint-topic partition.
type cpState struct {
	mu sync.Mutex
	m  map[string]*Checkpoint
}

func (s *cpState) Reset() {
	s.mu.Lock()
	s.m = map[string]*Checkpoint{}
	s.mu.Unlock()
}

func (s *cpState) Apply(r seglog.Record) {
	var rec cpRecord
	if json.Unmarshal(r.Value, &rec) != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := cpKey(rec.Group, rec.Topic, rec.Partition)
	cur := s.m[k]
	switch rec.Kind {
	case "claim":
		if cur == nil {
			cur = &Checkpoint{Group: rec.Group, Topic: rec.Topic, Partition: rec.Partition}
			s.m[k] = cur
		}
		cur.Epoch, cur.Member, cur.UpdatedAt = rec.Epoch, rec.Member, rec.UpdatedAt
	case "commit":
		if cur == nil || rec.Epoch != cur.Epoch {
			return // validated on the leader; ignore anything stale defensively
		}
		cur.Low, cur.Acked, cur.InFlight, cur.UpdatedAt = rec.Low, rec.Acked, rec.InFlight, rec.UpdatedAt
		cur.Commits++
	}
}

func (s *cpState) get(k string) (Checkpoint, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.m[k]
	if !ok {
		return Checkpoint{}, false
	}
	out := *c
	out.Acked = append([][2]uint64(nil), c.Acked...)
	out.InFlight = append([]uint64(nil), c.InFlight...)
	return out, true
}

func (s *cpState) all() []Checkpoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Checkpoint, 0, len(s.m))
	for _, c := range s.m {
		out = append(out, *c)
	}
	return out
}

type checkpoints struct {
	b      *Broker
	mu     sync.Mutex
	states map[meta.PartKey]*cpState
}

func newCheckpoints(b *Broker) *checkpoints {
	return &checkpoints{b: b, states: map[meta.PartKey]*cpState{}}
}

// applierFor gives checkpoint-topic partitions their state machine.
func (c *checkpoints) applierFor(topic string, part int) replica.Applier {
	if !strings.HasPrefix(topic, "__cp.") {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := meta.PartKey{Topic: topic, Partition: part}
	s := c.states[k]
	if s == nil {
		s = &cpState{m: map[string]*Checkpoint{}}
		c.states[k] = s
	}
	return s
}

// cpOp is one checkpoint operation, forwarded to the checkpoint partition's leader.
type cpOp struct {
	Op        string      `json:"op"` // claim | commit | get
	Group     string      `json:"group"`
	Topic     string      `json:"topic"`
	Partition int         `json:"partition"`
	Member    string      `json:"member"`
	Epoch     uint64      `json:"epoch"`
	Low       uint64      `json:"low"`
	Acked     [][2]uint64 `json:"acked,omitempty"`
	InFlight  []uint64    `json:"in_flight,omitempty"`
	Hops      int         `json:"hops"`
}

func normaliseRanges(low uint64, rs [][2]uint64) ([][2]uint64, error) {
	if len(rs) > maxCPRanges {
		return nil, api.Errorf(api.CodeInvalidRequest, "checkpoint has %d ranges; at most %d (commit more often)", len(rs), maxCPRanges)
	}
	out := make([][2]uint64, 0, len(rs))
	for _, r := range rs {
		if r[1] <= r[0] || r[1] <= low {
			continue
		}
		out = append(out, [2]uint64{max(r[0], low+1), r[1]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	merged := out[:0]
	for _, r := range out {
		if n := len(merged); n > 0 && r[0] <= merged[n-1][1] {
			merged[n-1][1] = max(merged[n-1][1], r[1])
			continue
		}
		merged = append(merged, r)
	}
	return merged, nil
}

// do runs op on this broker if it leads the checkpoint partition, else forwards it.
func (c *checkpoints) do(ctx context.Context, op cpOp) (*Checkpoint, error) {
	b := c.b
	data, err := b.topicMeta(op.Topic)
	if err != nil {
		return nil, err
	}
	if op.Partition < 0 || op.Partition >= len(data.Partitions) {
		return nil, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", op.Topic, op.Partition)
	}
	cpName := cpTopicFor(data.Config.Residency)
	t, err := b.topicMeta(cpName)
	if err != nil {
		// First use for this residency: create the checkpoint topic with the data's own policy.
		if _, cerr := b.CreateTopic(ctx, api.TopicConfig{Name: cpName, Partitions: cpPartitions, Residency: data.Config.Residency}); cerr != nil {
			return nil, cerr
		}
		if t, err = b.topicMeta(cpName); err != nil {
			return nil, err
		}
	}
	key := cpKey(op.Group, op.Topic, op.Partition)
	part := cpPartition(key)
	local, cands, err := b.targets(t, part, op.Hops)
	if err != nil {
		return nil, err
	}
	if !local {
		var out Checkpoint
		op.Hops++
		if err := b.forward(ctx, t, part, spec{}, cands, pathCP, b.cfg.Replica.ProduceTimeout+2*b.cfg.Replica.RPCTimeout, op, &out); err != nil {
			return nil, err
		}
		return &out, nil
	}
	p := b.parts.Get(meta.PartKey{Topic: cpName, Partition: part})
	if p == nil {
		return nil, api.Errorf(api.CodeNotLeader, "%s has no replica of %s/%d", b.cfg.Info.ID, cpName, part)
	}
	st, _ := p.Applier().(*cpState)
	if st == nil {
		return nil, api.Errorf(api.CodeInternal, "checkpoint state machine missing")
	}
	now := time.Now().UnixMilli()
	switch op.Op {
	case "get":
		var cp Checkpoint
		var ok bool
		p.WithLock(func() { cp, ok = st.get(key) })
		if !ok {
			return nil, api.Errorf(api.CodeUnknownTopic, "no checkpoint for group %q on %s/%d", op.Group, op.Topic, op.Partition)
		}
		return &cp, nil
	case "claim":
		rec := cpRecord{Kind: "claim", Checkpoint: Checkpoint{Group: op.Group, Topic: op.Topic, Partition: op.Partition, Member: op.Member, UpdatedAt: now}}
		recs := []seglog.Record{{}}
		_, err := p.ProduceIf(ctx, recs, replica.AcksQuorum, func() error {
			cur, _ := st.get(key)
			rec.Epoch = cur.Epoch + 1 // a new owner always fences the previous one
			recs[0].Value, _ = json.Marshal(rec)
			return nil
		})
		if err != nil {
			return nil, err
		}
		cp, _ := st.get(key)
		cp.Epoch, cp.Member = rec.Epoch, op.Member
		b.stats.cpClaims.Add(1)
		return &cp, nil
	case "commit":
		ranges, err := normaliseRanges(op.Low, op.Acked)
		if err != nil {
			return nil, err
		}
		rec := cpRecord{Kind: "commit", Checkpoint: Checkpoint{Group: op.Group, Topic: op.Topic, Partition: op.Partition,
			Member: op.Member, Epoch: op.Epoch, Low: op.Low, Acked: ranges, InFlight: op.InFlight, UpdatedAt: now}}
		val, _ := json.Marshal(rec)
		recs := []seglog.Record{{Value: val}}
		_, err = p.ProduceIf(ctx, recs, replica.AcksQuorum, func() error {
			cur, ok := st.get(key)
			if !ok || cur.Epoch != op.Epoch {
				b.stats.cpFenced.Add(1)
				e := api.Errorf(api.CodeStaleEpoch, "checkpoint of group %q on %s/%d is owned at epoch %d; commit at epoch %d refused",
					op.Group, op.Topic, op.Partition, cur.Epoch, op.Epoch)
				e.Reason = "stale_owner"
				return e
			}
			if op.Low < cur.Low {
				e := api.Errorf(api.CodeInvalidRequest, "checkpoint would move back from %d to %d", cur.Low, op.Low)
				e.Reason = "checkpoint_regression"
				return e
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		b.stats.cpCommits.Add(1)
		return &rec.Checkpoint, nil
	}
	return nil, api.Errorf(api.CodeInvalidRequest, "unknown checkpoint op %q", op.Op)
}

func (c *checkpoints) handleInternal(ctx context.Context, op *cpOp) (*Checkpoint, error) {
	return c.do(ctx, *op)
}

// register adds the public consumer-group API.
func (c *checkpoints) register(mux *http.ServeMux) {
	rpc.Handle(mux, pathCP, c.handleInternal)
	handle := func(op string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var req cpOp
			if r.Method == http.MethodGet {
				q := r.URL.Query()
				req.Topic = q.Get("topic")
				req.Partition, _ = strconv.Atoi(q.Get("partition"))
			} else if !httpapi.Decode(w, r, &req) {
				return
			}
			req.Op, req.Group, req.Hops = op, r.PathValue("group"), 0
			if req.Group == "" || req.Topic == "" {
				httpapi.WriteError(w, api.Errorf(api.CodeInvalidRequest, "group and topic are required"))
				return
			}
			// Entry hop: checkpoint data has the data topic's residency.
			if t, err := c.b.topicMeta(req.Topic); err == nil {
				if err := c.b.guard.Do(residency.Check{Topic: req.Topic, Partition: req.Partition, Residency: t.Config.Residency,
					Hop: "entry-checkpoint", Need: residency.Transit, Quiet: true}); err != nil {
					httpapi.WriteError(w, err)
					return
				}
			}
			cp, err := c.do(r.Context(), req)
			if err != nil {
				httpapi.WriteError(w, err)
				return
			}
			httpapi.WriteJSON(w, http.StatusOK, cp)
		}
	}
	mux.HandleFunc("POST /v1/groups/{group}/claim", handle("claim"))
	mux.HandleFunc("POST /v1/groups/{group}/commit", handle("commit"))
	mux.HandleFunc("GET /v1/groups/{group}/checkpoint", handle("get"))
}

// Groups lists checkpoints this broker leads (for the dashboard and forecasts).
func (c *checkpoints) Groups() []Checkpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Checkpoint
	for k, s := range c.states {
		if p := c.b.parts.Get(k); p != nil && p.Status().Role == replica.RoleLeader {
			out = append(out, s.all()...)
		}
	}
	return out
}

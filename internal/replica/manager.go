package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

const (
	PathAppend = "/internal/v1/replica/append"
	PathFence  = "/internal/v1/replica/fence"
)

// Manager owns this broker's partition replicas.
type Manager struct {
	self    string
	dataDir string
	logCfg  seglog.Config
	rpc     *rpc.Client
	timing  Timing
	guard   *residency.Guard

	mu     sync.RWMutex
	parts  map[meta.PartKey]*Partition
	closed bool
}

func NewManager(self, dataDir string, logCfg seglog.Config, client *rpc.Client, timing Timing, guard *residency.Guard) *Manager {
	return &Manager{self: self, dataDir: dataDir, logCfg: logCfg, rpc: client, timing: timing, guard: guard, parts: map[meta.PartKey]*Partition{}}
}

func (m *Manager) Get(key meta.PartKey) *Partition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.parts[key]
}

func (m *Manager) All() []*Partition {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*Partition, 0, len(m.parts))
	for _, p := range m.parts {
		out = append(out, p)
	}
	return out
}

// ensure opens (creating if needed) our replica of a partition. Nothing is written for a topic
// whose residency forbids this broker's region.
func (m *Manager) ensure(cfg api.TopicConfig, part int) (*Partition, error) {
	if err := m.guard.CheckStore(cfg.Name, cfg.Residency); err != nil {
		return nil, err
	}
	key := meta.PartKey{Topic: cfg.Name, Partition: part}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, errors.New("replica manager closed")
	}
	if p, ok := m.parts[key]; ok {
		return p, nil
	}
	topicDir := layout.TopicDir(m.dataDir, cfg.Name)
	metaPath := filepath.Join(topicDir, layout.TopicMetaFile)
	if _, err := os.Stat(metaPath); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(topicDir, 0o755); err != nil {
			return nil, err
		}
		if err := fsutil.WriteJSONAtomic(metaPath, layout.TopicMeta{TopicConfig: cfg, CreatedAt: time.Now().UTC()}); err != nil {
			return nil, err
		}
	}
	p, err := openPartition(layout.PartitionDir(m.dataDir, cfg.Name, part), key, cfg.Residency, m.self, m.logCfg, m.rpc, m.timing, m.guard)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", key, err)
	}
	m.parts[key] = p
	return p, nil
}

// Open opens every replica assigned to us in md without taking any role (used at startup,
// before this broker has synced with the controller).
func (m *Manager) Open(md *meta.Metadata) error {
	for _, t := range md.Topics {
		for i, pm := range t.Partitions {
			if slices.Contains(pm.Replicas, m.self) {
				if _, err := m.ensure(t.Config, i); err != nil && api.CodeOf(err) != api.CodeResidencyViolation {
					return err
				}
			}
		}
	}
	return nil
}

// Apply moves every replica we own to the role committed metadata assigns it.
func (m *Manager) Apply(md *meta.Metadata) {
	for _, t := range md.Topics {
		for i, pm := range t.Partitions {
			if !slices.Contains(pm.Replicas, m.self) {
				continue
			}
			p, err := m.ensure(t.Config, i)
			if err != nil {
				slog.Error("open replica", "topic", t.Config.Name, "partition", i, "err", err)
				continue
			}
			switch pm.Leader {
			case "":
				p.becomeNone()
			case m.self:
				p.becomeLeader(pm.LeaderEpoch, pm.Replicas)
			default:
				p.becomeFollower(pm.Leader, pm.LeaderEpoch, pm.Replicas)
			}
		}
	}
}

// Stuck lists partitions md says we lead, but that were fenced to a newer epoch and never
// re-assigned (e.g. the controller died mid-election). The controller re-elects them.
func (m *Manager) Stuck(md *meta.Metadata) []meta.PartKey {
	var out []meta.PartKey
	for name, t := range md.Topics {
		for i := range t.Partitions {
			key := meta.PartKey{Topic: name, Partition: i}
			if p := m.Get(key); p != nil && p.stuck(&t.Partitions[i], time.Second) {
				out = append(out, key)
			}
		}
	}
	return out
}

func (m *Manager) Register(mux *http.ServeMux) {
	rpc.Handle(mux, PathAppend, func(ctx context.Context, req *AppendRequest) (*AppendResponse, error) {
		p := m.Get(meta.PartKey{Topic: req.Topic, Partition: req.Partition})
		res := req.Residency
		if p != nil {
			res = p.residency // our own tag is authoritative
		}
		// Refuse resident data that would land here, or that arrives from outside its region.
		if err := m.guard.CheckStore(req.Topic, res); err != nil {
			return nil, err
		}
		for _, from := range []string{rpc.From(ctx), req.Leader} {
			if err := m.guard.CheckPeer(req.Topic, res, from, "accept replication from"); err != nil {
				return nil, err
			}
		}
		if p == nil {
			return nil, api.Errorf(api.CodeUnknownPartition, "%s has no replica of %s/%d", m.self, req.Topic, req.Partition)
		}
		return p.handleAppend(req)
	})
	rpc.Handle(mux, PathFence, func(_ context.Context, req *FenceRequest) (*FenceResponse, error) {
		p := m.Get(meta.PartKey{Topic: req.Topic, Partition: req.Partition})
		if p == nil {
			return nil, api.Errorf(api.CodeUnknownPartition, "%s has no replica of %s/%d", m.self, req.Topic, req.Partition)
		}
		return p.fence(req.Epoch), nil
	})
}

type FenceRequest struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Epoch     uint64 `json:"epoch"`
}

// Fence fences our own replica (the controller calls this for itself instead of an RPC).
func (m *Manager) Fence(req *FenceRequest) (*FenceResponse, error) {
	p := m.Get(meta.PartKey{Topic: req.Topic, Partition: req.Partition})
	if p == nil {
		return nil, api.Errorf(api.CodeUnknownPartition, "%s has no replica of %s/%d", m.self, req.Topic, req.Partition)
	}
	return p.fence(req.Epoch), nil
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for _, p := range m.parts {
		errs = append(errs, p.close())
	}
	m.parts = map[meta.PartKey]*Partition{}
	m.closed = true
	return errors.Join(errs...)
}

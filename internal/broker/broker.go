// Package broker implements api.Service on top of per-partition segment logs.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

const (
	identityFile  = "broker.json"
	topicsDir     = "topics"
	topicMetaFile = "topic.json"
	maxPartitions = 256
)

// Lower-case only: topic names are directory names and Windows/macOS file systems are case-insensitive.
var topicName = regexp.MustCompile(`^[a-z0-9._-]{1,128}$`)

type Config struct {
	Info    api.Info
	DataDir string
	Regions []string // regions in the cluster; a resident topic must name one of these
	Log     seglog.Config
}

type Broker struct {
	cfg     Config
	regions map[string]bool

	mu     sync.RWMutex
	topics map[string]*topic
}

type topic struct {
	cfg   api.TopicConfig
	parts []*seglog.Log
	rr    atomic.Uint64 // round-robin cursor for keyless records
}

// identity is persisted in the data dir so a dir can't be reused by another broker or region.
type identity struct {
	ID     string `json:"id"`
	Region string `json:"region"`
	Zone   string `json:"zone"`
}

type topicMeta struct {
	api.TopicConfig
	CreatedAt time.Time `json:"created_at"`
}

// Open opens (or initialises) the broker's data directory and loads existing topics.
func Open(cfg Config) (*Broker, error) {
	if cfg.Info.ID == "" || cfg.Info.Region == "" {
		return nil, errors.New("broker: id and region are required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, topicsDir), 0o755); err != nil {
		return nil, err
	}
	if err := checkIdentity(cfg.DataDir, cfg.Info); err != nil {
		return nil, err
	}
	b := &Broker{cfg: cfg, regions: map[string]bool{}, topics: map[string]*topic{}}
	for _, r := range cfg.Regions {
		b.regions[r] = true
	}
	if err := b.loadTopics(); err != nil {
		b.Close()
		return nil, err
	}
	return b, nil
}

func checkIdentity(dir string, info api.Info) error {
	path := filepath.Join(dir, identityFile)
	want := identity{ID: info.ID, Region: info.Region, Zone: info.Zone}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return writeJSONAtomic(path, want)
	}
	if err != nil {
		return err
	}
	var got identity
	if err := json.Unmarshal(data, &got); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}
	if got.ID != want.ID || got.Region != want.Region {
		return fmt.Errorf("data dir %s belongs to broker %s in region %s, refusing to start as %s in %s",
			dir, got.ID, got.Region, want.ID, want.Region)
	}
	if got.Zone != want.Zone {
		return writeJSONAtomic(path, want)
	}
	return nil
}

func (b *Broker) loadTopics() error {
	root := filepath.Join(b.cfg.DataDir, topicsDir)
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(root, e.Name(), topicMetaFile))
		if errors.Is(err, os.ErrNotExist) {
			slog.Warn("topic dir without metadata, skipping", "dir", e.Name())
			continue
		}
		if err != nil {
			return err
		}
		var m topicMeta
		if err := json.Unmarshal(data, &m); err != nil {
			return fmt.Errorf("parse topic %s: %w", e.Name(), err)
		}
		t, err := b.openTopic(m.TopicConfig)
		if err != nil {
			return err
		}
		b.topics[m.Name] = t
	}
	return nil
}

func (b *Broker) openTopic(cfg api.TopicConfig) (*topic, error) {
	t := &topic{cfg: cfg}
	for p := 0; p < cfg.Partitions; p++ {
		l, err := seglog.Open(b.partitionDir(cfg.Name, p), b.cfg.Log)
		if err != nil {
			for _, o := range t.parts {
				o.Close()
			}
			return nil, fmt.Errorf("open %s/%d: %w", cfg.Name, p, err)
		}
		t.parts = append(t.parts, l)
	}
	return t, nil
}

func (b *Broker) partitionDir(topic string, p int) string {
	return filepath.Join(b.cfg.DataDir, topicsDir, topic, fmt.Sprint(p))
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var errs []error
	for _, t := range b.topics {
		for _, l := range t.parts {
			errs = append(errs, l.Close())
		}
	}
	return errors.Join(errs...)
}

func (b *Broker) Info() api.Info { return b.cfg.Info }

func (b *Broker) normalise(cfg api.TopicConfig) (api.TopicConfig, error) {
	if !topicName.MatchString(cfg.Name) || cfg.Name == "." || cfg.Name == ".." {
		return cfg, api.Errorf(api.CodeInvalidRequest, "invalid topic name %q", cfg.Name)
	}
	if cfg.Partitions < 1 || cfg.Partitions > maxPartitions {
		return cfg, api.Errorf(api.CodeInvalidRequest, "partitions must be 1..%d", maxPartitions)
	}
	if strings.EqualFold(cfg.Residency, api.ResidencyGlobal) {
		cfg.Residency = api.ResidencyGlobal
	} else {
		cfg.Residency = strings.ToUpper(cfg.Residency)
		if !b.regions[cfg.Residency] {
			return cfg, api.Errorf(api.CodeInvalidRequest,
				"residency must be %q or one of the cluster regions, got %q", api.ResidencyGlobal, cfg.Residency)
		}
	}
	return cfg, nil
}

func (b *Broker) CreateTopic(_ context.Context, cfg api.TopicConfig) (api.TopicInfo, error) {
	cfg, err := b.normalise(cfg)
	if err != nil {
		return api.TopicInfo{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if t, ok := b.topics[cfg.Name]; ok {
		if t.cfg != cfg {
			return api.TopicInfo{}, api.Errorf(api.CodeTopicExists,
				"topic %q exists with partitions=%d residency=%s", cfg.Name, t.cfg.Partitions, t.cfg.Residency)
		}
		return t.info(), nil
	}
	// Metadata is written first so no partition data ever exists without a residency tag.
	dir := filepath.Join(b.cfg.DataDir, topicsDir, cfg.Name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return api.TopicInfo{}, err
	}
	if err := writeJSONAtomic(filepath.Join(dir, topicMetaFile), topicMeta{cfg, time.Now().UTC()}); err != nil {
		return api.TopicInfo{}, err
	}
	t, err := b.openTopic(cfg)
	if err != nil {
		return api.TopicInfo{}, err
	}
	b.topics[cfg.Name] = t
	slog.Info("topic created", "topic", cfg.Name, "partitions", cfg.Partitions, "residency", cfg.Residency)
	return t.info(), nil
}

func (t *topic) info() api.TopicInfo {
	info := api.TopicInfo{TopicConfig: t.cfg, PartitionStates: make([]api.PartitionInfo, len(t.parts))}
	for p, l := range t.parts {
		info.PartitionStates[p] = api.PartitionInfo{Partition: p, LogStart: l.StartOffset(), HighWatermark: l.EndOffset()}
	}
	return info
}

func (b *Broker) Topics(context.Context) ([]api.TopicInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make([]api.TopicInfo, 0, len(b.topics))
	for _, t := range b.topics {
		out = append(out, t.info())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (b *Broker) Topic(_ context.Context, name string) (api.TopicInfo, error) {
	t, err := b.topic(name)
	if err != nil {
		return api.TopicInfo{}, err
	}
	return t.info(), nil
}

func (b *Broker) topic(name string) (*topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	if !ok {
		return nil, api.Errorf(api.CodeUnknownTopic, "unknown topic %q", name)
	}
	return t, nil
}

func (b *Broker) Produce(_ context.Context, req api.ProduceRequest) (api.ProduceResponse, error) {
	t, err := b.topic(req.Topic)
	if err != nil {
		return api.ProduceResponse{}, err
	}
	if len(req.Records) == 0 || len(req.Records) > api.MaxProduceRecords {
		return api.ProduceResponse{}, api.Errorf(api.CodeInvalidRequest, "a produce request needs 1..%d records", api.MaxProduceRecords)
	}

	n := len(t.parts)
	sticky := int(t.rr.Add(1) % uint64(n))
	groups := make(map[int][]int)
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
		}
		groups[p] = append(groups[p], i)
	}

	resp := api.ProduceResponse{Offsets: make([]api.RecordOffset, len(req.Records))}
	for _, p := range order {
		idx := groups[p]
		recs := make([]seglog.Record, len(idx))
		for j, i := range idx {
			r := req.Records[i]
			if r.Key != "" {
				recs[j].Key = []byte(r.Key)
			}
			recs[j].Value = []byte(r.Value)
		}
		base, err := t.parts[p].Append(recs)
		if errors.Is(err, seglog.ErrRecordTooLarge) {
			return api.ProduceResponse{}, api.Errorf(api.CodeInvalidRequest, "record larger than %d bytes", seglog.MaxRecordSize)
		}
		if err != nil {
			return api.ProduceResponse{}, api.Errorf(api.CodeInternal, "append %s/%d: %v", req.Topic, p, err)
		}
		for j, i := range idx {
			resp.Offsets[i] = api.RecordOffset{Partition: p, Offset: base + uint64(j)}
		}
	}
	return resp, nil
}

func (b *Broker) Fetch(ctx context.Context, req api.FetchRequest) (api.FetchResponse, error) {
	t, err := b.topic(req.Topic)
	if err != nil {
		return api.FetchResponse{}, err
	}
	if req.Partition < 0 || req.Partition >= len(t.parts) {
		return api.FetchResponse{}, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	l := t.parts[req.Partition]
	// Milestone 2: the high watermark becomes the quorum-committed offset, not the log end.
	hw, start := l.EndOffset(), l.StartOffset()
	if req.Offset < start || req.Offset > hw {
		return api.FetchResponse{}, api.Errorf(api.CodeOffsetOutOfRange,
			"offset %d outside [%d, %d] for %s/%d", req.Offset, start, hw, req.Topic, req.Partition)
	}
	if req.Offset == hw && req.Wait > 0 {
		wctx, cancel := context.WithTimeout(ctx, min(req.Wait, api.MaxFetchWait))
		_ = l.Wait(wctx, req.Offset) // timing out just means an empty response
		cancel()
		hw = l.EndOffset()
	}
	maxRecs := min(req.MaxRecords, api.MaxFetchRecords)
	recs, err := l.Read(req.Offset, hw, maxRecs, req.MaxBytes)
	if err != nil {
		return api.FetchResponse{}, api.Errorf(api.CodeInternal, "read %s/%d: %v", req.Topic, req.Partition, err)
	}
	resp := api.FetchResponse{
		Topic: req.Topic, Partition: req.Partition, Records: make([]api.FetchedRecord, len(recs)),
		NextOffset: req.Offset, HighWatermark: hw, LogStart: start,
	}
	for i, r := range recs {
		resp.Records[i] = api.FetchedRecord{Offset: r.Offset, Timestamp: r.Timestamp, Key: string(r.Key), Value: string(r.Value)}
		resp.NextOffset = r.Offset + 1
	}
	return resp, nil
}

func writeJSONAtomic(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

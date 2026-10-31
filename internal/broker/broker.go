// Package broker implements api.Service for one broker in a region: it routes produce and
// fetch to partition leaders, and (when it holds the controller lease) places topics and
// elects partition leaders.
package broker

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/coord"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/manifest"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

const (
	coordFile     = "coord.json"
	keyFile       = "manifest.key"
	maxPartitions = 256

	pathProduce = "/internal/v1/broker/produce"
	pathFetch   = "/internal/v1/broker/fetch"
	pathCreate  = "/internal/v1/broker/create-topic"
)

// Lower-case only: topic names are directory names and Windows/macOS file systems are case-insensitive.
var topicName = regexp.MustCompile(`^[a-z0-9._-]{1,128}$`)

type Config struct {
	Info    api.Info
	DataDir string
	Regions []string          // regions in the cluster; a resident topic must name one of these
	Peers   map[string]string // id -> addr of every broker in our region, including us
	// Directory maps every broker id in the cluster to its configured identity. Residency checks
	// use it to learn which region a broker is in. Defaults to "all peers are in our region".
	Directory map[string]api.Info
	Log       seglog.Config
	Coord     coord.Timing
	Replica   replica.Timing
	Net       *rpc.Net // optional; shared link blocker for partition simulation
}

type Broker struct {
	cfg     Config
	regions map[string]bool
	peers   []string // sorted ids in our region
	net     *rpc.Net
	rpc     *rpc.Client
	coord   coord.Coordinator
	parts   *replica.Manager
	guard   *residency.Guard
	key     ed25519.PrivateKey

	rrMu sync.Mutex
	rr   map[string]*atomic.Uint64

	ctrlMu   sync.Mutex
	maxEpoch map[meta.PartKey]uint64 // controller: highest epoch seen while fencing

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open initialises the data directory and the broker's components. Call Start to run it.
func Open(cfg Config) (*Broker, error) {
	if cfg.Info.ID == "" || cfg.Info.Region == "" {
		return nil, errors.New("broker: id and region are required")
	}
	if len(cfg.Peers) == 0 {
		cfg.Peers = map[string]string{cfg.Info.ID: ""}
	}
	if _, ok := cfg.Peers[cfg.Info.ID]; !ok {
		return nil, fmt.Errorf("broker: %s missing from its own peer list", cfg.Info.ID)
	}
	if cfg.Coord == (coord.Timing{}) {
		cfg.Coord = coord.DefaultTiming()
	}
	if cfg.Replica == (replica.Timing{}) {
		cfg.Replica = replica.DefaultTiming()
	}
	if cfg.Net == nil {
		cfg.Net = rpc.NewNet()
	}
	if cfg.Directory == nil {
		cfg.Directory = map[string]api.Info{}
		for id := range cfg.Peers {
			cfg.Directory[id] = api.Info{ID: id, Region: cfg.Info.Region}
		}
	}
	cfg.Directory[cfg.Info.ID] = cfg.Info
	if err := os.MkdirAll(filepath.Join(cfg.DataDir, layout.TopicsDir), 0o755); err != nil {
		return nil, err
	}
	if err := checkIdentity(cfg.DataDir, cfg.Info); err != nil {
		return nil, err
	}
	key, err := manifest.LoadOrCreateKey(filepath.Join(cfg.DataDir, keyFile))
	if err != nil {
		return nil, err
	}

	b := &Broker{
		cfg: cfg, regions: map[string]bool{}, net: cfg.Net,
		rr: map[string]*atomic.Uint64{}, maxEpoch: map[meta.PartKey]uint64{}, key: key,
	}
	b.guard = residency.NewGuard(cfg.Info.ID, cfg.Info.Region, func(id string) string { return cfg.Directory[id].Region })
	for _, r := range cfg.Regions {
		b.regions[r] = true
	}
	for id := range cfg.Peers {
		b.peers = append(b.peers, id)
	}
	sort.Strings(b.peers)
	b.rpc = rpc.NewClient(cfg.Info.ID, cfg.Peers, cfg.Net)
	b.parts = replica.NewManager(cfg.Info.ID, cfg.DataDir, cfg.Log, b.rpc, cfg.Replica, b.guard)
	lease, err := coord.NewLease(coord.LeaseConfig{
		Self: cfg.Info.ID, Peers: b.peers, RPC: b.rpc,
		StateFile: filepath.Join(cfg.DataDir, coordFile),
		Timing:    cfg.Coord,
		OnCommit:  b.parts.Apply,
		Stuck:     b.parts.Stuck,
	})
	if err != nil {
		return nil, err
	}
	b.coord = lease
	// Open replicas we already have so their data is present, but take no role until synced.
	if err := b.parts.Open(lease.Committed()); err != nil {
		b.parts.Close()
		return nil, err
	}
	return b, nil
}

func checkIdentity(dir string, info api.Info) error {
	path := filepath.Join(dir, layout.IdentityFile)
	want := layout.Identity{ID: info.ID, Region: info.Region, Zone: info.Zone}
	var got layout.Identity
	err := fsutil.ReadJSON(path, &got)
	if errors.Is(err, os.ErrNotExist) {
		return fsutil.WriteJSONAtomic(path, want)
	}
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if got.ID != want.ID || got.Region != want.Region {
		return fmt.Errorf("data dir %s belongs to broker %s in region %s, refusing to start as %s in %s",
			dir, got.ID, got.Region, want.ID, want.Region)
	}
	if got.Zone != want.Zone {
		return fsutil.WriteJSONAtomic(path, want)
	}
	return nil
}

// Handler serves the public API, internal broker-to-broker RPCs and admin endpoints.
func (b *Broker) Handler() http.Handler {
	mux := http.NewServeMux()
	httpapi.Register(mux, b)
	b.coord.Register(mux)
	b.parts.Register(mux)
	rpc.Handle(mux, pathProduce, b.handleProduce)
	rpc.Handle(mux, pathFetch, b.handleFetch)
	rpc.Handle(mux, pathCreate, b.handleCreate)
	b.registerAdmin(mux)
	return b.net.Middleware(mux)
}

// Start runs the coordinator and controller loops.
func (b *Broker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(2)
	go func() { defer b.wg.Done(); b.coord.Run(ctx) }()
	go func() { defer b.wg.Done(); b.runController(ctx) }()
}

func (b *Broker) Close() error {
	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
	}
	return b.parts.Close()
}

func (b *Broker) Info() api.Info                 { return b.cfg.Info }
func (b *Broker) Net() *rpc.Net                  { return b.net }
func (b *Broker) Coordinator() coord.Coordinator { return b.coord }
func (b *Broker) Replicas() *replica.Manager     { return b.parts }

func (b *Broker) normalise(cfg api.TopicConfig) (api.TopicConfig, error) {
	if !topicName.MatchString(cfg.Name) || cfg.Name == "." || cfg.Name == ".." {
		return cfg, api.Errorf(api.CodeInvalidRequest, "invalid topic name %q (lower-case letters, digits, . _ -)", cfg.Name)
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

func (b *Broker) controllerOrUnavailable() (string, error) {
	id, _ := b.coord.Controller()
	if id == "" || (id == b.cfg.Info.ID && !b.coord.IsController()) {
		e := api.Errorf(api.CodeRegionUnavailable, "region %s has no controller (needs a majority of its brokers)", b.cfg.Info.Region)
		e.Reason = "no_controller"
		return "", e
	}
	return id, nil
}

// checkRegion rejects a topic that is resident in another region: this region may not place it.
func (b *Broker) checkRegion(cfg api.TopicConfig) error {
	if residency.Allowed(cfg.Residency, b.cfg.Info.Region) {
		return nil
	}
	e := api.Errorf(api.CodeResidencyViolation, "topic %q is resident in %s and cannot be placed in region %s",
		cfg.Name, cfg.Residency, b.cfg.Info.Region)
	e.Reason = "wrong_region"
	return e
}

func (b *Broker) CreateTopic(ctx context.Context, cfg api.TopicConfig) (api.TopicInfo, error) {
	cfg, err := b.normalise(cfg)
	if err != nil {
		return api.TopicInfo{}, err
	}
	if err := b.checkRegion(cfg); err != nil {
		return api.TopicInfo{}, err
	}
	if b.coord.IsController() {
		if _, err := b.createAsController(ctx, cfg); err != nil {
			return api.TopicInfo{}, err
		}
	} else {
		ctrl, err := b.controllerOrUnavailable()
		if err != nil {
			return api.TopicInfo{}, err
		}
		if err := b.rpc.Call(ctx, ctrl, pathCreate, cfg, &struct{}{}); err != nil {
			return api.TopicInfo{}, asUnavailable(err, ctrl)
		}
	}
	// Wait until our own committed metadata includes the topic.
	deadline := time.Now().Add(2 * time.Second)
	for {
		info, err := b.Topic(ctx, cfg.Name)
		if err == nil || time.Now().After(deadline) {
			return info, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (b *Broker) handleCreate(ctx context.Context, cfg *api.TopicConfig) (*struct{}, error) {
	if !b.coord.IsController() {
		return nil, api.Errorf(api.CodeNotLeader, "%s is not the controller", b.cfg.Info.ID)
	}
	n, err := b.normalise(*cfg)
	if err != nil {
		return nil, err
	}
	if err := b.checkRegion(n); err != nil {
		return nil, err
	}
	_, err = b.createAsController(ctx, n)
	return &struct{}{}, err
}

var errNoChange = errors.New("no change")

func (b *Broker) createAsController(ctx context.Context, cfg api.TopicConfig) (*meta.Metadata, error) {
	live := b.coord.Live()
	quorum := len(b.peers)/2 + 1
	if len(live) < quorum {
		return nil, api.Errorf(api.CodeNotEnoughReplicas, "only %d of %d brokers live", len(live), len(b.peers))
	}
	// Placement: only brokers whose configured region satisfies the topic's residency are
	// eligible, even if the region's peer list were misconfigured to include outsiders.
	var eligible []string
	for _, id := range b.peers {
		if residency.Allowed(cfg.Residency, b.guard.RegionOf(id)) {
			eligible = append(eligible, id)
		} else {
			slog.Warn("placement excluded broker outside residency region", "topic", cfg.Name,
				"residency", cfg.Residency, "broker", id, "broker_region", b.guard.RegionOf(id))
		}
	}
	if len(eligible) == 0 {
		e := api.Errorf(api.CodeResidencyViolation, "no broker in region %s may hold topic %q (residency %s)",
			b.cfg.Info.Region, cfg.Name, cfg.Residency)
		e.Reason = "no_eligible_brokers"
		return nil, e
	}
	md, err := b.coord.Propose(ctx, func(m *meta.Metadata) error {
		if t, ok := m.Topics[cfg.Name]; ok {
			if t.Config != cfg {
				return api.Errorf(api.CodeTopicExists, "topic %q exists with partitions=%d residency=%s",
					cfg.Name, t.Config.Partitions, t.Config.Residency)
			}
			return errNoChange
		}
		t := &meta.TopicMeta{Config: cfg}
		for p := 0; p < cfg.Partitions; p++ {
			// Rotate replica order so preferred leaders spread across zones.
			replicas := make([]string, len(eligible))
			for i := range eligible {
				replicas[i] = eligible[(p+i)%len(eligible)]
			}
			leader := ""
			for _, r := range replicas {
				if live[r] {
					leader = r
					break
				}
			}
			t.Partitions = append(t.Partitions, meta.PartitionMeta{Replicas: replicas, Leader: leader, LeaderEpoch: 1})
		}
		m.Topics[cfg.Name] = t
		return nil
	})
	if errors.Is(err, errNoChange) {
		return b.coord.Committed(), nil
	}
	if errors.Is(err, coord.ErrNotController) {
		return nil, api.Errorf(api.CodeRegionUnavailable, "lost the controller lease")
	}
	if err == nil {
		slog.Info("topic created", "topic", cfg.Name, "partitions", cfg.Partitions, "residency", cfg.Residency)
	}
	return md, err
}

func (b *Broker) topicInfo(t *meta.TopicMeta) api.TopicInfo {
	info := api.TopicInfo{TopicConfig: t.Config, PartitionStates: make([]api.PartitionInfo, len(t.Partitions))}
	for i, pm := range t.Partitions {
		pi := api.PartitionInfo{Partition: i, Leader: pm.Leader, LeaderEpoch: pm.LeaderEpoch, Replicas: pm.Replicas}
		if p := b.parts.Get(meta.PartKey{Topic: t.Config.Name, Partition: i}); p != nil {
			st := p.Status()
			pi.LogStart, pi.HighWatermark = p.Log().StartOffset(), st.HW
		}
		info.PartitionStates[i] = pi
	}
	return info
}

func (b *Broker) Topics(context.Context) ([]api.TopicInfo, error) {
	md := b.coord.Committed()
	out := make([]api.TopicInfo, 0, len(md.Topics))
	for _, t := range md.Topics {
		out = append(out, b.topicInfo(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (b *Broker) Topic(_ context.Context, name string) (api.TopicInfo, error) {
	t, ok := b.coord.Committed().Topics[name]
	if !ok {
		return api.TopicInfo{}, api.Errorf(api.CodeUnknownTopic, "unknown topic %q", name)
	}
	return b.topicInfo(t), nil
}

func (b *Broker) topicMeta(name string) (*meta.TopicMeta, error) {
	t, ok := b.coord.Committed().Topics[name]
	if !ok {
		return nil, api.Errorf(api.CodeUnknownTopic, "unknown topic %q", name)
	}
	return t, nil
}

func (b *Broker) sticky(topic string, n int) int {
	b.rrMu.Lock()
	c, ok := b.rr[topic]
	if !ok {
		c = &atomic.Uint64{}
		b.rr[topic] = c
	}
	b.rrMu.Unlock()
	return int(c.Add(1) % uint64(n))
}

func (b *Broker) Produce(ctx context.Context, req api.ProduceRequest) (api.ProduceResponse, error) {
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		return api.ProduceResponse{}, err
	}
	if err := b.guard.CheckPeer(req.Topic, t.Config.Residency, b.cfg.Info.ID, "relay data through"); err != nil {
		return api.ProduceResponse{}, err
	}
	if len(req.Records) == 0 || len(req.Records) > api.MaxProduceRecords {
		return api.ProduceResponse{}, api.Errorf(api.CodeInvalidRequest, "a produce request needs 1..%d records", api.MaxProduceRecords)
	}
	n := len(t.Partitions)
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
		}
		groups[p] = append(groups[p], i)
	}

	resp := api.ProduceResponse{Offsets: make([]api.RecordOffset, len(req.Records))}
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
			base, err := b.producePartition(ctx, req.Topic, p, t.Partitions[p].Leader, recs)
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
	return resp, nil
}

type partitionProduce struct {
	Topic     string              `json:"topic"`
	Partition int                 `json:"partition"`
	Records   []api.ProduceRecord `json:"records"`
}

type partitionProduceResp struct {
	Base uint64 `json:"base"`
}

// checkForward refuses to relay a resident topic's data through a broker outside its region.
func (b *Broker) checkForward(topic, to string) error {
	t, ok := b.coord.Committed().Topics[topic]
	if !ok {
		return api.Errorf(api.CodeUnknownTopic, "unknown topic %q", topic)
	}
	return b.guard.CheckPeer(topic, t.Config.Residency, to, "forward to")
}

func (b *Broker) producePartition(ctx context.Context, topic string, part int, leader string, recs []api.ProduceRecord) (uint64, error) {
	switch leader {
	case b.cfg.Info.ID:
		return b.produceLocal(ctx, topic, part, recs)
	case "":
		e := api.Errorf(api.CodeNotEnoughReplicas, "%s/%d has no leader", topic, part)
		e.Reason = "partition_offline"
		return 0, e
	}
	if err := b.checkForward(topic, leader); err != nil {
		return 0, err
	}
	cctx, cancel := context.WithTimeout(ctx, b.cfg.Replica.ProduceTimeout+b.cfg.Replica.RPCTimeout)
	defer cancel()
	var out partitionProduceResp
	if err := b.rpc.Call(cctx, leader, pathProduce, partitionProduce{topic, part, recs}, &out); err != nil {
		return 0, asUnavailable(err, leader)
	}
	return out.Base, nil
}

func (b *Broker) produceLocal(ctx context.Context, topic string, part int, recs []api.ProduceRecord) (uint64, error) {
	p := b.parts.Get(meta.PartKey{Topic: topic, Partition: part})
	if p == nil {
		return 0, api.Errorf(api.CodeNotLeader, "%s has no replica of %s/%d", b.cfg.Info.ID, topic, part)
	}
	lrecs := make([]seglog.Record, len(recs))
	for i, r := range recs {
		if r.Key != "" {
			lrecs[i].Key = []byte(r.Key)
		}
		lrecs[i].Value = []byte(r.Value)
	}
	return p.Produce(ctx, lrecs)
}

func (b *Broker) handleProduce(ctx context.Context, req *partitionProduce) (*partitionProduceResp, error) {
	base, err := b.produceLocal(ctx, req.Topic, req.Partition, req.Records)
	if err != nil {
		return nil, err
	}
	return &partitionProduceResp{Base: base}, nil
}

type partitionFetch struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     uint64 `json:"offset"`
	MaxRecords int    `json:"max_records"`
	MaxBytes   int    `json:"max_bytes"`
	WaitMs     int64  `json:"wait_ms"`
}

func (b *Broker) Fetch(ctx context.Context, req api.FetchRequest) (api.FetchResponse, error) {
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		return api.FetchResponse{}, err
	}
	if err := b.guard.CheckPeer(req.Topic, t.Config.Residency, b.cfg.Info.ID, "relay data through"); err != nil {
		return api.FetchResponse{}, err
	}
	if req.Partition < 0 || req.Partition >= len(t.Partitions) {
		return api.FetchResponse{}, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	req.MaxRecords = min(req.MaxRecords, api.MaxFetchRecords)
	req.Wait = min(req.Wait, api.MaxFetchWait)
	leader := t.Partitions[req.Partition].Leader
	switch leader {
	case b.cfg.Info.ID:
		return b.fetchLocal(ctx, req)
	case "":
		e := api.Errorf(api.CodeNotEnoughReplicas, "%s/%d has no leader", req.Topic, req.Partition)
		e.Reason = "partition_offline"
		return api.FetchResponse{}, e
	}
	if err := b.checkForward(req.Topic, leader); err != nil {
		return api.FetchResponse{}, err
	}
	cctx, cancel := context.WithTimeout(ctx, req.Wait+b.cfg.Replica.RPCTimeout)
	defer cancel()
	var out api.FetchResponse
	in := partitionFetch{req.Topic, req.Partition, req.Offset, req.MaxRecords, req.MaxBytes, req.Wait.Milliseconds()}
	if err := b.rpc.Call(cctx, leader, pathFetch, in, &out); err != nil {
		return api.FetchResponse{}, asUnavailable(err, leader)
	}
	return out, nil
}

func (b *Broker) fetchLocal(ctx context.Context, req api.FetchRequest) (api.FetchResponse, error) {
	p := b.parts.Get(meta.PartKey{Topic: req.Topic, Partition: req.Partition})
	if p == nil {
		return api.FetchResponse{}, api.Errorf(api.CodeNotLeader, "%s has no replica of %s/%d", b.cfg.Info.ID, req.Topic, req.Partition)
	}
	return p.Fetch(ctx, req.Offset, req.MaxRecords, req.MaxBytes, req.Wait)
}

func (b *Broker) handleFetch(ctx context.Context, req *partitionFetch) (*api.FetchResponse, error) {
	resp, err := b.fetchLocal(ctx, api.FetchRequest{
		Topic: req.Topic, Partition: req.Partition, Offset: req.Offset,
		MaxRecords: req.MaxRecords, MaxBytes: req.MaxBytes, Wait: time.Duration(req.WaitMs) * time.Millisecond,
	})
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// asUnavailable turns a transport failure reaching peer into a fast, explicit client error.
func asUnavailable(err error, peer string) error {
	var e *api.Error
	if errors.As(err, &e) {
		return e
	}
	out := api.Errorf(api.CodeNotLeader, "cannot reach %s: %v", peer, err)
	out.Reason = "leader_unreachable"
	out.Leader = peer
	return out
}

func (b *Broker) registerAdmin(mux *http.ServeMux) {
	type netState struct {
		Blocked []string `json:"blocked"`
	}
	mux.HandleFunc("GET /admin/v1/net", func(w http.ResponseWriter, r *http.Request) {
		blocked := b.net.List()
		slices.Sort(blocked)
		httpapi.WriteJSON(w, http.StatusOK, netState{Blocked: blocked})
	})
	mux.HandleFunc("POST /admin/v1/net", func(w http.ResponseWriter, r *http.Request) {
		var s netState
		if !httpapi.Decode(w, r, &s) {
			return
		}
		b.net.Set(s.Blocked)
		slog.Warn("network links changed", "blocked", s.Blocked)
		httpapi.WriteJSON(w, http.StatusOK, s)
	})
	mux.HandleFunc("GET /admin/v1/status", func(w http.ResponseWriter, r *http.Request) {
		ctrl, epoch := b.coord.Controller()
		httpapi.WriteJSON(w, http.StatusOK, map[string]any{
			"broker": b.cfg.Info, "controller": ctrl, "controller_epoch": epoch,
			"is_controller": b.coord.IsController(), "synced": b.coord.Synced(),
			"metadata_version":     b.coord.Committed().Version,
			"residency_rejections": b.guard.Rejected(),
		})
	})
	// The signed manifest of every segment this broker stores (see internal/manifest).
	mux.HandleFunc("GET /admin/v1/manifest", func(w http.ResponseWriter, r *http.Request) {
		s, err := manifest.Build(b.cfg.DataDir, b.cfg.Info, b.key)
		if err != nil {
			httpapi.WriteError(w, err)
			return
		}
		httpapi.WriteJSON(w, http.StatusOK, s)
	})
}

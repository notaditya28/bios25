// Package broker implements api.Service for one broker in a region: it routes produce and
// fetch to partition leaders, and (when it holds the controller lease) places topics and
// elects partition leaders.
package broker

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/notaditya28/hydra-stream/internal/flow"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/geo"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/manifest"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/placement"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/route"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
	"github.com/notaditya28/hydra-stream/internal/trail"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

const (
	coordFile     = "coord.json"
	keyFile       = "manifest.key"
	maxPartitions = 256

	pathProduce = "/internal/v1/broker/produce"
	pathFetch   = "/internal/v1/broker/fetch"
	pathCreate  = "/internal/v1/broker/create-topic"
	// pathCreateLocal creates a topic in the receiving broker's region only (cross-region fan-out).
	pathCreateLocal = "/internal/v1/broker/create-topic-local"
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
	// Addrs maps every broker id in the cluster to its address (for cross-region traffic).
	// Defaults to Peers.
	Addrs   map[string]string
	Log     seglog.Config
	Coord   coord.Timing
	Replica replica.Timing
	Net     *rpc.Net      // optional; shared link blocker / delay injector for chaos tests
	Probe   time.Duration // inter-region latency probe interval (default 200ms)
	// PolicyKey verifies signed policy documents. Without it, only the built-in rules
	// (region-resident and global) work; "policy:<class>" topics fail closed.
	PolicyKey ed25519.PublicKey
	// Flow configures inter-region bandwidth scheduling (zero value: adaptive controller,
	// links unlimited until a capacity is set via /admin/v1/net).
	Flow flow.Config
	// DiskBudget is the storage budget for forecasts (time until the data dir is full).
	DiskBudget int64
	// NoDedup turns off wire-level payload dedup for cross-region replication (for benchmarks).
	NoDedup bool
}

const policyFile = "policies.signed.json"

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
	geo     *geo.Router
	policy  *policy.Engine
	trail   *trail.Log
	fc      *forecaster
	cp      *checkpoints
	stats   *metrics

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
	// Own copy: the caller's map may be shared (e.g. by several brokers in one test process).
	dir := map[string]api.Info{}
	if cfg.Directory == nil {
		for id := range cfg.Peers {
			dir[id] = api.Info{ID: id, Region: cfg.Info.Region}
		}
	}
	for id, info := range cfg.Directory {
		dir[id] = info
	}
	dir[cfg.Info.ID] = cfg.Info
	cfg.Directory = dir
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
		rr: map[string]*atomic.Uint64{}, maxEpoch: map[meta.PartKey]uint64{}, key: key, stats: newMetrics(),
	}
	regionList := cfg.Regions
	if len(regionList) == 0 {
		regionList = placement.Regions(cfg.Directory)
	}
	b.policy = policy.NewEngine(cfg.PolicyKey, regionList)
	var signed policy.Signed
	if err := fsutil.ReadJSON(filepath.Join(cfg.DataDir, policyFile), &signed); err == nil {
		if err := b.policy.Install(&signed); err != nil {
			slog.Warn("stored policy document rejected", "err", err)
		}
	}
	if b.trail, err = trail.Open(trail.Path(cfg.DataDir)); err != nil {
		return nil, err
	}
	b.guard = residency.NewGuard(cfg.Info.ID, cfg.Info.Region, func(id string) string { return cfg.Directory[id].Region }, b.policy, b.trail)
	for _, r := range cfg.Regions {
		b.regions[r] = true
	}
	for id := range cfg.Peers {
		b.peers = append(b.peers, id)
	}
	sort.Strings(b.peers)
	addrs := cfg.Addrs
	if addrs == nil {
		addrs = cfg.Peers
	}
	b.rpc = rpc.NewClient(cfg.Info.ID, addrs, cfg.Net)
	b.parts = replica.NewManager(cfg.Info.ID, cfg.DataDir, cfg.Log, b.rpc, cfg.Replica, b.guard)
	b.parts.SetSelfElected(func(t api.TopicConfig) bool {
		if tm, ok := b.coord.Committed().Topics[t.Name]; ok {
			return b.spansRegions(tm)
		}
		return b.crossRegion(t.Residency)
	})
	b.parts.SetDedup(!cfg.NoDedup)
	b.geo = geo.New(geo.Config{
		Self: cfg.Info, Directory: cfg.Directory, RPC: b.rpc, Guard: b.guard, Interval: cfg.Probe,
		Topics: b.topicSummaries, Flow: cfg.Flow,
	})
	b.parts.SetSender(b.geo)
	b.cp = newCheckpoints(b)
	b.parts.SetApplier(b.cp.applierFor)
	b.fc = newForecaster(b)
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
	rpc.Handle(mux, pathCreateLocal, b.handleCreateLocal)
	b.geo.Register(mux)
	b.cp.register(mux)
	b.registerAdmin(mux)
	b.registerAdminExtra(mux)
	return b.net.Middleware(mux)
}

// Start runs the coordinator, controller, latency-probe and forecast loops.
func (b *Broker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(4)
	go func() { defer b.wg.Done(); b.coord.Run(ctx) }()
	go func() { defer b.wg.Done(); b.runController(ctx) }()
	go func() { defer b.wg.Done(); b.geo.Run(ctx) }()
	go func() { defer b.wg.Done(); b.fc.run(ctx) }()
}

func (b *Broker) regionNames() []string { return placement.Regions(b.cfg.Directory) }

func (b *Broker) regionBrokers(region string) []string {
	return placement.RegionBrokers(b.cfg.Directory, region)
}

// storeRegions lists the regions a residency's policy allows storage in (nil if unresolvable).
func (b *Broker) storeRegions(res string) []string {
	eff, err := b.policy.Resolve(res)
	if err != nil {
		return nil
	}
	return placement.StorageRegions(b.cfg.Directory, eff)
}

// crossRegion reports whether a residency's data is replicated across regions (self-elected
// leaders, one replica per allowed region) rather than inside one region.
func (b *Broker) crossRegion(res string) bool { return len(b.storeRegions(res)) >= 2 }

// canStoreHere: may this broker's region store data under res?
func (b *Broker) canStoreHere(res string) bool {
	eff, err := b.policy.Resolve(res)
	return err == nil && eff.CanStore(b.cfg.Info.Region)
}

// topicSummaries is what this broker tells other regions about its region's topics.
func (b *Broker) topicSummaries() []geo.TopicSummary {
	md := b.coord.Committed()
	out := make([]geo.TopicSummary, 0, len(md.Topics))
	for _, t := range md.Topics {
		out = append(out, geo.TopicSummary{Name: t.Config.Name, Residency: t.Config.Residency, Partitions: t.Config.Partitions, Priority: t.Config.Priority})
	}
	return out
}

func (b *Broker) Close() error {
	if b.cancel != nil {
		b.cancel()
		b.wg.Wait()
	}
	b.geo.Close()
	err := b.parts.Close()
	b.trail.Close()
	return err
}

// Policy returns the broker's policy engine.
func (b *Broker) Policy() *policy.Engine { return b.policy }

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
	switch {
	case strings.EqualFold(cfg.Residency, api.ResidencyGlobal):
		cfg.Residency = api.ResidencyGlobal
	case strings.HasPrefix(strings.ToLower(cfg.Residency), policy.Prefix):
		cfg.Residency = policy.Prefix + strings.ToLower(strings.TrimPrefix(strings.ToLower(cfg.Residency), policy.Prefix))
		if _, err := b.policy.Resolve(cfg.Residency); err != nil {
			return cfg, err // fail closed: no valid signed policy for this class
		}
	default:
		cfg.Residency = strings.ToUpper(cfg.Residency)
		if !b.regions[cfg.Residency] {
			return cfg, api.Errorf(api.CodeInvalidRequest,
				"residency must be %q, a cluster region or policy:<class>, got %q", api.ResidencyGlobal, cfg.Residency)
		}
	}
	if cfg.Priority != "" && cfg.Priority != replica.PriorityUrgent {
		return cfg, api.Errorf(api.CodeInvalidRequest, "priority must be empty or %q", replica.PriorityUrgent)
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

// checkRegion rejects a topic this region may not hold.
func (b *Broker) checkRegion(cfg api.TopicConfig) error {
	if b.canStoreHere(cfg.Residency) {
		return nil
	}
	if _, err := b.policy.Resolve(cfg.Residency); err != nil {
		return err
	}
	e := api.Errorf(api.CodeResidencyViolation, "topic %q (%s) may not be stored in region %s (allowed: %v)",
		cfg.Name, cfg.Residency, b.cfg.Info.Region, b.storeRegions(cfg.Residency))
	e.Reason = "wrong_region"
	return e
}

func (b *Broker) CreateTopic(ctx context.Context, cfg api.TopicConfig) (api.TopicInfo, error) {
	cfg, err := b.normalise(cfg)
	if err != nil {
		return api.TopicInfo{}, err
	}
	if !b.crossRegion(cfg.Residency) {
		return b.createInRegion(ctx, cfg)
	}
	// Cross-region data: the topic must exist in a majority of the regions allowed to store it.
	// Those that miss it catch up through topic gossip (see syncGlobalTopics) when reachable.
	targets := b.storeRegions(cfg.Residency)
	accepted := 0
	var failed []string
	for _, region := range targets {
		var err error
		if region == b.cfg.Info.Region {
			_, err = b.createInRegion(ctx, cfg)
		} else {
			err = b.createRemote(ctx, region, cfg)
		}
		if err != nil {
			if api.CodeOf(err) == api.CodeTopicExists {
				return api.TopicInfo{}, err
			}
			failed = append(failed, region)
			continue
		}
		accepted++
	}
	if quorum := len(targets)/2 + 1; accepted < quorum {
		e := api.Errorf(api.CodeNotEnoughReplicas, "topic %q accepted by %d of %d storage regions (failed: %v); it will spread when they return",
			cfg.Name, accepted, len(targets), failed)
		e.Reason = "regions_unreachable"
		return api.TopicInfo{}, e
	}
	if b.canStoreHere(cfg.Residency) {
		b.waitGlobalLeaders(ctx, cfg.Name, 5*time.Second)
		return b.Topic(ctx, cfg.Name)
	}
	// This region may not hold the topic (it only forwarded the request); describe it anyway.
	return api.TopicInfo{TopicConfig: cfg}, nil
}

// waitGlobalLeaders waits (bounded) until every partition of a new global topic has elected a
// leader, so clients can use the topic as soon as creation returns.
func (b *Broker) waitGlobalLeaders(ctx context.Context, topic string, limit time.Duration) {
	deadline := time.Now().Add(limit)
	t, err := b.topicMeta(topic)
	if err != nil {
		return
	}
	for p := range t.Partitions {
		for time.Now().Before(deadline) {
			_, err := b.fetchPartition(ctx, t, api.FetchRequest{Topic: topic, Partition: p, MaxRecords: 1}, 0)
			if api.CodeOf(err) != api.CodeNotEnoughReplicas && api.CodeOf(err) != api.CodeNotLeader {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// createRemote asks any reachable broker in region to create the topic in its region.
func (b *Broker) createRemote(ctx context.Context, region string, cfg api.TopicConfig) error {
	var lastErr error = fmt.Errorf("region %s unreachable", region)
	for _, id := range b.regionBrokers(region) {
		if !b.geo.Reachable(id) {
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		// Metadata only (no message data): plain control traffic.
		err := b.geo.Send(cctx, id, pathCreateLocal, route.Opts{Topic: cfg.Name, Residency: api.ResidencyGlobal, Class: route.Control}, cfg, &struct{}{})
		cancel()
		if err == nil || api.CodeOf(err) == api.CodeTopicExists {
			return err
		}
		lastErr = err
	}
	return lastErr
}

func (b *Broker) handleCreateLocal(ctx context.Context, cfg *api.TopicConfig) (*struct{}, error) {
	n, err := b.normalise(*cfg)
	if err != nil {
		return nil, err
	}
	_, err = b.createInRegion(ctx, n)
	return &struct{}{}, err
}

// createInRegion creates the topic in this region's metadata (via the region controller).
func (b *Broker) createInRegion(ctx context.Context, cfg api.TopicConfig) (api.TopicInfo, error) {
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
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = b.rpc.Call(cctx, ctrl, pathCreate, cfg, &struct{}{})
		cancel()
		if err != nil {
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
	// The forwarding broker already checked the topic belongs in this region. The controller's
	// own region doesn't matter (it only writes metadata); placement below only picks brokers
	// whose configured region satisfies the residency, and refuses if there are none.
	_, err = b.createAsController(ctx, n)
	return &struct{}{}, err
}

func (b *Broker) createAsController(ctx context.Context, cfg api.TopicConfig) (*meta.Metadata, error) {
	live := b.coord.Live()
	quorum := len(b.peers)/2 + 1
	if len(live) < quorum {
		return nil, api.Errorf(api.CodeNotEnoughReplicas, "only %d of %d brokers live", len(live), len(b.peers))
	}
	eff, err := b.policy.Resolve(cfg.Residency)
	if err != nil {
		return nil, err
	}
	if b.crossRegion(cfg.Residency) {
		return b.proposeTopic(ctx, cfg, func(p int) meta.PartitionMeta {
			// One replica per allowed region; the replicas elect their own leader.
			return meta.PartitionMeta{Replicas: placement.CrossRegionReplicas(b.cfg.Directory, eff, cfg.Name, p)}
		})
	}
	// Placement: only brokers whose configured region satisfies the topic's policy are
	// eligible, even if the region's peer list were misconfigured to include outsiders.
	var eligible []string
	for _, id := range b.peers {
		if eff.CanStore(b.guard.RegionOf(id)) {
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
	return b.proposeTopic(ctx, cfg, func(p int) meta.PartitionMeta {
		replicas := placement.InRegionReplicas(eligible, p)
		leader := ""
		for _, r := range replicas {
			if live[r] {
				leader = r
				break
			}
		}
		return meta.PartitionMeta{Replicas: replicas, Leader: leader, LeaderEpoch: 1}
	})
}

func (b *Broker) proposeTopic(ctx context.Context, cfg api.TopicConfig, place func(p int) meta.PartitionMeta) (*meta.Metadata, error) {
	// "Already exists" must be judged on *committed* metadata. The proposal base is the latest
	// accepted version, which can hold a topic that a previous controller never committed (it
	// lost its lease mid-proposal); in that case we still propose, which commits it.
	if t, ok := b.coord.Committed().Topics[cfg.Name]; ok && t.Config == cfg {
		return b.coord.Committed(), nil
	}
	md, err := b.coord.Propose(ctx, func(m *meta.Metadata) error {
		if t, ok := m.Topics[cfg.Name]; ok {
			if t.Config != cfg {
				return api.Errorf(api.CodeTopicExists, "topic %q exists with partitions=%d residency=%s",
					cfg.Name, t.Config.Partitions, t.Config.Residency)
			}
			return nil // accepted but not yet committed: committing this version commits it
		}
		t := &meta.TopicMeta{Config: cfg}
		for p := 0; p < cfg.Partitions; p++ {
			t.Partitions = append(t.Partitions, place(p))
		}
		m.Topics[cfg.Name] = t
		return nil
	})
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
			if b.spansRegions(t) { // self-elected: as this replica sees it
				pi.Leader, pi.LeaderEpoch = st.Leader, st.LeaderEpoch
			}
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
		return nil, b.unknownTopic(name)
	}
	return t, nil
}

// unknownTopic explains a topic this region doesn't have. A topic stored only in other regions
// gets a fast, explicit answer: WRONG_REGION with the brokers that hold it, or
// REGION_UNAVAILABLE if none of those regions can be reached.
func (b *Broker) unknownTopic(name string) error {
	ts, ok := b.geo.TopicHome(name)
	if !ok {
		return api.Errorf(api.CodeUnknownTopic, "unknown topic %q", name)
	}
	regions := b.storeRegions(ts.Residency)
	if regions == nil {
		_, err := b.policy.Resolve(ts.Residency)
		return err // fail closed: can't even evaluate its policy here
	}
	if eff, err := b.policy.Resolve(ts.Residency); err == nil && !eff.CanTransit(b.cfg.Info.Region) {
		// The request reached a region this data may not even pass through: it is refused here
		// (not stored, not forwarded) and the refusal goes on the record.
		b.stats.policyDenied.Add(1)
		b.trail.Record(trail.Decision{Broker: b.cfg.Info.ID, Topic: name, Partition: -1, Hop: "entry", Policy: ts.Residency,
			PolicyVersion: eff.Version, Decision: trail.Deny, Reason: fmt.Sprintf("transit_not_permitted: client redirected to %v", regions)})
	}
	var addrs, reachable []string
	for _, r := range regions {
		if r == b.cfg.Info.Region {
			return api.Errorf(api.CodeUnknownTopic, "topic %q is not created in region %s yet", name, r)
		}
		if b.geo.RegionReachable(r) {
			reachable = append(reachable, r)
			for _, id := range b.regionBrokers(r) {
				addrs = append(addrs, b.rpc.Addr(id))
			}
		}
	}
	if len(reachable) == 0 {
		e := api.Errorf(api.CodeRegionUnavailable, "topic %q is stored only in %v, which is unreachable; its data cannot be served from elsewhere", name, regions)
		e.Reason = "region_unreachable"
		return e
	}
	e := api.Errorf(api.CodeWrongRegion, "topic %q is stored in %v; connect to their brokers", name, regions)
	e.Reason = "resident_elsewhere"
	e.Leader = strings.Join(addrs, ",")
	return e
}

// proxyTarget picks a broker to proxy a request to when this region may carry a topic's data
// (transit) but not store it: the client gets served instead of redirected, and the data never
// touches this region's disks.
func (b *Broker) proxyTarget(name string) (geo.TopicSummary, string, bool) {
	ts, ok := b.geo.TopicHome(name)
	if !ok {
		return ts, "", false
	}
	eff, err := b.policy.Resolve(ts.Residency)
	if err != nil || eff.CanStore(b.cfg.Info.Region) || !eff.CanTransit(b.cfg.Info.Region) {
		return ts, "", false
	}
	for _, r := range placement.StorageRegions(b.cfg.Directory, eff) {
		for _, id := range b.regionBrokers(r) {
			if b.geo.Reachable(id) {
				return ts, id, true
			}
		}
	}
	return ts, "", false
}

// spec is a produce request's resolved policy and durability.
type spec struct {
	pid, seq uint64
	class    string            // per-message class ("" = none)
	narrow   *policy.Effective // its rule
	eff      policy.Effective  // topic rule ∩ narrow
	acks     string            // applied durability
	via      string            // requested relay region
}

// resolveSpec turns a request's class/acks/via into an effective rule, enforcing that the
// producer can only narrow the policy and can't go below its durability floor.
func (b *Broker) resolveSpec(t *meta.TopicMeta, pid, seq uint64, class, acks, via string) (spec, error) {
	s := spec{pid: pid, seq: seq, class: class, via: via}
	eff, err := b.policy.Resolve(t.Config.Residency)
	if err != nil {
		return s, err
	}
	if class != "" {
		n, err := b.policy.Class(class)
		if err != nil {
			return s, err
		}
		s.narrow = &n
		eff = eff.Narrow(n)
	}
	s.eff = eff
	switch acks {
	case "", replica.AcksQuorum:
		acks = replica.AcksQuorum
	case replica.AcksLeader, replica.AcksAll:
	default:
		return s, api.Errorf(api.CodeInvalidRequest, "acks must be leader, quorum or all")
	}
	if eff.MinAcks != "" && policy.AcksRank(eff.MinAcks) > policy.AcksRank(acks) {
		acks = eff.MinAcks // the policy's durability floor wins
	}
	s.acks = acks
	if via != "" && !eff.CanTransit(via) {
		e := api.Errorf(api.CodeResidencyViolation, "requested route via %s is not permitted for %s", via, eff.Name)
		e.Reason = "route_not_permitted"
		return s, e
	}
	return s, nil
}

// checkPlacement verifies every replica of a partition may store data under s (a per-message
// class can make a partition's existing placement unacceptable: then the write is refused).
func (b *Broker) checkPlacement(t *meta.TopicMeta, part int, s spec) error {
	if s.narrow == nil {
		return nil
	}
	for _, r := range t.Partitions[part].Replicas {
		if err := b.guard.Do(residency.Check{Topic: t.Config.Name, Partition: part, Residency: t.Config.Residency,
			Narrow: s.narrow, Hop: "placement", Peer: r, Need: residency.Store, Quiet: true}); err != nil {
			e := api.Errorf(api.CodeResidencyViolation, "class %q forbids storing on replica %s of %s/%d", s.class, r, t.Config.Name, part)
			e.Reason = "policy_unsatisfiable"
			return e
		}
	}
	return nil
}

func headersFor(class string) []byte {
	if class == "" {
		return nil
	}
	h, _ := json.Marshal(map[string]string{"class": class})
	return h
}

// targets decides where a request for a partition goes: handled here (local), or forwarded to
// one of cands (tried in order). hops counts forwards so far; requests are forwarded at most twice
// (any broker -> its region's replica -> the leader).
func (b *Broker) targets(t *meta.TopicMeta, part, hops int) (local bool, cands []string, err error) {
	pm := t.Partitions[part]
	key := meta.PartKey{Topic: t.Config.Name, Partition: part}
	if !b.spansRegions(t) {
		switch pm.Leader {
		case b.cfg.Info.ID:
			return true, nil, nil
		case "":
			e := api.Errorf(api.CodeNotEnoughReplicas, "%s has no leader", key)
			e.Reason = "partition_offline"
			return false, nil, e
		}
		if hops >= 2 {
			return false, nil, api.Errorf(api.CodeNotLeader, "%s: too many forwards", key)
		}
		return false, []string{pm.Leader}, nil
	}
	// Global partition: the leader is elected by the replicas themselves.
	if p := b.parts.Get(key); p != nil {
		st := p.Status()
		switch {
		case st.Role == replica.RoleLeader:
			return true, nil, nil
		case st.Leader != "" && hops < 2:
			return false, []string{st.Leader}, nil
		}
		return false, nil, b.noGlobalLeader(key, t.Config.Residency)
	}
	if hops >= 2 {
		return false, nil, b.noGlobalLeader(key, t.Config.Residency)
	}
	// Not a replica: our region's replica first (cheap hop), then reachable replicas elsewhere.
	for _, r := range pm.Replicas {
		if b.cfg.Directory[r].Region == b.cfg.Info.Region {
			cands = append(cands, r)
		}
	}
	for _, r := range pm.Replicas {
		if b.cfg.Directory[r].Region != b.cfg.Info.Region && b.geo.Reachable(r) {
			cands = append(cands, r)
		}
	}
	if len(cands) == 0 {
		return false, nil, b.noGlobalLeader(key, t.Config.Residency)
	}
	return false, cands, nil
}

func (b *Broker) noGlobalLeader(key meta.PartKey, res string) error {
	regions := b.storeRegions(res)
	reachable := 0
	for _, r := range regions {
		if b.geo.RegionReachable(r) {
			reachable++
		}
	}
	if reachable < len(regions)/2+1 {
		e := api.Errorf(api.CodeNotEnoughReplicas, "cross-region partition %s needs a majority of its regions %v; region %s can reach only %d",
			key, regions, b.cfg.Info.Region, reachable)
		e.Reason = "region_isolated"
		return e
	}
	e := api.Errorf(api.CodeNotEnoughReplicas, "cross-region partition %s has no leader yet (election in progress)", key)
	e.Reason = "electing_leader"
	return e
}

// forward sends an internal request to the first candidate that answers, checking at this
// hop that the candidate may hold the data. Broker errors are returned as-is; transport
// failures move on to the next candidate.
func (b *Broker) forward(ctx context.Context, t *meta.TopicMeta, part int, s spec, cands []string, path string, timeout time.Duration, in, out any) error {
	var lastErr error
	o := route.Opts{Topic: t.Config.Name, Residency: t.Config.Residency, Narrow: s.narrow, Class: route.Live, Via: s.via}
	if s.class != "" {
		o.Classes = []string{s.class}
	}
	if t.Config.Priority == replica.PriorityUrgent {
		o.Class = route.Urgent
	}
	for _, to := range cands {
		if err := b.guard.Do(residency.Check{Topic: t.Config.Name, Partition: part, Residency: t.Config.Residency, Narrow: s.narrow,
			Hop: "forward", Peer: to, Need: residency.Store, Quiet: path != pathProduce}); err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, timeout)
		err := b.geo.Send(cctx, to, path, o, in, out)
		cancel()
		if err == nil {
			return nil
		}
		var ae *api.Error
		if errors.As(err, &ae) {
			return ae
		}
		lastErr = asUnavailable(err, to)
	}
	return lastErr
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

type partitionFetch struct {
	Topic      string `json:"topic"`
	Partition  int    `json:"partition"`
	Offset     uint64 `json:"offset"`
	MaxRecords int    `json:"max_records"`
	MaxBytes   int    `json:"max_bytes"`
	WaitMs     int64  `json:"wait_ms"`
	Hops       int    `json:"hops"`
}

func (b *Broker) Fetch(ctx context.Context, req api.FetchRequest) (api.FetchResponse, error) {
	req.MaxRecords = min(req.MaxRecords, api.MaxFetchRecords)
	req.Wait = min(req.Wait, api.MaxFetchWait)
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		if ts, target, ok := b.proxyTarget(req.Topic); ok {
			// Transit-only region: serve the read through a storage region without storing it.
			if err := b.guard.CheckTransit(req.Topic, ts.Residency, "proxy-read"); err != nil {
				return api.FetchResponse{}, err
			}
			var out api.FetchResponse
			in := partitionFetch{req.Topic, req.Partition, req.Offset, req.MaxRecords, req.MaxBytes, req.Wait.Milliseconds(), 0}
			cctx, cancel := context.WithTimeout(ctx, req.Wait+3*b.cfg.Replica.RPCTimeout)
			defer cancel()
			if err := b.geo.Send(cctx, target, pathFetch, route.Opts{Topic: req.Topic, Residency: ts.Residency, Class: route.Live}, in, &out); err != nil {
				return api.FetchResponse{}, asUnavailable(err, target)
			}
			return out, nil
		}
		return api.FetchResponse{}, err
	}
	// Reading moves data out through this broker: it must be allowed to carry it.
	if err := b.guard.CheckTransit(req.Topic, t.Config.Residency, "entry-read"); err != nil {
		return api.FetchResponse{}, err
	}
	if req.Partition < 0 || req.Partition >= len(t.Partitions) {
		return api.FetchResponse{}, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	return b.fetchPartition(ctx, t, req, 0)
}

func (b *Broker) fetchPartition(ctx context.Context, t *meta.TopicMeta, req api.FetchRequest, hops int) (api.FetchResponse, error) {
	local, cands, err := b.targets(t, req.Partition, hops)
	if err != nil {
		return api.FetchResponse{}, err
	}
	if local {
		return b.fetchLocal(ctx, req)
	}
	var out api.FetchResponse
	in := partitionFetch{req.Topic, req.Partition, req.Offset, req.MaxRecords, req.MaxBytes, req.Wait.Milliseconds(), hops + 1}
	if err := b.forward(ctx, t, req.Partition, spec{}, cands, pathFetch, req.Wait+2*b.cfg.Replica.RPCTimeout, in, &out); err != nil {
		return api.FetchResponse{}, err
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
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		return nil, err
	}
	if req.Partition < 0 || req.Partition >= len(t.Partitions) {
		return nil, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	resp, err := b.fetchPartition(ctx, t, api.FetchRequest{
		Topic: req.Topic, Partition: req.Partition, Offset: req.Offset,
		MaxRecords: req.MaxRecords, MaxBytes: req.MaxBytes, Wait: time.Duration(req.WaitMs) * time.Millisecond,
	}, req.Hops)
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
	// Simulated network: blocked peer links and extra one-way delay (ms) per peer.
	// Link capacities (megabits/s per destination region, 0 = unlimited) drive the bandwidth
	// scheduler for inter-region traffic.
	type netState struct {
		Blocked      []string           `json:"blocked"`
		DelaysMs     map[string]int64   `json:"delays_ms,omitempty"`
		CapacityMbps map[string]float64 `json:"link_capacity_mbps,omitempty"`
	}
	current := func() netState {
		s := netState{Blocked: b.net.List(), DelaysMs: map[string]int64{}, CapacityMbps: map[string]float64{}}
		slices.Sort(s.Blocked)
		for id, d := range b.net.Delays() {
			s.DelaysMs[id] = d.Milliseconds()
		}
		for region, st := range b.geo.FlowStats() {
			if st.CapacityBps > 0 {
				s.CapacityMbps[region] = st.CapacityBps * 8 / 1e6
			}
		}
		return s
	}
	mux.HandleFunc("GET /admin/v1/net", func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, current())
	})
	mux.HandleFunc("POST /admin/v1/net", func(w http.ResponseWriter, r *http.Request) {
		var s netState
		if !httpapi.Decode(w, r, &s) {
			return
		}
		b.net.Set(s.Blocked)
		delays := map[string]time.Duration{}
		for id, ms := range s.DelaysMs {
			delays[id] = time.Duration(ms) * time.Millisecond
		}
		b.net.SetDelays(delays)
		for _, region := range b.regionNames() {
			if region != b.cfg.Info.Region {
				b.geo.SetLinkCapacity(region, s.CapacityMbps[region]*1e6/8)
			}
		}
		slog.Warn("network links changed", "blocked", s.Blocked, "delays_ms", s.DelaysMs, "capacity_mbps", s.CapacityMbps)
		httpapi.WriteJSON(w, http.StatusOK, current())
	})
	mux.HandleFunc("GET /admin/v1/status", func(w http.ResponseWriter, r *http.Request) {
		ctrl, epoch := b.coord.Controller()
		regions := map[string]bool{}
		for _, r := range b.regionNames() {
			regions[r] = b.geo.RegionReachable(r)
		}
		httpapi.WriteJSON(w, http.StatusOK, map[string]any{
			"broker": b.cfg.Info, "controller": ctrl, "controller_epoch": epoch,
			"is_controller": b.coord.IsController(), "synced": b.coord.Synced(),
			"metadata_version":     b.coord.Committed().Version,
			"residency_rejections": b.guard.Rejected(),
			"regions_reachable":    regions,
			"relayed_requests":     b.geo.Relayed(),
		})
	})
	// Inter-region links as this broker measures them, and the path global traffic takes.
	mux.HandleFunc("GET /admin/v1/routes", func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, map[string]any{"from": b.cfg.Info.ID, "links": b.geo.Links()})
	})
	// Per-partition replica state on this broker (role, epoch, offsets).
	mux.HandleFunc("GET /admin/v1/partitions", func(w http.ResponseWriter, r *http.Request) {
		type ps struct {
			Topic     string `json:"topic"`
			Partition int    `json:"partition"`
			Role      string `json:"role"`
			Leader    string `json:"leader"`
			Epoch     uint64 `json:"epoch"`
			HW        uint64 `json:"hw"`
			End       uint64 `json:"end_offset"`
		}
		var out []ps
		for _, p := range b.parts.All() {
			st := p.Status()
			out = append(out, ps{p.Key.Topic, p.Key.Partition, st.Role.String(), st.Leader, st.Epoch, st.HW, st.EndOffset})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Topic != out[j].Topic {
				return out[i].Topic < out[j].Topic
			}
			return out[i].Partition < out[j].Partition
		})
		httpapi.WriteJSON(w, http.StatusOK, out)
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

// spansRegions reports whether a topic's partitions are replicated across regions. Such
// partitions elect their own leaders; the region controller must never assign or fence them.
// It is read from the committed placement (not the policy), so it stays right even if the
// policy document later becomes unavailable.
func (b *Broker) spansRegions(t *meta.TopicMeta) bool {
	if len(t.Partitions) == 0 {
		return false
	}
	regions := map[string]bool{}
	for _, r := range t.Partitions[0].Replicas {
		regions[b.guard.RegionOf(r)] = true
	}
	return len(regions) >= 2
}

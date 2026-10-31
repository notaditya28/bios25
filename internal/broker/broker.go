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
	"github.com/notaditya28/hydra-stream/internal/geo"
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
	geo     *geo.Router

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
	addrs := cfg.Addrs
	if addrs == nil {
		addrs = cfg.Peers
	}
	b.rpc = rpc.NewClient(cfg.Info.ID, addrs, cfg.Net)
	b.parts = replica.NewManager(cfg.Info.ID, cfg.DataDir, cfg.Log, b.rpc, cfg.Replica, b.guard)
	b.geo = geo.New(geo.Config{
		Self: cfg.Info, Directory: cfg.Directory, RPC: b.rpc, Guard: b.guard, Interval: cfg.Probe,
		Topics: b.topicSummaries,
	})
	b.parts.SetSender(b.geo)
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
	b.registerAdmin(mux)
	return b.net.Middleware(mux)
}

// Start runs the coordinator, controller and latency-probe loops.
func (b *Broker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(3)
	go func() { defer b.wg.Done(); b.coord.Run(ctx) }()
	go func() { defer b.wg.Done(); b.runController(ctx) }()
	go func() { defer b.wg.Done(); b.geo.Run(ctx) }()
}

func (b *Broker) regionNames() []string {
	seen := map[string]bool{}
	var out []string
	for _, info := range b.cfg.Directory {
		if !seen[info.Region] {
			seen[info.Region] = true
			out = append(out, info.Region)
		}
	}
	sort.Strings(out)
	return out
}

func (b *Broker) regionBrokers(region string) []string {
	var out []string
	for id, info := range b.cfg.Directory {
		if info.Region == region {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// globalReplicas places a global partition on one broker per region. It is a pure function of
// the cluster config, so every region computes the same placement without coordinating.
func (b *Broker) globalReplicas(topic string, p int) []string {
	h := fnv.New32a()
	h.Write([]byte(topic))
	var out []string
	for _, region := range b.regionNames() {
		ids := b.regionBrokers(region)
		out = append(out, ids[(int(h.Sum32()%1024)+p)%len(ids)])
	}
	return out
}

// topicSummaries is what this broker tells other regions about its region's topics.
func (b *Broker) topicSummaries() []geo.TopicSummary {
	md := b.coord.Committed()
	out := make([]geo.TopicSummary, 0, len(md.Topics))
	for _, t := range md.Topics {
		out = append(out, geo.TopicSummary{Name: t.Config.Name, Residency: t.Config.Residency, Partitions: t.Config.Partitions})
	}
	return out
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
	info, err := b.createInRegion(ctx, cfg)
	if err != nil || cfg.Residency != api.ResidencyGlobal {
		return info, err
	}
	// A global topic exists once a majority of regions has it; the others catch up through
	// topic gossip (see syncGlobalTopics) when they are reachable again.
	accepted := 1
	var failed []string
	for _, region := range b.regionNames() {
		if region == b.cfg.Info.Region {
			continue
		}
		if err := b.createRemote(ctx, region, cfg); err != nil {
			if api.CodeOf(err) == api.CodeTopicExists {
				return api.TopicInfo{}, err
			}
			failed = append(failed, region)
			continue
		}
		accepted++
	}
	if quorum := len(b.regionNames())/2 + 1; accepted < quorum {
		e := api.Errorf(api.CodeNotEnoughReplicas, "global topic %q accepted by %d of %d regions (unreachable: %v); it will spread when they return",
			cfg.Name, accepted, len(b.regionNames()), failed)
		e.Reason = "regions_unreachable"
		return api.TopicInfo{}, e
	}
	b.waitGlobalLeaders(ctx, cfg.Name, 5*time.Second)
	return b.Topic(ctx, cfg.Name)
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
		err := b.geo.Send(cctx, id, pathCreateLocal, cfg.Name, cfg.Residency, cfg, &struct{}{})
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
	if err := b.checkRegion(n); err != nil {
		return nil, err
	}
	_, err = b.createAsController(ctx, n)
	return &struct{}{}, err
}

func (b *Broker) createAsController(ctx context.Context, cfg api.TopicConfig) (*meta.Metadata, error) {
	live := b.coord.Live()
	quorum := len(b.peers)/2 + 1
	if len(live) < quorum {
		return nil, api.Errorf(api.CodeNotEnoughReplicas, "only %d of %d brokers live", len(live), len(b.peers))
	}
	if cfg.Residency == api.ResidencyGlobal {
		return b.proposeTopic(ctx, cfg, func(p int) meta.PartitionMeta {
			return meta.PartitionMeta{Replicas: b.globalReplicas(cfg.Name, p)} // leader elected by the replicas
		})
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
	return b.proposeTopic(ctx, cfg, func(p int) meta.PartitionMeta {
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
			if t.Config.Residency == api.ResidencyGlobal { // self-elected: as this replica sees it
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

// unknownTopic explains a topic this region doesn't have. A topic resident in another region
// gets a fast, explicit answer instead of a relay (which residency forbids): WRONG_REGION with
// that region's brokers, or REGION_UNAVAILABLE if the region can't be reached.
func (b *Broker) unknownTopic(name string) error {
	ts, ok := b.geo.TopicHome(name)
	if !ok || ts.Residency == api.ResidencyGlobal {
		return api.Errorf(api.CodeUnknownTopic, "unknown topic %q", name)
	}
	if !b.geo.RegionReachable(ts.Residency) {
		e := api.Errorf(api.CodeRegionUnavailable, "topic %q is resident in region %s, which is unreachable; its data cannot be served from elsewhere", name, ts.Residency)
		e.Reason = "region_unreachable"
		return e
	}
	var addrs []string
	for _, id := range b.regionBrokers(ts.Residency) {
		addrs = append(addrs, b.rpc.Addr(id))
	}
	e := api.Errorf(api.CodeWrongRegion, "topic %q is resident in region %s; connect to its brokers", name, ts.Residency)
	e.Reason = "resident_elsewhere"
	e.Leader = strings.Join(addrs, ",")
	return e
}

// targets decides where a request for a partition goes: handled here (local), or forwarded to
// one of cands (tried in order). hops counts forwards so far; requests are forwarded at most twice
// (any broker -> its region's replica -> the leader).
func (b *Broker) targets(t *meta.TopicMeta, part, hops int) (local bool, cands []string, err error) {
	pm := t.Partitions[part]
	key := meta.PartKey{Topic: t.Config.Name, Partition: part}
	if t.Config.Residency != api.ResidencyGlobal {
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
		return false, nil, b.noGlobalLeader(key)
	}
	if hops >= 2 {
		return false, nil, b.noGlobalLeader(key)
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
		return false, nil, b.noGlobalLeader(key)
	}
	return false, cands, nil
}

func (b *Broker) noGlobalLeader(key meta.PartKey) error {
	reachable := 1
	for _, r := range b.regionNames() {
		if r != b.cfg.Info.Region && b.geo.RegionReachable(r) {
			reachable++
		}
	}
	if reachable < len(b.regionNames())/2+1 {
		e := api.Errorf(api.CodeNotEnoughReplicas, "global partition %s needs a majority of regions; region %s can reach only %d of %d",
			key, b.cfg.Info.Region, reachable, len(b.regionNames()))
		e.Reason = "region_isolated"
		return e
	}
	e := api.Errorf(api.CodeNotEnoughReplicas, "global partition %s has no leader yet (election in progress)", key)
	e.Reason = "electing_leader"
	return e
}

// forward sends an internal request to the first candidate that answers. Broker errors are
// returned as-is; transport failures move on to the next candidate.
func (b *Broker) forward(ctx context.Context, t *meta.TopicMeta, cands []string, path string, timeout time.Duration, in, out any) error {
	var lastErr error
	for _, to := range cands {
		if err := b.guard.CheckPeer(t.Config.Name, t.Config.Residency, to, "forward to"); err != nil {
			return err
		}
		cctx, cancel := context.WithTimeout(ctx, timeout)
		err := b.geo.Send(cctx, to, path, t.Config.Name, t.Config.Residency, in, out)
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
		base, err := b.producePartition(ctx, t, *p, req.Records, req.ProducerID, req.BaseSeq, 0)
		if err != nil {
			return api.ProduceResponse{}, err
		}
		resp := api.ProduceResponse{Offsets: make([]api.RecordOffset, len(req.Records))}
		for i := range resp.Offsets {
			resp.Offsets[i] = api.RecordOffset{Partition: *p, Offset: base + uint64(i)}
		}
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
			base, err := b.producePartition(ctx, t, p, recs, 0, 0, 0)
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
	Topic      string              `json:"topic"`
	Partition  int                 `json:"partition"`
	Records    []api.ProduceRecord `json:"records"`
	ProducerID uint64              `json:"producer_id,omitempty"`
	BaseSeq    uint64              `json:"base_seq,omitempty"`
	Hops       int                 `json:"hops"`
}

type partitionProduceResp struct {
	Base uint64 `json:"base"`
}

func (b *Broker) producePartition(ctx context.Context, t *meta.TopicMeta, part int, recs []api.ProduceRecord, pid, seq uint64, hops int) (uint64, error) {
	local, cands, err := b.targets(t, part, hops)
	if err != nil {
		return 0, err
	}
	if local {
		return b.produceLocal(ctx, t.Config.Name, part, recs, pid, seq)
	}
	var out partitionProduceResp
	in := partitionProduce{t.Config.Name, part, recs, pid, seq, hops + 1}
	if err := b.forward(ctx, t, cands, pathProduce, b.cfg.Replica.ProduceTimeout+2*b.cfg.Replica.RPCTimeout, in, &out); err != nil {
		return 0, err
	}
	return out.Base, nil
}

func (b *Broker) produceLocal(ctx context.Context, topic string, part int, recs []api.ProduceRecord, pid, seq uint64) (uint64, error) {
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
	return p.Produce(ctx, lrecs, pid, seq)
}

func (b *Broker) handleProduce(ctx context.Context, req *partitionProduce) (*partitionProduceResp, error) {
	t, err := b.topicMeta(req.Topic)
	if err != nil {
		return nil, err
	}
	if req.Partition < 0 || req.Partition >= len(t.Partitions) {
		return nil, api.Errorf(api.CodeUnknownPartition, "topic %q has no partition %d", req.Topic, req.Partition)
	}
	base, err := b.producePartition(ctx, t, req.Partition, req.Records, req.ProducerID, req.BaseSeq, req.Hops)
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
	Hops       int    `json:"hops"`
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
	if err := b.forward(ctx, t, cands, pathFetch, req.Wait+2*b.cfg.Replica.RPCTimeout, in, &out); err != nil {
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
	type netState struct {
		Blocked  []string         `json:"blocked"`
		DelaysMs map[string]int64 `json:"delays_ms,omitempty"`
	}
	current := func() netState {
		s := netState{Blocked: b.net.List(), DelaysMs: map[string]int64{}}
		slices.Sort(s.Blocked)
		for id, d := range b.net.Delays() {
			s.DelaysMs[id] = d.Milliseconds()
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
		slog.Warn("network links changed", "blocked", s.Blocked, "delays_ms", s.DelaysMs)
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

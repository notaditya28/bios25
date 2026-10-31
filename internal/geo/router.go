// Package geo measures inter-region latency and routes cross-region traffic.
//
// Every broker probes every broker in other regions (EWMA of round-trip times) and shares its
// latency table and its region's topic list in the probe replies. Global-topic traffic to
// another region goes direct, or via a broker in a third region when that path is clearly
// faster. Resident data is never routed this way: it doesn't leave its region, and the relay
// re-checks residency itself.
package geo

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/flow"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/residency"
	"github.com/notaditya28/hydra-stream/internal/route"
	"github.com/notaditya28/hydra-stream/internal/rpc"
)

const (
	PathPing  = "/internal/v1/geo/ping"
	PathRelay = "/internal/v1/geo/relay"

	alpha       = 0.3 // EWMA weight of the newest sample
	switchRatio = 0.7 // take a relay only if it is at least 30% faster than direct...
	stayRatio   = 0.9 // ...and keep it while it stays 10% faster (hysteresis, avoids flapping)
	minGainMs   = 10  // and only if it saves at least this much
)

type TopicSummary struct {
	Name       string `json:"name"`
	Residency  string `json:"residency"`
	Partitions int    `json:"partitions"`
	Priority   string `json:"priority,omitempty"`
}

type PingRequest struct {
	From string `json:"from"`
}

type PingResponse struct {
	ID     string             `json:"id"`
	Region string             `json:"region"`
	RTT    map[string]float64 `json:"rtt_ms"` // responder's view of brokers it can reach
	Topics []TopicSummary     `json:"topics"`
}

type RelayRequest struct {
	To        string          `json:"to"`
	Path      string          `json:"path"`
	Topic     string          `json:"topic"`
	Residency string          `json:"residency"`
	Classes   []string        `json:"classes,omitempty"` // per-message policy classes in the payload
	Body      json.RawMessage `json:"body"`
	Hops      int             `json:"hops"`
	Visited   []string        `json:"visited"`
	Class     route.Class     `json:"class"`
}

// NarrowFor resolves per-message classes into one combined (intersected) rule.
func (r *Router) NarrowFor(classes []string) (*policy.Effective, error) {
	if len(classes) == 0 {
		return nil, nil
	}
	var out *policy.Effective
	for _, c := range classes {
		e, err := r.cfg.Guard.Engine().Class(c)
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = &e
		} else {
			n := out.Narrow(e)
			out = &n
		}
	}
	return out, nil
}

type Config struct {
	Self      api.Info
	Directory map[string]api.Info // every broker in the cluster
	RPC       *rpc.Client
	Guard     *residency.Guard
	Interval  time.Duration         // probe interval
	Topics    func() []TopicSummary // this broker's view of its region's topics
	Flow      flow.Config           // inter-region bandwidth scheduling
}

type Router struct {
	cfg Config

	mu     sync.RWMutex
	rtt    map[string]float64 // EWMA round trip in ms to brokers in other regions
	lastOK map[string]time.Time
	tables map[string]map[string]float64 // their RTT tables
	topics map[string][]TopicSummary     // by broker id
	routes map[string]string             // destination -> relay broker ("" = direct)
	links  map[string]*flow.Link         // destination region -> bandwidth scheduler

	relayed atomic.Int64
}

func New(cfg Config) *Router {
	if cfg.Interval == 0 {
		cfg.Interval = 200 * time.Millisecond
	}
	r := &Router{cfg: cfg, rtt: map[string]float64{}, lastOK: map[string]time.Time{},
		tables: map[string]map[string]float64{}, topics: map[string][]TopicSummary{}, routes: map[string]string{},
		links: map[string]*flow.Link{}}
	for _, id := range r.remote() {
		region := cfg.Directory[id].Region
		if r.links[region] == nil {
			r.links[region] = flow.NewLink(cfg.Flow)
		}
	}
	return r
}

// Close stops the link schedulers.
func (r *Router) Close() {
	for _, l := range r.links {
		l.Close()
	}
}

// SetLinkCapacity sets the capacity (bytes/s, 0 = unlimited) of our link towards region.
func (r *Router) SetLinkCapacity(region string, bps float64) {
	if l := r.links[region]; l != nil {
		l.SetCapacity(bps)
	}
}

// FlowStats reports each outgoing link's scheduler state.
func (r *Router) FlowStats() map[string]flow.Stats {
	out := map[string]flow.Stats{}
	for region, l := range r.links {
		out[region] = l.Stats()
	}
	return out
}

// shape waits until the link towards peer's region admits in (no-op on unlimited links and
// inside our own region).
func (r *Router) shape(ctx context.Context, peer string, class route.Class, in any) error {
	l := r.links[r.cfg.Directory[peer].Region]
	if l == nil || l.Capacity() <= 0 {
		return nil
	}
	n := 256
	if v, ok := in.(json.RawMessage); ok {
		n = len(v)
	} else if data, err := json.Marshal(in); err == nil {
		n = len(data)
	}
	return l.Acquire(ctx, class, n)
}

// call shapes and sends one request.
func (r *Router) call(ctx context.Context, to, path string, class route.Class, in, out any) error {
	if err := r.shape(ctx, to, class, in); err != nil {
		return err
	}
	return r.cfg.RPC.Call(ctx, to, path, in, out)
}

func (r *Router) Register(mux *http.ServeMux) {
	rpc.Handle(mux, PathPing, func(_ context.Context, _ *PingRequest) (*PingResponse, error) {
		resp := &PingResponse{ID: r.cfg.Self.ID, Region: r.cfg.Self.Region, RTT: r.table()}
		if r.cfg.Topics != nil {
			resp.Topics = r.cfg.Topics()
		}
		return resp, nil
	})
	rpc.Handle(mux, PathRelay, r.handleRelay)
}

func (r *Router) remote() []string {
	var out []string
	for id, info := range r.cfg.Directory {
		if info.Region != r.cfg.Self.Region {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// Run probes every broker in other regions until ctx is done.
func (r *Router) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		var wg sync.WaitGroup
		for _, id := range r.remote() {
			wg.Add(1)
			go func() { defer wg.Done(); r.probe(ctx, id) }()
		}
		wg.Wait()
		r.recompute()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (r *Router) probe(ctx context.Context, id string) {
	ctx, cancel := context.WithTimeout(ctx, max(5*r.cfg.Interval, time.Second))
	defer cancel()
	start := time.Now()
	var resp PingResponse
	if err := r.cfg.RPC.Call(ctx, id, PathPing, PingRequest{From: r.cfg.Self.ID}, &resp); err != nil {
		return
	}
	ms := float64(time.Since(start).Microseconds()) / 1000
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.rtt[id]; ok && time.Since(r.lastOK[id]) < r.staleAfter() {
		ms = alpha*ms + (1-alpha)*old
	}
	r.rtt[id], r.lastOK[id] = ms, time.Now()
	r.tables[id] = resp.RTT
	r.topics[id] = resp.Topics
}

func (r *Router) upLocked(id string) bool {
	t, ok := r.lastOK[id]
	return ok && time.Since(t) < r.staleAfter()
}

// Reachable reports whether a broker in another region answered probes recently.
// Brokers in our own region are always reported reachable (in-region health is the controller's job).
func (r *Router) Reachable(id string) bool {
	if r.cfg.Directory[id].Region == r.cfg.Self.Region {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.upLocked(id)
}

func (r *Router) RegionReachable(region string) bool {
	if region == r.cfg.Self.Region {
		return true
	}
	for id, info := range r.cfg.Directory {
		if info.Region == region && r.Reachable(id) {
			return true
		}
	}
	return false
}

func (r *Router) table() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := map[string]float64{}
	for id, ms := range r.rtt {
		if r.upLocked(id) {
			out[id] = ms
		}
	}
	return out
}

// bestRelayLocked returns the fastest relay broker towards to (only through regions allowed
// says yes to; nil = any) and its estimated round trip.
func (r *Router) bestRelayLocked(to string, allowed func(region string) bool) (string, float64) {
	toRegion := r.cfg.Directory[to].Region
	best, cost := "", math.Inf(1)
	for via, ms := range r.rtt {
		vr := r.cfg.Directory[via].Region
		if vr == toRegion || vr == r.cfg.Self.Region || !r.upLocked(via) || (allowed != nil && !allowed(vr)) {
			continue
		}
		onward, ok := r.tables[via][to]
		if !ok {
			continue
		}
		if c := ms + onward; c < cost {
			best, cost = via, c
		}
	}
	return best, cost
}

func (r *Router) recompute() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, to := range r.remote() {
		direct := math.Inf(1)
		if r.upLocked(to) {
			direct = r.rtt[to]
		}
		via, cost := r.bestRelayLocked(to, nil)
		cur := r.routes[to]
		next := ""
		switch {
		case via == "":
		case math.IsInf(direct, 1):
			next = via // direct link down: any working relay beats nothing
		case direct-cost < minGainMs:
			// Not worth an extra hop: sub-10ms differences are measurement noise on a LAN.
		case cur != "" && cost < stayRatio*direct:
			next = via
		case cost < switchRatio*direct:
			next = via
		}
		if next != cur {
			if next == "" {
				slog.Info("route changed", "to", to, "path", "direct", "direct_ms", round(direct))
			} else {
				slog.Info("route changed", "to", to, "path", "via "+next, "direct_ms", round(direct), "relay_ms", round(cost))
			}
			r.routes[to] = next
		}
	}
}

func round(f float64) float64 {
	if math.IsInf(f, 0) {
		return -1
	}
	return math.Round(f*10) / 10
}

// chooseVia picks the relay for a send, honouring the payload's policy: only regions the data
// may transit are candidates. A producer-requested relay region must itself be permitted.
func (r *Router) chooseVia(to string, eff policy.Effective, want string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if want != "" {
		if !eff.CanTransit(want) {
			e := api.Errorf(api.CodeResidencyViolation, "requested route via %s is not permitted for %s", want, eff.Name)
			e.Reason = "route_not_permitted"
			return "", e
		}
		best, bestRTT := "", math.Inf(1)
		for _, id := range r.remote() {
			if r.cfg.Directory[id].Region == want && r.upLocked(id) && r.rtt[id] < bestRTT {
				best, bestRTT = id, r.rtt[id]
			}
		}
		if want == r.cfg.Directory[to].Region || want == r.cfg.Self.Region {
			return "", nil // "via" the source or destination region is just the direct path
		}
		if best == "" {
			return "", api.Errorf(api.CodeRegionUnavailable, "requested relay region %s is unreachable", want)
		}
		return best, nil
	}
	cached := r.routes[to]
	if cached == "" {
		return "", nil // direct is best (or as good as anything)
	}
	if eff.CanTransit(r.cfg.Directory[cached].Region) {
		return cached, nil
	}
	// The usual relay is forbidden for this data: is another permitted relay worth it?
	direct := math.Inf(1)
	if r.upLocked(to) {
		direct = r.rtt[to]
	}
	via, cost := r.bestRelayLocked(to, eff.CanTransit)
	if via != "" && (math.IsInf(direct, 1) || (cost < switchRatio*direct && direct-cost >= minGainMs)) {
		return via, nil
	}
	return "", nil
}

// Send delivers an internal request to broker to, relaying through a third region when that
// is faster and the data's policy allows it to transit there.
func (r *Router) Send(ctx context.Context, to, path string, o route.Opts, in, out any) error {
	if r.cfg.Directory[to].Region == r.cfg.Self.Region {
		return r.cfg.RPC.Call(ctx, to, path, in, out)
	}
	eff, err := r.cfg.Guard.Policy(o.Residency)
	if err != nil {
		return err // fail closed
	}
	if o.Narrow != nil {
		eff = eff.Narrow(*o.Narrow)
	}
	via, err := r.chooseVia(to, eff, o.Via)
	if err != nil {
		return err
	}
	if via == "" {
		return r.call(ctx, to, path, o.Class, in, out)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	var raw json.RawMessage
	req := RelayRequest{To: to, Path: path, Topic: o.Topic, Residency: o.Residency, Classes: o.Classes,
		Body: body, Hops: 1, Visited: []string{r.cfg.Self.ID}, Class: o.Class}
	err = r.call(ctx, via, PathRelay, o.Class, req, &raw)
	var ae *api.Error
	if err != nil && !errors.As(err, &ae) && ctx.Err() == nil && o.Via == "" {
		return r.call(ctx, to, path, o.Class, in, out) // relay unreachable: try direct
	}
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

const maxRelayHops = 2

func (r *Router) handleRelay(ctx context.Context, req *RelayRequest) (*json.RawMessage, error) {
	if req.Hops > maxRelayHops {
		e := api.Errorf(api.CodeResidencyViolation, "relay hop limit exceeded (%d)", req.Hops)
		e.Reason = "hop_limit"
		return nil, e
	}
	for _, v := range req.Visited {
		if v == r.cfg.Self.ID {
			e := api.Errorf(api.CodeResidencyViolation, "routing loop through %s", v)
			e.Reason = "routing_loop"
			return nil, e
		}
	}
	// Re-check with our own copy of the policy (the sender's choice is not trusted): we may
	// carry the data, and the next hop may store it. Per-message classes narrow the rule.
	narrow, err := r.NarrowFor(req.Classes)
	if err != nil {
		return nil, err
	}
	g := r.cfg.Guard
	if err := g.Do(residency.Check{Topic: req.Topic, Partition: -1, Residency: req.Residency, Narrow: narrow,
		Hop: "relay", Need: residency.Transit, Batch: req.Path}); err != nil {
		return nil, err
	}
	if err := g.Do(residency.Check{Topic: req.Topic, Partition: -1, Residency: req.Residency, Narrow: narrow,
		Hop: "relay-to", Peer: req.To, Need: residency.Store, Quiet: true}); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := r.call(ctx, req.To, req.Path, req.Class, req.Body, &raw); err != nil {
		var ae *api.Error
		if errors.As(err, &ae) {
			return nil, ae
		}
		e := api.Errorf(api.CodeNotLeader, "relay %s cannot reach %s: %v", r.cfg.Self.ID, req.To, err)
		e.Reason = "relay_target_unreachable"
		return nil, e
	}
	r.relayed.Add(1)
	return &raw, nil
}

// TopicHome finds a topic another region told us about.
func (r *Router) TopicHome(name string) (TopicSummary, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, ts := range r.topics {
		for _, t := range ts {
			if t.Name == name {
				return t, true
			}
		}
	}
	return TopicSummary{}, false
}

// RemoteTopics lists topics known to other (reachable) regions; keep filters them.
func (r *Router) RemoteTopics(keep func(TopicSummary) bool) []TopicSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]TopicSummary{}
	for id, ts := range r.topics {
		if !r.upLocked(id) {
			continue
		}
		for _, t := range ts {
			if keep == nil || keep(t) {
				seen[t.Name] = t
			}
		}
	}
	out := make([]TopicSummary, 0, len(seen))
	for _, t := range seen {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

type Link struct {
	To     string  `json:"to"`
	Region string  `json:"region"`
	Up     bool    `json:"up"`
	RTTms  float64 `json:"rtt_ms"`
	Via    string  `json:"via,omitempty"` // relay used for global traffic, "" = direct
}

func (r *Router) Links() []Link {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Link
	for _, id := range r.remote() {
		l := Link{To: id, Region: r.cfg.Directory[id].Region, Up: r.upLocked(id), Via: r.routes[id]}
		if l.Up {
			l.RTTms = round(r.rtt[id])
		}
		out = append(out, l)
	}
	return out
}

// Route returns the relay currently used towards to ("" = direct).
func (r *Router) Route(to string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.routes[to]
}

func (r *Router) Relayed() int64 { return r.relayed.Load() }

// staleAfter is how long a broker may go unanswered before we treat it (and, if all its region's
// brokers are silent, its region) as unreachable: about three missed probes.
func (r *Router) staleAfter() time.Duration { return 3*r.cfg.Interval + 300*time.Millisecond }

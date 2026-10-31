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
	"github.com/notaditya28/hydra-stream/internal/residency"
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
	Body      json.RawMessage `json:"body"`
}

type Config struct {
	Self      api.Info
	Directory map[string]api.Info // every broker in the cluster
	RPC       *rpc.Client
	Guard     *residency.Guard
	Interval  time.Duration         // probe interval
	Topics    func() []TopicSummary // this broker's view of its region's topics
}

type Router struct {
	cfg Config

	mu     sync.RWMutex
	rtt    map[string]float64 // EWMA round trip in ms to brokers in other regions
	lastOK map[string]time.Time
	tables map[string]map[string]float64 // their RTT tables
	topics map[string][]TopicSummary     // by broker id
	routes map[string]string             // destination -> relay broker ("" = direct)

	relayed atomic.Int64
}

func New(cfg Config) *Router {
	if cfg.Interval == 0 {
		cfg.Interval = 200 * time.Millisecond
	}
	return &Router{cfg: cfg, rtt: map[string]float64{}, lastOK: map[string]time.Time{},
		tables: map[string]map[string]float64{}, topics: map[string][]TopicSummary{}, routes: map[string]string{}}
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

// bestRelayLocked returns the fastest relay broker towards to and its estimated round trip.
func (r *Router) bestRelayLocked(to string) (string, float64) {
	toRegion := r.cfg.Directory[to].Region
	best, cost := "", math.Inf(1)
	for via, ms := range r.rtt {
		vr := r.cfg.Directory[via].Region
		if vr == toRegion || vr == r.cfg.Self.Region || !r.upLocked(via) {
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
		via, cost := r.bestRelayLocked(to)
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

// Send delivers an internal request for topic (with residency res) to broker to, relaying
// through a third region when that is faster. Resident data always goes direct.
func (r *Router) Send(ctx context.Context, to, path, topic, res string, in, out any) error {
	if res != api.ResidencyGlobal || r.cfg.Directory[to].Region == r.cfg.Self.Region {
		return r.cfg.RPC.Call(ctx, to, path, in, out)
	}
	r.mu.RLock()
	via := r.routes[to]
	r.mu.RUnlock()
	if via == "" {
		return r.cfg.RPC.Call(ctx, to, path, in, out)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	var raw json.RawMessage
	err = r.cfg.RPC.Call(ctx, via, PathRelay, RelayRequest{To: to, Path: path, Topic: topic, Residency: res, Body: body}, &raw)
	var ae *api.Error
	if err != nil && !errors.As(err, &ae) && ctx.Err() == nil {
		return r.cfg.RPC.Call(ctx, to, path, in, out) // relay unreachable: try direct
	}
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func (r *Router) handleRelay(ctx context.Context, req *RelayRequest) (*json.RawMessage, error) {
	if err := r.cfg.Guard.CheckPeer(req.Topic, req.Residency, r.cfg.Self.ID, "relay data through"); err != nil {
		return nil, err
	}
	if err := r.cfg.Guard.CheckPeer(req.Topic, req.Residency, req.To, "relay to"); err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := r.cfg.RPC.Call(ctx, req.To, req.Path, req.Body, &raw); err != nil {
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

// GlobalTopics lists global topics known to other (reachable) regions.
func (r *Router) GlobalTopics() []TopicSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]TopicSummary{}
	for id, ts := range r.topics {
		if !r.upLocked(id) {
			continue
		}
		for _, t := range ts {
			if t.Residency == api.ResidencyGlobal {
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

// Package residency enforces jurisdiction rules at every point where message data could be
// stored on, or pass through, a broker: replica creation, replication in both directions,
// request forwarding and relaying, and replica placement. Rules come from the policy engine
// (built-in region/global rules or signed policy classes); every decision is recorded.
package residency

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/trail"
)

// Allowed reports whether data with a built-in residency may be stored in region. Only for
// callers without a policy engine (e.g. tests); brokers use Guard.
func Allowed(residency, region string) bool {
	return residency == api.ResidencyGlobal || (residency != "" && residency == region)
}

const (
	Store   = "store"   // the broker persists the data
	Transit = "transit" // the data only passes through the broker's memory
)

// Check describes one decision.
type Check struct {
	Topic     string
	Partition int
	Residency string            // the topic's residency string
	Narrow    *policy.Effective // stricter per-message rule, if any
	Hop       string            // entry | forward | relay | replicate-send | replicate-accept | store | proxy
	Peer      string            // broker the data goes to / comes from ("" = this broker)
	Need      string            // Store or Transit
	Batch     string
	Records   int
	Quiet     bool // don't log allow decisions (hot paths); denies are always logged
}

// Guard checks rules for one broker. RegionOf maps any broker id to its configured region
// ("" for unknown brokers, which are never allowed anything but public data).
type Guard struct {
	self     string
	region   string
	regionOf func(id string) string
	engine   *policy.Engine
	trail    *trail.Log
	rejected atomic.Int64
}

func NewGuard(self, region string, regionOf func(string) string, engine *policy.Engine, log *trail.Log) *Guard {
	return &Guard{self: self, region: region, regionOf: regionOf, engine: engine, trail: log}
}

func (g *Guard) Region() string            { return g.region }
func (g *Guard) RegionOf(id string) string { return g.regionOf(id) }
func (g *Guard) Rejected() int64           { return g.rejected.Load() }
func (g *Guard) Engine() *policy.Engine    { return g.engine }
func (g *Guard) Trail() *trail.Log         { return g.trail }

// Policy resolves a residency string (fail closed if it can't be resolved).
func (g *Guard) Policy(res string) (policy.Effective, error) { return g.engine.Resolve(res) }

// Do evaluates c and records the decision.
func (g *Guard) Do(c Check) error {
	start := time.Now()
	peer, region := c.Peer, g.region
	if peer == "" {
		peer = g.self
	} else {
		region = g.regionOf(peer)
	}
	eff, err := g.engine.Resolve(c.Residency)
	if err == nil && c.Narrow != nil {
		eff = eff.Narrow(*c.Narrow)
	}
	reason := ""
	switch {
	case err != nil:
		reason = "policy_unavailable"
	case c.Need == Transit && !eff.CanTransit(region):
		reason = "transit_not_permitted"
	case c.Need != Transit && !eff.CanStore(region):
		reason = "outside_storage_region"
	}
	d := trail.Decision{Broker: g.self, Topic: c.Topic, Partition: c.Partition, Batch: c.Batch, Records: c.Records,
		Hop: c.Hop, Peer: peer, PeerRegion: region, Policy: eff.Name, PolicyVersion: eff.Version,
		Decision: trail.Allow, Reason: "permitted", CheckNanos: time.Since(start).Nanoseconds()}
	if reason == "" {
		if !c.Quiet {
			g.trail.Record(d)
		}
		return nil
	}
	if d.Policy == "" {
		d.Policy = c.Residency
	}
	d.Decision, d.Reason = trail.Deny, reason
	g.trail.Record(d)
	g.rejected.Add(1)
	slog.Warn("jurisdiction violation blocked", "hop", c.Hop, "topic", c.Topic, "residency", c.Residency,
		"broker", peer, "broker_region", region, "reason", reason)
	if err != nil {
		if ae, ok := err.(*api.Error); ok {
			return ae
		}
		return policy.Unavailable("%v", err)
	}
	verb := "be stored"
	if c.Need == Transit {
		verb = "pass through"
	}
	e := api.Errorf(api.CodeResidencyViolation, "%s refused (%s): topic %q (%s) may not %s on broker %s in region %q",
		c.Hop, reason, c.Topic, eff.Name, verb, peer, region)
	e.Reason = reason
	return e
}

// CheckStore: may this broker store data of topic?
func (g *Guard) CheckStore(topic, res string) error {
	return g.Do(Check{Topic: topic, Partition: -1, Residency: res, Hop: "store", Need: Store, Quiet: true})
}

// CheckPeer: may broker peer store data of topic? action names the hop for logs.
func (g *Guard) CheckPeer(topic, res, peer, action string) error {
	return g.Do(Check{Topic: topic, Partition: -1, Residency: res, Hop: action, Peer: peer, Need: Store, Quiet: true})
}

// CheckTransit: may data of topic pass through this broker?
func (g *Guard) CheckTransit(topic, res, action string) error {
	return g.Do(Check{Topic: topic, Partition: -1, Residency: res, Hop: action, Need: Transit, Quiet: true})
}

// CheckPeerTransit: may data of topic pass through broker peer?
func (g *Guard) CheckPeerTransit(topic, res, peer, action string) error {
	return g.Do(Check{Topic: topic, Partition: -1, Residency: res, Hop: action, Peer: peer, Need: Transit, Quiet: true})
}

// Package residency enforces data-residency rules at every point where message data could be
// stored on, or sent to, a broker: replica creation, replication in both directions, request
// forwarding and replica placement. A resident topic's data may only touch brokers in its region.
package residency

import (
	"log/slog"
	"sync/atomic"

	"github.com/notaditya28/hydra-stream/internal/api"
)

// Allowed reports whether data of a topic with this residency may live in (or pass through) region.
func Allowed(residency, region string) bool {
	return residency == api.ResidencyGlobal || (residency != "" && residency == region)
}

// Guard checks residency for one broker. RegionOf maps any broker id in the cluster to its
// configured region ("" for unknown brokers, which never receive resident data).
type Guard struct {
	self     string
	region   string
	regionOf func(id string) string
	rejected atomic.Int64
}

func NewGuard(self, region string, regionOf func(string) string) *Guard {
	return &Guard{self: self, region: region, regionOf: regionOf}
}

func (g *Guard) Region() string            { return g.region }
func (g *Guard) RegionOf(id string) string { return g.regionOf(id) }
func (g *Guard) Rejected() int64           { return g.rejected.Load() }

// CheckStore: may this broker store data of topic?
func (g *Guard) CheckStore(topic, residency string) error {
	if Allowed(residency, g.region) {
		return nil
	}
	return g.reject("store data on", topic, residency, g.self, g.region)
}

// CheckPeer: may data of topic be sent to, or accepted from, broker peer? action names the
// direction for logs ("replicate to", "accept from", "forward to").
func (g *Guard) CheckPeer(topic, residency, peer, action string) error {
	r := g.regionOf(peer)
	if Allowed(residency, r) {
		return nil
	}
	return g.reject(action, topic, residency, peer, r)
}

func (g *Guard) reject(action, topic, residency, broker, region string) error {
	g.rejected.Add(1)
	slog.Warn("residency violation blocked", "action", action, "topic", topic, "residency", residency,
		"broker", broker, "broker_region", region)
	e := api.Errorf(api.CodeResidencyViolation, "refusing to %s %s: topic %q is resident in %s, broker %s is in %q",
		action, broker, topic, residency, broker, region)
	e.Reason = "outside_residency_region"
	return e
}

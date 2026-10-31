// Package route describes how a broker-to-broker message may travel: which data it carries
// (so the router only picks paths the data's policy permits), which traffic class it belongs
// to (so the bandwidth controller can prioritise it), and any relay a producer asked for.
package route

import (
	"context"

	"github.com/notaditya28/hydra-stream/internal/policy"
)

// Class is a traffic class for bandwidth scheduling on inter-region links.
type Class int

const (
	Control  Class = iota // votes, heartbeats, metadata: tiny and latency-critical
	Urgent                // data of topics marked urgent
	Live                  // replication of fresh data (follower nearly caught up)
	Recovery              // catch-up of a follower that fell behind (e.g. after an outage)
	NumClasses
)

func (c Class) String() string {
	return [...]string{"control", "urgent", "live", "recovery"}[c]
}

// Opts describes a send.
type Opts struct {
	Topic     string
	Residency string            // the topic's residency string (region | global | policy:<class>)
	Narrow    *policy.Effective // stricter rule for the payload (per-message classes), if any
	Classes   []string          // the per-message classes behind Narrow (for relays to re-check)
	Class     Class
	Via       string // producer-requested relay region ("" = router decides); must be permitted
}

// Sender delivers an internal request to another broker.
type Sender interface {
	Send(ctx context.Context, to, path string, o Opts, in, out any) error
}

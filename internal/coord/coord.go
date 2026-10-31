// Package coord elects a region's controller and replicates the region's metadata.
//
// The broker only talks to the Coordinator interface, so the lease-based implementation here
// could be replaced by a Raft library (whose log would hold the metadata) without touching
// broker code.
package coord

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/notaditya28/hydra-stream/internal/meta"
)

var ErrNotController = errors.New("coord: not the controller")

type Coordinator interface {
	// Run drives elections and lease renewal until ctx is done.
	Run(ctx context.Context)
	// IsController reports whether this broker holds a valid controller lease.
	IsController() bool
	// Controller is the controller this broker last heard from (or itself) and its epoch.
	Controller() (id string, epoch uint64)
	// Synced reports whether this broker's committed metadata has caught up with a live
	// controller since it started. Brokers don't lead partitions until synced.
	Synced() bool
	// Committed returns the latest committed metadata (never nil). Callers must not modify it.
	Committed() *meta.Metadata
	// Propose applies mutate to a copy of the latest metadata and commits it to a majority.
	// Only the controller can propose.
	Propose(ctx context.Context, mutate func(*meta.Metadata) error) (*meta.Metadata, error)
	// Live returns brokers the controller heard from recently (controller only; includes itself).
	Live() map[string]bool
	// Stuck returns partitions brokers reported as fenced past their assigned leader epoch.
	Stuck() []meta.PartKey
	// Register adds the coordinator's internal RPC routes.
	Register(mux *http.ServeMux)
}

type Timing struct {
	LeaseDuration time.Duration // a granted lease lasts this long
	RenewInterval time.Duration // controller renews (and heartbeats) this often
	FailTimeout   time.Duration // a peer silent for this long is considered down
	RPCTimeout    time.Duration
}

func DefaultTiming() Timing {
	return Timing{
		LeaseDuration: 1000 * time.Millisecond,
		RenewInterval: 150 * time.Millisecond,
		FailTimeout:   500 * time.Millisecond,
		RPCTimeout:    300 * time.Millisecond,
	}
}

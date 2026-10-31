// Package api defines the broker's protocol-independent service interface and wire types.
// Transports (HTTP today, possibly gRPC later) adapt this interface; clients speak these types.
package api

import (
	"context"
	"time"
)

// Service is what a broker exposes to clients, independent of transport.
type Service interface {
	Info() Info
	CreateTopic(ctx context.Context, cfg TopicConfig) (TopicInfo, error)
	Topics(ctx context.Context) ([]TopicInfo, error)
	Topic(ctx context.Context, name string) (TopicInfo, error)
	Produce(ctx context.Context, req ProduceRequest) (ProduceResponse, error)
	Fetch(ctx context.Context, req FetchRequest) (FetchResponse, error)
}

const ResidencyGlobal = "global"

const (
	MaxProduceRecords = 100_000
	MaxFetchRecords   = 10_000
	MaxFetchWait      = 30 * time.Second
)

type Info struct {
	ID     string `json:"id"`
	Region string `json:"region"`
	Zone   string `json:"zone"`
}

type TopicConfig struct {
	Name       string `json:"name"`
	Partitions int    `json:"partitions"`
	Residency  string `json:"residency"`          // "global", a region name such as "IN", or "policy:<class>"
	Priority   string `json:"priority,omitempty"` // "urgent": replication gets the urgent bandwidth class
}

type PartitionInfo struct {
	Partition     int      `json:"partition"`
	Leader        string   `json:"leader"`
	LeaderEpoch   uint64   `json:"leader_epoch"`
	Replicas      []string `json:"replicas"`
	LogStart      uint64   `json:"log_start"`
	HighWatermark uint64   `json:"high_watermark"` // as seen by this broker's replica
}

type TopicInfo struct {
	TopicConfig
	PartitionStates []PartitionInfo `json:"partition_states"`
}

type ProduceRecord struct {
	Partition *int   `json:"partition,omitempty"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

type ProduceRequest struct {
	Topic   string          `json:"-"`
	Records []ProduceRecord `json:"records"`
	// Idempotent produce: all records go to one explicit partition; the batch's records get
	// sequence numbers BaseSeq, BaseSeq+1, ... A retry must resend the same batch with the same
	// numbers; the broker then returns the original offsets instead of writing it twice.
	ProducerID uint64 `json:"producer_id,omitempty"`
	BaseSeq    uint64 `json:"base_seq,omitempty"`

	// Class tags these messages with a stricter policy class. It can only NARROW the topic's
	// policy (where they may be stored / pass through), never widen it.
	Class string `json:"class,omitempty"`
	// Acks is the durability required before the broker acknowledges:
	//   "leader" - appended on the leader (fastest; lost if the leader dies before replicating)
	//   "quorum" - on a majority of replicas (default)
	//   "all"    - on every replica (for cross-region data: in every allowed region)
	// A policy's min_acks raises it; a producer can't go below the policy floor.
	Acks string `json:"acks,omitempty"`
	// Via asks for cross-region forwarding through a specific region. It is honoured only if
	// the data's policy permits transit there; otherwise the request is refused.
	Via string `json:"via,omitempty"`
}

type ProduceResponse struct {
	Offsets []RecordOffset `json:"offsets"`          // aligned with the request's records
	Acks    string         `json:"acks,omitempty"`   // durability actually applied
	Policy  string         `json:"policy,omitempty"` // policy (and version) the data was checked against
}

type RecordOffset struct {
	Partition int    `json:"partition"`
	Offset    uint64 `json:"offset"`
}

type FetchRequest struct {
	Topic      string
	Partition  int
	Offset     uint64
	MaxRecords int
	MaxBytes   int
	Wait       time.Duration // long-poll: wait up to this long if no records are available
}

type FetchedRecord struct {
	Offset    uint64 `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	Key       string `json:"key,omitempty"`
	Value     string `json:"value"`
}

type FetchResponse struct {
	Topic         string          `json:"topic"`
	Partition     int             `json:"partition"`
	Records       []FetchedRecord `json:"records"`
	NextOffset    uint64          `json:"next_offset"`
	HighWatermark uint64          `json:"high_watermark"`
	LogStart      uint64          `json:"log_start"`
}

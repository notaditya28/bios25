// Package meta holds a region's replicated cluster metadata: topics and partition assignments.
package meta

import (
	"fmt"

	"github.com/notaditya28/hydra-stream/internal/api"
)

type PartKey struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

func (k PartKey) String() string { return fmt.Sprintf("%s/%d", k.Topic, k.Partition) }

type PartitionMeta struct {
	Replicas    []string `json:"replicas"`
	Leader      string   `json:"leader"` // "" = offline
	LeaderEpoch uint64   `json:"leader_epoch"`
}

type TopicMeta struct {
	Config     api.TopicConfig `json:"config"`
	Partitions []PartitionMeta `json:"partitions"`
}

// Metadata is versioned; (Epoch, Version) orders versions written by different controllers.
type Metadata struct {
	Epoch   uint64                `json:"epoch"` // controller epoch that wrote this version
	Version uint64                `json:"version"`
	Topics  map[string]*TopicMeta `json:"topics"`
}

func Empty() *Metadata { return &Metadata{Topics: map[string]*TopicMeta{}} }

// Newer reports whether a is a strictly later version than b. nil is older than everything.
func Newer(a, b *Metadata) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	if a.Epoch != b.Epoch {
		return a.Epoch > b.Epoch
	}
	return a.Version > b.Version
}

func (m *Metadata) Clone() *Metadata {
	c := &Metadata{Epoch: m.Epoch, Version: m.Version, Topics: make(map[string]*TopicMeta, len(m.Topics))}
	for name, t := range m.Topics {
		tc := &TopicMeta{Config: t.Config, Partitions: make([]PartitionMeta, len(t.Partitions))}
		for i, p := range t.Partitions {
			tc.Partitions[i] = PartitionMeta{Replicas: append([]string(nil), p.Replicas...), Leader: p.Leader, LeaderEpoch: p.LeaderEpoch}
		}
		c.Topics[name] = tc
	}
	return c
}

// Partition returns the assignment for key, or nil.
func (m *Metadata) Partition(key PartKey) *PartitionMeta {
	t, ok := m.Topics[key.Topic]
	if !ok || key.Partition < 0 || key.Partition >= len(t.Partitions) {
		return nil
	}
	return &t.Partitions[key.Partition]
}

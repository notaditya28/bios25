// Package placement decides which brokers hold a partition's replicas. The functions are pure
// (cluster directory + policy in, broker ids out) so that brokers and the scenario simulator
// compute exactly the same placement without coordinating.
package placement

import (
	"hash/fnv"
	"sort"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

const ReplicationFactor = 3

// Regions lists the regions in the directory, sorted.
func Regions(dir map[string]api.Info) []string {
	seen := map[string]bool{}
	var out []string
	for _, info := range dir {
		if !seen[info.Region] {
			seen[info.Region] = true
			out = append(out, info.Region)
		}
	}
	sort.Strings(out)
	return out
}

// RegionBrokers lists the brokers configured in region, sorted.
func RegionBrokers(dir map[string]api.Info, region string) []string {
	var out []string
	for id, info := range dir {
		if info.Region == region {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// StorageRegions lists the regions a rule allows storage in.
func StorageRegions(dir map[string]api.Info, eff policy.Effective) []string {
	return eff.StorageRegions(Regions(dir))
}

// CrossRegion reports whether data under eff is replicated across regions (it may be stored in
// at least two). Such partitions elect their own leaders; single-region ones are led by their
// region's controller.
func CrossRegion(dir map[string]api.Info, eff policy.Effective) bool {
	return len(StorageRegions(dir, eff)) >= 2
}

// CrossRegionReplicas places a cross-region partition: one replica per allowed region (rotated
// by topic hash and partition so leaders spread), topped up to the replication factor from the
// allowed regions when fewer than three regions are allowed.
func CrossRegionReplicas(dir map[string]api.Info, eff policy.Effective, topic string, p int) []string {
	h := fnv.New32a()
	h.Write([]byte(topic))
	start := int(h.Sum32()%1024) + p
	regions := StorageRegions(dir, eff)
	var out []string
	used := map[string]bool{}
	for round := 0; len(out) < ReplicationFactor && round < ReplicationFactor; round++ {
		for _, region := range regions {
			if len(out) >= ReplicationFactor && round > 0 {
				break
			}
			ids := RegionBrokers(dir, region)
			if round >= len(ids) {
				continue
			}
			id := ids[(start+round)%len(ids)]
			if !used[id] {
				used[id] = true
				out = append(out, id)
			}
		}
	}
	return out
}

// InRegionReplicas places a single-region partition on the eligible brokers of a coordination
// group, rotated so preferred leaders spread across zones.
func InRegionReplicas(eligible []string, p int) []string {
	out := make([]string, len(eligible))
	for i := range eligible {
		out[i] = eligible[(p+i)%len(eligible)]
	}
	return out
}

// Quorum is the number of replicas that must acknowledge a write.
func Quorum(replicas int) int { return replicas/2 + 1 }

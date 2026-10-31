package broker

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/sim"
)

// TestSimulatorMatchesCluster checks the scenario simulator against a real cluster: predicted
// placement must equal actual placement, and after killing a region, every (partition, client
// region) pair the simulator calls writable must accept writes and every other pair must refuse.
func TestSimulatorMatchesCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-region cluster")
	}
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.installPolicies(testPolicies...)
	topics := []api.TopicConfig{
		{Name: "in-orders", Partitions: 1, Residency: "IN"},
		{Name: "prices", Partitions: 3, Residency: api.ResidencyGlobal},
		{Name: "fin", Partitions: 2, Residency: "policy:financial"},
		{Name: "eup", Partitions: 1, Residency: "policy:eu-personal"},
		{Name: "pub", Partitions: 2, Residency: "policy:public"},
	}
	home := map[string]int{"in-orders": c.byRegion("IN")[0], "prices": c.byRegion("EU")[0], "fin": c.byRegion("EU")[0],
		"eup": c.byRegion("EU")[0], "pub": c.byRegion("EU")[0]}
	for _, tc := range topics {
		if err := c.createAt(home[tc.Name], tc); err != nil {
			t.Fatalf("create %s: %v", tc.Name, err)
		}
	}
	m := sim.Model{Dir: c.dir, Policy: policy.Document{Version: 1, Policies: testPolicies}, Topics: topics}

	// 1. Placement: the simulator must place every replica exactly where the brokers did.
	base, err := sim.Run(m, sim.Scenario{Name: "now"})
	if err != nil {
		t.Fatal(err)
	}
	for _, po := range base.Partitions {
		var got []string
		waitFor(t, 10*time.Second, "topic metadata "+po.Topic, func() bool {
			info, err := c.client(home[po.Topic]).Topic(bg, po.Topic)
			if err != nil || len(info.PartitionStates) <= po.Partition {
				return false
			}
			got = info.PartitionStates[po.Partition].Replicas
			return true
		})
		a, b := slices.Clone(got), slices.Clone(po.Replicas)
		slices.Sort(a)
		slices.Sort(b)
		if !slices.Equal(a, b) {
			t.Fatalf("%s/%d: simulator placed %v, cluster placed %v", po.Topic, po.Partition, po.Replicas, got)
		}
	}
	// Everything writable from its permitted regions before the fault.
	for _, po := range base.Partitions {
		for _, r := range po.WritableFrom {
			if !writable(c, r, po.Topic, po.Partition, 15*time.Second) {
				t.Fatalf("before the fault: %s/%d not writable from %s", po.Topic, po.Partition, r)
			}
		}
	}

	// 2. Availability under a regional outage.
	pred, err := sim.Run(m, sim.Scenario{Name: "IN down", Steps: []sim.Step{{Kind: "region_down", Region: "IN"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range c.byRegion("IN") {
		c.kill(i)
	}
	agree, total := 0, 0
	for _, po := range pred.Partitions {
		for _, r := range []string{"EU", "US"} {
			want := slices.Contains(po.WritableFrom, r)
			timeout := 2 * time.Second // a "no" must hold for a while
			if want {
				timeout = 15 * time.Second // a "yes" may need a leader election first
			}
			got := writable(c, r, po.Topic, po.Partition, timeout)
			total++
			if got == want {
				agree++
			} else {
				t.Errorf("%s/%d from %s: simulator said writable=%v, cluster said %v (%s)", po.Topic, po.Partition, r, want, got, po.Reason)
			}
		}
	}
	t.Logf("simulator vs cluster after killing IN: %d/%d (partition, client region) outcomes agree", agree, total)
}

// writable reports whether a client in region can get one record acknowledged within timeout.
func writable(c *cluster, region, topic string, part int, timeout time.Duration) bool {
	ok := false
	deadline := time.Now().Add(timeout)
	for !ok && time.Now().Before(deadline) {
		for _, i := range c.byRegion(region) {
			if !c.nodes[i].up {
				continue
			}
			p := part
			_, err := produceOne(c.client(i), topic, api.ProduceRequest{Records: []api.ProduceRecord{{Partition: &p, Value: fmt.Sprint(time.Now().UnixNano())}}})
			if err == nil {
				ok = true
				break
			}
			if c := api.CodeOf(err); c == api.CodeResidencyViolation || c == api.CodeWrongRegion {
				return false // definitive refusal, not a transient one
			}
		}
		if !ok {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return ok
}

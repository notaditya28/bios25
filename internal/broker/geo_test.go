package broker

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/replica"
)

var regions3 = []string{"IN", "EU", "US"}

// isolateRegion cuts every link between region and the rest of the cluster (both directions).
func (c *cluster) isolateRegion(region string) {
	for _, a := range c.nodes {
		for _, b := range c.nodes {
			if (c.dir[a.id].Region == region) != (c.dir[b.id].Region == region) {
				a.net.Block(b.id)
			}
		}
	}
}

// slowLink adds d of one-way delay on every link between regions ra and rb.
func (c *cluster) slowLink(ra, rb string, d time.Duration) {
	for _, a := range c.nodes {
		delays := map[string]time.Duration{}
		for _, b := range c.nodes {
			x, y := c.dir[a.id].Region, c.dir[b.id].Region
			if (x == ra && y == rb) || (x == rb && y == ra) {
				delays[b.id] = d
			}
		}
		a.net.SetDelays(delays)
	}
}

func (c *cluster) regionOf(i int) string { return c.dir[c.nodes[i].id].Region }

func (c *cluster) clients(idx ...int) []*client.Client {
	var out []*client.Client
	for _, i := range idx {
		out = append(out, c.client(i))
	}
	return out
}

func (c *cluster) createIn(region string, cfg api.TopicConfig) {
	c.t.Helper()
	i := c.byRegion(region)[0]
	var err error
	start := time.Now()
	for {
		if _, err = c.client(i).CreateTopic(bg, cfg); err == nil {
			return
		}
		if time.Since(start) > 10*time.Second {
			c.t.Fatalf("create %s in %s: %v", cfg.Name, region, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) waitControllers() {
	c.t.Helper()
	waitFor(c.t, 10*time.Second, "a controller in every region", func() bool {
		for _, r := range regions3 {
			ok := false
			for _, i := range c.byRegion(r) {
				if c.nodes[i].up && c.nodes[i].b.coord.IsController() {
					ok = true
				}
			}
			if !ok {
				return false
			}
		}
		return true
	})
}

// globalLeader waits for a leader of a global partition, optionally outside a region.
func (c *cluster) globalLeader(topic string, part int, notIn string) int {
	c.t.Helper()
	idx := -1
	waitFor(c.t, 10*time.Second, fmt.Sprintf("leader of %s/%d", topic, part), func() bool {
		for i, nd := range c.nodes {
			if !nd.up || c.regionOf(i) == notIn {
				continue
			}
			if p := nd.b.parts.Get(meta.PartKey{Topic: topic, Partition: part}); p != nil && p.Status().Role == replica.RoleLeader {
				idx = i
				return true
			}
		}
		return false
	})
	return idx
}

func TestGlobalTopicSpansRegions(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.createIn("IN", api.TopicConfig{Name: "prices", Partitions: 3, Residency: "global"})
	c.createIn("IN", api.TopicConfig{Name: "aadhaar", Partitions: 1, Residency: "IN"})

	// Every region knows the global topic; each partition has one replica per region.
	eu, us := c.byRegion("EU")[0], c.byRegion("US")[0]
	// Creation succeeds at a majority of regions; a region that was busy (e.g. changing its
	// controller) gets the topic from the background sync shortly after.
	var info api.TopicInfo
	var err error
	waitFor(t, 5*time.Second, "US to learn the global topic", func() bool {
		info, err = c.client(us).Topic(bg, "prices")
		return err == nil
	})
	for _, p := range info.PartitionStates {
		var rs []string
		for _, r := range p.Replicas {
			rs = append(rs, c.dir[r].Region)
		}
		sort.Strings(rs)
		if strings.Join(rs, ",") != "EU,IN,US" {
			t.Fatalf("partition %d replicas %v in regions %v", p.Partition, p.Replicas, rs)
		}
	}

	// Produce from EU, consume from US: requests route to wherever each leader is.
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	for part := 0; part < 3; part++ {
		vals := []string{fmt.Sprintf("p%d-a", part), fmt.Sprintf("p%d-b", part)}
		if _, err := produceRetry(ctx, c.clients(eu), "prices", part, vals); err != nil {
			t.Fatal(err)
		}
	}
	for part := 0; part < 3; part++ {
		var got []string
		for _, r := range consumeAll(t, c.client(us), "prices", part) {
			got = append(got, r.Value)
		}
		if want := fmt.Sprintf("p%d-a,p%d-b", part, part); strings.Join(got, ",") != want {
			t.Fatalf("partition %d: got %v want %s", part, got, want)
		}
		c.waitConverged("prices", part)
	}

	// A resident topic is never served from another region: EU sends the client home.
	waitFor(t, 5*time.Second, "topic gossip", func() bool {
		_, err := c.client(eu).Fetch(bg, api.FetchRequest{Topic: "aadhaar"})
		return api.CodeOf(err) == api.CodeWrongRegion
	})
	_, err = c.client(eu).Fetch(bg, api.FetchRequest{Topic: "aadhaar"})
	t.Logf("EU answer for IN topic: %v", err)
}

func TestRegionCrashDoesNotFreezeOthers(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.createIn("IN", api.TopicConfig{Name: "prices", Partitions: 1, Residency: "global"})
	for _, r := range regions3 {
		c.createIn(r, api.TopicConfig{Name: strings.ToLower(r) + "-orders", Partitions: 1, Residency: r})
	}
	dead := c.regionOf(c.globalLeader("prices", 0, ""))
	var alive []string
	for _, r := range regions3 {
		if r != dead {
			alive = append(alive, r)
		}
	}
	t.Logf("killing region %s, which leads the global partition", dead)

	ctx, cancel := context.WithTimeout(bg, 4*time.Second)
	defer cancel()
	type result struct {
		name  string
		acks  []ack
		stall time.Duration
	}
	results := make(chan result, 3)
	run := func(name, topic string, idx []int) {
		acks, stall := load(ctx, c.clients(idx...), topic, 0, 20)
		results <- result{name, acks, stall}
	}
	survivors := append(c.byRegion(alive[0]), c.byRegion(alive[1])...)
	go run(alive[0]+" resident", strings.ToLower(alive[0])+"-orders", c.byRegion(alive[0]))
	go run(alive[1]+" resident", strings.ToLower(alive[1])+"-orders", c.byRegion(alive[1]))
	go run("global", "prices", survivors)

	time.Sleep(1 * time.Second)
	killedAt := time.Now()
	for _, i := range c.byRegion(dead) {
		c.kill(i)
	}
	newLeader := c.globalLeader("prices", 0, dead)
	t.Logf("global partition re-elected on %s after %v", c.nodes[newLeader].id, time.Since(killedAt).Round(time.Millisecond))

	byName := map[string]result{}
	for range 3 {
		r := <-results
		byName[r.name] = r
		t.Logf("%-12s acked=%d max stall=%v", r.name, len(r.acks), r.stall.Round(time.Millisecond))
	}
	for _, r := range alive {
		if s := byName[r+" resident"].stall; s > time.Second {
			t.Errorf("%s resident traffic stalled %v while %s died", r, s, dead)
		}
	}
	got := consumeAll(t, c.client(newLeader), "prices", 0)
	verifyAcked(t, byName["global"].acks, got)

	// Clients asking a surviving region for the dead region's resident data get a fast, explicit no.
	waitFor(t, 5*time.Second, "dead region noticed", func() bool { return !c.nodes[survivors[0]].b.geo.RegionReachable(dead) })
	start := time.Now()
	_, err := c.client(survivors[0]).Produce(bg, strings.ToLower(dead)+"-orders", []api.ProduceRecord{{Value: "x"}})
	if api.CodeOf(err) != api.CodeRegionUnavailable || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("produce to dead region's topic: %v after %v", err, time.Since(start))
	}
	t.Logf("dead region's resident topic: %v (in %v)", err, time.Since(start).Round(time.Microsecond))
}

func TestIsolatedRegionIsFencedForGlobalButServesResident(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.createIn("IN", api.TopicConfig{Name: "prices", Partitions: 1, Residency: "global"})
	for _, r := range regions3 {
		c.createIn(r, api.TopicConfig{Name: strings.ToLower(r) + "-orders", Partitions: 1, Residency: r})
	}
	oldLeader := c.globalLeader("prices", 0, "")
	iso := c.regionOf(oldLeader)
	isoNodes := c.byRegion(iso)
	var others []int
	for i := range c.nodes {
		if c.regionOf(i) != iso {
			others = append(others, i)
		}
	}
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	if _, err := produceRetry(ctx, c.clients(others...), "prices", 0, []string{"before"}); err != nil {
		t.Fatal(err)
	}
	oldEpoch := c.nodes[oldLeader].b.parts.Get(meta.PartKey{Topic: "prices", Partition: 0}).Status().LeaderEpoch

	c.isolateRegion(iso)
	t.Logf("isolated region %s (it led the global partition at epoch %d)", iso, oldEpoch)

	// Inside the isolated region: resident traffic keeps flowing...
	start := time.Now()
	p0 := 0
	if _, err := c.client(isoNodes[0]).Produce(bg, strings.ToLower(iso)+"-orders", []api.ProduceRecord{{Partition: &p0, Value: "local"}}); err != nil {
		t.Fatalf("resident write inside isolated region: %v", err)
	}
	t.Logf("resident write inside isolated region took %v", time.Since(start).Round(time.Millisecond))
	// ...but the global partition can't commit there; after check-quorum it fails fast.
	_, err := c.client(oldLeader).Produce(bg, "prices", []api.ProduceRecord{{Partition: &p0, Value: "stale"}})
	if err == nil {
		t.Fatal("isolated minority acknowledged a global write")
	}
	time.Sleep(500 * time.Millisecond)
	start = time.Now()
	_, err = c.client(isoNodes[1]).Produce(bg, "prices", []api.ProduceRecord{{Partition: &p0, Value: "stale-2"}})
	if api.CodeOf(err) != api.CodeNotEnoughReplicas || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("global write in isolated region: %v after %v (want fast NOT_ENOUGH_REPLICAS)", err, time.Since(start))
	}
	t.Logf("global write inside isolated region: %v (in %v)", err, time.Since(start).Round(time.Microsecond))

	// The majority elects a new leader at a higher epoch and keeps writing.
	newLeader := c.globalLeader("prices", 0, iso)
	if _, err := produceRetry(ctx, c.clients(others...), "prices", 0, []string{"after"}); err != nil {
		t.Fatal(err)
	}
	t.Logf("majority leader %s; epoch %d -> %d", c.nodes[newLeader].id, oldEpoch,
		c.nodes[newLeader].b.parts.Get(meta.PartKey{Topic: "prices", Partition: 0}).Status().LeaderEpoch)

	// Heal: the isolated replica rejoins without manual steps and drops its unacknowledged tail.
	c.heal()
	c.waitConverged("prices", 0)
	var vals []string
	for _, r := range consumeAll(t, c.client(isoNodes[0]), "prices", 0) {
		vals = append(vals, r.Value)
	}
	if got := strings.Join(vals, ","); got != "before,after" {
		t.Fatalf("global log after heal = %s", got)
	}
	if l := c.globalLeader("prices", 0, ""); l != newLeader {
		t.Errorf("leadership moved back to %s after heal (pre-vote should prevent disruption)", c.nodes[l].id)
	}
}

func TestGlobalTrafficReroutesAroundSlowLink(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.createIn("IN", api.TopicConfig{Name: "prices", Partitions: 1, Residency: "global"})
	leader := c.globalLeader("prices", 0, "")
	ra := c.regionOf(leader)
	// rb: a region whose replica follows the leader; we fetch through it.
	pm := c.nodes[leader].b.coord.Committed().Topics["prices"].Partitions[0]
	var follower int
	for _, r := range pm.Replicas {
		if i := c.index(r); c.regionOf(i) != ra {
			follower = i
			break
		}
	}
	rb := c.regionOf(follower)
	var rc string
	for _, r := range regions3 {
		if r != ra && r != rb {
			rc = r
		}
	}
	ctx, cancel := context.WithTimeout(bg, 10*time.Second)
	defer cancel()
	if _, err := produceRetry(ctx, c.clients(follower), "prices", 0, []string{"v"}); err != nil {
		t.Fatal(err)
	}
	fetchMs := func() time.Duration {
		var samples []time.Duration
		for range 7 {
			start := time.Now()
			if _, err := c.client(follower).Fetch(bg, api.FetchRequest{Topic: "prices", Offset: 0}); err != nil {
				t.Fatal(err)
			}
			samples = append(samples, time.Since(start))
		}
		slices.Sort(samples)
		return samples[len(samples)/2]
	}
	base := fetchMs()

	const delay = 60 * time.Millisecond // one way: +120 ms per round trip on the direct link
	c.slowLink(ra, rb, delay)
	geoB := c.nodes[follower].b.geo
	leaderID := c.nodes[leader].id
	took := waitFor(t, 10*time.Second, "reroute via "+rc, func() bool {
		via := geoB.Route(leaderID)
		return via != "" && c.dir[via].Region == rc
	})
	direct := 0.0
	waitFor(t, 10*time.Second, "direct RTT estimate to converge", func() bool {
		for _, l := range geoB.Links() {
			if l.To == leaderID {
				direct = l.RTTms
			}
		}
		return direct > 1.6*float64(delay.Milliseconds())
	})
	rerouted := fetchMs()
	t.Logf("%s<->%s slowed by %v each way: route via %s after %v; fetch p50 %v before, %v after reroute; direct RTT measured %.0f ms",
		rb, ra, delay, geoB.Route(leaderID), took.Round(time.Millisecond), base.Round(time.Millisecond), rerouted.Round(time.Millisecond), direct)
	if float64(rerouted.Milliseconds()) > direct/2 {
		t.Fatalf("rerouted fetch %v is not clearly faster than the direct path (%.0f ms)", rerouted, direct)
	}
	var relayed int64
	for _, i := range c.byRegion(rc) {
		relayed += c.nodes[i].b.geo.Relayed()
	}
	if relayed == 0 {
		t.Fatalf("no request was relayed through %s", rc)
	}

	// Heal the link: traffic goes back to the direct path.
	c.slowLink(ra, rb, 0)
	waitFor(t, 10*time.Second, "direct route again", func() bool { return geoB.Route(leaderID) == "" })
}

package broker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/coord"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

var bg = context.Background()

func FastTiming() (coord.Timing, replica.Timing) {
	return coord.Timing{LeaseDuration: 400 * time.Millisecond, RenewInterval: 50 * time.Millisecond, FailTimeout: 200 * time.Millisecond, RPCTimeout: 150 * time.Millisecond},
		replica.Timing{Heartbeat: 30 * time.Millisecond, FollowerTimeout: 300 * time.Millisecond, ProduceTimeout: 800 * time.Millisecond, RPCTimeout: 300 * time.Millisecond, MaxBatch: 2000}
}

type node struct {
	id, addr, dir string
	net           *rpc.Net
	b             *Broker
	srv           *http.Server
	up            bool
}

type cluster struct {
	t     *testing.T
	nodes []*node
	peers map[string]string
	dir   map[string]api.Info
	root  string
}

func newCluster(t *testing.T, n int) *cluster {
	regions := make([]string, n)
	for i := range regions {
		regions[i] = "IN"
	}
	return newClusterRegions(t, regions)
}

// newClusterRegions starts one coordination group whose brokers are configured in the given
// regions. Mixing regions models a misconfigured peer list.
func newClusterRegions(t *testing.T, regions []string) *cluster {
	t.Helper()
	c := &cluster{t: t, peers: map[string]string{}, dir: map[string]api.Info{}, root: t.TempDir()}
	count := map[string]int{}
	for i, region := range regions {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		count[region]++
		id := fmt.Sprintf("%s-%d", strings.ToLower(region), count[region])
		nd := &node{id: id, addr: ln.Addr().String(), dir: filepath.Join(c.root, id), net: rpc.NewNet()}
		ln.Close()
		c.nodes = append(c.nodes, nd)
		c.peers[id] = nd.addr
		c.dir[id] = api.Info{ID: id, Region: region, Zone: fmt.Sprintf("%s-%c", strings.ToLower(region), 'a'+i)}
	}
	for i := range c.nodes {
		c.start(i)
	}
	t.Cleanup(func() {
		for i := range c.nodes {
			c.kill(i)
		}
	})
	return c
}

func (c *cluster) start(i int) {
	c.t.Helper()
	nd := c.nodes[i]
	ct, rt := FastTiming()
	b, err := Open(Config{
		Info:    c.dir[nd.id],
		DataDir: nd.dir, Regions: []string{"IN", "EU", "US"}, Peers: c.peers, Directory: c.dir,
		Log: seglog.Config{SegmentBytes: 256 << 10}, Coord: ct, Replica: rt, Net: nd.net,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	var ln net.Listener
	for try := 0; ; try++ {
		if ln, err = net.Listen("tcp", nd.addr); err == nil {
			break
		}
		if try > 50 {
			c.t.Fatalf("relisten %s: %v", nd.addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	nd.b, nd.srv, nd.up = b, &http.Server{Handler: b.Handler()}, true
	go nd.srv.Serve(ln)
	b.Start()
}

// kill stops a broker abruptly: listener and connections closed, background loops stopped.
func (c *cluster) kill(i int) {
	nd := c.nodes[i]
	if !nd.up {
		return
	}
	nd.up = false
	nd.srv.Close()
	nd.b.Close()
}

// isolate cuts all broker-to-broker links between node i and the rest (clients can still reach it).
func (c *cluster) isolate(i int) {
	for j, nd := range c.nodes {
		if j == i {
			continue
		}
		nd.net.Block(c.nodes[i].id)
		c.nodes[i].net.Block(nd.id)
	}
}

func (c *cluster) heal() {
	for _, nd := range c.nodes {
		nd.net.Set(nil)
	}
}

func (c *cluster) client(i int) *client.Client { return client.New(c.nodes[i].addr) }

func (c *cluster) index(id string) int {
	for i, nd := range c.nodes {
		if nd.id == id {
			return i
		}
	}
	return -1
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for !cond() {
		if time.Since(start) > timeout {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
	return time.Since(start)
}

// controller waits for exactly one live controller and returns its index.
func (c *cluster) controller() int {
	c.t.Helper()
	idx := -1
	waitFor(c.t, 5*time.Second, "a controller", func() bool {
		idx = -1
		for i, nd := range c.nodes {
			if nd.up && nd.b.coord.IsController() {
				if idx >= 0 {
					c.t.Fatalf("two controllers: %s and %s", c.nodes[idx].id, nd.id)
				}
				idx = i
			}
		}
		return idx >= 0
	})
	return idx
}

func (c *cluster) createTopic(cfg api.TopicConfig) {
	c.t.Helper()
	c.controller()
	var err error
	waitFor(c.t, 5*time.Second, "topic creation", func() bool {
		_, err = c.client(0).CreateTopic(bg, cfg)
		return err == nil
	})
}

// leaderOf returns the index of the broker that currently acts as leader of the partition.
func (c *cluster) leaderOf(topic string, part int) int {
	c.t.Helper()
	idx := -1
	waitFor(c.t, 5*time.Second, "partition leader", func() bool {
		for i, nd := range c.nodes {
			if !nd.up {
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

// produceRetry produces to an explicit partition, retrying through the given brokers until acked.
func produceRetry(ctx context.Context, cs []*client.Client, topic string, part int, values []string) (api.ProduceResponse, error) {
	recs := make([]api.ProduceRecord, len(values))
	for i, v := range values {
		recs[i] = api.ProduceRecord{Partition: &part, Value: v}
	}
	var lastErr error
	for attempt := 0; ctx.Err() == nil; attempt++ {
		resp, err := cs[attempt%len(cs)].Produce(ctx, topic, recs)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		time.Sleep(20 * time.Millisecond)
	}
	return api.ProduceResponse{}, errors.Join(ctx.Err(), lastErr)
}

func consumeAll(t *testing.T, c *client.Client, topic string, part int) []api.FetchedRecord {
	t.Helper()
	var out []api.FetchedRecord
	var off uint64
	for {
		var resp api.FetchResponse
		var err error
		waitFor(t, 5*time.Second, "fetch", func() bool {
			resp, err = c.Fetch(bg, api.FetchRequest{Topic: topic, Partition: part, Offset: off, MaxRecords: 5000})
			return err == nil
		})
		if len(resp.Records) == 0 && resp.NextOffset >= resp.HighWatermark {
			return out
		}
		out = append(out, resp.Records...)
		off = resp.NextOffset
	}
}

// localRecords reads a replica's full log directly from disk (including uncommitted records).
func localRecords(t *testing.T, nd *node, topic string, part int) []seglog.Record {
	t.Helper()
	p := nd.b.parts.Get(meta.PartKey{Topic: topic, Partition: part})
	if p == nil {
		return nil
	}
	var out []seglog.Record
	for off := uint64(0); ; {
		recs, err := p.Log().Read(off, ^uint64(0), 5000, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			return out
		}
		out = append(out, recs...)
		off = recs[len(recs)-1].Offset + 1
	}
}

func sameLog(a, b []seglog.Record) error {
	if len(a) != len(b) {
		return fmt.Errorf("lengths %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Offset != b[i].Offset || a[i].Epoch != b[i].Epoch || string(a[i].Value) != string(b[i].Value) {
			return fmt.Errorf("record %d differs: %+v vs %+v", i, a[i], b[i])
		}
	}
	return nil
}

// waitConverged waits until every live replica of the partition has an identical log.
func (c *cluster) waitConverged(topic string, part int) {
	c.t.Helper()
	var lastErr error
	cond := func() bool {
		var ref []seglog.Record
		first := true
		for _, nd := range c.nodes {
			if !nd.up {
				continue
			}
			recs := localRecords(c.t, nd, topic, part)
			if first {
				ref, first = recs, false
				continue
			}
			if err := sameLog(ref, recs); err != nil {
				lastErr = fmt.Errorf("%s: %w", nd.id, err)
				return false
			}
		}
		return true
	}
	start := time.Now()
	for !cond() {
		if time.Since(start) > 10*time.Second {
			c.t.Fatalf("replicas of %s/%d did not converge: %v", topic, part, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReplicationCommitsOnAllZones(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "orders", Partitions: 3, Residency: "IN"})

	// Produce through one broker; it forwards to each partition's leader.
	for part := 0; part < 3; part++ {
		var vals []string
		for i := 0; i < 1000; i++ {
			vals = append(vals, fmt.Sprintf("p%d-%d", part, i))
		}
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		resp, err := produceRetry(ctx, []*client.Client{c.client(0)}, "orders", part, vals)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Offsets) != 1000 {
			t.Fatalf("got %d offsets", len(resp.Offsets))
		}
	}
	for part := 0; part < 3; part++ {
		c.waitConverged("orders", part)
		// Consume through a different broker than we produced to.
		got := consumeAll(t, c.client(2), "orders", part)
		if len(got) != 1000 {
			t.Fatalf("partition %d: consumed %d, want 1000", part, len(got))
		}
		for i, r := range got {
			if want := fmt.Sprintf("p%d-%d", part, i); r.Value != want {
				t.Fatalf("partition %d record %d = %q, want %q", part, i, r.Value, want)
			}
		}
	}
	// Leaders are spread across zones.
	leaders := map[int]bool{}
	for part := 0; part < 3; part++ {
		leaders[c.leaderOf("orders", part)] = true
	}
	if len(leaders) != 3 {
		t.Errorf("leaders on %d brokers, want 3", len(leaders))
	}
}

type ack struct {
	offset uint64
	value  string
	at     time.Time
}

// load produces batches to one partition until ctx ends, recording every acknowledged message.
func load(ctx context.Context, cs []*client.Client, topic string, part, batch int) (acks []ack, maxStall time.Duration) {
	last := time.Now()
	for seq := 0; ctx.Err() == nil; {
		vals := make([]string, batch)
		for i := range vals {
			vals[i] = fmt.Sprintf("m-%d", seq+i)
		}
		resp, err := produceRetry(ctx, cs, topic, part, vals)
		if err != nil {
			return
		}
		now := time.Now()
		maxStall = max(maxStall, now.Sub(last))
		last = now
		for i, o := range resp.Offsets {
			acks = append(acks, ack{o.Offset, vals[i], now})
		}
		seq += batch
	}
	return
}

// verifyAcked checks every acknowledged message is in the log at the offset it was acked with,
// and returns the number of duplicates (from retried, ambiguous writes).
func verifyAcked(t *testing.T, acks []ack, got []api.FetchedRecord) (dups int) {
	t.Helper()
	byOffset := map[uint64]string{}
	seen := map[string]int{}
	var prev uint64
	for i, r := range got {
		if i > 0 && r.Offset <= prev {
			t.Fatalf("offsets not increasing: %d after %d", r.Offset, prev)
		}
		prev = r.Offset
		byOffset[r.Offset] = r.Value
		seen[r.Value]++
	}
	lost := 0
	for _, a := range acks {
		if byOffset[a.offset] != a.value {
			lost++
			if lost < 5 {
				t.Errorf("acked %q at offset %d, log has %q", a.value, a.offset, byOffset[a.offset])
			}
		}
	}
	if lost > 0 {
		t.Fatalf("%d of %d acknowledged messages lost", lost, len(acks))
	}
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	return dups
}

func TestLeaderCrashUnderLoad(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "orders", Partitions: 1, Residency: "IN"})
	leader := c.leaderOf("orders", 0)
	var others []*client.Client
	for i := range c.nodes {
		if i != leader {
			others = append(others, c.client(i))
		}
	}

	ctx, cancel := context.WithTimeout(bg, 4*time.Second)
	defer cancel()
	var acks []ack
	var stall time.Duration
	done := make(chan struct{})
	go func() { acks, stall = load(ctx, others, "orders", 0, 50); close(done) }()

	time.Sleep(1 * time.Second)
	killedAt := time.Now()
	c.kill(leader)
	t.Logf("killed leader %s", c.nodes[leader].id)
	newLeader := c.leaderOf("orders", 0)
	t.Logf("new leader %s elected %v after kill", c.nodes[newLeader].id, time.Since(killedAt).Round(time.Millisecond))
	<-done

	got := consumeAll(t, c.client(newLeader), "orders", 0)
	dups := verifyAcked(t, acks, got)
	t.Logf("acked=%d in log=%d duplicates(from retries)=%d lost=0 max stall=%v", len(acks), len(got), dups, stall.Round(time.Millisecond))

	// The crashed broker comes back as a follower and converges to the same log.
	c.start(leader)
	c.waitConverged("orders", 0)
	if c.leaderOf("orders", 0) == leader {
		t.Fatal("restarted broker took leadership back without an election")
	}
}

func TestControllerCrash(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "orders", Partitions: 3, Residency: "IN"})
	ctrl := c.controller()
	killedAt := time.Now()
	c.kill(ctrl)
	newCtrl := c.controller()
	if newCtrl == ctrl {
		t.Fatal("dead broker still controller")
	}
	t.Logf("controller %s -> %s in %v", c.nodes[ctrl].id, c.nodes[newCtrl].id, time.Since(killedAt).Round(time.Millisecond))

	// Metadata survived: the new controller knows the topic and can create more.
	cl := c.client(newCtrl)
	if _, err := cl.Topic(bg, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CreateTopic(bg, api.TopicConfig{Name: "audit", Partitions: 1, Residency: "global"}); err != nil {
		t.Fatal(err)
	}
	// Every partition (including those led by the dead controller) is writable again.
	for part := 0; part < 3; part++ {
		ctx, cancel := context.WithTimeout(bg, 5*time.Second)
		if _, err := produceRetry(ctx, []*client.Client{cl}, "orders", part, []string{"after"}); err != nil {
			t.Fatalf("partition %d: %v", part, err)
		}
		cancel()
	}
}

func TestStaleLeaderAfterPauseIsFenced(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "orders", Partitions: 3, Residency: "IN"})
	ctrl := c.controller()
	part, old := -1, -1
	for p := 0; p < 3; p++ {
		if l := c.leaderOf("orders", p); l != ctrl {
			part, old = p, l
			break
		}
	}
	others := []*client.Client{}
	for i := range c.nodes {
		if i != old {
			others = append(others, c.client(i))
		}
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	if _, err := produceRetry(ctx, others, "orders", part, []string{"before-1", "before-2"}); err != nil {
		t.Fatal(err)
	}
	oldEpoch := c.nodes[old].b.parts.Get(meta.PartKey{Topic: "orders", Partition: part}).Status().LeaderEpoch

	// The leader is cut off from its peers (as if paused); it still believes it leads.
	c.isolate(old)
	start := time.Now()
	_, err := c.client(old).Produce(bg, "orders", []api.ProduceRecord{{Partition: &part, Value: "stale-write"}})
	if api.CodeOf(err) != api.CodeNotEnoughReplicas {
		t.Fatalf("isolated leader accepted or misreported a write: %v", err)
	}
	t.Logf("isolated leader rejected the write in %v: %v", time.Since(start).Round(time.Millisecond), err)
	start = time.Now()
	_, err = c.client(old).Produce(bg, "orders", []api.ProduceRecord{{Partition: &part, Value: "stale-write-2"}})
	if api.CodeOf(err) != api.CodeNotEnoughReplicas || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("second write to isolated leader: %v after %v (want fast NOT_ENOUGH_REPLICAS)", err, time.Since(start))
	}

	// The majority elects a new leader with a higher epoch and keeps accepting writes.
	var newLeader int
	waitFor(t, 5*time.Second, "new leader", func() bool {
		for i, nd := range c.nodes {
			if i == old {
				continue
			}
			if st := nd.b.parts.Get(meta.PartKey{Topic: "orders", Partition: part}).Status(); st.Role == replica.RoleLeader && st.LeaderEpoch > oldEpoch {
				newLeader = i
				return true
			}
		}
		return false
	})
	if _, err := produceRetry(ctx, others, "orders", part, []string{"after-1"}); err != nil {
		t.Fatal(err)
	}

	// Network heals: the stale leader is fenced, truncates its unacknowledged tail and follows.
	c.heal()
	waitFor(t, 5*time.Second, "stale leader to step down", func() bool {
		st := c.nodes[old].b.parts.Get(meta.PartKey{Topic: "orders", Partition: part}).Status()
		return st.Role == replica.RoleFollower && st.Epoch > oldEpoch
	})
	c.waitConverged("orders", part)
	var vals []string
	for _, r := range consumeAll(t, c.client(old), "orders", part) {
		vals = append(vals, r.Value)
	}
	if got := strings.Join(vals, ","); got != "before-1,before-2,after-1" {
		t.Fatalf("log after heal = %s", got)
	}
	t.Logf("new leader %s; stale leader %s fenced and converged", c.nodes[newLeader].id, c.nodes[old].id)
}

func TestTopicValidationAndResidency(t *testing.T) {
	c := newCluster(t, 1)
	c.controller()
	cl := c.client(0)
	info, err := cl.CreateTopic(bg, api.TopicConfig{Name: "orders", Partitions: 2, Residency: "in"})
	if err != nil || info.Residency != "IN" {
		t.Fatalf("create = %+v, %v", info, err)
	}
	if _, err := cl.CreateTopic(bg, api.TopicConfig{Name: "orders", Partitions: 2, Residency: "IN"}); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	cases := []struct {
		cfg  api.TopicConfig
		code api.Code
	}{
		{api.TopicConfig{Name: "orders", Partitions: 3, Residency: "IN"}, api.CodeTopicExists},
		{api.TopicConfig{Name: "x", Partitions: 1, Residency: "MARS"}, api.CodeInvalidRequest},
		{api.TopicConfig{Name: "x", Partitions: 0, Residency: "global"}, api.CodeInvalidRequest},
		{api.TopicConfig{Name: "../escape", Partitions: 1, Residency: "global"}, api.CodeInvalidRequest},
		{api.TopicConfig{Name: "Upper", Partitions: 1, Residency: "global"}, api.CodeInvalidRequest},
		{api.TopicConfig{Name: "eu-data", Partitions: 1, Residency: "EU"}, api.CodeResidencyViolation},
	}
	for _, tc := range cases {
		if _, err := cl.CreateTopic(bg, tc.cfg); api.CodeOf(err) != tc.code {
			t.Errorf("%+v: got %v, want %s", tc.cfg, err, tc.code)
		}
	}
}

func TestSingleBrokerRestartKeepsData(t *testing.T) {
	c := newCluster(t, 1)
	c.createTopic(api.TopicConfig{Name: "t", Partitions: 1, Residency: "IN"})
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	vals := make([]string, 500)
	for i := range vals {
		vals[i] = fmt.Sprint(i)
	}
	if _, err := produceRetry(ctx, []*client.Client{c.client(0)}, "t", 0, vals); err != nil {
		t.Fatal(err)
	}
	c.kill(0)
	c.start(0)
	c.controller()
	var got []api.FetchedRecord
	waitFor(t, 5*time.Second, "data after restart", func() bool {
		got = consumeAll(t, c.client(0), "t", 0)
		return len(got) == 500
	})
	if got[499].Value != "499" {
		t.Fatalf("last = %+v", got[499])
	}
}

func TestIdentityProtectsDataDir(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Info: api.Info{ID: "in-1", Region: "IN", Zone: "in-a"}, DataDir: dir, Regions: []string{"IN", "EU"}}
	b, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	moved := cfg
	moved.Info = api.Info{ID: "in-1", Region: "EU", Zone: "eu-a"}
	if _, err := Open(moved); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("broker started in another region on the same data dir: %v", err)
	}
	other := cfg
	other.Info.ID = "in-2"
	if _, err := Open(other); err == nil {
		t.Fatal("another broker id reused the data dir")
	}
}

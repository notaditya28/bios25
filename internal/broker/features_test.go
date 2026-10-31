package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/geo"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/meta"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/trail"
)

var testPolicies = []policy.Policy{
	{Class: "public", StorageRegions: []string{"*"}},
	{Class: "pii-in", StorageRegions: []string{"IN"}},
	{Class: "eu-personal", StorageRegions: []string{"EU"}, TransitRegions: []string{"US"}},
	{Class: "financial", StorageRegions: []string{"IN", "EU"}, MinAcks: policy.AcksAll},
}

// installPolicies signs a document with the cluster's admin key and installs it on every broker
// through the admin API.
func (c *cluster) installPolicies(ps ...policy.Policy) {
	c.t.Helper()
	now := time.Now()
	s, err := policy.Sign(policy.Document{Version: uint64(now.UnixNano()), IssuedAt: now, ExpiresAt: now.Add(time.Hour), Policies: ps}, c.admin)
	if err != nil {
		c.t.Fatal(err)
	}
	data, _ := json.Marshal(s)
	for _, nd := range c.nodes {
		if !nd.up {
			continue
		}
		resp, err := http.Post("http://"+nd.addr+"/admin/v1/policies", "application/json", bytes.NewReader(data))
		if err != nil || resp.StatusCode != 200 {
			c.t.Fatalf("install on %s: %v %v", nd.id, err, resp)
		}
		resp.Body.Close()
	}
}

func produceOne(c *client.Client, topic string, req api.ProduceRequest) (api.ProduceResponse, error) {
	if len(req.Records) == 0 {
		p := 0
		req.Records = []api.ProduceRecord{{Partition: &p, Value: "v"}}
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	return c.ProduceRequest(ctx, topic, req)
}

func (c *cluster) createAt(i int, cfg api.TopicConfig) error {
	var err error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err = c.client(i).CreateTopic(bg, cfg); err == nil || api.CodeOf(err) == api.CodePolicyUnavailable || api.CodeOf(err) == api.CodeResidencyViolation {
			return err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return err
}

func TestJurisdictionPolicies(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	in, eu, us := c.byRegion("IN")[0], c.byRegion("EU")[0], c.byRegion("US")[0]

	// Fail closed: without a signed document, policy classes can't be used at all.
	if err := c.createAt(in, api.TopicConfig{Name: "fin-tx", Partitions: 1, Residency: "policy:financial"}); api.CodeOf(err) != api.CodePolicyUnavailable {
		t.Fatalf("create without policy document: %v", err)
	}
	c.installPolicies(testPolicies...)

	// financial: stored in IN and EU only. Created from US, it lands only in IN and EU.
	if err := c.createAt(us, api.TopicConfig{Name: "fin-tx", Partitions: 1, Residency: "policy:financial"}); err != nil {
		t.Fatal(err)
	}
	var info api.TopicInfo
	waitFor(t, 5*time.Second, "financial topic in IN", func() bool {
		var err error
		info, err = c.client(in).Topic(bg, "fin-tx")
		return err == nil
	})
	for _, r := range info.PartitionStates[0].Replicas {
		if reg := c.dir[r].Region; reg == "US" {
			t.Fatalf("financial replica placed in US: %v", info.PartitionStates[0].Replicas)
		}
	}
	if _, err := c.client(us).Topic(bg, "fin-tx"); err == nil {
		t.Fatal("US holds financial topic metadata as a stored topic")
	}

	// The policy's durability floor overrides a weaker request.
	var resp api.ProduceResponse
	waitFor(t, 10*time.Second, "financial produce", func() bool {
		var err error
		resp, err = produceOne(c.client(in), "fin-tx", api.ProduceRequest{Acks: "leader"})
		return err == nil
	})
	if resp.Acks != policy.AcksAll {
		t.Fatalf("acks applied = %q, want all (policy floor)", resp.Acks)
	}
	// A producer can't route financial data through the US, even if it asks.
	if _, err := produceOne(c.client(in), "fin-tx", api.ProduceRequest{Via: "US"}); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("producer-requested US route: %v", err)
	}
	// A US client is sent away: US may neither store nor carry financial data.
	if _, err := produceOne(c.client(us), "fin-tx", api.ProduceRequest{}); err == nil {
		t.Fatal("US broker accepted financial data")
	}

	// Slow IN<->EU so the router prefers relaying through US for ordinary traffic...
	c.createIn("IN", api.TopicConfig{Name: "prices", Partitions: 1, Residency: "global"})
	c.slowLink("IN", "EU", 60*time.Millisecond)
	waitFor(t, 10*time.Second, "IN->EU traffic rerouted via US", func() bool {
		for _, id := range c.nodes[in].b.regionBrokers("EU") {
			if via := c.nodes[in].b.geo.Route(id); via != "" && c.dir[via].Region == "US" {
				return true
			}
		}
		return false
	})
	// ...but financial replication between IN and EU still never touches US. (Like any client,
	// retry transient errors; count them so instability on the slow link stays visible.)
	retries, codes := 0, map[string]int{}
	for i := 0; i < 20; i++ {
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err := produceOne(c.client(in), "fin-tx", api.ProduceRequest{})
			if err == nil {
				break
			}
			if code := api.CodeOf(err); code == api.CodeResidencyViolation || code == api.CodePolicyUnavailable || time.Now().After(deadline) {
				t.Fatal(err)
			}
			retries++
			codes[err.Error()[:min(60, len(err.Error()))]]++
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Logf("financial writes over a 120 ms-RTT IN<->EU link: 20 committed, %d transient retries %v", retries, codes)
	c.waitConverged("fin-tx", 0)
	for _, i := range c.byRegion("US") {
		ds := c.nodes[i].b.trail.Recent(2000, func(d trail.Decision) bool { return d.Topic == "fin-tx" && d.Decision == trail.Allow })
		if len(ds) > 0 {
			t.Fatalf("US broker %s carried financial data: %+v", c.nodes[i].id, ds[0])
		}
	}
	c.slowLink("IN", "EU", 0)

	// eu-personal: stored only in EU, may transit US. A US client is served through a proxy;
	// nothing is stored in the US.
	if err := c.createAt(eu, api.TopicConfig{Name: "eu-profile", Partitions: 1, Residency: "policy:eu-personal"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "US proxies eu-personal", func() bool {
		_, err := produceOne(c.client(us), "eu-profile", api.ProduceRequest{})
		return err == nil
	})
	for _, i := range c.byRegion("US") {
		if _, err := os.Stat(layout.TopicDir(c.nodes[i].dir, "eu-profile")); !os.IsNotExist(err) {
			t.Fatalf("US broker stored eu-personal data")
		}
	}
	if _, err := produceOne(c.client(in), "eu-profile", api.ProduceRequest{}); err == nil {
		t.Fatal("IN broker carried eu-personal data (IN is not a transit region)")
	}

	// Per-message classes can only narrow: pii-in data can't go into a global topic.
	if _, err := produceOne(c.client(in), "prices", api.ProduceRequest{Class: "pii-in"}); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("pii-in message into a global topic: %v", err)
	}
	c.createIn("IN", api.TopicConfig{Name: "in-events", Partitions: 1, Residency: "IN"})
	if _, err := produceOne(c.client(in), "in-events", api.ProduceRequest{Class: "pii-in"}); err != nil {
		t.Fatalf("pii-in message into an IN topic: %v", err)
	}
	if _, err := produceOne(c.client(in), "in-events", api.ProduceRequest{Class: "eu-personal"}); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("eu-personal message into an IN topic: %v", err)
	}

	// A rogue relay request can't make a US broker carry financial data, and loops are refused.
	rogue := rpc.NewClient("in-1", c.peers, rpc.NewNet())
	relay := geo.RelayRequest{To: "eu-1", Path: replica.PathAppend, Topic: "fin-tx", Residency: "policy:financial", Body: []byte(`{}`), Hops: 1}
	if err := rogue.Call(bg, "us-1", geo.PathRelay, relay, nil); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("rogue relay through US: %v", err)
	}
	relay.Topic, relay.Residency, relay.Visited = "prices", "global", []string{"us-1"}
	if err := rogue.Call(bg, "us-1", geo.PathRelay, relay, nil); api.CodeOf(err) != api.CodeResidencyViolation || !strings.Contains(err.Error(), "loop") {
		t.Fatalf("routing loop: %v", err)
	}
	allows, denies := c.nodes[us].b.trail.Counts()
	t.Logf("US broker decisions: %d allowed, %d denied", allows, denies)
}

func TestGranularCheckpointsAndFencing(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "jobs", Partitions: 1, Residency: "IN"})
	vals := make([]string, 100)
	for i := range vals {
		vals[i] = fmt.Sprint("job-", i)
	}
	ctx, cancel := context.WithTimeout(bg, 20*time.Second)
	defer cancel()
	if _, err := produceRetry(ctx, c.clients(0), "jobs", 0, vals); err != nil {
		t.Fatal(err)
	}
	addrs := []string{c.nodes[0].addr, c.nodes[1].addr, c.nodes[2].addr}

	// Owner A processes everything except one slow message, out of order, and commits.
	run := func(granular bool, group string) (redelivered int) {
		a := client.NewGroupConsumer(group, "a", "jobs", 0, granular, addrs...)
		if err := a.Claim(ctx); err != nil {
			t.Fatal(err)
		}
		recs, err := a.Poll(ctx, 1000, 0)
		if err != nil || len(recs) != 100 {
			t.Fatalf("poll: %d records, %v", len(recs), err)
		}
		stuck := recs[0].Offset
		for i := len(recs) - 1; i >= 1; i-- {
			a.Ack(recs[i].Offset)
		}
		if err := a.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		// Failover: B takes over (fencing A) and resumes from the checkpoint.
		b := client.NewGroupConsumer(group, "b", "jobs", 0, granular, addrs...)
		if err := b.Claim(ctx); err != nil {
			t.Fatal(err)
		}
		if b.Epoch() <= a.Epoch() {
			t.Fatalf("epoch did not advance: %d -> %d", a.Epoch(), b.Epoch())
		}
		got, err := b.Poll(ctx, 1000, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 || got[0].Offset != stuck {
			t.Fatalf("stuck message %d not redelivered: %+v", stuck, got)
		}
		// The zombie owner can no longer move the checkpoint.
		a.Ack(stuck)
		if err := a.Commit(ctx); api.CodeOf(err) != api.CodeStaleEpoch {
			t.Fatalf("zombie commit: %v", err)
		}
		return len(got)
	}
	granular := run(true, "g-granular")
	offsetOnly := run(false, "g-offset")
	t.Logf("after failover: granular checkpoint redelivered %d message(s); offset-only redelivered %d", granular, offsetOnly)
	if granular != 1 || offsetOnly != 100 {
		t.Fatalf("redelivered granular=%d (want 1), offset-only=%d (want 100)", granular, offsetOnly)
	}

	// Checkpoints are replicated: they survive the death of the checkpoint partition's leader.
	cpTopic := cpTopicFor("IN")
	part := cpPartition(cpKey("g-granular", "jobs", 0))
	leader := c.leaderOf(cpTopic, part)
	c.kill(leader)
	var others []string
	for i, nd := range c.nodes {
		if i != leader {
			others = append(others, nd.addr)
		}
	}
	d := client.NewGroupConsumer("g-granular", "d", "jobs", 0, true, others...)
	if err := d.Claim(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := d.Poll(ctx, 1000, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("after checkpoint-leader failover: %d records, %v", len(got), err)
	}
}

func TestAcksModes(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "acks", Partitions: 1, Residency: "IN"})
	leader := c.leaderOf("acks", 0)
	cl := c.client(leader)
	for _, mode := range []string{"leader", "quorum", "all"} {
		var resp api.ProduceResponse
		var err error
		waitFor(t, 5*time.Second, "acks "+mode, func() bool {
			resp, err = produceOne(cl, "acks", api.ProduceRequest{Acks: mode})
			return err == nil
		})
		if resp.Acks != mode {
			t.Fatalf("acks %s reported as %q (response %+v)", mode, resp.Acks, resp)
		}
	}
	// One follower down: quorum still commits, "all" can't and says so explicitly.
	c.kill((leader + 1) % 3)
	time.Sleep(400 * time.Millisecond)
	if _, err := produceOne(cl, "acks", api.ProduceRequest{Acks: "quorum"}); err != nil {
		t.Fatalf("quorum with one follower down: %v", err)
	}
	start := time.Now()
	_, err := produceOne(cl, "acks", api.ProduceRequest{Acks: "all"})
	if api.CodeOf(err) != api.CodeNotEnoughReplicas {
		t.Fatalf("acks=all with a follower down: %v", err)
	}
	t.Logf("acks=all with a follower down: %v after %v", err, time.Since(start).Round(time.Millisecond))
}

func TestCrossRegionDedup(t *testing.T) {
	c := newGeo(t, regions3, 3)
	c.waitControllers()
	c.createIn("IN", api.TopicConfig{Name: "blobs", Partitions: 1, Residency: "global"})
	big := strings.Repeat("config-blob-", 700) // ~8 KB, identical in every message
	cl := c.client(c.byRegion("IN")[0])
	for i := 0; i < 40; i++ {
		p := 0
		req := api.ProduceRequest{Records: []api.ProduceRecord{{Partition: &p, Value: big}, {Partition: &p, Value: fmt.Sprint("small-", i)}}}
		var err error
		waitFor(t, 10*time.Second, "produce blob", func() bool {
			_, err = produceOne(cl, "blobs", req)
			return err == nil
		})
	}
	c.waitConverged("blobs", 0)
	leader := c.globalLeader("blobs", 0, "")
	refs, saved := c.nodes[leader].b.parts.Get(meta.PartKey{Topic: "blobs", Partition: 0}).DedupStats()
	t.Logf("dedup: %d values sent as references, %d bytes saved", refs, saved)
	if refs == 0 {
		t.Fatal("no value was deduplicated on the wire")
	}
	// Every replica holds the full payloads (the log never stores references).
	for i, nd := range c.nodes {
		if p := nd.b.parts.Get(meta.PartKey{Topic: "blobs", Partition: 0}); p != nil {
			for _, r := range localRecords(t, c.nodes[i], "blobs", 0) {
				if !r.IsControl() && len(r.Ref) != 0 {
					t.Fatalf("%s stored a reference", nd.id)
				}
				if strings.HasPrefix(string(r.Value), "config-blob-") && string(r.Value) != big {
					t.Fatalf("%s has a corrupted blob", nd.id)
				}
			}
		}
	}
}

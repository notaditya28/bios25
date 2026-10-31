package broker

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/audit"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/rpc"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

// clusterConfig describes the test cluster the way the audit tool sees it.
func (c *cluster) clusterConfig() *config.Cluster {
	cc := &config.Cluster{}
	index := map[string]int{}
	for _, nd := range c.nodes {
		info := c.dir[nd.id]
		i, ok := index[info.Region]
		if !ok {
			i = len(cc.Regions)
			index[info.Region] = i
			cc.Regions = append(cc.Regions, config.Region{Name: info.Region})
		}
		cc.Regions[i].Brokers = append(cc.Regions[i].Brokers, config.Broker{ID: nd.id, Zone: info.Zone, Addr: nd.addr})
	}
	return cc
}

func (c *cluster) audit(online bool) *audit.Report {
	c.t.Helper()
	rep, err := audit.Run(context.Background(), c.clusterConfig(), audit.Options{
		DataRoot: c.root, Online: online, TrustFile: filepath.Join(c.root, "trust.json"),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	return rep
}

func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestResidencyEnforcedDespiteMisconfiguredPeers(t *testing.T) {
	// An EU broker was wrongly put into IN's coordination group.
	c := newClusterRegions(t, []string{"IN", "IN", "EU"})
	in1, in2, eu1 := 0, 1, 2
	c.createTopic(api.TopicConfig{Name: "aadhaar", Partitions: 3, Residency: "IN"})
	c.createTopic(api.TopicConfig{Name: "prices", Partitions: 1, Residency: "global"})

	// Placement: resident partitions only on IN brokers; global ones may use every broker.
	info, err := c.client(in1).Topic(bg, "aadhaar")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range info.PartitionStates {
		if slices.Contains(p.Replicas, "eu-1") || len(p.Replicas) != 2 {
			t.Fatalf("partition %d placed on %v", p.Partition, p.Replicas)
		}
	}
	if g, _ := c.client(in1).Topic(bg, "prices"); !slices.Contains(g.PartitionStates[0].Replicas, "eu-1") {
		t.Fatalf("global topic should use all brokers, got %v", g.PartitionStates[0].Replicas)
	}

	// Writes and reads work in-region. Relaying them through the EU broker is refused, so the
	// data never passes through it.
	ctx, cancel := context.WithTimeout(bg, 5*time.Second)
	defer cancel()
	for part := 0; part < 3; part++ {
		if _, err := produceRetry(ctx, []*client.Client{c.client(in2)}, "aadhaar", part, []string{"id-1", "id-2"}); err != nil {
			t.Fatal(err)
		}
	}
	p0 := 0
	_, err = c.client(eu1).Produce(bg, "aadhaar", []api.ProduceRecord{{Partition: &p0, Value: "via-eu"}})
	if api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("produce relayed through EU broker: %v", err)
	}
	if _, err := c.client(eu1).Fetch(bg, api.FetchRequest{Topic: "aadhaar"}); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("fetch relayed through EU broker: %v", err)
	}

	// A rogue replication request can't plant IN data on the EU broker...
	rogue := rpc.NewClient("in-1", c.peers, rpc.NewNet())
	req := replica.AppendRequest{Topic: "aadhaar", Partition: 0, Residency: "IN", Leader: "in-1", LeaderEpoch: 99,
		Records: []seglog.Record{{Offset: 0, Epoch: 99, Value: []byte("smuggled")}}}
	var resp replica.AppendResponse
	if err := rogue.Call(bg, "eu-1", replica.PathAppend, req, &resp); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("append to EU broker: %v", err)
	}
	// ...and an IN broker refuses resident data arriving from outside the region.
	fromEU := rpc.NewClient("eu-1", c.peers, rpc.NewNet())
	if err := fromEU.Call(bg, "in-2", replica.PathAppend, req, &resp); api.CodeOf(err) != api.CodeResidencyViolation {
		t.Fatalf("append from EU broker: %v", err)
	}
	if n := c.nodes[eu1].b.guard.Rejected(); n == 0 {
		t.Fatal("EU broker counted no rejections")
	}
	if _, err := os.Stat(layout.TopicDir(c.nodes[eu1].dir, "aadhaar")); !os.IsNotExist(err) {
		t.Fatalf("EU broker has a directory for the IN topic: %v", err)
	}

	// The audit (disk scan + signed manifests) passes...
	rep := c.audit(true)
	if rep.Verdict != "PASS" {
		rep.WriteText(os.Stderr, true)
		t.Fatal("audit failed on a compliant cluster")
	}
	for _, b := range rep.Brokers {
		if b.Manifest != "verified" {
			t.Fatalf("%s manifest %s", b.ID, b.Manifest)
		}
	}

	// ...and catches resident data that appears on the EU broker behind the broker's back.
	src := layout.PartitionDir(c.nodes[in1].dir, "aadhaar", 0)
	if _, err := os.Stat(src); err != nil {
		src = layout.PartitionDir(c.nodes[in2].dir, "aadhaar", 0)
	}
	copyDir(t, filepath.Dir(src), layout.TopicDir(c.nodes[eu1].dir, "aadhaar"))
	rep = c.audit(true)
	if rep.Verdict != "FAIL" {
		t.Fatal("audit passed with IN data on an EU broker")
	}
	found := false
	for _, f := range rep.Findings {
		if f.Rule == "residency" && f.Broker == "eu-1" && f.Topic == "aadhaar" {
			found = true
		}
	}
	if !found {
		rep.WriteText(os.Stderr, true)
		t.Fatal("no residency finding for eu-1")
	}
}

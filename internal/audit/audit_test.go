package audit

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/layout"
)

var cluster = &config.Cluster{Regions: []config.Region{
	{Name: "IN", Brokers: []config.Broker{{ID: "in-1", Zone: "in-a", Addr: "127.0.0.1:1"}}},
	{Name: "EU", Brokers: []config.Broker{{ID: "eu-1", Zone: "eu-a", Addr: "127.0.0.1:2"}}},
}}

func writeBroker(t *testing.T, root, id, region string) string {
	t.Helper()
	dir := filepath.Join(root, id)
	os.MkdirAll(dir, 0o755)
	if err := fsutil.WriteJSONAtomic(filepath.Join(dir, layout.IdentityFile), layout.Identity{ID: id, Region: region}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writePartition(t *testing.T, dir, topic, res string, tag bool) {
	t.Helper()
	os.MkdirAll(layout.PartitionDir(dir, topic, 0), 0o755)
	if tag {
		fsutil.WriteJSONAtomic(filepath.Join(layout.TopicDir(dir, topic), layout.TopicMetaFile),
			layout.TopicMeta{TopicConfig: api.TopicConfig{Name: topic, Partitions: 1, Residency: res}})
	}
	os.WriteFile(filepath.Join(layout.PartitionDir(dir, topic, 0), "00000000000000000000.log"), []byte("data"), 0o644)
}

func run(t *testing.T, root string) *Report {
	t.Helper()
	rep, err := Run(context.Background(), cluster, Options{DataRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func rules(r *Report) map[string]int {
	out := map[string]int{}
	for _, f := range r.Findings {
		if f.Severity == Violation {
			out[f.Rule]++
		}
	}
	return out
}

func TestCompliantClusterPasses(t *testing.T) {
	root := t.TempDir()
	in := writeBroker(t, root, "in-1", "IN")
	eu := writeBroker(t, root, "eu-1", "EU")
	writePartition(t, in, "aadhaar", "IN", true)
	writePartition(t, in, "prices", "global", true)
	writePartition(t, eu, "prices", "global", true)
	writePartition(t, eu, "gdpr", "EU", true)
	if rep := run(t, root); rep.Verdict != "PASS" || rep.Violations != 0 {
		t.Fatalf("verdict %s, findings %+v", rep.Verdict, rep.Findings)
	}
}

func TestViolationsAreFound(t *testing.T) {
	root := t.TempDir()
	in := writeBroker(t, root, "in-1", "IN")
	eu := writeBroker(t, root, "eu-1", "EU")
	writePartition(t, eu, "aadhaar", "IN", true) // resident data outside its region
	writePartition(t, in, "mystery", "", false)  // no residency tag at all
	writePartition(t, in, "mars", "MARS", true)  // tag naming no region
	stray := filepath.Join(root, "old-broker")   // data dir nobody owns
	writePartition(t, stray, "x", "IN", true)
	fsutil.WriteJSONAtomic(filepath.Join(eu, layout.IdentityFile), layout.Identity{ID: "eu-1", Region: "IN"}) // relabelled dir

	rep := run(t, root)
	got := rules(rep)
	for _, want := range []string{"residency", "untagged-data", "unknown-residency", "unknown-broker", "identity-mismatch"} {
		if got[want] == 0 {
			t.Errorf("missing %s violation; got %v", want, got)
		}
	}
	if rep.Verdict != "FAIL" {
		t.Fatal("verdict PASS with violations")
	}
}

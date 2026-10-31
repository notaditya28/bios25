package sim

import (
	"strings"
	"testing"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

func model() Model {
	dir := map[string]api.Info{}
	for _, r := range []string{"IN", "EU", "US"} {
		for i, z := range []string{"a", "b", "c"} {
			id := strings.ToLower(r) + "-" + string(rune('1'+i))
			dir[id] = api.Info{ID: id, Region: r, Zone: strings.ToLower(r) + "-" + z}
		}
	}
	return Model{
		Dir: dir,
		Policy: policy.Document{Version: 1, Policies: []policy.Policy{
			{Class: "financial", StorageRegions: []string{"IN", "EU"}, MinAcks: "all"},
			{Class: "eu-personal", StorageRegions: []string{"EU"}, TransitRegions: []string{"US"}},
		}},
		Topics: []api.TopicConfig{
			{Name: "in-orders", Partitions: 1, Residency: "IN"},
			{Name: "prices", Partitions: 3, Residency: "global"},
			{Name: "payments", Partitions: 2, Residency: "policy:financial"},
		},
		RTT: map[string]float64{Pair("IN", "EU"): 120, Pair("IN", "US"): 200, Pair("EU", "US"): 80},
	}
}

func find(p *Prediction, topic string, part int) PartitionOutcome {
	for _, o := range p.Partitions {
		if o.Topic == topic && o.Partition == part {
			return o
		}
	}
	return PartitionOutcome{}
}

func TestRegionDown(t *testing.T) {
	p, err := Run(model(), Scenario{Name: "in down", Steps: []Step{{Kind: "region_down", Region: "IN"}}})
	if err != nil {
		t.Fatal(err)
	}
	if o := find(p, "in-orders", 0); o.Writable {
		t.Fatalf("IN-resident topic must not fail over: %+v", o)
	}
	if o := find(p, "prices", 0); !o.Writable {
		t.Fatalf("global topic should survive one region: %+v", o)
	}
	// financial keeps a quorum in EU, but its policy demands acks=all, so losing the IN replica
	// stops writes (the real cluster agrees: see broker.TestSimulatorMatchesCluster).
	for part := 0; part < 2; part++ {
		if o := find(p, "payments", part); o.Writable {
			t.Fatalf("payments/%d: acks=all with a replica down must not be writable: %+v", part, o)
		}
	}
	m := model()
	m.Policy.Policies[0].MinAcks = ""
	p, _ = Run(m, Scenario{Steps: []Step{{Kind: "region_down", Region: "IN"}}})
	for part := 0; part < 2; part++ {
		o := find(p, "payments", part)
		eu := 0
		for _, r := range o.Replicas {
			if strings.HasPrefix(r, "eu-") {
				eu++
			}
		}
		if o.Writable != (eu >= 2) || contains(o.WritableFrom, "US") {
			t.Fatalf("payments/%d at quorum acks: %+v", part, o)
		}
	}
	if len(p.Backlogs) == 0 {
		t.Fatal("expected backlog for IN replicas of global partitions")
	}
}

func TestIsolationKeepsResidentWritable(t *testing.T) {
	p, _ := Run(model(), Scenario{Steps: []Step{{Kind: "region_isolate", Region: "IN"}}})
	o := find(p, "in-orders", 0)
	if !o.Writable || len(o.WritableFrom) != 1 || o.WritableFrom[0] != "IN" {
		t.Fatalf("isolated IN keeps its own data writable from IN only: %+v", o)
	}
	if o := find(p, "prices", 0); !o.Writable || contains(o.WritableFrom, "IN") {
		t.Fatalf("global topic writable from the majority side only: %+v", o)
	}
}

func TestSlowLinkRelayAndTransitFinding(t *testing.T) {
	// IN-EU 10x slower: 1200 ms direct vs 280 via US. Financial data may not transit US.
	p, _ := Run(model(), Scenario{Steps: []Step{{Kind: "link_slow", A: "IN", B: "EU", Factor: 10}}})
	var r RouteOutcome
	for _, x := range p.Routes {
		if x.From == "IN" && x.To == "EU" {
			r = x
		}
	}
	if r.Via != "US" || !contains(r.Forbidden, "financial") {
		t.Fatalf("expected relay via US forbidden for financial: %+v", r)
	}
}

func TestPolicyChangeFindsStrandedData(t *testing.T) {
	p, _ := Run(model(), Scenario{Steps: []Step{{Kind: "storage_remove", Class: "financial", Region: "EU"}}})
	if len(p.PolicyFindings) == 0 {
		t.Fatal("removing EU from financial storage must flag the EU replicas")
	}
	if _, err := Run(model(), Scenario{Steps: []Step{{Kind: "storage_remove", Class: "nope", Region: "EU"}}}); err == nil {
		t.Fatal("unknown class should error")
	}
}

func TestParseStep(t *testing.T) {
	st, err := ParseStep("link_slow:in:eu:10")
	if err != nil || st.A != "IN" || st.B != "EU" || st.Factor != 10 {
		t.Fatalf("%+v %v", st, err)
	}
	if _, err := ParseStep("region_down"); err == nil {
		t.Fatal("missing argument should fail")
	}
}

func contains(s []string, x string) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

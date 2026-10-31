// Command hydra-sim predicts what a failure or a policy change would do BEFORE it happens.
//
//	hydra-sim -step region_down:IN
//	hydra-sim -step link_slow:IN:EU:10 -step traffic:5
//	hydra-sim -step storage_remove:financial:EU         # dry-run a policy change
//	hydra-sim -preset guide                              # the six standard what-ifs
//	hydra-sim -broker 127.0.0.1:9101 -step region_isolate:US   # use the live topics and RTTs
//
// It reuses the brokers' placement, quorum, policy and path-choice code (see internal/sim).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/forecast"
	"github.com/notaditya28/hydra-stream/internal/geo"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/sim"
)

type steps []string

func (s *steps) String() string     { return strings.Join(*s, ",") }
func (s *steps) Set(v string) error { *s = append(*s, v); return nil }

// Presets are the standard what-ifs to run before a deployment.
var presets = map[string][]sim.Scenario{
	"guide": {
		{Name: "a region goes offline (IN)", Steps: []sim.Step{{Kind: "region_down", Region: "IN"}}},
		{Name: "the IN↔EU link becomes 10x slower", Steps: []sim.Step{{Kind: "link_slow", A: "IN", B: "EU", Factor: 10}}},
		{Name: "US removed from eu-personal's allowed transit regions", Steps: []sim.Step{{Kind: "transit_remove", Class: "eu-personal", Region: "US"}}},
		{Name: "traffic grows 5x", Steps: []sim.Step{{Kind: "traffic", Factor: 5}}},
		{Name: "destination storage full (EU)", Steps: []sim.Step{{Kind: "storage_full", Region: "EU"}}},
		{Name: "two regions recover after 5 minutes away (EU, US)", OutageSeconds: 300, Steps: []sim.Step{{Kind: "recovering", Region: "EU"}, {Kind: "recovering", Region: "US"}}},
	},
}

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config file")
	polPath := flag.String("policies", "configs/policies.json", "policy document (plain or signed JSON)")
	topicsPath := flag.String("topics", "", "JSON array of topic configs (default: the demo topics)")
	broker := flag.String("broker", "", "read topics and measured RTTs from this live broker instead")
	preset := flag.String("preset", "", "run a preset list of scenarios (guide)")
	name := flag.String("name", "", "scenario name")
	outage := flag.Float64("outage", 60, "how long the fault lasts, seconds (backlog math)")
	rate := flag.Float64("rate", sim.DefaultRate, "messages/s per topic")
	msgBytes := flag.Float64("msg-bytes", 200, "average message size")
	capMbps := flag.Float64("link-mbps", 0, "inter-region link capacity in Mbit/s (0 = unlimited)")
	jsonOut := flag.Bool("json", false, "print predictions as JSON")
	var st steps
	flag.Var(&st, "step", "scenario step kind:args (repeatable; see internal/sim.ParseStep)")
	flag.Parse()

	if *preset != "" && *capMbps == 0 {
		*capMbps = 50 // presets assume modest inter-region links so capacity findings show up
	}
	m, err := buildModel(*cfgPath, *polPath, *topicsPath, *broker, *rate, *msgBytes, *capMbps)
	if err != nil {
		fail(err)
	}
	var scs []sim.Scenario
	switch {
	case *preset != "":
		var ok bool
		if scs, ok = presets[*preset]; !ok {
			fail(fmt.Errorf("unknown preset %q", *preset))
		}
	case len(st) > 0:
		sc := sim.Scenario{Name: *name, OutageSeconds: *outage}
		for _, s := range st {
			step, err := sim.ParseStep(s)
			if err != nil {
				fail(err)
			}
			sc.Steps = append(sc.Steps, step)
		}
		if sc.Name == "" {
			sc.Name = strings.Join(st, " + ")
		}
		scs = []sim.Scenario{sc}
	default:
		fail(fmt.Errorf("give -step or -preset"))
	}
	var preds []*sim.Prediction
	for _, sc := range scs {
		if sc.OutageSeconds == 0 {
			sc.OutageSeconds = *outage
		}
		p, err := sim.Run(m, sc)
		if err != nil {
			fail(fmt.Errorf("%s: %w", sc.Name, err))
		}
		preds = append(preds, p)
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(preds)
		return
	}
	for _, p := range preds {
		print(p)
	}
}

func print(p *sim.Prediction) {
	fmt.Printf("\n=== %s ===\n", p.Scenario)
	fmt.Printf("writable partitions: %d / %d\n", p.Writable, p.Total)
	if p.PeakLink != "" {
		fmt.Printf("busiest link: %s at %.1f%% of capacity (steady replication)\n", p.PeakLink, p.PeakLinkUtil)
	}
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "  PARTITION\tRESIDENCY\tREPLICAS\tSTATE")
	for _, o := range p.Partitions {
		fmt.Fprintf(tw, "  %s/%d\t%s\t%s\t%s\n", o.Topic, o.Partition, o.Residency, strings.Join(o.Replicas, ","), o.Reason)
	}
	tw.Flush()
	var relays []string
	for _, r := range p.Routes {
		if r.Via != "" {
			s := fmt.Sprintf("  %s→%s via %s (%.0f ms vs %.0f ms direct)", r.From, r.To, r.Via, r.PathMs, r.DirectMs)
			if len(r.Forbidden) > 0 {
				s += fmt.Sprintf(" — not allowed for %v", r.Forbidden)
			}
			relays = append(relays, s)
		}
	}
	if len(relays) > 0 {
		fmt.Println("relayed paths:")
		fmt.Println(strings.Join(relays, "\n"))
	}
	if len(p.Recovery) > 0 {
		fmt.Println("backlog to recover:")
		for _, r := range p.Recovery {
			eta := "never at current rates"
			if r.CatchUpSecs >= 0 {
				eta = forecast.HumanDuration(r.CatchUpSecs)
			}
			fmt.Printf("  %s: %d replicas behind, %.0f records (%.1f MB), catch-up %s\n", r.Region, r.Replicas, r.Records, r.Bytes/1e6, eta)
		}
	}
	for _, f := range p.PolicyFindings {
		fmt.Println("POLICY:", f)
	}
	for _, r := range p.Recommendations {
		fmt.Println("→", r)
	}
}

func buildModel(cfgPath, polPath, topicsPath, broker string, rate, msgBytes, capMbps float64) (sim.Model, error) {
	cluster, err := config.Load(cfgPath)
	if err != nil {
		return sim.Model{}, err
	}
	m := sim.Model{Dir: map[string]api.Info{}, RTT: map[string]float64{}, Capacity: map[string]float64{}, Rate: map[string]float64{}, MsgBytes: msgBytes}
	for _, r := range cluster.Regions {
		for _, b := range r.Brokers {
			m.Dir[b.ID] = api.Info{ID: b.ID, Region: r.Name, Zone: b.Zone}
		}
	}
	regions := cluster.RegionNames()
	for i := range regions {
		for j := i + 1; j < len(regions); j++ {
			m.RTT[sim.Pair(regions[i], regions[j])] = 1
			if capMbps > 0 {
				m.Capacity[sim.Pair(regions[i], regions[j])] = capMbps * 1e6 / 8
			}
		}
	}
	// Typical inter-region round trips until measured ones replace them (-broker).
	typical := map[string]float64{sim.Pair("IN", "EU"): 120, sim.Pair("IN", "US"): 220, sim.Pair("EU", "US"): 80}
	for k, v := range typical {
		if _, ok := m.RTT[k]; ok {
			m.RTT[k] = v
		}
	}
	if data, err := os.ReadFile(polPath); err == nil {
		var signed policy.Signed
		if json.Unmarshal(data, &signed) == nil && signed.Signature != "" {
			m.Policy = signed.Document
		} else if err := json.Unmarshal(data, &m.Policy); err != nil {
			return m, fmt.Errorf("%s: %w", polPath, err)
		}
	} else if !os.IsNotExist(err) {
		return m, err
	}
	switch {
	case broker != "":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ts, err := client.New(broker).Topics(ctx)
		if err != nil {
			return m, fmt.Errorf("list topics from %s: %w", broker, err)
		}
		for _, t := range ts {
			m.Topics = append(m.Topics, t.TopicConfig)
		}
		// Measured RTTs: each region's first broker's view of the others.
		for _, r := range cluster.Regions {
			var out struct {
				Links []geo.Link `json:"links"`
			}
			if getJSON(ctx, r.Brokers[0].Addr, "/admin/v1/routes", &out) != nil {
				continue
			}
			for _, l := range out.Links {
				if l.Up && l.RTTms > 0 {
					m.RTT[sim.Pair(r.Name, l.Region)] = l.RTTms
				}
			}
		}
	case topicsPath != "":
		data, err := os.ReadFile(topicsPath)
		if err != nil {
			return m, err
		}
		if err := json.Unmarshal(data, &m.Topics); err != nil {
			return m, fmt.Errorf("%s: %w", topicsPath, err)
		}
	default:
		// The demo/chaos topics plus one per policy class.
		for _, r := range regions {
			m.Topics = append(m.Topics, api.TopicConfig{Name: strings.ToLower(r) + "-orders", Partitions: 1, Residency: r})
		}
		m.Topics = append(m.Topics, api.TopicConfig{Name: "prices", Partitions: len(regions), Residency: api.ResidencyGlobal})
		for _, p := range m.Policy.Policies {
			m.Topics = append(m.Topics, api.TopicConfig{Name: p.Class + "-events", Partitions: 2, Residency: policy.Prefix + p.Class})
		}
	}
	for _, t := range m.Topics {
		m.Rate[t.Name] = rate
	}
	return m, nil
}

func getJSON(ctx context.Context, addr, path string, out any) error {
	return client.New(addr).Get(ctx, path, out)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}

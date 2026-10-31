// Package sim is Hydra Stream's pre-deployment failure and policy simulator.
//
// Given the cluster topology, the policy document, the topics and measured link latencies, it
// predicts what a scenario does BEFORE it happens: which partitions stay writable (and from
// where), which inter-region paths traffic takes and which data classes may not use them, where
// backlog builds up and how long recovery will take, and which data a proposed policy change
// would leave stored where it is no longer allowed.
//
// It does not re-implement the system: placement (placement), quorum rules, policy resolution
// (policy), path choice (geo.ChooseRelay) and recovery math (forecast) are the very functions the
// brokers run. Predictions are checked against real runs (see the chaos harness).
package sim

import (
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/forecast"
	"github.com/notaditya28/hydra-stream/internal/geo"
	"github.com/notaditya28/hydra-stream/internal/placement"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

// Model is the system as it is now.
type Model struct {
	Dir      map[string]api.Info // every broker
	Policy   policy.Document     // installed policy document (may be empty)
	Topics   []api.TopicConfig
	RTT      map[string]float64 // "A|B" (sorted) -> round trip ms between regions; missing = 1 ms
	Capacity map[string]float64 // "A|B" -> link capacity bytes/s; missing/0 = unlimited
	Rate     map[string]float64 // topic -> messages/s (missing = DefaultRate)
	MsgBytes float64            // average message size (default 200)
}

const (
	DefaultRate       = 1000.0
	unlimitedDrainMsg = 50000.0 // assumed catch-up rate on an unlimited link, messages/s
)

func Pair(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// Step is one change in a scenario.
type Step struct {
	Kind    string  `json:"kind"` // region_down | region_isolate | link_slow | link_cut | traffic | transit_remove | storage_remove | storage_full | recovering
	Region  string  `json:"region,omitempty"`
	A       string  `json:"a,omitempty"`
	B       string  `json:"b,omitempty"`
	Class   string  `json:"class,omitempty"`
	Factor  float64 `json:"factor,omitempty"`
	Seconds float64 `json:"seconds,omitempty"`
}

type Scenario struct {
	Name          string  `json:"name"`
	Steps         []Step  `json:"steps"`
	OutageSeconds float64 `json:"outage_seconds"` // how long the fault lasts (for backlog math; default 60)
}

// ParseStep reads "kind:arg:arg" shorthand, e.g. region_down:IN, link_slow:IN:EU:10,
// link_cut:IN:US, traffic:5, transit_remove:financial:US, storage_remove:financial:EU, storage_full:EU.
func ParseStep(s string) (Step, error) {
	f := strings.Split(s, ":")
	st := Step{Kind: f[0]}
	need := map[string]int{"region_down": 2, "region_isolate": 2, "link_slow": 4, "link_cut": 3, "traffic": 2,
		"transit_remove": 3, "storage_remove": 3, "storage_full": 2, "recovering": 2}
	n, ok := need[st.Kind]
	if !ok {
		return st, fmt.Errorf("unknown step %q", f[0])
	}
	if len(f) != n {
		return st, fmt.Errorf("%s takes %d arguments", st.Kind, n-1)
	}
	switch st.Kind {
	case "region_down", "region_isolate", "storage_full", "recovering":
		st.Region = strings.ToUpper(f[1])
	case "link_cut":
		st.A, st.B = strings.ToUpper(f[1]), strings.ToUpper(f[2])
	case "link_slow":
		st.A, st.B = strings.ToUpper(f[1]), strings.ToUpper(f[2])
		st.Factor, _ = strconv.ParseFloat(f[3], 64)
	case "traffic":
		st.Factor, _ = strconv.ParseFloat(f[1], 64)
	case "transit_remove", "storage_remove":
		st.Class, st.Region = f[1], strings.ToUpper(f[2])
	}
	return st, nil
}

// PartitionOutcome is the predicted state of one partition.
type PartitionOutcome struct {
	Topic        string          `json:"topic"`
	Partition    int             `json:"partition"`
	Residency    string          `json:"residency"`
	Replicas     []string        `json:"replicas"`
	Writable     bool            `json:"writable"`
	WritableFrom []string        `json:"writable_from"` // regions whose clients can still write
	Reason       string          `json:"reason"`
	Regions      map[string]bool `json:"-"`
}

type RouteOutcome struct {
	From      string   `json:"from"`
	To        string   `json:"to"`
	DirectMs  float64  `json:"direct_ms"` // -1 = cut
	Via       string   `json:"via,omitempty"`
	PathMs    float64  `json:"path_ms"`                 // -1 = unreachable
	Forbidden []string `json:"forbidden_for,omitempty"` // classes that may not take this relay
}

type BacklogOutcome struct {
	Topic       string  `json:"topic"`
	Partition   int     `json:"partition"`
	Replica     string  `json:"replica"`
	Region      string  `json:"region"`
	Records     float64 `json:"backlog_records"`
	Bytes       float64 `json:"backlog_bytes"`
	ArrivalRate float64 `json:"arrival_rate"` // new messages/s it must also keep up with
}

// RegionRecovery: all of a region's lagging replicas catch up over the same links at once, so
// catch-up time is the region's total backlog against its shared link capacity.
type RegionRecovery struct {
	Region      string  `json:"region"`
	Replicas    int     `json:"replicas"`
	Records     float64 `json:"backlog_records"`
	Bytes       float64 `json:"backlog_bytes"`
	CatchUpSecs float64 `json:"catch_up_s"` // -1 = never at current rates
	LinkLimited bool    `json:"link_limited"`
}

type Prediction struct {
	Scenario        string             `json:"scenario"`
	Partitions      []PartitionOutcome `json:"partitions"`
	Writable        int                `json:"writable_partitions"`
	Total           int                `json:"total_partitions"`
	Routes          []RouteOutcome     `json:"routes"`
	Backlogs        []BacklogOutcome   `json:"backlogs"`
	Recovery        []RegionRecovery   `json:"recovery"` // backlog summed per lagging region
	PolicyFindings  []string           `json:"policy_findings"`
	LinkFindings    []string           `json:"link_findings"`
	PeakLink        string             `json:"peak_link,omitempty"` // busiest inter-region link
	PeakLinkUtil    float64            `json:"peak_link_util_pct"`  // its steady replication load, % of capacity
	Recommendations []string           `json:"recommendations"`
}

type world struct {
	m        Model
	regions  []string
	down     map[string]bool
	isolated map[string]bool
	cut      map[string]bool
	full     map[string]bool
	recover  map[string]bool // back after OutageSeconds away: serving, but must catch up
	rtt      map[string]float64
	factor   float64
	current  *policy.Engine // policy before the scenario (placement was decided with it)
	proposed *policy.Engine // policy after the scenario's policy changes
}

func (w *world) linkUp(a, b string) bool {
	return !w.down[a] && !w.down[b] && !w.isolated[a] && !w.isolated[b] && !w.cut[Pair(a, b)]
}

func (w *world) rttOf(a, b string) float64 {
	if !w.linkUp(a, b) {
		return math.Inf(1)
	}
	if v, ok := w.rtt[Pair(a, b)]; ok {
		return v
	}
	return 1
}

// component returns the regions reachable from r (directly or through others).
func (w *world) component(r string) map[string]bool {
	if w.down[r] {
		return map[string]bool{}
	}
	seen := map[string]bool{r: true}
	queue := []string{r}
	for len(queue) > 0 {
		x := queue[0]
		queue = queue[1:]
		for _, y := range w.regions {
			if !seen[y] && w.linkUp(x, y) {
				seen[y] = true
				queue = append(queue, y)
			}
		}
	}
	return seen
}

// Run predicts the outcome of sc on m.
func Run(m Model, sc Scenario) (*Prediction, error) {
	if m.MsgBytes <= 0 {
		m.MsgBytes = 200
	}
	if sc.OutageSeconds <= 0 {
		sc.OutageSeconds = 60
	}
	w := &world{m: m, regions: placement.Regions(m.Dir), down: map[string]bool{}, isolated: map[string]bool{},
		cut: map[string]bool{}, full: map[string]bool{}, recover: map[string]bool{}, rtt: map[string]float64{}, factor: 1}
	for k, v := range m.RTT {
		w.rtt[k] = v
	}
	w.current = policy.ForSimulation(m.Policy, w.regions)
	doc := m.Policy
	doc.Policies = append([]policy.Policy(nil), doc.Policies...)
	for _, st := range sc.Steps {
		switch st.Kind {
		case "region_down":
			w.down[st.Region] = true
		case "region_isolate":
			w.isolated[st.Region] = true
		case "link_cut":
			w.cut[Pair(st.A, st.B)] = true
		case "link_slow":
			k := Pair(st.A, st.B)
			base := w.rtt[k]
			if base == 0 {
				base = 1
			}
			w.rtt[k] = base * math.Max(1, st.Factor)
		case "traffic":
			w.factor *= math.Max(0, st.Factor)
		case "recovering":
			w.recover[st.Region] = true
		case "storage_full":
			w.full[st.Region] = true
		case "transit_remove", "storage_remove":
			found := false
			for i := range doc.Policies {
				p := &doc.Policies[i]
				if p.Class != st.Class {
					continue
				}
				found = true
				drop := func(list []string) []string {
					var out []string
					for _, r := range list {
						if r == policy.Any {
							for _, x := range w.regions {
								if x != st.Region {
									out = append(out, x)
								}
							}
							continue
						}
						if r != st.Region {
							out = append(out, r)
						}
					}
					return out
				}
				if st.Kind == "storage_remove" {
					p.StorageRegions = drop(p.StorageRegions)
				}
				p.TransitRegions = drop(p.TransitRegions)
				if st.Kind == "transit_remove" && slices.Contains(p.StorageRegions, st.Region) {
					// Storage implies transit: removing transit means removing storage too.
					p.StorageRegions = drop(p.StorageRegions)
				}
			}
			if !found {
				return nil, fmt.Errorf("class %q is not in the policy document", st.Class)
			}
		default:
			return nil, fmt.Errorf("unknown step %q", st.Kind)
		}
	}
	w.proposed = policy.ForSimulation(doc, w.regions)

	pred := &Prediction{Scenario: sc.Name, Partitions: []PartitionOutcome{}, Routes: []RouteOutcome{},
		Backlogs: []BacklogOutcome{}, Recovery: []RegionRecovery{}, PolicyFindings: []string{}, LinkFindings: []string{}, Recommendations: []string{}}
	w.partitions(pred, sc)
	w.recovery(pred)
	w.routes(pred)
	w.links(pred)
	w.recommend(pred)
	return pred, nil
}

func (w *world) partitions(pred *Prediction, sc Scenario) {
	topics := append([]api.TopicConfig(nil), w.m.Topics...)
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	for _, t := range topics {
		if strings.HasPrefix(t.Name, "__") {
			continue // internal topics follow their data topic
		}
		cur, err := w.current.Resolve(t.Residency)
		if err != nil {
			pred.PolicyFindings = append(pred.PolicyFindings, fmt.Sprintf("topic %s: policy %s can't be resolved now (%v) — it is failing closed", t.Name, t.Residency, err))
			continue
		}
		next, nerr := w.proposed.Resolve(t.Residency)
		rate := w.m.Rate[t.Name]
		if rate == 0 {
			rate = DefaultRate
		}
		rate *= w.factor / float64(max(1, t.Partitions))
		if nerr == nil {
			for _, client := range w.regions {
				was, now := cur.CanStore(client) || cur.CanTransit(client), next.CanStore(client) || next.CanTransit(client)
				if was && !now {
					pred.PolicyFindings = append(pred.PolicyFindings, fmt.Sprintf("topic %s: clients in %s can produce and consume today; under the proposed policy they would get RESIDENCY_VIOLATION (transit_not_permitted)", t.Name, client))
				}
			}
		}
		for p := 0; p < t.Partitions; p++ {
			// Placement exactly as the brokers computed it when the topic was created.
			var replicas []string
			if placement.CrossRegion(w.m.Dir, cur) {
				replicas = placement.CrossRegionReplicas(w.m.Dir, cur, t.Name, p)
			} else {
				regs := placement.StorageRegions(w.m.Dir, cur)
				if len(regs) == 1 {
					var eligible []string
					for _, id := range placement.RegionBrokers(w.m.Dir, regs[0]) {
						if cur.CanStore(w.m.Dir[id].Region) {
							eligible = append(eligible, id)
						}
					}
					replicas = placement.InRegionReplicas(eligible, p)
				}
			}
			out := PartitionOutcome{Topic: t.Name, Partition: p, Residency: t.Residency, Replicas: replicas, WritableFrom: []string{}, Regions: map[string]bool{}}
			for _, r := range replicas {
				out.Regions[w.m.Dir[r].Region] = true
			}
			// Policy change: data already stored where the new policy forbids it.
			if nerr != nil {
				pred.PolicyFindings = append(pred.PolicyFindings, fmt.Sprintf("topic %s: the proposed policy makes %s unresolvable — all its traffic would fail closed", t.Name, t.Residency))
			} else {
				for _, r := range replicas {
					if reg := w.m.Dir[r].Region; !next.CanStore(reg) {
						pred.PolicyFindings = append(pred.PolicyFindings, fmt.Sprintf("topic %s/%d: replica on %s (%s) would hold data the proposed policy forbids storing there — migrate or delete before applying",
							t.Name, p, r, reg))
					}
				}
			}
			// Availability: a partition commits in a connected group of regions holding a quorum
			// of its replicas (and not out of disk).
			// A policy's durability floor raises the bar: acks=all needs every replica.
			quorum := placement.Quorum(len(replicas))
			floor := cur.MinAcks
			if nerr == nil {
				floor = next.MinAcks
			}
			if floor == policy.AcksAll {
				quorum = len(replicas)
			}
			var permitted []string // live regions whose clients this policy lets write at all
			for _, client := range w.regions {
				if w.down[client] {
					continue
				}
				comp := w.component(client)
				have := 0
				for _, r := range replicas {
					reg := w.m.Dir[r].Region
					if comp[reg] && !w.full[reg] {
						have++
					}
				}
				policyOK := nerr == nil && (next.CanStore(client) || next.CanTransit(client))
				if policyOK {
					permitted = append(permitted, client)
				}
				if have >= quorum && len(replicas) > 0 && policyOK {
					out.WritableFrom = append(out.WritableFrom, client)
				}
			}
			out.Writable = len(out.WritableFrom) > 0
			switch {
			case len(replicas) == 0:
				out.Reason = "no eligible placement"
			case out.Writable && len(out.WritableFrom) < len(permitted):
				out.Reason = "writable only from " + strings.Join(out.WritableFrom, ", ") + " (the others lost contact with a quorum)"
			case out.Writable:
				out.Reason = "writable from " + strings.Join(out.WritableFrom, ", ")
			case len(out.Regions) == 1:
				out.Reason = "its only region is down or full: resident data can't fail over (by design)"
			case floor == policy.AcksAll:
				out.Reason = "its policy requires acks=all and a replica is unreachable — writes fail with explicit timeouts until it returns"
			default:
				out.Reason = "no connected group of regions holds a quorum of replicas"
			}
			pred.Total++
			if out.Writable {
				pred.Writable++
			}
			pred.Partitions = append(pred.Partitions, out)
			// Backlog for replicas cut off while the partition kept committing elsewhere: only if
			// a quorum of replicas outside the faulted regions carried on without them. Each
			// recovering region is assumed to have been away on its own; all of them catch up at
			// the same time, sharing the source's links.
			for _, r := range replicas {
				reg := w.m.Dir[r].Region
				if !(w.down[reg] || w.isolated[reg] || w.full[reg] || w.recover[reg]) {
					continue
				}
				healthy := 0
				for _, o := range replicas {
					oreg := w.m.Dir[o].Region
					if oreg != reg && !(w.down[oreg] || w.isolated[oreg] || w.full[oreg]) {
						healthy++
					}
				}
				if healthy < quorum {
					continue // nothing committed without it, so nothing to catch up
				}
				b := BacklogOutcome{Topic: t.Name, Partition: p, Replica: r, Region: reg, Records: rate * sc.OutageSeconds, ArrivalRate: rate}
				b.Bytes = b.Records * w.m.MsgBytes
				pred.Backlogs = append(pred.Backlogs, b)
			}
		}
	}
}

func (w *world) recovery(pred *Prediction) {
	by := map[string]*RegionRecovery{}
	arrival := map[string]float64{}
	for _, b := range pred.Backlogs {
		r := by[b.Region]
		if r == nil {
			r = &RegionRecovery{Region: b.Region}
			by[b.Region] = r
		}
		r.Replicas++
		r.Records += b.Records
		r.Bytes += b.Bytes
		arrival[b.Region] += b.ArrivalRate
	}
	for _, reg := range w.regions {
		r := by[reg]
		if r == nil {
			continue
		}
		drain := unlimitedDrainMsg * float64(r.Replicas) // unlimited links: per-replica apply rate
		lagging := len(by)
		for _, other := range w.regions {
			if c := w.m.Capacity[Pair(reg, other)]; c > 0 && other != reg {
				// The region pulls from every source region at once; lagging regions share them.
				drain = math.Min(drain, float64(len(w.regions)-1)*c/w.m.MsgBytes/float64(lagging))
				r.LinkLimited = true
			}
		}
		clear := forecast.TimeToClear(r.Records, arrival[reg], drain)
		r.CatchUpSecs = -1
		if !math.IsInf(clear, 1) {
			r.CatchUpSecs = math.Round(clear*10) / 10
		}
		pred.Recovery = append(pred.Recovery, *r)
	}
}

func (w *world) liveRegions() []string {
	var out []string
	for _, r := range w.regions {
		if !w.down[r] {
			out = append(out, r)
		}
	}
	return out
}

// routes predicts each region pair's path with the routers' own rule and flags policy classes
// that may not use the chosen relay.
func (w *world) routes(pred *Prediction) {
	type classRule struct {
		name string
		eff  policy.Effective
	}
	var classes []classRule
	for _, p := range w.proposed.Document().Policies {
		if eff, err := w.proposed.Class(p.Class); err == nil {
			classes = append(classes, classRule{p.Class, eff})
		}
	}
	for _, a := range w.regions {
		for _, b := range w.regions {
			if a == b || w.down[a] || w.down[b] {
				continue
			}
			direct := w.rttOf(a, b)
			via, best := "", math.Inf(1)
			for _, c := range w.regions {
				if c == a || c == b {
					continue
				}
				if cost := w.rttOf(a, c) + w.rttOf(c, b); cost < best {
					via, best = c, cost
				}
			}
			r := RouteOutcome{From: a, To: b, DirectMs: finite(direct), PathMs: finite(direct)}
			if via != "" && geo.ChooseRelay(direct, best, false) {
				r.Via, r.PathMs = via, finite(best)
				for _, c := range classes {
					if c.eff.CanTransit(a) && c.eff.CanTransit(b) && !c.eff.CanTransit(via) {
						r.Forbidden = append(r.Forbidden, c.name)
					}
				}
			}
			pred.Routes = append(pred.Routes, r)
		}
	}
}

// links checks inter-region replication load against link capacity.
func (w *world) links(pred *Prediction) {
	load := map[string]float64{} // pair -> bytes/s of replication
	for _, po := range pred.Partitions {
		if len(po.Regions) < 2 {
			continue
		}
		rate := w.m.Rate[po.Topic]
		if rate == 0 {
			rate = DefaultRate
		}
		var t api.TopicConfig
		for _, x := range w.m.Topics {
			if x.Name == po.Topic {
				t = x
			}
		}
		bps := rate * w.factor / float64(max(1, t.Partitions)) * w.m.MsgBytes
		regs := make([]string, 0, len(po.Regions))
		for r := range po.Regions {
			regs = append(regs, r)
		}
		sort.Strings(regs)
		for i := range regs {
			for j := i + 1; j < len(regs); j++ {
				if w.linkUp(regs[i], regs[j]) {
					load[Pair(regs[i], regs[j])] += bps
				}
			}
		}
	}
	keys := make([]string, 0, len(load))
	for k := range load {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	worst, worstKey := 0.0, ""
	for _, k := range keys {
		if c := w.m.Capacity[k]; c > 0 && load[k]/c > worst {
			worst, worstKey = load[k]/c, k
		}
	}
	if worstKey != "" {
		pred.PeakLink = strings.ReplaceAll(worstKey, "|", "↔")
		pred.PeakLinkUtil = math.Round(worst*1000) / 10
	}
	for _, k := range keys {
		c := w.m.Capacity[k]
		name := strings.ReplaceAll(k, "|", "↔")
		switch {
		case c <= 0:
		case load[k] > c:
			pred.LinkFindings = append(pred.LinkFindings, fmt.Sprintf("link %s is OVER capacity: replication needs ~%.1f MB/s of %.1f MB/s (%.0f%%) — followers across it fall further behind every second; quorum writes still commit where a quorum is on the fast side, but add capacity or shed traffic",
				name, load[k]/1e6, c/1e6, 100*load[k]/c))
		case load[k] > 0.8*c:
			pred.LinkFindings = append(pred.LinkFindings, fmt.Sprintf("link %s: replication needs ~%.1f MB/s of %.1f MB/s capacity (%.0f%%) — little headroom left for catch-up after outages; the adaptive scheduler keeps live traffic first",
				name, load[k]/1e6, c/1e6, 100*load[k]/c))
		}
	}
}

func (w *world) recommend(pred *Prediction) {
	lost := pred.Total - pred.Writable
	if lost > 0 {
		pred.Recommendations = append(pred.Recommendations, fmt.Sprintf("%d of %d partitions would stop accepting writes; resident data in a lost region can't fail over by law — tell those clients to expect REGION_UNAVAILABLE.", lost, pred.Total))
	}
	if len(pred.PolicyFindings) > 0 {
		pred.Recommendations = append(pred.Recommendations, "the proposed policy conflicts with where data already lives — fix the findings before signing and pushing it.")
	}
	worst := 0.0
	for _, r := range pred.Recovery {
		if r.CatchUpSecs < 0 {
			pred.Recommendations = append(pred.Recommendations, fmt.Sprintf("%s's replicas can never catch up at current rates — add link capacity or cut traffic before it returns.", r.Region))
		}
		worst = math.Max(worst, r.CatchUpSecs)
	}
	if worst > 0 {
		pred.Recommendations = append(pred.Recommendations, fmt.Sprintf("recovering replicas will need up to ~%s to catch up after the outage; keep the adaptive bandwidth controller on so live traffic isn't starved.", forecast.HumanDuration(worst)))
	}
	for _, r := range pred.Routes {
		if len(r.Forbidden) > 0 {
			pred.Recommendations = append(pred.Recommendations, fmt.Sprintf("%s→%s traffic is relayed via %s, which classes %v may not transit — their data keeps the slower direct path (%.0f ms vs %.0f ms).",
				r.From, r.To, r.Via, r.Forbidden, r.DirectMs, r.PathMs))
		}
	}
	pred.Recommendations = append(pred.Recommendations, pred.LinkFindings...)
}

func finite(x float64) float64 {
	if math.IsInf(x, 0) {
		return -1
	}
	return math.Round(x*10) / 10
}

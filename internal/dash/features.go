package dash

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/chaos"
	"github.com/notaditya28/hydra-stream/internal/flow"
	"github.com/notaditya28/hydra-stream/internal/geo"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/sim"
	"github.com/notaditya28/hydra-stream/internal/trail"
)

// Feature views: what each region's first running broker reports about jurisdiction decisions,
// link scheduling and recovery forecasts.

type flowLinkView struct {
	To             string  `json:"to"`
	CapacityMbps   float64 `json:"capacity_mbps"`
	RecoveryWeight float64 `json:"recovery_weight"`
	LiveP95ms      float64 `json:"live_p95_ms"`
	LiveBytes      int64   `json:"live_bytes"`
	RecoveryBytes  int64   `json:"recovery_bytes"`
}

type backlogView struct {
	Partition string  `json:"partition"`
	Follower  string  `json:"follower"`
	Bytes     float64 `json:"bytes"`
	ClearS    float64 `json:"clear_s"` // -1 = growing / unknown
}

type regionFeatures struct {
	Broker          string           `json:"broker"`
	Allows          int64            `json:"allows"`
	Denies          int64            `json:"denies"`
	Decisions       []trail.Decision `json:"decisions"`
	Links           []flowLinkView   `json:"links"`
	Backlogs        []backlogView    `json:"backlogs"`
	Recommendations []string         `json:"recommendations"`
}

type featuresView struct {
	PolicyVersion uint64                    `json:"policy_version"`
	Classes       []policy.Policy           `json:"classes"`
	Regions       map[string]regionFeatures `json:"regions"`
}

func (d *Dash) firstUp(region string) (chaos.BrokerRef, bool) {
	for _, b := range d.p.Region(region) {
		if d.p.Up(b.ID) {
			return b, true
		}
	}
	return chaos.BrokerRef{}, false
}

func (d *Dash) features() featuresView {
	out := featuresView{Regions: map[string]regionFeatures{}}
	for _, r := range d.p.Regions() {
		b, ok := d.firstUp(r)
		if !ok {
			continue
		}
		rf := regionFeatures{Broker: b.ID, Decisions: []trail.Decision{}, Links: []flowLinkView{}, Backlogs: []backlogView{}, Recommendations: []string{}}
		var dec struct {
			Allows, Denies int64
			Decisions      []trail.Decision
		}
		if getJSON(b.Addr, "/admin/v1/decisions?limit=6&deny=1", &dec) == nil {
			rf.Allows, rf.Denies = dec.Allows, dec.Denies
			if dec.Decisions != nil {
				rf.Decisions = dec.Decisions
			}
		}
		var fl map[string]flow.Stats
		if getJSON(b.Addr, "/admin/v1/flow", &fl) == nil {
			for to, s := range fl {
				rf.Links = append(rf.Links, flowLinkView{To: to, CapacityMbps: s.CapacityBps * 8 / 1e6, RecoveryWeight: s.RecoveryWeight,
					LiveP95ms: s.Classes["live"].DelayP95, LiveBytes: s.Classes["live"].Bytes, RecoveryBytes: s.Classes["recovery"].Bytes})
			}
			sort.Slice(rf.Links, func(i, j int) bool { return rf.Links[i].To < rf.Links[j].To })
		}
		var fc struct {
			Replication []struct {
				Topic        string  `json:"topic"`
				Partition    int     `json:"partition"`
				Follower     string  `json:"follower"`
				BacklogBytes float64 `json:"backlog_bytes"`
				ClearSeconds float64 `json:"clear_s"`
				Shrinking    bool    `json:"shrinking"`
			} `json:"replication"`
			Recommendations []string `json:"recommendations"`
		}
		if getJSON(b.Addr, "/admin/v1/forecast", &fc) == nil {
			for _, x := range fc.Replication {
				if x.BacklogBytes <= 0 {
					continue
				}
				clear := x.ClearSeconds
				if !x.Shrinking {
					clear = -1
				}
				rf.Backlogs = append(rf.Backlogs, backlogView{Partition: x.Topic + "/" + itoa(x.Partition), Follower: x.Follower, Bytes: x.BacklogBytes, ClearS: clear})
			}
			if fc.Recommendations != nil {
				rf.Recommendations = fc.Recommendations
			}
		}
		if out.PolicyVersion == 0 {
			var s policy.Signed
			if getJSON(b.Addr, "/admin/v1/policies", &s) == nil {
				out.PolicyVersion, out.Classes = s.Document.Version, s.Document.Policies
			}
		}
		out.Regions[r] = rf
	}
	return out
}

func itoa(i int) string {
	b, _ := json.Marshal(i)
	return string(b)
}

// simulate runs a what-if on the live cluster: its brokers, topics, installed policy and
// measured RTTs.
func (d *Dash) simulate(steps []string, outage float64) (*sim.Prediction, error) {
	m := sim.Model{Dir: map[string]api.Info{}, RTT: map[string]float64{}, Rate: map[string]float64{}}
	for _, b := range d.p.Brokers() {
		m.Dir[b.ID] = api.Info{ID: b.ID, Region: b.Region, Zone: b.Zone}
	}
	for _, r := range d.p.Regions() {
		b, ok := d.firstUp(r)
		if !ok {
			continue
		}
		var s policy.Signed
		if m.Policy.Version == 0 && getJSON(b.Addr, "/admin/v1/policies", &s) == nil {
			m.Policy = s.Document
		}
		var ts struct {
			Topics []api.TopicInfo `json:"topics"`
		}
		if getJSON(b.Addr, "/v1/topics", &ts) == nil {
			seen := map[string]bool{}
			for _, t := range m.Topics {
				seen[t.Name] = true
			}
			for _, t := range ts.Topics {
				if !seen[t.Name] {
					m.Topics = append(m.Topics, t.TopicConfig)
				}
			}
		}
		var rt struct {
			Links []geo.Link `json:"links"`
		}
		if getJSON(b.Addr, "/admin/v1/routes", &rt) == nil {
			for _, l := range rt.Links {
				if l.Up && l.RTTms > 0 && l.Region != r {
					m.RTT[sim.Pair(r, l.Region)] = l.RTTms
				}
			}
		}
	}
	sc := sim.Scenario{Name: "what-if", OutageSeconds: outage}
	for _, s := range steps {
		st, err := sim.ParseStep(s)
		if err != nil {
			return nil, err
		}
		sc.Steps = append(sc.Steps, st)
	}
	return sim.Run(m, sc)
}

func (d *Dash) featureRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/features", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.features())
	})
	mux.HandleFunc("POST /api/simulate", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Steps  []string `json:"steps"`
			Outage float64  `json:"outage_seconds"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		p, err := d.simulate(req.Steps, req.Outage)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p)
	})
}

// Package dash serves the Hydra Stream dashboard: live cluster state, link latencies,
// throughput and audit status, plus buttons that inject faults (kill/restart a region or a
// broker, isolate a region, slow a link). It owns the broker processes and a background load.
package dash

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/audit"
	"github.com/notaditya28/hydra-stream/internal/chaos"
)

//go:embed index.html
var indexHTML []byte

const (
	sampleEvery = 500 * time.Millisecond
	keepSamples = 120 // one minute of history
)

type Dash struct {
	p         *chaos.Procs
	trustFile string
	streams   []*chaos.Stream

	mu       sync.Mutex
	isolated map[string]bool
	slow     map[string]int64 // "A|B" (sorted) -> one-way ms
	series   map[string][]float64
	errRate  map[string]float64
	last     map[string]int64
	lastAt   time.Time
	brokers  map[string]brokerView
	links    map[string]map[string]chaos.Link // from broker -> to broker -> link
	global   []globalView
	auditV   auditView
	events   []event
	prevCtl  map[string]string
	prevLead map[int]string
}

type brokerView struct {
	ID           string `json:"id"`
	Region       string `json:"region"`
	Zone         string `json:"zone"`
	Addr         string `json:"addr"`
	Running      bool   `json:"running"`
	Healthy      bool   `json:"healthy"`
	Controller   bool   `json:"controller"`
	Rejections   int64  `json:"residency_rejections"`
	Relayed      int64  `json:"relayed"`
	Leads        int    `json:"leads"`
	MetaVersion  uint64 `json:"metadata_version"`
	ControllerOf string `json:"-"`
}

type globalView struct {
	Partition int    `json:"partition"`
	Leader    string `json:"leader"`
	Region    string `json:"region"`
	Epoch     uint64 `json:"epoch"`
}

type auditView struct {
	Verdict    string    `json:"verdict"`
	Violations int       `json:"violations"`
	Warnings   int       `json:"warnings"`
	At         time.Time `json:"at"`
	Running    bool      `json:"running"`
	Findings   []string  `json:"findings"`
}

type event struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"` // action | change | audit
	Text string    `json:"text"`
}

func New(p *chaos.Procs, trustFile string) *Dash {
	return &Dash{p: p, trustFile: trustFile, isolated: map[string]bool{}, slow: map[string]int64{},
		series: map[string][]float64{}, errRate: map[string]float64{}, last: map[string]int64{},
		brokers: map[string]brokerView{}, links: map[string]map[string]chaos.Link{},
		prevCtl: map[string]string{}, prevLead: map[int]string{}}
}

func (d *Dash) logEvent(kind, format string, args ...any) {
	e := event{At: time.Now(), Kind: kind, Text: fmt.Sprintf(format, args...)}
	slog.Info("dashboard", "kind", kind, "event", e.Text)
	d.mu.Lock()
	d.events = append(d.events, e)
	if len(d.events) > 200 {
		d.events = d.events[len(d.events)-200:]
	}
	d.mu.Unlock()
}

// Run starts the background load and the samplers until ctx ends.
func (d *Dash) Run(ctx context.Context, batch int, interval time.Duration) {
	regions := d.p.Regions()
	addrsOf := func(r string) []string {
		var out []string
		for _, b := range d.p.Region(r) {
			out = append(out, b.Addr)
		}
		return out
	}
	for _, r := range regions {
		d.streams = append(d.streams, &chaos.Stream{Name: "live-" + strings.ToLower(r), Topic: strings.ToLower(r) + "-orders",
			Residency: r, Home: r, Addrs: addrsOf(r), Light: true})
	}
	for i, r := range regions {
		as := addrsOf(r)
		for _, o := range regions {
			if o != r {
				as = append(as, addrsOf(o)...)
			}
		}
		d.streams = append(d.streams, &chaos.Stream{Name: fmt.Sprintf("live-g%d", i), Topic: "prices", Partition: i,
			Residency: api.ResidencyGlobal, Home: r, Addrs: as, Light: true})
	}
	for _, s := range d.streams {
		go s.Run(ctx, batch, interval)
	}
	d.lastAt = time.Now()
	go d.loop(ctx, sampleEvery, d.sample)
	go d.loop(ctx, sampleEvery, d.poll)
	go d.loop(ctx, 5*time.Second, func() { d.runAudit(ctx) })
	d.logEvent("action", "cluster started: %d brokers, %d load streams", len(d.p.Brokers()), len(d.streams))
}

func (d *Dash) loop(ctx context.Context, every time.Duration, f func()) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		f()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (d *Dash) sample() {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	dt := now.Sub(d.lastAt).Seconds()
	if dt <= 0 {
		return
	}
	d.lastAt = now
	for _, s := range d.streams {
		acked, failed, _, _ := s.Counters()
		rate := float64(acked-d.last[s.Name]) / dt
		errs := float64(failed-d.last[s.Name+"/err"]) / dt
		d.last[s.Name], d.last[s.Name+"/err"] = acked, failed
		ser := append(d.series[s.Name], rate)
		if len(ser) > keepSamples {
			ser = ser[len(ser)-keepSamples:]
		}
		d.series[s.Name] = ser
		d.errRate[s.Name] = errs
	}
}

type statusResp struct {
	IsController bool   `json:"is_controller"`
	Controller   string `json:"controller"`
	Rejections   int64  `json:"residency_rejections"`
	Relayed      int64  `json:"relayed_requests"`
	MetaVersion  uint64 `json:"metadata_version"`
}

func (d *Dash) poll() {
	type result struct {
		view  brokerView
		parts []chaos.PartitionState
		links []chaos.Link
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	results := map[string]result{}
	for _, b := range d.p.Brokers() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := result{view: brokerView{ID: b.ID, Region: b.Region, Zone: b.Zone, Addr: b.Addr, Running: d.p.Up(b.ID)}}
			if r.view.Running {
				var st statusResp
				if getJSON(b.Addr, "/admin/v1/status", &st) == nil {
					r.view.Healthy, r.view.Controller = true, st.IsController
					r.view.Rejections, r.view.Relayed, r.view.MetaVersion = st.Rejections, st.Relayed, st.MetaVersion
					_ = getJSON(b.Addr, "/admin/v1/partitions", &r.parts)
					var routes struct {
						Links []chaos.Link `json:"links"`
					}
					_ = getJSON(b.Addr, "/admin/v1/routes", &routes)
					r.links = routes.Links
				}
			}
			for _, p := range r.parts {
				if p.Role == "leader" {
					r.view.Leads++
				}
			}
			mu.Lock()
			results[b.ID] = r
			mu.Unlock()
		}()
	}
	wg.Wait()

	globals := map[int]globalView{}
	for id, r := range results {
		for _, p := range r.parts {
			if p.Topic == "prices" && p.Role == "leader" {
				if g, ok := globals[p.Partition]; !ok || p.Epoch > g.Epoch {
					globals[p.Partition] = globalView{Partition: p.Partition, Leader: id, Region: r.view.Region, Epoch: p.Epoch}
				}
			}
		}
	}
	var gl []globalView
	for _, g := range globals {
		gl = append(gl, g)
	}
	sort.Slice(gl, func(i, j int) bool { return gl[i].Partition < gl[j].Partition })

	var changes []string
	d.mu.Lock()
	for id, r := range results {
		d.brokers[id] = r.view
		lm := map[string]chaos.Link{}
		for _, l := range r.links {
			lm[l.To] = l
		}
		d.links[id] = lm
	}
	for _, region := range d.p.Regions() {
		ctl := ""
		for _, b := range d.p.Region(region) {
			if results[b.ID].view.Controller {
				ctl = b.ID
			}
		}
		if prev, ok := d.prevCtl[region]; ok && prev != ctl && ctl != "" {
			changes = append(changes, fmt.Sprintf("%s controller: %s → %s", region, orDash(prev), ctl))
		}
		if ctl != "" || d.prevCtl[region] == "" {
			d.prevCtl[region] = ctl
		}
	}
	for _, g := range gl {
		if prev, ok := d.prevLead[g.Partition]; ok && prev != g.Leader {
			changes = append(changes, fmt.Sprintf("global prices/%d leader: %s → %s (epoch %d)", g.Partition, orDash(prev), g.Leader, g.Epoch))
		}
		d.prevLead[g.Partition] = g.Leader
	}
	d.global = gl
	d.mu.Unlock()
	for _, c := range changes {
		d.logEvent("change", "%s", c)
	}
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func (d *Dash) runAudit(ctx context.Context) {
	d.mu.Lock()
	if d.auditV.Running {
		d.mu.Unlock()
		return
	}
	d.auditV.Running = true
	prev := d.auditV.Verdict
	d.mu.Unlock()
	rep, err := audit.Run(ctx, d.p.Cluster, audit.Options{DataRoot: d.p.DataRoot, Online: true, TrustFile: d.trustFile, Timeout: 3 * time.Second})
	v := auditView{At: time.Now()}
	if err != nil {
		v.Verdict = "ERROR"
		v.Findings = []string{err.Error()}
	} else {
		v.Verdict, v.Violations, v.Warnings = rep.Verdict, rep.Violations, rep.Warnings
		for _, f := range rep.Findings {
			if f.Severity == audit.Violation {
				where := f.Broker
				if f.Topic != "" {
					where += " " + f.Topic
				}
				v.Findings = append(v.Findings, fmt.Sprintf("[%s] %s: %s", f.Rule, where, f.Detail))
			}
		}
	}
	d.mu.Lock()
	d.auditV = v
	d.mu.Unlock()
	if v.Verdict != prev {
		d.logEvent("audit", "residency audit: %s (%d violations, %d warnings)", v.Verdict, v.Violations, v.Warnings)
	}
}

func pairKey(a, b string) string {
	if a > b {
		a, b = b, a
	}
	return a + "|" + b
}

// applyNet pushes the desired isolation + slow-link state to every running broker.
func (d *Dash) applyNet() {
	d.mu.Lock()
	iso := map[string]bool{}
	for r, v := range d.isolated {
		iso[r] = v
	}
	slow := map[string]int64{}
	for k, v := range d.slow {
		slow[k] = v
	}
	d.mu.Unlock()
	d.p.SetNet(
		func(a, b chaos.BrokerRef) bool {
			return a.Region != b.Region && (iso[a.Region] || iso[b.Region])
		},
		func(a, b chaos.BrokerRef) int64 {
			if a.Region == b.Region {
				return 0
			}
			return slow[pairKey(a.Region, b.Region)]
		})
}

type actionReq struct {
	Action string `json:"action"`
	Region string `json:"region"`
	ID     string `json:"id"`
	A      string `json:"a"`
	B      string `json:"b"`
	Ms     int64  `json:"ms"`
}

func (d *Dash) act(ctx context.Context, req actionReq) error {
	switch req.Action {
	case "kill-region":
		for _, b := range d.p.Region(req.Region) {
			d.p.Kill(b.ID)
		}
		d.logEvent("action", "killed every broker in %s", req.Region)
	case "start-region":
		for _, b := range d.p.Region(req.Region) {
			if err := d.p.Start(b.ID); err != nil {
				return err
			}
		}
		d.logEvent("action", "restarted %s", req.Region)
		go d.reapplyNetSoon()
	case "kill-broker":
		d.p.Kill(req.ID)
		d.logEvent("action", "killed broker %s", req.ID)
	case "start-broker":
		if err := d.p.Start(req.ID); err != nil {
			return err
		}
		d.logEvent("action", "restarted broker %s", req.ID)
		go d.reapplyNetSoon()
	case "isolate-region":
		d.mu.Lock()
		d.isolated[req.Region] = true
		d.mu.Unlock()
		d.applyNet()
		d.logEvent("action", "isolated %s from the other regions", req.Region)
	case "unisolate-region":
		d.mu.Lock()
		delete(d.isolated, req.Region)
		d.mu.Unlock()
		d.applyNet()
		d.logEvent("action", "reconnected %s", req.Region)
	case "slow-link":
		if req.A == req.B || req.A == "" || req.B == "" {
			return fmt.Errorf("pick two different regions")
		}
		d.mu.Lock()
		if req.Ms <= 0 {
			delete(d.slow, pairKey(req.A, req.B))
		} else {
			d.slow[pairKey(req.A, req.B)] = req.Ms
		}
		d.mu.Unlock()
		d.applyNet()
		if req.Ms <= 0 {
			d.logEvent("action", "restored link %s ↔ %s", req.A, req.B)
		} else {
			d.logEvent("action", "slowed link %s ↔ %s by %d ms each way", req.A, req.B, req.Ms)
		}
	case "heal-all":
		d.mu.Lock()
		d.isolated, d.slow = map[string]bool{}, map[string]int64{}
		d.mu.Unlock()
		for _, b := range d.p.Brokers() {
			_ = d.p.Start(b.ID)
		}
		d.applyNet()
		go d.reapplyNetSoon()
		d.logEvent("action", "healed everything: all brokers started, all links restored")
	case "audit":
		go d.runAudit(ctx)
	default:
		return fmt.Errorf("unknown action %q", req.Action)
	}
	return nil
}

// reapplyNetSoon re-sends the network state once restarted brokers are listening (a fresh
// process starts with a clean network).
func (d *Dash) reapplyNetSoon() {
	for range 10 {
		time.Sleep(500 * time.Millisecond)
		d.applyNet()
	}
}

type regionView struct {
	Name       string `json:"name"`
	Status     string `json:"status"` // up | degraded | isolated | down
	Controller string `json:"controller"`
	Up         int    `json:"up"`
	Total      int    `json:"total"`
}

type linkView struct {
	A        string  `json:"a"`
	B        string  `json:"b"`
	RTTms    float64 `json:"rtt_ms"` // best measured round trip, -1 if unreachable
	SlowedMs int64   `json:"slowed_ms"`
	Blocked  bool    `json:"blocked"`
	ViaAB    string  `json:"via_ab"` // region relaying A->B global traffic ("" = direct)
	ViaBA    string  `json:"via_ba"`
}

type streamView struct {
	Name      string    `json:"name"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Residency string    `json:"residency"`
	Home      string    `json:"home"`
	Rate      float64   `json:"rate"`
	ErrRate   float64   `json:"err_rate"`
	LastErr   string    `json:"last_err"`
	StalledMs int64     `json:"stalled_ms"`
	Series    []float64 `json:"series"`
}

type stateView struct {
	Now      time.Time    `json:"now"`
	Regions  []regionView `json:"regions"`
	Brokers  []brokerView `json:"brokers"`
	Links    []linkView   `json:"links"`
	Streams  []streamView `json:"streams"`
	Global   []globalView `json:"global"`
	Audit    auditView    `json:"audit"`
	Events   []event      `json:"events"`
	Total    float64      `json:"total_rate"`
	Isolated []string     `json:"isolated"`
}

func (d *Dash) state() stateView {
	d.mu.Lock()
	defer d.mu.Unlock()
	st := stateView{Now: time.Now(), Global: d.global, Audit: d.auditV}
	regions := d.p.Regions()
	for _, r := range regions {
		rv := regionView{Name: r}
		for _, b := range d.p.Region(r) {
			bv := d.brokers[b.ID]
			if bv.ID == "" {
				bv = brokerView{ID: b.ID, Region: b.Region, Zone: b.Zone, Addr: b.Addr}
			}
			st.Brokers = append(st.Brokers, bv)
			rv.Total++
			if bv.Running && bv.Healthy {
				rv.Up++
			}
			if bv.Controller {
				rv.Controller = bv.ID
			}
		}
		switch {
		case rv.Up == 0:
			rv.Status = "down"
		case d.isolated[r]:
			rv.Status = "isolated"
		case rv.Up < rv.Total:
			rv.Status = "degraded"
		default:
			rv.Status = "up"
		}
		if d.isolated[r] {
			st.Isolated = append(st.Isolated, r)
		}
		st.Regions = append(st.Regions, rv)
	}
	viaRegion := func(from, to string) string {
		// the relay most brokers in `from` use towards `to`
		count := map[string]int{}
		for _, a := range d.p.Region(from) {
			for _, b := range d.p.Region(to) {
				if l, ok := d.links[a.ID][b.ID]; ok && l.Via != "" {
					count[d.p.Broker(l.Via).Region]++
				}
			}
		}
		best, n := "", 0
		for r, c := range count {
			if c > n {
				best, n = r, c
			}
		}
		return best
	}
	for i := range regions {
		for j := i + 1; j < len(regions); j++ {
			a, b := regions[i], regions[j]
			lv := linkView{A: a, B: b, RTTms: -1, SlowedMs: d.slow[pairKey(a, b)], Blocked: d.isolated[a] || d.isolated[b]}
			for _, x := range d.p.Region(a) {
				for _, y := range d.p.Region(b) {
					if l, ok := d.links[x.ID][y.ID]; ok && l.Up && (lv.RTTms < 0 || l.RTTms < lv.RTTms) {
						lv.RTTms = l.RTTms
					}
				}
			}
			lv.ViaAB, lv.ViaBA = viaRegion(a, b), viaRegion(b, a)
			st.Links = append(st.Links, lv)
		}
	}
	for _, s := range d.streams {
		_, _, lastAck, lastErr := s.Counters()
		sv := streamView{Name: s.Name, Topic: s.Topic, Partition: s.Partition, Residency: s.Residency, Home: s.Home,
			ErrRate: d.errRate[s.Name], LastErr: lastErr, Series: append([]float64(nil), d.series[s.Name]...)}
		if n := len(sv.Series); n > 0 {
			sv.Rate = sv.Series[n-1]
		}
		if !lastAck.IsZero() {
			sv.StalledMs = time.Since(lastAck).Milliseconds()
		}
		st.Total += sv.Rate
		st.Streams = append(st.Streams, sv)
	}
	n := len(d.events)
	st.Events = append([]event(nil), d.events[max(0, n-40):]...)
	return st
}

// Handler serves the page and its JSON API.
func (d *Dash) Handler(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(d.state())
	})
	mux.HandleFunc("POST /api/action", func(w http.ResponseWriter, r *http.Request) {
		var req actionReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := d.act(ctx, req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	})
	return mux
}

var httpc = &http.Client{Timeout: 700 * time.Millisecond}

func getJSON(addr, path string, out any) error {
	resp, err := httpc.Get("http://" + addr + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Package chaos runs fault-injection scenarios against a cluster of real broker processes
// while a load generator writes through idempotent producers, then verifies every
// acknowledged message (loss, duplicates, order) and the residency audit.
package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/config"
)

// Procs manages broker processes started from the cluster config.
type Procs struct {
	Cluster  *config.Cluster
	Config   string // path of the cluster config
	Bin      string // hydra-broker binary
	DataRoot string
	LogDir   string
	Policies string   // signed policy document each broker installs at start ("" = none)
	Args     []string // extra broker flags (e.g. -coordination global for the baseline)

	mu      sync.Mutex
	cmds    map[string]*exec.Cmd
	capMbps float64
}

type BrokerRef struct {
	ID, Region, Zone, Addr string
}

func (p *Procs) Brokers() []BrokerRef {
	var out []BrokerRef
	for _, r := range p.Cluster.Regions {
		for _, b := range r.Brokers {
			out = append(out, BrokerRef{b.ID, r.Name, b.Zone, b.Addr})
		}
	}
	return out
}

func (p *Procs) Region(region string) []BrokerRef {
	var out []BrokerRef
	for _, b := range p.Brokers() {
		if b.Region == region {
			out = append(out, b)
		}
	}
	return out
}

func (p *Procs) Regions() []string {
	var out []string
	for _, r := range p.Cluster.Regions {
		out = append(out, r.Name)
	}
	return out
}

func (p *Procs) Broker(id string) BrokerRef {
	for _, b := range p.Brokers() {
		if b.ID == id {
			return b
		}
	}
	return BrokerRef{}
}

func (p *Procs) Start(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cmds == nil {
		p.cmds = map[string]*exec.Cmd{}
	}
	if c := p.cmds[id]; c != nil && c.ProcessState == nil {
		return nil
	}
	if err := os.MkdirAll(p.LogDir, 0o755); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(p.LogDir, id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	args := []string{"-config", p.Config, "-id", id, "-data", p.DataRoot}
	if p.Policies != "" {
		args = append(args, "-policies", p.Policies)
	}
	args = append(args, p.Args...)
	cmd := exec.Command(p.Bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	p.cmds[id] = cmd
	go func() { cmd.Wait(); logf.Close() }()
	return nil
}

// Kill hard-kills a broker (no graceful shutdown), like a crash.
func (p *Procs) Kill(id string) {
	p.mu.Lock()
	c := p.cmds[id]
	delete(p.cmds, id)
	p.mu.Unlock()
	if c != nil && c.Process != nil {
		c.Process.Kill()
	}
}

func (p *Procs) Up(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cmds[id] != nil
}

func (p *Procs) StartAll() error {
	for _, b := range p.Brokers() {
		if err := p.Start(b.ID); err != nil {
			return err
		}
	}
	return nil
}

func (p *Procs) KillAll() {
	for _, b := range p.Brokers() {
		p.Kill(b.ID)
	}
}

var httpc = &http.Client{Timeout: 3 * time.Second}

func getJSON(addr, path string, out any) error {
	resp, err := httpc.Get("http://" + addr + path)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("GET %s%s: HTTP %d", addr, path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func postJSON(addr, path string, in any) error {
	data, _ := json.Marshal(in)
	resp, err := httpc.Post("http://"+addr+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("POST %s%s: HTTP %d", addr, path, resp.StatusCode)
	}
	return nil
}

// WaitHealthy waits until every running broker answers and every region with a live majority
// has a controller.
func (p *Procs) WaitHealthy(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	global := slices.Contains(p.Args, "global") // baseline: one controller for every region
	for {
		ok, anyCtrl := true, false
		for _, region := range p.Regions() {
			up, ctrl := 0, false
			for _, b := range p.Region(region) {
				if !p.Up(b.ID) {
					continue
				}
				var st struct {
					IsController bool `json:"is_controller"`
				}
				if err := getJSON(b.Addr, "/admin/v1/status", &st); err != nil {
					ok = false
					continue
				}
				up++
				ctrl = ctrl || st.IsController
			}
			anyCtrl = anyCtrl || ctrl
			if up >= 2 && !ctrl && !global {
				ok = false
			}
		}
		if global && !anyCtrl {
			ok = false
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("cluster not healthy after %v", timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

type netState struct {
	Blocked      []string           `json:"blocked"`
	DelaysMs     map[string]int64   `json:"delays_ms,omitempty"`
	CapacityMbps map[string]float64 `json:"link_capacity_mbps,omitempty"`
}

// SetNet applies simulated network state on every running broker: blocked(a) lists the
// peers a can't reach, delay(a, b) the extra one-way delay on a->b.
func (p *Procs) SetNet(blocked func(a, b BrokerRef) bool, delayMs func(a, b BrokerRef) int64) {
	for _, a := range p.Brokers() {
		if !p.Up(a.ID) {
			continue
		}
		s := netState{Blocked: []string{}, DelaysMs: map[string]int64{}, CapacityMbps: map[string]float64{}}
		if mbps := p.linkMbps(); mbps > 0 {
			for _, r := range p.Regions() {
				if r != a.Region {
					s.CapacityMbps[r] = mbps
				}
			}
		}
		for _, b := range p.Brokers() {
			if a.ID == b.ID {
				continue
			}
			if blocked != nil && blocked(a, b) {
				s.Blocked = append(s.Blocked, b.ID)
			}
			if delayMs != nil {
				if d := delayMs(a, b); d > 0 {
					s.DelaysMs[b.ID] = d
				}
			}
		}
		sort.Strings(s.Blocked)
		_ = postJSON(a.Addr, "/admin/v1/net", s)
	}
}

func (p *Procs) HealNet() { p.SetNet(nil, nil) }

// SetLinkMbps caps every inter-region link at mbps (0 = unlimited) from the next SetNet on.
func (p *Procs) SetLinkMbps(mbps float64) {
	p.mu.Lock()
	p.capMbps = mbps
	p.mu.Unlock()
}

func (p *Procs) linkMbps() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.capMbps
}

type PartitionState struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Role      string `json:"role"`
	Leader    string `json:"leader"`
	Epoch     uint64 `json:"epoch"`
}

// Leader finds the broker currently leading topic/partition (asking live brokers).
func (p *Procs) Leader(topic string, part int) (BrokerRef, bool) {
	for _, b := range p.Brokers() {
		if !p.Up(b.ID) {
			continue
		}
		var ps []PartitionState
		if getJSON(b.Addr, "/admin/v1/partitions", &ps) != nil {
			continue
		}
		for _, s := range ps {
			if s.Topic == topic && s.Partition == part && s.Role == "leader" {
				return b, true
			}
		}
	}
	return BrokerRef{}, false
}

// Controller returns the controller responsible for region: its own (Hydra), or the single
// global controller wherever it runs (baseline).
func (p *Procs) Controller(region string) (BrokerRef, bool) {
	if b, ok := p.controllerIn(p.Region(region)); ok {
		return b, true
	}
	return p.controllerIn(p.Brokers())
}

func (p *Procs) controllerIn(bs []BrokerRef) (BrokerRef, bool) {
	for _, b := range bs {
		var st struct {
			IsController bool `json:"is_controller"`
		}
		if p.Up(b.ID) && getJSON(b.Addr, "/admin/v1/status", &st) == nil && st.IsController {
			return b, true
		}
	}
	return BrokerRef{}, false
}

type Link struct {
	To    string  `json:"to"`
	Up    bool    `json:"up"`
	RTTms float64 `json:"rtt_ms"`
	Via   string  `json:"via"`
}

func (p *Procs) Routes(id string) []Link {
	var out struct {
		Links []Link `json:"links"`
	}
	_ = getJSON(p.Broker(id).Addr, "/admin/v1/routes", &out)
	return out.Links
}

package broker

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/manifest"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/replica"
	"github.com/notaditya28/hydra-stream/internal/trail"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

// metrics are counters the broker keeps for /metrics (Prometheus text format).
type metrics struct {
	produced, proxied, policyDenied atomic.Int64
	cpClaims, cpCommits, cpFenced   atomic.Int64
}

func newMetrics() *metrics { return &metrics{} }

func (b *Broker) registerAdminExtra(mux *http.ServeMux) {
	// Signed policy documents: install (verified, versioned, persisted) and inspect.
	mux.HandleFunc("GET /admin/v1/policies", func(w http.ResponseWriter, r *http.Request) {
		cur := b.policy.Current()
		if cur == nil {
			httpapi.WriteError(w, policy.Unavailable("no signed policy document installed"))
			return
		}
		httpapi.WriteJSON(w, http.StatusOK, cur)
	})
	mux.HandleFunc("POST /admin/v1/policies", func(w http.ResponseWriter, r *http.Request) {
		var s policy.Signed
		if !httpapi.Decode(w, r, &s) {
			return
		}
		if err := b.policy.Install(&s); err != nil {
			e := api.Errorf(api.CodeInvalidRequest, "policy document refused: %v", err)
			e.Reason = "policy_rejected"
			httpapi.WriteError(w, e)
			return
		}
		if err := fsutil.WriteJSONAtomic(filepath.Join(b.cfg.DataDir, policyFile), &s); err != nil {
			httpapi.WriteError(w, err)
			return
		}
		b.trail.Record(trail.Decision{Broker: b.cfg.Info.ID, Hop: "policy-install", Policy: "document",
			PolicyVersion: s.Document.Version, Decision: trail.Allow, Reason: "signature verified"})
		httpapi.WriteJSON(w, http.StatusOK, map[string]any{"installed_version": b.policy.Current().Document.Version})
	})
	// The per-hop decision trail.
	mux.HandleFunc("GET /admin/v1/decisions", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		limit, _ := strconv.Atoi(q.Get("limit"))
		if limit <= 0 || limit > 2000 {
			limit = 100
		}
		denyOnly, topic := q.Get("deny") == "1", q.Get("topic")
		ds := b.trail.Recent(limit, func(d trail.Decision) bool {
			return (!denyOnly || d.Decision == trail.Deny) && (topic == "" || d.Topic == topic)
		})
		allows, denies := b.trail.Counts()
		httpapi.WriteJSON(w, http.StatusOK, map[string]any{"allows": allows, "denies": denies, "decisions": ds})
	})
	mux.HandleFunc("GET /admin/v1/forecast", func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, b.fc.snapshot())
	})
	mux.HandleFunc("GET /admin/v1/flow", func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, b.geo.FlowStats())
	})
	mux.HandleFunc("GET /admin/v1/groups", func(w http.ResponseWriter, r *http.Request) {
		httpapi.WriteJSON(w, http.StatusOK, b.cp.Groups())
	})
	// Compliance evidence: what this broker stores, under which policy version, and every
	// jurisdiction decision it made — signed with the broker's key.
	mux.HandleFunc("GET /admin/v1/compliance", func(w http.ResponseWriter, r *http.Request) {
		ev, err := b.compliance(r.URL.Query().Get("topic"))
		if err != nil {
			httpapi.WriteError(w, err)
			return
		}
		httpapi.WriteJSON(w, http.StatusOK, ev)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		b.writeMetrics(w)
	})
}

// Evidence is a signed compliance bundle.
type Evidence struct {
	Body      EvidenceBody `json:"body"`
	PublicKey string       `json:"public_key"`
	Signature string       `json:"signature"` // Ed25519 over the JSON encoding of Body
}

type EvidenceBody struct {
	Broker         string            `json:"broker"`
	Region         string            `json:"region"`
	GeneratedAt    time.Time         `json:"generated_at"`
	PolicyVersion  uint64            `json:"policy_version"`
	PolicySig      string            `json:"policy_signature,omitempty"`
	Topics         []api.TopicConfig `json:"topics"`
	Allows         int64             `json:"decisions_allowed"`
	Denies         int64             `json:"decisions_denied"`
	DeniesByReason map[string]int    `json:"recent_denies_by_reason"`
	RecentDenies   []trail.Decision  `json:"recent_denies"`
	ManifestSig    string            `json:"manifest_signature"`
	ManifestFiles  int               `json:"manifest_segments"`
}

func (b *Broker) compliance(topic string) (*Evidence, error) {
	man, err := manifest.Build(b.cfg.DataDir, b.cfg.Info, b.key)
	if err != nil {
		return nil, err
	}
	body := EvidenceBody{Broker: b.cfg.Info.ID, Region: b.cfg.Info.Region, GeneratedAt: time.Now().UTC(),
		DeniesByReason: map[string]int{}, ManifestSig: man.Signature, ManifestFiles: len(man.Manifest.Entries)}
	if cur := b.policy.Current(); cur != nil {
		body.PolicyVersion, body.PolicySig = cur.Document.Version, cur.Signature
	}
	for _, t := range b.coord.Committed().Topics {
		if topic == "" || t.Config.Name == topic {
			body.Topics = append(body.Topics, t.Config)
		}
	}
	sort.Slice(body.Topics, func(i, j int) bool { return body.Topics[i].Name < body.Topics[j].Name })
	body.Allows, body.Denies = b.trail.Counts()
	body.RecentDenies = b.trail.Recent(200, func(d trail.Decision) bool {
		return d.Decision == trail.Deny && (topic == "" || d.Topic == topic)
	})
	for _, d := range body.RecentDenies {
		body.DeniesByReason[d.Reason]++
	}
	data, _ := json.Marshal(body)
	return &Evidence{Body: body, PublicKey: hex.EncodeToString(b.key.Public().(ed25519.PublicKey)),
		Signature: hex.EncodeToString(ed25519.Sign(b.key, data))}, nil
}

func (b *Broker) writeMetrics(w io.Writer) {
	id := b.cfg.Info.ID
	l := fmt.Sprintf(`broker=%q,region=%q`, id, b.cfg.Info.Region)
	gauge := func(name, help string, v float64, extra string) {
		labels := l
		if extra != "" {
			labels += "," + extra
		}
		fmt.Fprintf(w, "# HELP %s %s\n%s{%s} %g\n", name, help, name, labels, v)
	}
	gauge("hydra_produced_records_total", "Records accepted from clients on this broker.", float64(b.stats.produced.Load()), "")
	gauge("hydra_proxied_records_total", "Records proxied from a transit-only region to a storage region.", float64(b.stats.proxied.Load()), "")
	gauge("hydra_policy_denied_requests_total", "Produce requests refused by policy evaluation.", float64(b.stats.policyDenied.Load()), "")
	allows, denies := b.trail.Counts()
	gauge("hydra_jurisdiction_decisions_total", "Recorded jurisdiction decisions.", float64(allows), `decision="allow"`)
	gauge("hydra_jurisdiction_decisions_total", "Recorded jurisdiction decisions.", float64(denies), `decision="deny"`)
	gauge("hydra_residency_rejections_total", "Moves of data blocked by residency/policy checks.", float64(b.guard.Rejected()), "")
	gauge("hydra_relayed_requests_total", "Requests this broker relayed between other regions.", float64(b.geo.Relayed()), "")
	gauge("hydra_checkpoint_claims_total", "Consumer-group ownership claims.", float64(b.stats.cpClaims.Load()), "")
	gauge("hydra_checkpoint_commits_total", "Granular checkpoint commits.", float64(b.stats.cpCommits.Load()), "")
	gauge("hydra_checkpoint_fenced_total", "Commits refused from stale (zombie) owners.", float64(b.stats.cpFenced.Load()), "")
	ver := 0.0
	if cur := b.policy.Current(); cur != nil {
		ver = float64(cur.Document.Version)
	}
	gauge("hydra_policy_version", "Installed signed policy document version (0 = none).", ver, "")
	ctl := 0.0
	if b.coord.IsController() {
		ctl = 1
	}
	gauge("hydra_is_controller", "1 if this broker is its region's controller.", ctl, "")
	var refs, saved int64
	leaders := 0
	for _, p := range b.parts.All() {
		r, s := p.DedupStats()
		refs, saved = refs+r, saved+s
		st := p.Status()
		if st.Role == replica.RoleLeader {
			leaders++
		}
		pl := fmt.Sprintf(`topic=%q,partition="%d"`, p.Key.Topic, p.Key.Partition)
		gauge("hydra_partition_high_watermark", "Committed offset of a partition replica.", float64(st.HW), pl)
		gauge("hydra_partition_end_offset", "Log end offset of a partition replica.", float64(st.EndOffset), pl)
	}
	gauge("hydra_partitions_led", "Partitions this broker leads.", float64(leaders), "")
	gauge("hydra_dedup_references_total", "Large values sent as references instead of bytes.", float64(refs), "")
	gauge("hydra_dedup_saved_bytes_total", "Inter-region bytes saved by payload dedup.", float64(saved), "")
	for region, st := range b.geo.FlowStats() {
		rl := fmt.Sprintf(`to_region=%q`, region)
		gauge("hydra_link_capacity_bytes_per_second", "Configured inter-region link capacity (0 = unlimited).", st.CapacityBps, rl)
		gauge("hydra_link_recovery_weight", "Adaptive controller's weight for catch-up traffic.", st.RecoveryWeight, rl)
		gauge("hydra_link_live_delay_ewma_ms", "Smoothed live-traffic queueing delay on the link.", st.LiveDelayEWMAms, rl)
		for class, cs := range st.Classes {
			cl := rl + fmt.Sprintf(`,class=%q`, class)
			gauge("hydra_link_bytes_total", "Bytes sent on the link per traffic class.", float64(cs.Bytes), cl)
			gauge("hydra_link_delay_p95_ms", "p95 queueing+transmit delay per traffic class.", cs.DelayP95, cl)
		}
	}
	fc := b.fc.snapshot()
	for _, r := range fc.Replication {
		rl := fmt.Sprintf(`topic=%q,partition="%d",follower=%q`, r.Topic, r.Partition, r.Follower)
		gauge("hydra_replication_backlog_records", "Records a follower is behind the leader.", float64(r.Backlog), rl)
		gauge("hydra_replication_clear_seconds", "Forecast time for the backlog to clear (-1 = not shrinking).", r.ClearSeconds, rl)
	}
	gauge("hydra_disk_used_bytes", "Bytes in this broker's data directory.", float64(fc.Disk.UsedBytes), "")
	gauge("hydra_disk_full_seconds", "Forecast time until the disk budget is reached (-1 = not growing/no budget).", fc.Disk.FullSeconds, "")
}

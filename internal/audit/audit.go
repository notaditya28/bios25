// Package audit proves (or disproves) that no resident data exists outside its region.
//
// Offline, it scans every broker data directory under the data root and checks each partition
// on disk against the broker's *configured* region (from the cluster config, not from what the
// broker says about itself). Online, it also fetches each broker's signed manifest, verifies
// the signature against a pinned key, applies the same rules to what the broker reports, and
// checks that the manifest matches the bytes on disk.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/layout"
	"github.com/notaditya28/hydra-stream/internal/manifest"
	"github.com/notaditya28/hydra-stream/internal/residency"
)

const (
	Violation = "violation"
	Warning   = "warning"
)

type Finding struct {
	Severity  string `json:"severity"`
	Broker    string `json:"broker"`
	Topic     string `json:"topic,omitempty"`
	Partition *int   `json:"partition,omitempty"`
	Rule      string `json:"rule"`
	Detail    string `json:"detail"`
}

type BrokerSummary struct {
	ID         string `json:"id"`
	Region     string `json:"region"`
	Zone       string `json:"zone"`
	DataDir    string `json:"data_dir"`
	Scanned    bool   `json:"scanned"`
	Partitions int    `json:"partitions"`
	Resident   int    `json:"resident_partitions"`
	Global     int    `json:"global_partitions"`
	Segments   int    `json:"segments"`
	Bytes      int64  `json:"bytes"`
	Manifest   string `json:"manifest"` // verified | unreachable | invalid | skipped
}

type Report struct {
	GeneratedAt time.Time       `json:"generated_at"`
	DataRoot    string          `json:"data_root"`
	Online      bool            `json:"online"`
	Brokers     []BrokerSummary `json:"brokers"`
	Findings    []Finding       `json:"findings"`
	Violations  int             `json:"violations"`
	Warnings    int             `json:"warnings"`
	Verdict     string          `json:"verdict"` // PASS | FAIL
}

type Options struct {
	DataRoot  string
	Online    bool          // also verify signed manifests from live brokers
	TrustFile string        // pinned broker public keys (trust on first use); "" = don't pin
	Timeout   time.Duration // per manifest request
}

func (r *Report) add(sev, broker, topic string, part *int, rule, format string, args ...any) {
	r.Findings = append(r.Findings, Finding{sev, broker, topic, part, rule, fmt.Sprintf(format, args...)})
	if sev == Violation {
		r.Violations++
	} else {
		r.Warnings++
	}
}

func intp(i int) *int { return &i }

// Run audits every broker in the cluster config.
func Run(ctx context.Context, cluster *config.Cluster, opts Options) (*Report, error) {
	if opts.Timeout == 0 {
		opts.Timeout = 10 * time.Second
	}
	rep := &Report{GeneratedAt: time.Now().UTC(), DataRoot: opts.DataRoot, Online: opts.Online, Findings: []Finding{}}
	regions := map[string]bool{}
	for _, r := range cluster.Regions {
		regions[r.Name] = true
	}
	trust := map[string]string{}
	if opts.Online && opts.TrustFile != "" {
		if err := fsutil.ReadJSON(opts.TrustFile, &trust); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read trust file: %w", err)
		}
	}
	trustChanged := false

	known := map[string]bool{}
	for _, region := range cluster.Regions {
		for _, bc := range region.Brokers {
			known[bc.ID] = true
			sum := BrokerSummary{ID: bc.ID, Region: region.Name, Zone: bc.Zone, DataDir: filepath.Join(opts.DataRoot, bc.ID), Manifest: "skipped"}
			// Fetch the manifest before scanning the disk: segments only grow between the two,
			// so the disk is never "smaller" than the manifest unless replication truncated it.
			var signed *manifest.Signed
			var fetchErr error
			if opts.Online {
				signed, fetchErr = fetchManifest(ctx, bc.Addr, opts.Timeout)
			}
			var scan *layout.BrokerScan
			if _, err := os.Stat(sum.DataDir); err == nil {
				s, err := layout.Scan(sum.DataDir)
				if err != nil {
					return nil, fmt.Errorf("scan %s: %w", sum.DataDir, err)
				}
				scan = s
				sum.Scanned = true
				checkScan(rep, &sum, bc.ID, region.Name, regions, scan)
			}
			if opts.Online {
				if checkManifest(rep, &sum, bc, region.Name, regions, scan, trust, signed, fetchErr) {
					trustChanged = true
				}
			}
			rep.Brokers = append(rep.Brokers, sum)
		}
	}

	// Data directories that don't belong to any configured broker.
	if entries, err := os.ReadDir(opts.DataRoot); err == nil {
		for _, e := range entries {
			if !e.IsDir() || known[e.Name()] {
				continue
			}
			s, err := layout.Scan(filepath.Join(opts.DataRoot, e.Name()))
			if err == nil && (s.HasData() || s.Identity != nil) {
				rep.add(Violation, e.Name(), "", nil, "unknown-broker",
					"data directory %s does not belong to any broker in the cluster config", filepath.Join(opts.DataRoot, e.Name()))
			}
		}
	}

	if trustChanged && opts.TrustFile != "" {
		if err := fsutil.WriteJSONAtomic(opts.TrustFile, trust); err != nil {
			return nil, fmt.Errorf("write trust file: %w", err)
		}
	}
	rep.Verdict = "PASS"
	if rep.Violations > 0 {
		rep.Verdict = "FAIL"
	}
	return rep, nil
}

func checkScan(rep *Report, sum *BrokerSummary, id, region string, regions map[string]bool, scan *layout.BrokerScan) {
	switch {
	case scan.Identity == nil && scan.HasData():
		rep.add(Violation, id, "", nil, "unidentified-data", "data directory has partitions but no readable %s", layout.IdentityFile)
	case scan.Identity != nil && (scan.Identity.ID != id || scan.Identity.Region != region):
		rep.add(Violation, id, "", nil, "identity-mismatch", "%s says broker %s in %s; cluster config says %s in %s",
			layout.IdentityFile, scan.Identity.ID, scan.Identity.Region, id, region)
	}
	for _, t := range scan.Topics {
		sum.Partitions += len(t.Partitions)
		sum.Segments += len(t.Segments)
		for _, s := range t.Segments {
			sum.Bytes += s.Bytes
		}
		if t.Meta == nil {
			if len(t.Partitions) > 0 {
				rep.add(Violation, id, t.Name, nil, "untagged-data", "partition data without %s (residency unknown)", layout.TopicMetaFile)
			}
			continue
		}
		res := t.Meta.Residency
		if res == api.ResidencyGlobal {
			sum.Global += len(t.Partitions)
		} else {
			sum.Resident += len(t.Partitions)
		}
		if res != api.ResidencyGlobal && !regions[res] {
			rep.add(Violation, id, t.Name, nil, "unknown-residency", "residency tag %q is neither global nor a cluster region", res)
			continue
		}
		if !residency.Allowed(res, region) {
			for _, p := range t.Partitions {
				rep.add(Violation, id, t.Name, intp(p), "residency",
					"data resident in %s is stored on %s, which is in region %s", res, id, region)
			}
		}
	}
}

// checkManifest verifies and checks a broker's fetched manifest. It reports whether trust changed.
func checkManifest(rep *Report, sum *BrokerSummary, bc config.Broker, region string,
	regions map[string]bool, scan *layout.BrokerScan, trust map[string]string, s *manifest.Signed, err error) bool {
	if err != nil {
		sum.Manifest = "unreachable"
		rep.add(Warning, bc.ID, "", nil, "manifest-unreachable", "could not fetch manifest (broker down?): %v; disk scan still applies", err)
		return false
	}
	changed := false
	pinned, ok := trust[bc.ID]
	switch {
	case !ok:
		trust[bc.ID] = s.Manifest.PublicKey
		pinned, changed = s.Manifest.PublicKey, true
		rep.add(Warning, bc.ID, "", nil, "key-pinned", "first audit of this broker: pinned its public key (trust on first use)")
	case pinned != s.Manifest.PublicKey:
		sum.Manifest = "invalid"
		rep.add(Violation, bc.ID, "", nil, "key-changed", "manifest is signed with a different key than the one pinned for %s", bc.ID)
		return false
	}
	if err := manifest.Verify(s, pinned); err != nil {
		sum.Manifest = "invalid"
		rep.add(Violation, bc.ID, "", nil, "bad-signature", "%v", err)
		return changed
	}
	m := s.Manifest
	if m.Broker != bc.ID || m.Region != region {
		rep.add(Violation, bc.ID, "", nil, "manifest-identity", "manifest says %s in %s; cluster config says %s in %s", m.Broker, m.Region, bc.ID, region)
	}
	sum.Manifest = "verified"

	type key struct {
		topic string
		part  int
		file  string
	}
	reported := map[key]bool{}
	for _, e := range m.Entries {
		reported[key{e.Topic, e.Partition, e.File}] = true
		if e.Residency == "" {
			rep.add(Violation, bc.ID, e.Topic, intp(e.Partition), "untagged-data", "manifest reports segment %s without a residency tag", e.File)
		} else if e.Residency != api.ResidencyGlobal && !regions[e.Residency] {
			rep.add(Violation, bc.ID, e.Topic, intp(e.Partition), "unknown-residency", "manifest reports residency %q", e.Residency)
		} else if !residency.Allowed(e.Residency, region) {
			rep.add(Violation, bc.ID, e.Topic, intp(e.Partition), "residency",
				"manifest reports data resident in %s on %s, which is in region %s", e.Residency, bc.ID, region)
		}
	}
	if scan == nil {
		return changed
	}

	// Cross-check the manifest with the bytes on disk.
	onDisk := map[key]layout.Segment{}
	lastSeg := map[string]string{} // topic/partition -> highest segment file
	for _, t := range scan.Topics {
		for _, sg := range t.Segments {
			onDisk[key{sg.Topic, sg.Partition, sg.File}] = sg
			tp := fmt.Sprintf("%s/%d", sg.Topic, sg.Partition)
			if sg.File > lastSeg[tp] {
				lastSeg[tp] = sg.File
			}
		}
	}
	for _, e := range m.Entries {
		sg, ok := onDisk[key{e.Topic, e.Partition, e.File}]
		active := lastSeg[fmt.Sprintf("%s/%d", e.Topic, e.Partition)] == e.File
		switch {
		case !ok:
			rep.add(Warning, bc.ID, e.Topic, intp(e.Partition), "manifest-stale", "segment %s in manifest is no longer on disk", e.File)
		case sg.Bytes < e.Bytes:
			rep.add(Warning, bc.ID, e.Topic, intp(e.Partition), "manifest-stale", "segment %s shrank since the manifest (replication truncation)", e.File)
		default:
			sumNow, err := manifest.HashPrefix(sg.Path, e.Bytes)
			if err == nil && sumNow == e.SHA256 {
				continue
			}
			if active {
				rep.add(Warning, bc.ID, e.Topic, intp(e.Partition), "manifest-stale", "active segment %s changed since the manifest", e.File)
			} else {
				rep.add(Violation, bc.ID, e.Topic, intp(e.Partition), "manifest-mismatch", "sealed segment %s on disk does not match the signed manifest", e.File)
			}
		}
	}
	for k := range onDisk {
		if !reported[k] {
			rep.add(Warning, bc.ID, k.topic, intp(k.part), "not-in-manifest", "segment %s on disk is not in the manifest (created after it?)", k.file)
		}
	}
	return changed
}

func fetchManifest(ctx context.Context, addr string, timeout time.Duration) (*manifest.Signed, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/admin/v1/manifest", nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var s manifest.Signed
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, err
	}
	return &s, nil
}

func human(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}

// WriteText prints a human-readable compliance report.
func (r *Report) WriteText(w io.Writer, showWarnings bool) {
	mode := "offline disk scan"
	if r.Online {
		mode = "disk scan + signed manifests"
	}
	fmt.Fprintf(w, "Hydra Stream residency audit  %s\ndata root: %s   mode: %s\n\n", r.GeneratedAt.Format(time.RFC3339), r.DataRoot, mode)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "BROKER\tREGION\tZONE\tPARTITIONS\tRESIDENT\tGLOBAL\tSEGMENTS\tBYTES\tMANIFEST")
	for _, b := range r.Brokers {
		if !b.Scanned {
			fmt.Fprintf(tw, "%s\t%s\t%s\t-\t-\t-\t-\tno data dir\t%s\n", b.ID, b.Region, b.Zone, b.Manifest)
			continue
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t%s\n", b.ID, b.Region, b.Zone, b.Partitions, b.Resident, b.Global, b.Segments, human(b.Bytes), b.Manifest)
	}
	tw.Flush()

	findings := append([]Finding(nil), r.Findings...)
	sort.SliceStable(findings, func(i, j int) bool { return findings[i].Severity > findings[j].Severity })
	shown := 0
	for _, f := range findings {
		if f.Severity == Warning && !showWarnings {
			continue
		}
		if shown == 0 {
			fmt.Fprintln(w, "\nFindings:")
		}
		shown++
		where := f.Broker
		if f.Topic != "" {
			where += " " + f.Topic
			if f.Partition != nil {
				where += fmt.Sprintf("/%d", *f.Partition)
			}
		}
		fmt.Fprintf(w, "  %-9s [%s] %s: %s\n", strings.ToUpper(f.Severity), f.Rule, where, f.Detail)
	}
	fmt.Fprintf(w, "\nVerdict: %s  (%d violations, %d warnings)\n", r.Verdict, r.Violations, r.Warnings)
}

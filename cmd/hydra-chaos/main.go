// Command hydra-chaos starts the whole cluster as local processes, runs fault-injection
// scenarios under load, verifies every acknowledged message and the residency audit, and
// prints a results table (also written to results/).
//
//	hydra-chaos                       # all scenarios against Hydra
//	hydra-chaos -only kill-region,slow-link
//	hydra-chaos -mode baseline        # the same scenarios against ONE global controller
//	hydra-chaos -mode compare         # both, side by side
//	hydra-chaos -keep                 # leave the cluster running afterwards
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/chaos"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

type suite struct {
	cfgPath, bin, dataRoot, policies string
	only                             []string
	keep                             bool
	opts                             chaos.Options
}

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config")
	bin := flag.String("broker", filepath.Join("bin", "hydra-broker.exe"), "broker binary")
	dataRoot := flag.String("data", "data-chaos", "data root (wiped at start)")
	out := flag.String("out", "results", "directory for the results files")
	only := flag.String("only", "", "comma-separated scenario names (default: all)")
	keep := flag.Bool("keep", false, "leave the cluster running when done")
	faulted := flag.String("region", "IN", "region targeted by region-level faults")
	hold := flag.Duration("hold", 6*time.Second, "how long each fault lasts")
	policySrc := flag.String("policies", "configs/policies.json", "policy document to sign and install on every broker (\"\" = none)")
	mode := flag.String("mode", "hydra", "hydra (one controller per region) | baseline (one global controller) | compare (both)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	s := suite{cfgPath: *cfgPath, bin: *bin, dataRoot: *dataRoot, policies: *policySrc, keep: *keep, opts: chaos.DefaultOptions()}
	if *only != "" {
		s.only = strings.Split(*only, ",")
	}
	s.opts.Faulted, s.opts.Hold = *faulted, *hold

	var modes []string
	switch *mode {
	case "hydra", "baseline":
		modes = []string{*mode}
	case "compare":
		modes = []string{"hydra", "baseline"}
		s.keep = false
	default:
		fail(fmt.Errorf("-mode must be hydra, baseline or compare"))
	}
	results := map[string][]*chaos.Result{}
	for _, m := range modes {
		rs, err := s.run(ctx, m)
		if err != nil {
			fail(err)
		}
		results[m] = rs
		fmt.Printf("\n%s\n", title(m))
		chaos.WriteTable(os.Stdout, rs)
		if ctx.Err() != nil {
			break
		}
	}

	if err := os.MkdirAll(*out, 0o755); err == nil {
		stamp := time.Now().Format("20060102-150405")
		name := "chaos-" + stamp
		switch *mode {
		case "baseline":
			name = "chaos-baseline-" + stamp
		case "compare":
			name = "compare-" + stamp
		}
		if f, err := os.Create(filepath.Join(*out, name+".md")); err == nil {
			if *mode == "compare" {
				writeComparison(f, results["hydra"], results["baseline"])
				fmt.Fprintln(f)
			}
			for _, m := range modes {
				if len(modes) > 1 {
					fmt.Fprintf(f, "\n## %s\n\n", title(m))
				}
				chaos.WriteMarkdown(f, results[m])
			}
			f.Close()
		}
		if data, err := json.MarshalIndent(results, "", "  "); err == nil {
			os.WriteFile(filepath.Join(*out, name+".json"), data, 0o644)
		}
		if *mode == "compare" {
			fmt.Println()
			writeComparison(os.Stdout, results["hydra"], results["baseline"])
		}
		fmt.Printf("\nresults written to %s\n", filepath.Join(*out, name+".{md,json}"))
	}
	// Only Hydra's results decide the exit code: the baseline is expected to fail some scenarios.
	for _, r := range results["hydra"] {
		if !r.Pass {
			os.Exit(1)
		}
	}
}

func title(mode string) string {
	if mode == "baseline" {
		return "Baseline: one global controller for all regions (naive)"
	}
	return "Hydra Stream: one controller per region + cross-region quorum"
}

// run starts a fresh cluster in the given coordination mode and runs the scenarios.
func (s suite) run(ctx context.Context, mode string) ([]*chaos.Result, error) {
	cluster, err := config.Load(s.cfgPath)
	if err != nil {
		return nil, err
	}
	dataRoot := s.dataRoot
	if mode == "baseline" {
		dataRoot += "-baseline"
	}
	if err := os.RemoveAll(dataRoot); err != nil {
		return nil, err
	}
	p := &chaos.Procs{Cluster: cluster, Config: s.cfgPath, Bin: s.bin, DataRoot: dataRoot, LogDir: filepath.Join(dataRoot, "logs")}
	if mode == "baseline" {
		p.Args = []string{"-coordination", "global"}
	}
	if s.policies != "" {
		signed, err := policy.EnsureSigned(cluster.PolicyKeyPath(), s.policies, strings.TrimSuffix(s.policies, ".json")+".signed.json")
		if err != nil {
			return nil, fmt.Errorf("policies: %w", err)
		}
		p.Policies = signed
	}
	slog.Info("starting cluster", "mode", mode)
	if err := p.StartAll(); err != nil {
		return nil, err
	}
	if !s.keep {
		defer p.KillAll()
	}
	if err := p.WaitHealthy(ctx, 60*time.Second); err != nil {
		return nil, err
	}
	if err := chaos.Setup(ctx, p); err != nil {
		return nil, err
	}
	opts := s.opts
	opts.TrustFile = filepath.Join(dataRoot, "audit-trust.json")

	var results []*chaos.Result
	for _, sc := range chaos.Scenarios() {
		if len(s.only) > 0 && !slices.Contains(s.only, sc.Name) {
			continue
		}
		r, err := chaos.RunScenario(ctx, p, sc, opts)
		if err != nil {
			slog.Error("scenario could not run", "mode", mode, "scenario", sc.Name, "err", err)
			continue
		}
		results = append(results, r)
		if ctx.Err() != nil {
			break
		}
	}
	return results, nil
}

func ms(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	return fmt.Sprintf("%d ms", d.Milliseconds())
}

// writeComparison puts the two designs side by side, scenario by scenario.
func writeComparison(w io.Writer, hydra, base []*chaos.Result) {
	byName := map[string]*chaos.Result{}
	for _, r := range base {
		byName[r.Scenario] = r
	}
	verdict := func(r *chaos.Result) string {
		if r.Pass {
			return "PASS"
		}
		return "FAIL"
	}
	fmt.Fprintln(w, "| Scenario | Max stall, surviving data (Hydra / baseline) | Max stall, global (Hydra / baseline) | Lost+dups (Hydra / baseline) | Result (Hydra / baseline) |")
	fmt.Fprintln(w, "|---|---|---|---|---|")
	for _, h := range hydra {
		b, ok := byName[h.Scenario]
		if !ok {
			continue
		}
		fmt.Fprintf(w, "| %s | %s / %s | %s / %s | %d / %d | %s / %s |\n", h.Scenario,
			ms(h.StallSurviving), ms(b.StallSurviving), ms(h.StallGlobal), ms(b.StallGlobal),
			h.Lost+h.Duplicates, b.Lost+b.Duplicates, verdict(h), verdict(b))
	}
	for _, b := range base {
		if !b.Pass {
			fmt.Fprintf(w, "\nBaseline %s failed: %s\n", b.Scenario, strings.Join(b.Failures, "; "))
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}

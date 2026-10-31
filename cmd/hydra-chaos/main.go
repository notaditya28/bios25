// Command hydra-chaos starts the whole cluster as local processes, runs fault-injection
// scenarios under load, verifies every acknowledged message and the residency audit, and
// prints a results table (also written to results/).
//
//	hydra-chaos                       # all scenarios
//	hydra-chaos -only kill-region,slow-link
//	hydra-chaos -keep                 # leave the cluster running afterwards
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/chaos"
	"github.com/notaditya28/hydra-stream/internal/config"
)

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config")
	bin := flag.String("broker", filepath.Join("bin", "hydra-broker.exe"), "broker binary")
	dataRoot := flag.String("data", "data-chaos", "data root (wiped at start)")
	out := flag.String("out", "results", "directory for the results files")
	only := flag.String("only", "", "comma-separated scenario names (default: all)")
	keep := flag.Bool("keep", false, "leave the cluster running when done")
	faulted := flag.String("region", "IN", "region targeted by region-level faults")
	hold := flag.Duration("hold", 6*time.Second, "how long each fault lasts")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cluster, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	if err := os.RemoveAll(*dataRoot); err != nil {
		fail(err)
	}
	p := &chaos.Procs{Cluster: cluster, Config: *cfgPath, Bin: *bin, DataRoot: *dataRoot, LogDir: filepath.Join(*dataRoot, "logs")}
	if err := p.StartAll(); err != nil {
		fail(err)
	}
	if !*keep {
		defer p.KillAll()
	}
	if err := p.WaitHealthy(ctx, 30*time.Second); err != nil {
		fail(err)
	}
	if err := chaos.Setup(ctx, p); err != nil {
		fail(err)
	}
	opts := chaos.DefaultOptions()
	opts.Faulted, opts.Hold = *faulted, *hold
	opts.TrustFile = filepath.Join(*dataRoot, "audit-trust.json")

	var results []*chaos.Result
	for _, sc := range chaos.Scenarios() {
		if *only != "" && !slices.Contains(strings.Split(*only, ","), sc.Name) {
			continue
		}
		r, err := chaos.RunScenario(ctx, p, sc, opts)
		if err != nil {
			slog.Error("scenario could not run", "scenario", sc.Name, "err", err)
			continue
		}
		results = append(results, r)
		if ctx.Err() != nil {
			break
		}
	}

	fmt.Println()
	chaos.WriteTable(os.Stdout, results)
	if err := os.MkdirAll(*out, 0o755); err == nil {
		stamp := time.Now().Format("20060102-150405")
		if f, err := os.Create(filepath.Join(*out, "chaos-"+stamp+".md")); err == nil {
			chaos.WriteMarkdown(f, results)
			f.Close()
		}
		if data, err := json.MarshalIndent(results, "", "  "); err == nil {
			os.WriteFile(filepath.Join(*out, "chaos-"+stamp+".json"), data, 0o644)
		}
		fmt.Printf("\nresults written to %s\n", filepath.Join(*out, "chaos-"+stamp+".{md,json}"))
	}
	for _, r := range results {
		if !r.Pass {
			os.Exit(1)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}

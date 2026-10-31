// Command residency_audit scans every broker's data directory (and, with -online, verifies each
// broker's signed segment manifest) and reports whether any resident data exists outside its
// region. Exit status: 0 = PASS, 1 = FAIL (violations found), 2 = the audit could not run.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/notaditya28/hydra-stream/internal/audit"
	"github.com/notaditya28/hydra-stream/internal/config"
)

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config file")
	dataRoot := flag.String("data", "data", "data root holding one directory per broker")
	online := flag.Bool("online", false, "also fetch and verify each live broker's signed manifest")
	trust := flag.String("trust", "", "pinned broker keys file (default <data>/audit-trust.json)")
	jsonOut := flag.String("json", "", "also write the report as JSON to this file")
	warnings := flag.Bool("warnings", false, "list warnings, not just violations")
	policies := flag.String("policies", "configs/policies.signed.json", "signed policy document for policy:<class> data")
	flag.Parse()

	cluster, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if *trust == "" {
		*trust = filepath.Join(*dataRoot, "audit-trust.json")
	}
	rep, err := audit.Run(context.Background(), cluster, audit.Options{DataRoot: *dataRoot, Online: *online, TrustFile: *trust, PolicyFile: *policies})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	rep.WriteText(os.Stdout, *warnings)
	if *jsonOut != "" {
		data, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(*jsonOut, data, 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(2)
		}
	}
	if rep.Verdict != "PASS" {
		os.Exit(1)
	}
}

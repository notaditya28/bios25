// Command hydra-broker runs one broker from the cluster config.
package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/broker"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/flow"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config file")
	id := flag.String("id", "", "broker id from the cluster config (required)")
	dataRoot := flag.String("data", "data", "data root; this broker uses <data>/<id>")
	segBytes := flag.Int64("segment-bytes", seglog.DefaultSegmentBytes, "roll segments at this size")
	fsync := flag.Bool("fsync", false, "fsync every append")
	flowMode := flag.String("flow", "adaptive", "inter-region bandwidth scheduling: adaptive | fifo | fixed")
	diskMB := flag.Int64("disk-budget-mb", 10240, "storage budget for time-to-full forecasts")
	noDedup := flag.Bool("no-dedup", false, "disable wire-level payload dedup for cross-region replication")
	policies := flag.String("policies", "", "signed policy document to install at start if newer than the stored one (signature is verified)")
	flag.Parse()

	opts := options{flow: flow.Config{Mode: flow.Mode(*flowMode)}, diskBudget: *diskMB << 20, noDedup: *noDedup, policies: *policies}
	if err := run(*cfgPath, *id, *dataRoot, seglog.Config{SegmentBytes: *segBytes, Fsync: *fsync}, opts); err != nil {
		slog.Error("broker failed", "err", err)
		os.Exit(1)
	}
}

type options struct {
	flow       flow.Config
	diskBudget int64
	noDedup    bool
	policies   string
}

func run(cfgPath, id, dataRoot string, logCfg seglog.Config, opts options) error {
	cluster, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	var policyKey ed25519.PublicKey
	if p := cluster.PolicyKeyPath(); p != "" {
		if k, err := policy.LoadPublicKey(p); err == nil {
			policyKey = k
		} else {
			slog.Warn("no policy public key: only built-in residency rules will work", "file", p, "err", err)
		}
	}
	bc, region, ok := cluster.Find(id)
	if !ok {
		return errors.New("broker id " + id + " not found in " + cfgPath)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)).With("broker", id, "region", region.Name))

	peers := map[string]string{}
	for _, p := range region.Brokers {
		peers[p.ID] = p.Addr
	}
	directory, addrs := map[string]api.Info{}, map[string]string{}
	for _, r := range cluster.Regions {
		for _, p := range r.Brokers {
			directory[p.ID] = api.Info{ID: p.ID, Region: r.Name, Zone: p.Zone}
			addrs[p.ID] = p.Addr
		}
	}
	b, err := broker.Open(broker.Config{
		Info:       api.Info{ID: id, Region: region.Name, Zone: bc.Zone},
		DataDir:    filepath.Join(dataRoot, id),
		Regions:    cluster.RegionNames(),
		Peers:      peers,
		Directory:  directory,
		Addrs:      addrs,
		Log:        logCfg,
		PolicyKey:  policyKey,
		Flow:       opts.flow,
		DiskBudget: opts.diskBudget,
		NoDedup:    opts.noDedup,
	})
	if err != nil {
		return err
	}
	defer b.Close()
	if opts.policies != "" {
		var s policy.Signed
		if err := fsutil.ReadJSON(opts.policies, &s); err != nil {
			slog.Warn("policy file not loaded", "file", opts.policies, "err", err)
		} else if cur := b.Policy().Current(); cur == nil || cur.Document.Version < s.Document.Version {
			if err := b.InstallPolicy(&s); err != nil {
				slog.Warn("policy file refused", "file", opts.policies, "err", err)
			} else {
				slog.Info("policy document installed", "version", s.Document.Version, "classes", len(s.Document.Policies))
			}
		}
	}

	srv := &http.Server{Addr: bc.Addr, Handler: b.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	b.Start()
	slog.Info("broker listening", "addr", bc.Addr, "zone", bc.Zone, "peers", len(peers))

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

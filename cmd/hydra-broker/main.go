// Command hydra-broker runs one broker from the cluster config.
package main

import (
	"context"
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
	"github.com/notaditya28/hydra-stream/internal/seglog"
	"github.com/notaditya28/hydra-stream/internal/transport/httpapi"
)

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config file")
	id := flag.String("id", "", "broker id from the cluster config (required)")
	dataRoot := flag.String("data", "data", "data root; this broker uses <data>/<id>")
	segBytes := flag.Int64("segment-bytes", seglog.DefaultSegmentBytes, "roll segments at this size")
	fsync := flag.Bool("fsync", false, "fsync every append")
	flag.Parse()

	if err := run(*cfgPath, *id, *dataRoot, seglog.Config{SegmentBytes: *segBytes, Fsync: *fsync}); err != nil {
		slog.Error("broker failed", "err", err)
		os.Exit(1)
	}
}

func run(cfgPath, id, dataRoot string, logCfg seglog.Config) error {
	cluster, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	bc, region, ok := cluster.Find(id)
	if !ok {
		return errors.New("broker id " + id + " not found in " + cfgPath)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)).With("broker", id, "region", region.Name))

	b, err := broker.Open(broker.Config{
		Info:    api.Info{ID: id, Region: region.Name, Zone: bc.Zone},
		DataDir: filepath.Join(dataRoot, id),
		Regions: cluster.RegionNames(),
		Log:     logCfg,
	})
	if err != nil {
		return err
	}
	defer b.Close()

	srv := &http.Server{Addr: bc.Addr, Handler: httpapi.NewHandler(b), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("broker listening", "addr", bc.Addr, "zone", bc.Zone)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

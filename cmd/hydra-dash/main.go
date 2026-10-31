// Command hydra-dash is the one command that runs the whole demo: it starts every broker in the
// cluster config as a local process, creates the demo topics, runs a steady background load and
// serves the dashboard (default http://localhost:8080).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/chaos"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/dash"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

func main() {
	cfgPath := flag.String("config", "configs/cluster.local.json", "cluster config")
	bin := flag.String("broker", filepath.Join("bin", "hydra-broker.exe"), "broker binary")
	dataRoot := flag.String("data", "data", "data root (wiped at start unless -keep-data)")
	keepData := flag.Bool("keep-data", false, "keep existing broker data")
	addr := flag.String("addr", "127.0.0.1:8080", "dashboard listen address")
	open := flag.Bool("open", true, "open the dashboard in a browser")
	rate := flag.Int("rate", 1000, "messages per second per load stream")
	policySrc := flag.String("policies", "configs/policies.json", "policy document to sign and install on every broker (\"\" = none)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	cluster, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	if _, err := os.Stat(*bin); err != nil {
		fail(fmt.Errorf("broker binary %s not found (run: go build -o bin/ ./cmd/...)", *bin))
	}
	if !*keepData {
		if err := os.RemoveAll(*dataRoot); err != nil {
			fail(err)
		}
	}
	p := &chaos.Procs{Cluster: cluster, Config: *cfgPath, Bin: *bin, DataRoot: *dataRoot, LogDir: filepath.Join(*dataRoot, "logs")}
	if *policySrc != "" {
		signed, err := policy.EnsureSigned(cluster.PolicyKeyPath(), *policySrc, strings.TrimSuffix(*policySrc, ".json")+".signed.json")
		if err != nil {
			fmt.Fprintln(os.Stderr, "policies:", err)
			os.Exit(2)
		}
		p.Policies = signed
	}
	if err := p.StartAll(); err != nil {
		fail(err)
	}
	defer p.KillAll()
	slog.Info("brokers starting", "count", len(p.Brokers()))
	if err := p.WaitHealthy(ctx, 30*time.Second); err != nil {
		fail(err)
	}
	if err := chaos.Setup(ctx, p); err != nil {
		fail(err)
	}

	if p.Policies != "" {
		go policyDemo(ctx, p)
	}
	d := dash.New(p, filepath.Join(*dataRoot, "audit-trust.json"))
	const batch = 20
	interval := time.Duration(float64(time.Second) * batch / float64(max(*rate, 1)))
	d.Run(ctx, batch, interval)

	srv := &http.Server{Addr: *addr, Handler: d.Handler(ctx), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		srv.Shutdown(sctx)
	}()
	url := "http://" + *addr
	fmt.Printf("\nHydra Stream dashboard: %s   (Ctrl+C stops the dashboard and every broker)\n\n", url)
	if *open {
		openBrowser(url)
	}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fail(err)
	}
}

func openBrowser(url string) {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	case "darwin":
		cmd = exec.Command("open", url)
	default:
		cmd = exec.Command("xdg-open", url)
	}
	_ = cmd.Start()
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(2)
}

// policyDemo creates topics under signed policy classes and keeps exercising them: allowed
// payments from IN and EU, and a US client that keeps trying to send financial data (denied
// at entry: US may neither store nor carry it), so the dashboard shows live decisions.
func policyDemo(ctx context.Context, p *chaos.Procs) {
	topics := []api.TopicConfig{
		{Name: "payments", Partitions: 2, Residency: "policy:financial"},
		{Name: "eu-profiles", Partitions: 1, Residency: "policy:eu-personal"},
		{Name: "sensor-telemetry", Partitions: 3, Residency: "policy:telemetry"},
	}
	first := func(region string) string { return p.Region(region)[0].Addr }
	for _, t := range topics {
		home := "EU"
		for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline) && ctx.Err() == nil; time.Sleep(300 * time.Millisecond) {
			if _, err := client.New(first(home)).CreateTopic(ctx, t); err == nil || api.CodeOf(err) == api.CodeTopicExists {
				break
			}
		}
	}
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	for i := 0; ctx.Err() == nil; i++ {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
		}
		for _, r := range []string{"IN", "EU", "US"} {
			for _, b := range p.Region(r) {
				if !p.Up(b.ID) {
					continue
				}
				c := client.New(b.Addr)
				cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
				part := i % 2
				c.ProduceRequest(cctx, "payments", api.ProduceRequest{Records: []api.ProduceRecord{{Partition: &part, Value: fmt.Sprintf("txn-%s-%d", r, i)}}})
				c.ProduceRequest(cctx, "eu-profiles", api.ProduceRequest{Records: []api.ProduceRecord{{Value: fmt.Sprintf("profile-%s-%d", r, i)}}})
				cancel()
				break
			}
		}
	}
}

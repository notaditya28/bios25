// Command hydra-policy manages signed jurisdiction policies.
//
//	hydra-policy init                       create the admin key pair (configs/policy-admin.key/.pub)
//	hydra-policy sign  [-in configs/policies.json] [-out configs/policies.signed.json] [-ttl 720h]
//	hydra-policy ensure                     init + sign only if missing, stale or expiring
//	hydra-policy push  [-signed configs/policies.signed.json]   install on every broker in the config
//	hydra-policy show  [-b 127.0.0.1:9101]                       print a broker's installed policy
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/notaditya28/hydra-stream/internal/config"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/policy"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: hydra-policy init|sign|ensure|push|show [flags]")
		os.Exit(2)
	}
	fs := flag.NewFlagSet(os.Args[1], flag.ExitOnError)
	key := fs.String("key", "configs/policy-admin.key", "admin private key (keep offline in production)")
	pub := fs.String("pub", "configs/policy-admin.pub", "admin public key (brokers read this)")
	in := fs.String("in", "configs/policies.json", "policy document to sign")
	out := fs.String("out", "configs/policies.signed.json", "signed output")
	signed := fs.String("signed", "configs/policies.signed.json", "signed document to push")
	ttl := fs.Duration("ttl", 30*24*time.Hour, "validity of a newly signed document")
	cfgPath := fs.String("config", "configs/cluster.local.json", "cluster config")
	addr := fs.String("b", "127.0.0.1:9101", "broker for show")
	fs.Parse(os.Args[2:])

	var err error
	switch os.Args[1] {
	case "init":
		_, err = policy.LoadOrCreateAdminKey(*key, *pub)
		if err == nil {
			fmt.Println("admin key ready:", *key, "(public:", *pub+")")
		}
	case "sign":
		var k []byte
		if k, err = policy.LoadOrCreateAdminKey(*key, *pub); err == nil {
			var s *policy.Signed
			if s, err = policy.SignFile(*in, k, *ttl); err == nil {
				if err = fsutil.WriteJSONAtomic(*out, s); err == nil {
					fmt.Printf("signed %d policies as version %d (expires %s) -> %s\n",
						len(s.Document.Policies), s.Document.Version, s.Document.ExpiresAt.Format(time.RFC3339), *out)
				}
			}
		}
	case "ensure": // key pair + signed document, only re-signing when needed (used by scripts)
		var cluster *config.Cluster
		if cluster, err = config.Load(*cfgPath); err == nil {
			var path string
			if path, err = policy.EnsureSigned(cluster.PolicyKeyPath(), *in, *out); err == nil && path != "" {
				fmt.Println("signed policies ready:", path)
			}
		}
	case "push":
		err = push(*cfgPath, *signed)
	case "show":
		var resp *http.Response
		if resp, err = http.Get("http://" + *addr + "/admin/v1/policies"); err == nil {
			defer resp.Body.Close()
			io.Copy(os.Stdout, resp.Body)
		}
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func push(cfgPath, path string) error {
	cluster, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var s policy.Signed
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	failed := 0
	for _, r := range cluster.Regions {
		for _, b := range r.Brokers {
			resp, err := http.Post("http://"+b.Addr+"/admin/v1/policies", "application/json", bytes.NewReader(data))
			if err != nil {
				fmt.Printf("  %-6s unreachable (%v)\n", b.ID, err)
				failed++
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 {
				fmt.Printf("  %-6s REFUSED: %s\n", b.ID, bytes.TrimSpace(body))
				failed++
				continue
			}
			fmt.Printf("  %-6s installed version %d\n", b.ID, s.Document.Version)
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d brokers did not install the document", failed)
	}
	return nil
}

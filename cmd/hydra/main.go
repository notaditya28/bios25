// Command hydra is a small CLI for the broker: topics, produce and consume.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/client"
)

const usage = `usage: hydra <command> [flags]

commands:
  info          show broker identity
  topics        list topics
  create-topic  -name NAME -partitions N -residency IN|EU|US|global
  produce       -topic NAME [-n 10000 -batch 1000 -size 100 -keys] | [-value V -key K]
  consume       -topic NAME -partition P [-offset 0 -max 0 -follow -quiet]

every command takes -b HOST:PORT (default 127.0.0.1:9101)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := fs.String("b", "127.0.0.1:9101", "broker address")
	var run func(context.Context, *client.Client) error
	switch cmd {
	case "info":
		run = func(ctx context.Context, c *client.Client) error {
			info, err := c.Info(ctx)
			return printJSON(info, err)
		}
	case "topics":
		run = func(ctx context.Context, c *client.Client) error {
			ts, err := c.Topics(ctx)
			return printJSON(ts, err)
		}
	case "create-topic":
		name := fs.String("name", "", "topic name")
		parts := fs.Int("partitions", 3, "partition count")
		res := fs.String("residency", api.ResidencyGlobal, "region name or global")
		run = func(ctx context.Context, c *client.Client) error {
			info, err := c.CreateTopic(ctx, api.TopicConfig{Name: *name, Partitions: *parts, Residency: *res})
			return printJSON(info, err)
		}
	case "produce":
		run = produceCmd(fs)
	case "consume":
		run = consumeCmd(fs)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	fs.Parse(args)
	if err := run(context.Background(), client.New(*addr)); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func printJSON(v any, err error) error {
	if err != nil {
		return err
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func produceCmd(fs *flag.FlagSet) func(context.Context, *client.Client) error {
	topic := fs.String("topic", "", "topic name")
	n := fs.Int("n", 10000, "messages to produce")
	batch := fs.Int("batch", 1000, "messages per request")
	size := fs.Int("size", 100, "value size in bytes")
	keys := fs.Bool("keys", false, "give each message a key (spreads across partitions by hash)")
	value := fs.String("value", "", "produce a single message with this value instead")
	key := fs.String("key", "", "key for -value")
	return func(ctx context.Context, c *client.Client) error {
		if *value != "" {
			resp, err := c.Produce(ctx, *topic, []api.ProduceRecord{{Key: *key, Value: *value}})
			return printJSON(resp, err)
		}
		pad := strings.Repeat("x", max(*size, 0))
		start := time.Now()
		for sent := 0; sent < *n; {
			recs := make([]api.ProduceRecord, min(*batch, *n-sent))
			for i := range recs {
				v := fmt.Sprintf("msg-%d-", sent+i)
				if len(v) < *size {
					v += pad[:*size-len(v)]
				}
				recs[i].Value = v
				if *keys {
					recs[i].Key = fmt.Sprintf("k%d", sent+i)
				}
			}
			if _, err := c.Produce(ctx, *topic, recs); err != nil {
				return fmt.Errorf("after %d messages: %w", sent, err)
			}
			sent += len(recs)
		}
		el := time.Since(start)
		fmt.Printf("produced %d messages in %v: %.0f msg/s, %.1f MB/s\n",
			*n, el.Round(time.Millisecond), float64(*n)/el.Seconds(), float64(*n**size)/el.Seconds()/1e6)
		return nil
	}
}

func consumeCmd(fs *flag.FlagSet) func(context.Context, *client.Client) error {
	topic := fs.String("topic", "", "topic name")
	part := fs.Int("partition", 0, "partition")
	offset := fs.Uint64("offset", 0, "start offset")
	maxN := fs.Int("max", 0, "stop after this many messages (0 = until caught up, or forever with -follow)")
	follow := fs.Bool("follow", false, "keep waiting for new messages")
	quiet := fs.Bool("quiet", false, "only print a summary")
	return func(ctx context.Context, c *client.Client) error {
		off, got := *offset, 0
		start := time.Now()
		for *maxN == 0 || got < *maxN {
			req := api.FetchRequest{Topic: *topic, Partition: *part, Offset: off, MaxRecords: 1000}
			if *follow {
				req.Wait = 5 * time.Second
			}
			resp, err := c.Fetch(ctx, req)
			if err != nil {
				return err
			}
			if len(resp.Records) == 0 && !*follow {
				break
			}
			for _, r := range resp.Records {
				if !*quiet {
					fmt.Printf("%d\t%s\t%s\n", r.Offset, r.Key, r.Value)
				}
				got++
				off = r.Offset + 1
				if *maxN > 0 && got >= *maxN {
					break
				}
			}
		}
		el := time.Since(start)
		fmt.Fprintf(os.Stderr, "consumed %d messages from %s/%d in %v (%.0f msg/s), next offset %d\n",
			got, *topic, *part, el.Round(time.Millisecond), float64(got)/el.Seconds(), off)
		return nil
	}
}

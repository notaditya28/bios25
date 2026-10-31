package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/broker"
	"github.com/notaditya28/hydra-stream/internal/client"
	"github.com/notaditya28/hydra-stream/internal/coord"
)

func setup(t *testing.T) (*client.Client, string) {
	t.Helper()
	// A one-broker region: it elects itself controller after one (short) lease period.
	b, err := broker.Open(broker.Config{
		Info:    api.Info{ID: "in-1", Region: "IN", Zone: "in-a"},
		DataDir: t.TempDir(),
		Regions: []string{"IN", "EU", "US"},
		Coord:   coord.Timing{LeaseDuration: 200 * time.Millisecond, RenewInterval: 30 * time.Millisecond, FailTimeout: 100 * time.Millisecond, RPCTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(b.Handler())
	b.Start()
	t.Cleanup(func() { srv.Close(); b.Close() })
	for !b.Coordinator().IsController() {
		time.Sleep(10 * time.Millisecond)
	}
	return client.New(srv.URL), srv.URL
}

func TestProduceConsumeOverHTTP(t *testing.T) {
	c, _ := setup(t)
	ctx := context.Background()
	if _, err := c.CreateTopic(ctx, api.TopicConfig{Name: "orders", Partitions: 2, Residency: "IN"}); err != nil {
		t.Fatal(err)
	}
	p := 1
	recs := make([]api.ProduceRecord, 5000)
	for i := range recs {
		recs[i] = api.ProduceRecord{Partition: &p, Key: "k", Value: "value with \"quotes\" and ünïcode"}
	}
	resp, err := c.Produce(ctx, "orders", recs)
	if err != nil {
		t.Fatal(err)
	}
	// Offset 0 holds the leader's (hidden) epoch marker, so user records start at 1.
	first := resp.Offsets[0].Offset
	if len(resp.Offsets) != 5000 || resp.Offsets[4999].Offset != first+4999 {
		t.Fatalf("offsets = %d, first %d, last %+v", len(resp.Offsets), first, resp.Offsets[len(resp.Offsets)-1])
	}

	var got int
	for off := uint64(0); ; {
		fr, err := c.Fetch(ctx, api.FetchRequest{Topic: "orders", Partition: 1, Offset: off, MaxRecords: 700})
		if err != nil {
			t.Fatal(err)
		}
		if len(fr.Records) == 0 {
			break
		}
		for _, r := range fr.Records {
			if r.Offset != first+uint64(got) || r.Value != recs[0].Value || r.Key != "k" {
				t.Fatalf("record %+v, want offset %d", r, first+uint64(got))
			}
			got++
		}
		off = fr.NextOffset
	}
	if got != 5000 {
		t.Fatalf("consumed %d, want 5000", got)
	}
}

func TestErrorsAreTyped(t *testing.T) {
	c, url := setup(t)
	ctx := context.Background()
	_, err := c.Fetch(ctx, api.FetchRequest{Topic: "missing"})
	if api.CodeOf(err) != api.CodeUnknownTopic {
		t.Fatalf("got %v", err)
	}
	c.CreateTopic(ctx, api.TopicConfig{Name: "t", Partitions: 1, Residency: "global"})
	_, err = c.Fetch(ctx, api.FetchRequest{Topic: "t", Offset: 7})
	if api.CodeOf(err) != api.CodeOffsetOutOfRange {
		t.Fatalf("got %v", err)
	}

	resp, err := http.Post(url+"/v1/topics/t/records", "application/json", strings.NewReader(`{"records":[{"value":"x","bogus":1}]}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field: status %d", resp.StatusCode)
	}
}

func TestLongPoll(t *testing.T) {
	c, _ := setup(t)
	ctx := context.Background()
	c.CreateTopic(ctx, api.TopicConfig{Name: "t", Partitions: 1, Residency: "global"})

	// Times out quickly with an empty response when nothing arrives.
	start := time.Now()
	fr, err := c.Fetch(ctx, api.FetchRequest{Topic: "t", Wait: 100 * time.Millisecond})
	if err != nil || len(fr.Records) != 0 {
		t.Fatalf("fetch = %+v, %v", fr, err)
	}
	if el := time.Since(start); el < 90*time.Millisecond || el > 2*time.Second {
		t.Fatalf("long poll took %v", el)
	}

	// Returns as soon as a record is produced.
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.Produce(ctx, "t", []api.ProduceRecord{{Value: "late"}})
	}()
	start = time.Now()
	fr, err = c.Fetch(ctx, api.FetchRequest{Topic: "t", Wait: 10 * time.Second})
	if err != nil || len(fr.Records) != 1 || fr.Records[0].Value != "late" {
		t.Fatalf("fetch = %+v, %v", fr, err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("long poll did not wake promptly: %v", el)
	}
}

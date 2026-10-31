package broker

import (
	"context"
	"strings"
	"testing"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

var ctx = context.Background()

func testConfig(dir string) Config {
	return Config{
		Info:    api.Info{ID: "in-1", Region: "IN", Zone: "in-a"},
		DataDir: dir,
		Regions: []string{"IN", "EU", "US"},
		Log:     seglog.Config{SegmentBytes: 4096},
	}
}

func open(t *testing.T, cfg Config) *Broker {
	t.Helper()
	b, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func wantCode(t *testing.T, err error, code api.Code) {
	t.Helper()
	if api.CodeOf(err) != code {
		t.Fatalf("got %v, want %s", err, code)
	}
}

func TestCreateTopicValidation(t *testing.T) {
	b := open(t, testConfig(t.TempDir()))
	defer b.Close()

	info, err := b.CreateTopic(ctx, api.TopicConfig{Name: "orders", Partitions: 3, Residency: "in"})
	if err != nil {
		t.Fatal(err)
	}
	if info.Residency != "IN" || len(info.PartitionStates) != 3 {
		t.Fatalf("info = %+v", info)
	}
	// Identical create is idempotent; a different config conflicts.
	if _, err := b.CreateTopic(ctx, api.TopicConfig{Name: "orders", Partitions: 3, Residency: "IN"}); err != nil {
		t.Fatalf("idempotent create: %v", err)
	}
	_, err = b.CreateTopic(ctx, api.TopicConfig{Name: "orders", Partitions: 3, Residency: "global"})
	wantCode(t, err, api.CodeTopicExists)

	for _, bad := range []api.TopicConfig{
		{Name: "x", Partitions: 1, Residency: "MARS"},
		{Name: "x", Partitions: 0, Residency: "global"},
		{Name: "../escape", Partitions: 1, Residency: "global"},
		{Name: "Upper", Partitions: 1, Residency: "global"},
		{Name: "", Partitions: 1, Residency: "global"},
	} {
		_, err := b.CreateTopic(ctx, bad)
		wantCode(t, err, api.CodeInvalidRequest)
	}
}

func TestProducePartitioning(t *testing.T) {
	b := open(t, testConfig(t.TempDir()))
	defer b.Close()
	b.CreateTopic(ctx, api.TopicConfig{Name: "t", Partitions: 4, Residency: "global"})

	// Same key always lands on the same partition, with increasing offsets.
	var recs []api.ProduceRecord
	for i := 0; i < 10; i++ {
		recs = append(recs, api.ProduceRecord{Key: "user-42", Value: "v"})
	}
	resp, err := b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: recs})
	if err != nil {
		t.Fatal(err)
	}
	p := resp.Offsets[0].Partition
	for i, o := range resp.Offsets {
		if o.Partition != p || o.Offset != uint64(i) {
			t.Fatalf("offset %d = %+v", i, o)
		}
	}

	// Explicit partition wins; out of range is rejected.
	two := 2
	resp, _ = b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: []api.ProduceRecord{{Partition: &two, Key: "user-42", Value: "v"}}})
	if resp.Offsets[0].Partition != 2 {
		t.Fatalf("explicit partition ignored: %+v", resp.Offsets[0])
	}
	nine := 9
	_, err = b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: []api.ProduceRecord{{Partition: &nine, Value: "v"}}})
	wantCode(t, err, api.CodeUnknownPartition)

	// Keyless batches are sticky within a request and rotate across requests.
	seen := map[int]bool{}
	for i := 0; i < 8; i++ {
		resp, _ := b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: []api.ProduceRecord{{Value: "a"}, {Value: "b"}}})
		if resp.Offsets[0].Partition != resp.Offsets[1].Partition {
			t.Fatal("keyless batch split across partitions")
		}
		seen[resp.Offsets[0].Partition] = true
	}
	if len(seen) != 4 {
		t.Fatalf("round robin hit %d partitions, want 4", len(seen))
	}

	_, err = b.Produce(ctx, api.ProduceRequest{Topic: "nope", Records: recs})
	wantCode(t, err, api.CodeUnknownTopic)
	_, err = b.Produce(ctx, api.ProduceRequest{Topic: "t"})
	wantCode(t, err, api.CodeInvalidRequest)
}

func TestFetch(t *testing.T) {
	b := open(t, testConfig(t.TempDir()))
	defer b.Close()
	b.CreateTopic(ctx, api.TopicConfig{Name: "t", Partitions: 1, Residency: "IN"})
	var recs []api.ProduceRecord
	for i := 0; i < 50; i++ {
		recs = append(recs, api.ProduceRecord{Value: strings.Repeat("v", i)})
	}
	b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: recs})

	resp, err := b.Fetch(ctx, api.FetchRequest{Topic: "t", Offset: 10, MaxRecords: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Records) != 5 || resp.Records[0].Offset != 10 || resp.NextOffset != 15 || resp.HighWatermark != 50 {
		t.Fatalf("resp = %+v", resp)
	}
	if resp.Records[2].Value != strings.Repeat("v", 12) {
		t.Fatalf("value = %q", resp.Records[2].Value)
	}
	resp, _ = b.Fetch(ctx, api.FetchRequest{Topic: "t", Offset: 50})
	if len(resp.Records) != 0 || resp.NextOffset != 50 {
		t.Fatalf("fetch at end = %+v", resp)
	}
	_, err = b.Fetch(ctx, api.FetchRequest{Topic: "t", Offset: 51})
	wantCode(t, err, api.CodeOffsetOutOfRange)
	_, err = b.Fetch(ctx, api.FetchRequest{Topic: "t", Partition: 1})
	wantCode(t, err, api.CodeUnknownPartition)
}

func TestRestartKeepsTopicsAndData(t *testing.T) {
	dir := t.TempDir()
	b := open(t, testConfig(dir))
	b.CreateTopic(ctx, api.TopicConfig{Name: "t", Partitions: 2, Residency: "EU"})
	recs := make([]api.ProduceRecord, 500)
	for i := range recs {
		recs[i] = api.ProduceRecord{Key: "k", Value: "hello"}
	}
	resp, _ := b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: recs})
	p := resp.Offsets[0].Partition
	b.Close()

	b = open(t, testConfig(dir))
	defer b.Close()
	info, err := b.Topic(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if info.Residency != "EU" || info.PartitionStates[p].HighWatermark != 500 {
		t.Fatalf("after restart: %+v", info)
	}
	more, _ := b.Produce(ctx, api.ProduceRequest{Topic: "t", Records: recs[:1]})
	if more.Offsets[0].Offset != 500 {
		t.Fatalf("offset after restart = %d", more.Offsets[0].Offset)
	}
}

func TestIdentityProtectsDataDir(t *testing.T) {
	dir := t.TempDir()
	b := open(t, testConfig(dir))
	b.Close()

	moved := testConfig(dir)
	moved.Info = api.Info{ID: "in-1", Region: "EU", Zone: "eu-a"}
	if _, err := Open(moved); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("broker started in another region on the same data dir: %v", err)
	}
	other := testConfig(dir)
	other.Info.ID = "in-2"
	if _, err := Open(other); err == nil {
		t.Fatal("another broker id reused the data dir")
	}
}

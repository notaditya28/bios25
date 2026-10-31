package broker

import (
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

func TestIdempotentRetriesAreNotDuplicated(t *testing.T) {
	c := newCluster(t, 3)
	c.createTopic(api.TopicConfig{Name: "pay", Partitions: 1, Residency: "IN"})
	p := 0
	batch := func(seq uint64, vals ...string) api.ProduceRequest {
		req := api.ProduceRequest{ProducerID: 42, BaseSeq: seq}
		for _, v := range vals {
			req.Records = append(req.Records, api.ProduceRecord{Partition: &p, Value: v})
		}
		return req
	}
	cl := c.client(1) // not necessarily the leader: forwarding keeps producer id + seq
	var first api.ProduceResponse
	waitFor(t, 5_000_000_000, "first batch", func() bool {
		var err error
		first, err = cl.ProduceRequest(bg, "pay", batch(0, "a", "b"))
		return err == nil
	})

	// The same batch again (a client retry after a lost response) returns the original offsets.
	again, err := cl.ProduceRequest(bg, "pay", batch(0, "a", "b"))
	if err != nil || again.Offsets[0] != first.Offsets[0] || again.Offsets[1] != first.Offsets[1] {
		t.Fatalf("retry = %+v, %v; first = %+v", again.Offsets, err, first.Offsets)
	}
	if _, err := cl.ProduceRequest(bg, "pay", batch(2, "c")); err != nil {
		t.Fatal(err)
	}
	// A gap in sequence numbers is refused (a batch would otherwise be silently missing).
	if _, err := cl.ProduceRequest(bg, "pay", batch(9, "x")); api.CodeOf(err) != api.CodeInvalidRequest {
		t.Fatalf("sequence gap accepted: %v", err)
	}
	// A retry that changed the batch is refused too.
	if _, err := cl.ProduceRequest(bg, "pay", batch(2, "c", "d")); api.CodeOf(err) != api.CodeInvalidRequest {
		t.Fatalf("altered retry accepted: %v", err)
	}

	// After the leader dies, the new leader recognises the retry from its own log.
	leader := c.leaderOf("pay", 0)
	c.kill(leader)
	other := (leader + 1) % 3
	var after api.ProduceResponse
	for start := time.Now(); ; {
		after, err = c.client(other).ProduceRequest(bg, "pay", batch(2, "c"))
		if err == nil {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("retry after failover: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	var vals []string
	for _, r := range consumeAll(t, c.client(other), "pay", 0) {
		vals = append(vals, r.Value)
	}
	if len(vals) != 3 || vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
		t.Fatalf("log = %v (retries must not duplicate)", vals)
	}
	if after.Offsets[0].Offset == 0 {
		t.Fatalf("offsets after failover: %+v", after.Offsets)
	}
}

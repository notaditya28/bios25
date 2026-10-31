package client

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

// Producer is an idempotent producer: every batch for a partition carries this producer's id
// and a sequence number, and a failed batch is retried with the same numbers (through the next
// broker in the list) until it is acknowledged. Retries therefore never create duplicates.
// Use one Send at a time per partition.
type Producer struct {
	id      uint64
	clients []*Client

	mu   sync.Mutex
	seq  map[string]uint64 // "topic/partition" -> next sequence number
	next int               // broker rotation
}

// NewProducer returns a producer that talks to the given brokers (first one preferred).
func NewProducer(addrs ...string) *Producer {
	p := &Producer{id: rand.Uint64N(1<<53-1) + 1, seq: map[string]uint64{}} // fits a JS number
	for _, a := range addrs {
		p.clients = append(p.clients, New(a))
	}
	return p
}

func (p *Producer) ID() uint64 { return p.id }

// Attempt describes one failed try of a batch.
type Attempt struct {
	Broker string
	Err    error
	Took   time.Duration
}

// Send produces values to one partition exactly once. It retries the same batch until it is
// acknowledged or ctx ends, calling onFail (if non-nil) for every failed attempt.
func (p *Producer) Send(ctx context.Context, topic string, part int, values []string, onFail func(Attempt)) (api.ProduceResponse, error) {
	key := fmt.Sprintf("%s/%d", topic, part)
	p.mu.Lock()
	base := p.seq[key]
	p.mu.Unlock()
	req := api.ProduceRequest{ProducerID: p.id, BaseSeq: base, Records: make([]api.ProduceRecord, len(values))}
	for i, v := range values {
		req.Records[i] = api.ProduceRecord{Partition: &part, Value: v}
	}
	backoff := 20 * time.Millisecond
	for {
		p.mu.Lock()
		c := p.clients[p.next%len(p.clients)]
		p.mu.Unlock()
		start := time.Now()
		actx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.ProduceRequest(actx, topic, req)
		cancel()
		if err == nil {
			p.mu.Lock()
			p.seq[key] = base + uint64(len(values))
			p.mu.Unlock()
			return resp, nil
		}
		if onFail != nil {
			onFail(Attempt{Broker: c.Addr(), Err: err, Took: time.Since(start)})
		}
		if ctx.Err() != nil {
			return api.ProduceResponse{}, ctx.Err()
		}
		if api.CodeOf(err) == api.CodeInvalidRequest {
			return api.ProduceResponse{}, err // our bug, not the cluster's: don't spin
		}
		p.mu.Lock()
		p.next++ // try another broker
		p.mu.Unlock()
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return api.ProduceResponse{}, ctx.Err()
		}
		backoff = min(backoff*2, 200*time.Millisecond)
	}
}

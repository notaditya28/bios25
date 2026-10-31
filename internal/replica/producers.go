package replica

import (
	"log/slog"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

// Idempotent producers. A producer tags each batch for a partition with its id and the
// sequence number of the first record; a retry resends the same batch with the same numbers.
// Every replica keeps the last few batches per producer, derived purely from its log (records
// carry producer id + seq), so after a failover the new leader recognises retries of batches
// the old leader appended. A retry of a known batch is answered with the original offsets
// once they are committed, instead of being written twice.

const keepBatches = 8

type batchMeta struct {
	baseSeq    uint64
	n          uint64
	baseOffset uint64
}

type producerState struct{ batches []batchMeta } // ascending seq, most recent last

func (s *producerState) last() *batchMeta {
	if len(s.batches) == 0 {
		return nil
	}
	return &s.batches[len(s.batches)-1]
}

// trackLocked folds records (already in our log, with offsets) into the producer table.
func (p *Partition) trackLocked(recs []seglog.Record) {
	for i := range recs {
		r := &recs[i]
		if r.ProducerID == 0 {
			continue
		}
		if p.producers == nil {
			p.producers = map[uint64]*producerState{}
		}
		s := p.producers[r.ProducerID]
		if s == nil {
			s = &producerState{}
			p.producers[r.ProducerID] = s
		}
		if b := s.last(); b != nil && r.Flags&seglog.FlagBatchStart == 0 && r.Seq == b.baseSeq+b.n && r.Offset == b.baseOffset+b.n {
			b.n++
			continue
		}
		s.batches = append(s.batches, batchMeta{baseSeq: r.Seq, n: 1, baseOffset: r.Offset})
		if len(s.batches) > keepBatches {
			s.batches = s.batches[len(s.batches)-keepBatches:]
		}
	}
}

// rebuildProducersLocked recomputes the producer table from the whole log (at open and after
// a truncation removed records).
func (p *Partition) rebuildProducersLocked() {
	p.producers = nil
	end := p.log.EndOffset()
	for off := p.log.StartOffset(); off < end; {
		recs, err := p.log.Read(off, end, 5000, 4<<20)
		if err != nil || len(recs) == 0 {
			if err != nil {
				slog.Error("rebuild producer state", "partition", p.Key, "err", err)
			}
			return
		}
		p.trackLocked(recs)
		off = recs[len(recs)-1].Offset + 1
	}
}

// dedupLocked classifies an incoming idempotent batch. It returns (offset, true) when the
// batch is a retry of one already in the log.
func (p *Partition) dedupLocked(pid, baseSeq uint64, n int) (uint64, bool, error) {
	s := p.producers[pid]
	if s == nil {
		return 0, false, nil // first batch we've seen from this producer
	}
	for _, b := range s.batches {
		if b.baseSeq == baseSeq {
			if b.n != uint64(n) {
				return 0, false, api.Errorf(api.CodeInvalidRequest, "producer %d resent sequence %d with %d records, originally %d", pid, baseSeq, n, b.n)
			}
			return b.baseOffset, true, nil
		}
	}
	last := s.last()
	next := last.baseSeq + last.n
	switch {
	case baseSeq == next:
		return 0, false, nil
	case baseSeq > next:
		e := api.Errorf(api.CodeInvalidRequest, "producer %d skipped sequence numbers: expected %d, got %d", pid, next, baseSeq)
		e.Reason = "out_of_order_sequence"
		return 0, false, e
	default:
		e := api.Errorf(api.CodeInvalidRequest, "producer %d sequence %d is older than the batches we remember", pid, baseSeq)
		e.Reason = "stale_sequence"
		return 0, false, e
	}
}

package replica

import (
	"crypto/sha256"
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/notaditya28/hydra-stream/internal/policy"
	"github.com/notaditya28/hydra-stream/internal/seglog"
)

// Cross-region payload deduplication — on the wire only.
//
// When a leader replicates to a follower in ANOTHER region, a large value (>= dedupMinBytes)
// that the follower is known to hold already is sent as its SHA-256 (Record.Ref) instead of the
// bytes. The follower looks the hash up in an index of large values in its OWN log, reads that
// record back, verifies the hash and re-inflates the value before appending. Anything it can't
// resolve (evicted, truncated, never seen) makes it answer NeedFull and the leader resends that
// batch in full.
//
// Unlike content-addressed storage, logs always hold complete payloads, so recovery, truncation,
// retention and audits never depend on a separate blob store; dedup is scoped to one partition
// (one topic), so one tenant can never reference another's data.

const (
	dedupMinBytes = 1024
	dedupKeep     = 20000 // hashes remembered per follower / per index
)

type hash = [32]byte

// hashSet is a bounded set with FIFO eviction.
type hashSet struct {
	m    map[hash]uint64
	ring []hash
	next int
}

func newHashSet() *hashSet { return &hashSet{m: map[hash]uint64{}, ring: make([]hash, 0, 64)} }

func (s *hashSet) put(h hash, v uint64) {
	if _, ok := s.m[h]; ok {
		s.m[h] = v
		return
	}
	if len(s.ring) < dedupKeep {
		s.ring = append(s.ring, h)
	} else {
		delete(s.m, s.ring[s.next])
		s.ring[s.next] = h
		s.next = (s.next + 1) % dedupKeep
	}
	s.m[h] = v
}

func (s *hashSet) get(h hash) (uint64, bool) { v, ok := s.m[h]; return v, ok }

type dedupState struct {
	mu      sync.Mutex
	known   map[string]*hashSet // follower -> hashes it is known to hold
	pending map[string][]hash   // follower -> hashes sent in full in the in-flight request
	index   *hashSet            // follower side: hash -> offset of a record holding that value

	refs       atomic.Int64 // values sent as references
	savedBytes atomic.Int64 // bytes not sent thanks to references
}

func (p *Partition) dd() *dedupState {
	p.ddOnce.Do(func() {
		p.dedup = &dedupState{known: map[string]*hashSet{}, pending: map[string][]hash{}, index: newHashSet()}
	})
	return p.dedup
}

// DedupStats reports values sent as references and the bytes saved.
func (p *Partition) DedupStats() (refs, saved int64) {
	d := p.dd()
	return d.refs.Load(), d.savedBytes.Load()
}

// dedupOutgoing replaces large values the (cross-region) follower already holds by references.
func (p *Partition) dedupOutgoing(to string, req *AppendRequest) {
	if !p.dedupOn || p.guard.RegionOf(to) == p.guard.Region() || len(req.Records) == 0 {
		return
	}
	d := p.dd()
	d.mu.Lock()
	defer d.mu.Unlock()
	known := d.known[to]
	if known == nil {
		known = newHashSet()
		d.known[to] = known
	}
	var pending []hash
	for i := range req.Records {
		r := &req.Records[i]
		if len(r.Value) < dedupMinBytes {
			continue
		}
		h := sha256.Sum256(r.Value)
		if _, ok := known.get(h); ok {
			d.refs.Add(1)
			d.savedBytes.Add(int64(len(r.Value)))
			r.Ref, r.Value = h[:], nil
			continue
		}
		pending = append(pending, h)
	}
	d.pending[to] = pending
}

// dedupAcked learns what the follower now holds (or forgets everything if it lost track).
func (p *Partition) dedupAcked(to string, req *AppendRequest, resp *AppendResponse) {
	if !p.dedupOn {
		return
	}
	d := p.dd()
	d.mu.Lock()
	defer d.mu.Unlock()
	if resp.NeedFull {
		d.known[to] = newHashSet()
		delete(d.pending, to)
		return
	}
	if !resp.OK {
		return
	}
	if known := d.known[to]; known != nil {
		for _, h := range d.pending[to] {
			known.put(h, 0)
		}
	}
	delete(d.pending, to)
}

// resolveRefsLocked re-inflates referenced values from our own log (follower side). It
// returns false if any reference can't be resolved and verified.
func (p *Partition) resolveRefsLocked(recs []seglog.Record) bool {
	d := p.dd()
	for i := range recs {
		r := &recs[i]
		if len(r.Ref) == 0 {
			continue
		}
		var h hash
		copy(h[:], r.Ref)
		off, ok := d.index.get(h)
		if !ok {
			return false
		}
		got, err := p.log.Read(off, off+1, 1, 0)
		if err != nil || len(got) != 1 || sha256.Sum256(got[0].Value) != h {
			return false // truncated, evicted or corrupt: ask for the full value
		}
		r.Value, r.Ref = append([]byte(nil), got[0].Value...), nil
	}
	return true
}

// indexValuesLocked remembers where large values live in our log (follower side).
func (p *Partition) indexValuesLocked(recs []seglog.Record) {
	if !p.dedupOn {
		return
	}
	d := p.dd()
	for i := range recs {
		if len(recs[i].Value) >= dedupMinBytes {
			d.index.put(sha256.Sum256(recs[i].Value), recs[i].Offset)
		}
	}
}

// HeaderClass extracts the per-message policy class from record headers.
func HeaderClass(h []byte) string {
	if len(h) == 0 {
		return ""
	}
	var v struct {
		Class string `json:"class"`
	}
	if json.Unmarshal(h, &v) != nil {
		return ""
	}
	return v.Class
}

// narrowFor resolves per-message classes into one combined rule. An unresolvable class yields
// a rule that permits no transit at all (fail closed: such data only moves on direct links).
func (p *Partition) narrowFor(classes []string) *policy.Effective {
	var out *policy.Effective
	for _, c := range classes {
		e, err := p.guard.Engine().Class(c)
		if err != nil {
			e = policy.Effective{Name: "unresolvable", Storage: map[string]bool{}, Transit: map[string]bool{}}
		}
		if out == nil {
			out = &e
		} else {
			n := out.Narrow(e)
			out = &n
		}
	}
	return out
}

// Package trail records every jurisdiction decision a broker makes about moving data: which
// hop, which peer, which policy version, allow or deny, and why. Records go to an append-only
// JSON-lines file in the broker's data dir (rotated at 64 MiB) and a ring buffer for the
// admin API, so an operator can show, per message batch, that every hop was checked.
package trail

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Allow = "allow"
	Deny  = "deny"

	ringSize   = 2000
	rotateSize = 64 << 20
)

type Decision struct {
	At            time.Time `json:"at"`
	Broker        string    `json:"broker"`
	Topic         string    `json:"topic"`
	Partition     int       `json:"partition"`
	Batch         string    `json:"batch,omitempty"` // producer:seq or offset range
	Records       int       `json:"records,omitempty"`
	Hop           string    `json:"hop"` // entry | forward | relay | replicate-send | replicate-accept | store | proxy
	Peer          string    `json:"peer,omitempty"`
	PeerRegion    string    `json:"peer_region,omitempty"`
	Policy        string    `json:"policy"`
	PolicyVersion uint64    `json:"policy_version"`
	Decision      string    `json:"decision"`
	Reason        string    `json:"reason"`
	CheckNanos    int64     `json:"check_ns,omitempty"` // time spent evaluating the policy
}

type Log struct {
	path string

	mu   sync.Mutex
	f    *os.File
	w    *bufio.Writer
	size int64
	ring []Decision
	next int

	allows, denies atomic.Int64
	stop           chan struct{}
	done           chan struct{}
}

// Open opens (appending) the decision log at path.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	st, _ := f.Stat()
	l := &Log{path: path, f: f, w: bufio.NewWriterSize(f, 64<<10), size: st.Size(), ring: make([]Decision, 0, ringSize),
		stop: make(chan struct{}), done: make(chan struct{})}
	go l.flusher()
	return l, nil
}

func (l *Log) flusher() {
	defer close(l.done)
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-t.C:
			l.mu.Lock()
			_ = l.w.Flush()
			l.mu.Unlock()
		}
	}
}

// Record appends a decision. Safe for concurrent use; never blocks on disk for long.
func (l *Log) Record(d Decision) {
	if l == nil {
		return
	}
	if d.At.IsZero() {
		d.At = time.Now()
	}
	if d.Decision == Deny {
		l.denies.Add(1)
	} else {
		l.allows.Add(1)
	}
	line, _ := json.Marshal(d)
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ring) < ringSize {
		l.ring = append(l.ring, d)
	} else {
		l.ring[l.next] = d
		l.next = (l.next + 1) % ringSize
	}
	if l.f == nil {
		return
	}
	n, _ := l.w.Write(append(line, '\n'))
	l.size += int64(n)
	if l.size > rotateSize {
		l.rotateLocked()
	}
}

func (l *Log) rotateLocked() {
	_ = l.w.Flush()
	_ = l.f.Close()
	_ = os.Rename(l.path, l.path+".1")
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		l.f = nil
		return
	}
	l.f, l.w, l.size = f, bufio.NewWriterSize(f, 64<<10), 0
}

// Recent returns up to n most recent decisions matching keep (nil = all), newest first.
func (l *Log) Recent(n int, keep func(Decision) bool) []Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Decision
	total := len(l.ring)
	for i := 0; i < total && len(out) < n; i++ {
		idx := (l.next - 1 - i + 2*total) % total
		if len(l.ring) < ringSize {
			idx = total - 1 - i
		}
		if d := l.ring[idx]; keep == nil || keep(d) {
			out = append(out, d)
		}
	}
	return out
}

func (l *Log) Counts() (allows, denies int64) {
	if l == nil {
		return 0, 0
	}
	return l.allows.Load(), l.denies.Load()
}

func (l *Log) Close() error {
	if l == nil {
		return nil
	}
	close(l.stop)
	<-l.done
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	_ = l.w.Flush()
	err := l.f.Close()
	l.f = nil
	return err
}

// Path of the decision log inside a broker data dir.
func Path(dataDir string) string { return filepath.Join(dataDir, "decisions.log") }

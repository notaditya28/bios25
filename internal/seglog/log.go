// Package seglog implements an append-only, segmented partition log stored in plain files.
package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrOffsetOutOfRange = errors.New("seglog: offset out of range")
	ErrClosed           = errors.New("seglog: log closed")
	ErrRecordTooLarge   = errors.New("seglog: record too large")
	ErrOffsetMismatch   = errors.New("seglog: records do not continue the log")
)

const (
	DefaultSegmentBytes = 64 << 20
	DefaultMaxRecords   = 1000
	DefaultMaxBytes     = 1 << 20
)

type Config struct {
	SegmentBytes int64 // roll to a new segment once the active one would exceed this
	Fsync        bool  // fsync after every append
}

// Log is a partition log. It is safe for concurrent use.
type Log struct {
	mu     sync.RWMutex
	dir    string
	cfg    Config
	segs   []*segment
	notify chan struct{} // closed and replaced on every append
	closed bool
}

// Open opens the log in dir, creating it if needed and recovering from torn writes.
func Open(dir string, cfg Config) (*Log, error) {
	if cfg.SegmentBytes <= 0 {
		cfg.SegmentBytes = DefaultSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		b, err := strconv.ParseUint(strings.TrimSuffix(name, ".log"), 10, 64)
		if err != nil {
			continue
		}
		bases = append(bases, b)
	}
	slices.Sort(bases)

	l := &Log{dir: dir, cfg: cfg, notify: make(chan struct{})}
	for i, b := range bases {
		if n := len(l.segs); n > 0 && l.segs[n-1].next() != b {
			// A gap means an earlier segment lost its tail; later segments are unreachable.
			for _, rest := range bases[i:] {
				if err := os.Remove(filepath.Join(dir, segmentName(rest))); err != nil {
					l.closeSegs()
					return nil, err
				}
			}
			break
		}
		s, err := openSegment(dir, b)
		if err != nil {
			l.closeSegs()
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	if len(l.segs) == 0 {
		s, err := openSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segs = append(l.segs, s)
	}
	return l, nil
}

func (l *Log) Dir() string { return l.dir }

func (l *Log) nextLocked() uint64 { return l.segs[len(l.segs)-1].next() }

// EndOffset is the offset the next appended record will get.
func (l *Log) EndOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextLocked()
}

// StartOffset is the first offset still in the log.
func (l *Log) StartOffset() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segs[0].base
}

// Append assigns offsets (and timestamps, if zero) to recs in place and appends them atomically.
// It returns the offset of the first record.
func (l *Log) Append(recs []Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	base := l.nextLocked()
	now := time.Now().UnixMilli()
	for i := range recs {
		recs[i].Offset = base + uint64(i)
		if recs[i].Timestamp == 0 {
			recs[i].Timestamp = now
		}
	}
	return base, l.writeLocked(recs)
}

// AppendWithOffsets appends records that already carry offsets (replication). The offsets
// must continue the log exactly.
func (l *Log) AppendWithOffsets(recs []Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	next := l.nextLocked()
	for i := range recs {
		if recs[i].Offset != next+uint64(i) {
			return fmt.Errorf("%w: got offset %d, want %d", ErrOffsetMismatch, recs[i].Offset, next+uint64(i))
		}
	}
	return l.writeLocked(recs)
}

func (l *Log) writeLocked(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	size := 0
	for i := range recs {
		if len(recs[i].Key)+len(recs[i].Value) > MaxRecordSize {
			return ErrRecordTooLarge
		}
		size += encodedSize(&recs[i])
	}
	buf := make([]byte, 0, size)
	rel := make([]int64, len(recs))
	for i := range recs {
		rel[i] = int64(len(buf))
		buf = appendEncoded(buf, &recs[i])
	}

	active := l.segs[len(l.segs)-1]
	if active.size > 0 && active.size+int64(len(buf)) > l.cfg.SegmentBytes {
		s, err := openSegment(l.dir, active.next())
		if err != nil {
			return err
		}
		l.segs = append(l.segs, s)
		active = s
	}
	if err := active.write(buf, rel); err != nil {
		return err
	}
	if l.cfg.Fsync {
		if err := active.f.Sync(); err != nil {
			return err
		}
	}
	close(l.notify)
	l.notify = make(chan struct{})
	return nil
}

// Read returns records starting at offset and before limit (exclusive; clamped to the log end),
// at most maxRecords and roughly maxBytes (at least one record if any is available).
// Reading at the end returns no records; reading outside [start, end] is ErrOffsetOutOfRange.
func (l *Log) Read(offset, limit uint64, maxRecords, maxBytes int) ([]Record, error) {
	if maxRecords <= 0 {
		maxRecords = DefaultMaxRecords
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	start, end := l.segs[0].base, l.nextLocked()
	if offset < start || offset > end {
		return nil, ErrOffsetOutOfRange
	}
	if limit > end {
		limit = end
	}
	if offset >= limit {
		return nil, nil
	}
	i := sort.Search(len(l.segs), func(i int) bool { return l.segs[i].base > offset }) - 1
	return l.segs[i].read(offset, limit, maxRecords, maxBytes)
}

// EpochAt returns the leader epoch of the record at offset.
func (l *Log) EpochAt(offset uint64) (uint64, error) {
	recs, err := l.Read(offset, offset+1, 1, 1)
	if err != nil {
		return 0, err
	}
	if len(recs) == 0 {
		return 0, ErrOffsetOutOfRange
	}
	return recs[0].Epoch, nil
}

// LastEpoch returns the epoch of the last record, or 0 for an empty log.
func (l *Log) LastEpoch() uint64 {
	end := l.EndOffset()
	if end == l.StartOffset() {
		return 0
	}
	e, _ := l.EpochAt(end - 1)
	return e
}

// Wait blocks until the log end is beyond offset, the log is closed, or ctx is done.
func (l *Log) Wait(ctx context.Context, offset uint64) error {
	l.mu.RLock()
	if l.closed || l.nextLocked() > offset {
		l.mu.RUnlock()
		return nil
	}
	ch := l.notify
	l.mu.RUnlock()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TruncateTo discards offset and every later record.
func (l *Log) TruncateTo(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if offset >= l.nextLocked() {
		return nil
	}
	if offset < l.segs[0].base {
		return ErrOffsetOutOfRange
	}
	for len(l.segs) > 1 && l.segs[len(l.segs)-1].base >= offset {
		if err := l.segs[len(l.segs)-1].remove(); err != nil {
			return err
		}
		l.segs = l.segs[:len(l.segs)-1]
	}
	return l.segs[len(l.segs)-1].truncate(offset)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	close(l.notify)
	return l.closeSegs()
}

func (l *Log) closeSegs() error {
	var errs []error
	for _, s := range l.segs {
		errs = append(errs, s.close())
	}
	return errors.Join(errs...)
}

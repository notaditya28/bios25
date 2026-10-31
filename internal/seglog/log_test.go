package seglog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mkRecs(from, n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{Key: []byte(fmt.Sprintf("k%d", from+i)), Value: []byte(fmt.Sprintf("value-%d", from+i))}
	}
	return recs
}

func appendN(t *testing.T, l *Log, total, batch int) {
	t.Helper()
	for i := 0; i < total; i += batch {
		n := min(batch, total-i)
		if _, err := l.Append(mkRecs(i, n)); err != nil {
			t.Fatal(err)
		}
	}
}

// readAll reads [from, end) in chunks and checks contents match mkRecs.
func readAll(t *testing.T, l *Log, from uint64) int {
	t.Helper()
	off := from
	for {
		recs, err := l.Read(off, ^uint64(0), 37, 4096)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			if r.Offset != off {
				t.Fatalf("offset = %d, want %d", r.Offset, off)
			}
			if want := fmt.Sprintf("value-%d", off); string(r.Value) != want {
				t.Fatalf("value at %d = %q, want %q", off, r.Value, want)
			}
			if want := fmt.Sprintf("k%d", off); string(r.Key) != want {
				t.Fatalf("key at %d = %q, want %q", off, r.Key, want)
			}
			off++
		}
	}
	return int(off - from)
}

func segFiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAppendRead(t *testing.T) {
	l, err := Open(t.TempDir(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	base, err := l.Append(mkRecs(0, 10))
	if err != nil || base != 0 {
		t.Fatalf("Append = %d, %v", base, err)
	}
	base, err = l.Append(mkRecs(10, 5))
	if err != nil || base != 10 {
		t.Fatalf("Append = %d, %v", base, err)
	}
	if got := readAll(t, l, 0); got != 15 {
		t.Fatalf("read %d records, want 15", got)
	}
	if got := readAll(t, l, 7); got != 8 {
		t.Fatalf("read %d records from 7, want 8", got)
	}
	if _, err := l.Read(16, ^uint64(0), 0, 0); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read past end: %v", err)
	}
	if recs, err := l.Read(15, ^uint64(0), 0, 0); err != nil || len(recs) != 0 {
		t.Fatalf("read at end = %d records, %v", len(recs), err)
	}
}

func TestReadLimits(t *testing.T) {
	l, err := Open(t.TempDir(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	appendN(t, l, 100, 100)
	recs, _ := l.Read(0, 20, 1000, 1<<20)
	if len(recs) != 20 {
		t.Fatalf("limit: got %d, want 20", len(recs))
	}
	recs, _ = l.Read(0, 100, 7, 1<<20)
	if len(recs) != 7 {
		t.Fatalf("maxRecords: got %d, want 7", len(recs))
	}
	recs, _ = l.Read(0, 100, 1000, 1) // maxBytes smaller than one record still returns one
	if len(recs) != 1 {
		t.Fatalf("maxBytes: got %d, want 1", len(recs))
	}
}

func TestSegmentRollAndReopen(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir, Config{SegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	appendN(t, l, 1000, 7)
	if n := len(segFiles(t, dir)); n < 10 {
		t.Fatalf("expected many segments, got %d", n)
	}
	if got := readAll(t, l, 0); got != 1000 {
		t.Fatalf("read %d, want 1000", got)
	}
	l.Close()

	l, err = Open(dir, Config{SegmentBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.EndOffset() != 1000 {
		t.Fatalf("EndOffset after reopen = %d", l.EndOffset())
	}
	if got := readAll(t, l, 0); got != 1000 {
		t.Fatalf("read after reopen %d, want 1000", got)
	}
	if base, err := l.Append(mkRecs(1000, 1)); err != nil || base != 1000 {
		t.Fatalf("append after reopen = %d, %v", base, err)
	}
}

func lastSeg(t *testing.T, dir string) string {
	t.Helper()
	files := segFiles(t, dir)
	return files[len(files)-1]
}

func TestRecoverTornTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{})
	appendN(t, l, 10, 10)
	l.Close()

	path := lastSeg(t, dir)
	fi, _ := os.Stat(path)
	if err := os.Truncate(path, fi.Size()-5); err != nil { // tear the last record
		t.Fatal(err)
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 3}) // plus some garbage
	f.Close()

	l, err := Open(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.EndOffset() != 9 {
		t.Fatalf("EndOffset = %d, want 9", l.EndOffset())
	}
	if got := readAll(t, l, 0); got != 9 {
		t.Fatalf("read %d, want 9", got)
	}
	if base, err := l.Append(mkRecs(9, 3)); err != nil || base != 9 {
		t.Fatalf("append after recovery = %d, %v", base, err)
	}
	if got := readAll(t, l, 0); got != 12 {
		t.Fatalf("read %d, want 12", got)
	}
}

func TestRecoverCorruptCRC(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{})
	appendN(t, l, 10, 10)
	recs, _ := l.Read(5, 6, 1, 0)
	_ = recs
	l.Close()

	// Flip a byte in the value of record 5.
	path := lastSeg(t, dir)
	data, _ := os.ReadFile(path)
	pos := 0
	for i := 0; i < 5; i++ {
		pos += headerSize + int(be.Uint32(data[pos:]))
	}
	data[pos+headerSize+fixedBodySize] ^= 0xff
	os.WriteFile(path, data, 0o644)

	l, err := Open(dir, Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if l.EndOffset() != 5 {
		t.Fatalf("EndOffset = %d, want 5", l.EndOffset())
	}
}

func TestTruncateAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Config{SegmentBytes: 512})
	appendN(t, l, 200, 3)
	before := len(segFiles(t, dir))
	if err := l.TruncateTo(37); err != nil {
		t.Fatal(err)
	}
	if l.EndOffset() != 37 {
		t.Fatalf("EndOffset = %d, want 37", l.EndOffset())
	}
	if after := len(segFiles(t, dir)); after >= before {
		t.Fatalf("segments not removed: %d -> %d", before, after)
	}
	if base, _ := l.Append(mkRecs(37, 10)); base != 37 {
		t.Fatalf("append after truncate base = %d", base)
	}
	l.Close()
	l, _ = Open(dir, Config{SegmentBytes: 512})
	defer l.Close()
	if got := readAll(t, l, 0); got != 47 {
		t.Fatalf("read %d after reopen, want 47", got)
	}
	if err := l.TruncateTo(0); err != nil || l.EndOffset() != 0 {
		t.Fatalf("truncate to 0: end=%d err=%v", l.EndOffset(), err)
	}
}

func TestAppendWithOffsets(t *testing.T) {
	l, _ := Open(t.TempDir(), Config{})
	defer l.Close()
	recs := mkRecs(0, 3)
	for i := range recs {
		recs[i].Offset = uint64(i)
		recs[i].Epoch = 4
	}
	if err := l.AppendWithOffsets(recs); err != nil {
		t.Fatal(err)
	}
	bad := mkRecs(3, 1)
	bad[0].Offset = 5
	if err := l.AppendWithOffsets(bad); !errors.Is(err, ErrOffsetMismatch) {
		t.Fatalf("gap accepted: %v", err)
	}
	got, _ := l.Read(0, 3, 0, 0)
	if len(got) != 3 || got[2].Epoch != 4 {
		t.Fatalf("got %+v", got)
	}
}

func TestWaitWakesOnAppend(t *testing.T) {
	l, _ := Open(t.TempDir(), Config{})
	defer l.Close()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done <- l.Wait(ctx, 0)
	}()
	time.Sleep(50 * time.Millisecond)
	l.Append(mkRecs(0, 1))
	if err := <-done; err != nil {
		t.Fatalf("Wait = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait with no data = %v", err)
	}
}

func TestConcurrentAppends(t *testing.T) {
	l, _ := Open(t.TempDir(), Config{SegmentBytes: 4096})
	defer l.Close()
	const writers, batches, per = 8, 50, 5
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := 0; b < batches; b++ {
				if _, err := l.Append(mkRecs(0, per)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	want := uint64(writers * batches * per)
	if l.EndOffset() != want {
		t.Fatalf("EndOffset = %d, want %d", l.EndOffset(), want)
	}
	var n uint64
	for off := uint64(0); off < want; {
		recs, err := l.Read(off, want, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if r.Offset != off {
				t.Fatalf("offset %d, want %d", r.Offset, off)
			}
			off++
			n++
		}
	}
	if n != want {
		t.Fatalf("read %d, want %d", n, want)
	}
}

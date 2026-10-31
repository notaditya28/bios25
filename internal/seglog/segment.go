package seglog

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// segment is one file of a partition log holding a contiguous run of offsets starting at base.
// It is not safe for concurrent use; Log serialises access.
type segment struct {
	base      uint64
	path      string
	f         *os.File
	size      int64
	positions []int64 // file position of record base+i
}

func segmentName(base uint64) string { return fmt.Sprintf("%020d.log", base) }

// openSegment opens (or creates) the segment with the given base offset, rebuilding its index
// and truncating any torn or corrupt tail.
func openSegment(dir string, base uint64) (*segment, error) {
	path := filepath.Join(dir, segmentName(base))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	s := &segment{base: base, path: path, f: f}
	if err := s.recover(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

func (s *segment) recover() error {
	fi, err := s.f.Stat()
	if err != nil {
		return err
	}
	total := fi.Size()
	r := bufio.NewReaderSize(io.NewSectionReader(s.f, 0, total), 1<<20)
	hdr := make([]byte, headerSize)
	var body []byte
	var pos int64
	for pos < total {
		if _, err := io.ReadFull(r, hdr); err != nil {
			break
		}
		n := int64(be.Uint32(hdr))
		if n < fixedBodySize || n > fixedBodySize+MaxRecordSize || n > total-pos-headerSize {
			break
		}
		if int64(cap(body)) < n {
			body = make([]byte, n)
		}
		body = body[:n]
		if _, err := io.ReadFull(r, body); err != nil {
			break
		}
		if crc32.Checksum(body, crcTable) != be.Uint32(hdr[4:]) {
			break
		}
		if be.Uint64(body) != s.base+uint64(len(s.positions)) {
			break
		}
		s.positions = append(s.positions, pos)
		pos += headerSize + n
	}
	if pos < total {
		if err := s.f.Truncate(pos); err != nil {
			return fmt.Errorf("truncate torn tail of %s: %w", s.path, err)
		}
	}
	s.size = pos
	return nil
}

func (s *segment) next() uint64 { return s.base + uint64(len(s.positions)) }

// recEnd returns the file position just past record index i.
func (s *segment) recEnd(i int) int64 {
	if i+1 < len(s.positions) {
		return s.positions[i+1]
	}
	return s.size
}

// write appends pre-encoded records; rel holds each record's position within buf.
func (s *segment) write(buf []byte, rel []int64) error {
	if _, err := s.f.WriteAt(buf, s.size); err != nil {
		_ = s.f.Truncate(s.size)
		return err
	}
	for _, p := range rel {
		s.positions = append(s.positions, s.size+p)
	}
	s.size += int64(len(buf))
	return nil
}

// read returns records from offset (which must be in this segment) up to limit (exclusive),
// at most maxRecords, stopping once maxBytes would be exceeded (always at least one record).
func (s *segment) read(offset, limit uint64, maxRecords, maxBytes int) ([]Record, error) {
	i := int(offset - s.base)
	end := len(s.positions)
	if l := int(limit - s.base); l < end {
		end = l
	}
	if i >= end {
		return nil, nil
	}
	if end-i > maxRecords {
		end = i + maxRecords
	}
	start := s.positions[i]
	j := i + 1
	for j < end && s.recEnd(j)-start <= int64(maxBytes) {
		j++
	}
	buf := make([]byte, s.recEnd(j-1)-start)
	if n, err := s.f.ReadAt(buf, start); n < len(buf) {
		return nil, fmt.Errorf("read %s: %w", s.path, err)
	}
	recs := make([]Record, 0, j-i)
	for p := 0; p < len(buf); {
		if len(buf)-p < headerSize {
			return nil, errCorrupt
		}
		n := int(be.Uint32(buf[p:]))
		if n > len(buf)-p-headerSize {
			return nil, errCorrupt
		}
		body := buf[p+headerSize : p+headerSize+n]
		if crc32.Checksum(body, crcTable) != be.Uint32(buf[p+4:]) {
			return nil, errCorrupt
		}
		rec, err := decodeBody(body)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
		p += headerSize + n
	}
	return recs, nil
}

// truncate discards offset and everything after it.
func (s *segment) truncate(offset uint64) error {
	i := int(offset - s.base)
	if i >= len(s.positions) {
		return nil
	}
	pos := s.positions[i]
	if err := s.f.Truncate(pos); err != nil {
		return err
	}
	s.positions = s.positions[:i]
	s.size = pos
	return nil
}

func (s *segment) close() error { return s.f.Close() }

func (s *segment) remove() error {
	s.f.Close()
	return os.Remove(s.path)
}

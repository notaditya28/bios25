package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Record is a single message in a partition log.
type Record struct {
	Offset     uint64
	Epoch      uint64 // leader epoch that wrote the record; used to truncate divergent tails after failover
	Timestamp  int64  // unix milliseconds
	Flags      uint8
	ProducerID uint64 // idempotent producer that wrote it (0 = none)
	Seq        uint64 // that producer's sequence number for this partition
	Headers    []byte // optional message metadata (JSON object), e.g. classification, priority
	Key        []byte
	Value      []byte

	// Ref is wire-only (never written to disk): during cross-region replication a large value
	// the follower already holds is sent as its SHA-256 instead of the bytes (see replica/dedup.go).
	Ref []byte `json:",omitempty"`
}

// FlagControl marks internal records (e.g. the marker a new leader writes); consumers skip them.
const FlagControl uint8 = 1

// FlagBatchStart marks the first record of an idempotent producer's batch, so every replica
// can recover the exact batch boundaries from its log.
const FlagBatchStart uint8 = 2

func (r *Record) IsControl() bool { return r.Flags&FlagControl != 0 }

// On-disk framing: u32 body length | u32 CRC32-C(body) | body
// body: u64 offset | u64 epoch | i64 timestamp | u8 flags | u64 producer id | u64 seq |
//
//	u32 hdrlen | headers | u32 keylen | key | u32 vallen | value
const (
	headerSize    = 8
	fixedBodySize = 8 + 8 + 8 + 1 + 8 + 8 + 4 + 4 + 4

	// MaxRecordSize bounds len(headers)+len(key)+len(value) of a single record.
	MaxRecordSize = 8 << 20
)

var (
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	be         = binary.BigEndian
	errCorrupt = errors.New("seglog: corrupt record")
)

func payloadSize(r *Record) int { return len(r.Headers) + len(r.Key) + len(r.Value) }

func encodedSize(r *Record) int {
	return headerSize + fixedBodySize + payloadSize(r)
}

func appendEncoded(dst []byte, r *Record) []byte {
	start := len(dst)
	dst = be.AppendUint32(dst, uint32(fixedBodySize+payloadSize(r)))
	dst = be.AppendUint32(dst, 0) // CRC, filled in below
	body := len(dst)
	dst = be.AppendUint64(dst, r.Offset)
	dst = be.AppendUint64(dst, r.Epoch)
	dst = be.AppendUint64(dst, uint64(r.Timestamp))
	dst = append(dst, r.Flags)
	dst = be.AppendUint64(dst, r.ProducerID)
	dst = be.AppendUint64(dst, r.Seq)
	dst = be.AppendUint32(dst, uint32(len(r.Headers)))
	dst = append(dst, r.Headers...)
	dst = be.AppendUint32(dst, uint32(len(r.Key)))
	dst = append(dst, r.Key...)
	dst = be.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	be.PutUint32(dst[start+4:], crc32.Checksum(dst[body:], crcTable))
	return dst
}

// decodeBody parses a record body (the bytes after the header). Headers, Key and Value alias body.
func decodeBody(body []byte) (Record, error) {
	if len(body) < fixedBodySize {
		return Record{}, errCorrupt
	}
	r := Record{
		Offset:     be.Uint64(body[0:]),
		Epoch:      be.Uint64(body[8:]),
		Timestamp:  int64(be.Uint64(body[16:])),
		Flags:      body[24],
		ProducerID: be.Uint64(body[25:]),
		Seq:        be.Uint64(body[33:]),
	}
	p := 41
	field := func() ([]byte, bool) {
		if len(body)-p < 4 {
			return nil, false
		}
		n := int(be.Uint32(body[p:]))
		p += 4
		if n > len(body)-p {
			return nil, false
		}
		f := body[p : p+n]
		p += n
		return f, true
	}
	var ok bool
	if r.Headers, ok = field(); !ok {
		return Record{}, errCorrupt
	}
	if r.Key, ok = field(); !ok {
		return Record{}, errCorrupt
	}
	if r.Value, ok = field(); !ok || p != len(body) {
		return Record{}, errCorrupt
	}
	if len(r.Headers) == 0 {
		r.Headers = nil
	}
	return r, nil
}

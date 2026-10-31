package seglog

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// Record is a single message in a partition log.
type Record struct {
	Offset    uint64
	Epoch     uint64 // leader epoch that wrote the record; used to truncate divergent tails after failover
	Timestamp int64  // unix milliseconds
	Key       []byte
	Value     []byte
}

// On-disk framing: u32 body length | u32 CRC32-C(body) | body
// body: u64 offset | u64 epoch | i64 timestamp | u32 keylen | key | u32 vallen | value
const (
	headerSize    = 8
	fixedBodySize = 8 + 8 + 8 + 4 + 4

	// MaxRecordSize bounds len(key)+len(value) of a single record.
	MaxRecordSize = 8 << 20
)

var (
	crcTable   = crc32.MakeTable(crc32.Castagnoli)
	be         = binary.BigEndian
	errCorrupt = errors.New("seglog: corrupt record")
)

func encodedSize(r *Record) int {
	return headerSize + fixedBodySize + len(r.Key) + len(r.Value)
}

func appendEncoded(dst []byte, r *Record) []byte {
	start := len(dst)
	dst = be.AppendUint32(dst, uint32(fixedBodySize+len(r.Key)+len(r.Value)))
	dst = be.AppendUint32(dst, 0) // CRC, filled in below
	body := len(dst)
	dst = be.AppendUint64(dst, r.Offset)
	dst = be.AppendUint64(dst, r.Epoch)
	dst = be.AppendUint64(dst, uint64(r.Timestamp))
	dst = be.AppendUint32(dst, uint32(len(r.Key)))
	dst = append(dst, r.Key...)
	dst = be.AppendUint32(dst, uint32(len(r.Value)))
	dst = append(dst, r.Value...)
	be.PutUint32(dst[start+4:], crc32.Checksum(dst[body:], crcTable))
	return dst
}

// decodeBody parses a record body (the bytes after the header). Key and Value alias body.
func decodeBody(body []byte) (Record, error) {
	if len(body) < fixedBodySize {
		return Record{}, errCorrupt
	}
	r := Record{
		Offset:    be.Uint64(body[0:]),
		Epoch:     be.Uint64(body[8:]),
		Timestamp: int64(be.Uint64(body[16:])),
	}
	p := 24
	kl := int(be.Uint32(body[p:]))
	p += 4
	if kl > len(body)-p-4 {
		return Record{}, errCorrupt
	}
	r.Key = body[p : p+kl]
	p += kl
	vl := int(be.Uint32(body[p:]))
	p += 4
	if vl != len(body)-p {
		return Record{}, errCorrupt
	}
	r.Value = body[p:]
	return r, nil
}

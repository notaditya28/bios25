// Package layout defines a broker's on-disk data directory and scans it. The broker writes
// this layout; the manifest and the residency audit read it.
//
//	<data>/<broker>/broker.json                         Identity
//	<data>/<broker>/topics/<topic>/topic.json            TopicMeta (incl. residency)
//	<data>/<broker>/topics/<topic>/<partition>/*.log     segment files
package layout

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
)

const (
	IdentityFile  = "broker.json"
	TopicsDir     = "topics"
	TopicMetaFile = "topic.json"
	SegmentExt    = ".log"
)

// Identity pins a data directory to one broker in one region.
type Identity struct {
	ID     string `json:"id"`
	Region string `json:"region"`
	Zone   string `json:"zone"`
}

// TopicMeta is written before any partition data, so every byte on disk carries its residency tag.
type TopicMeta struct {
	api.TopicConfig
	CreatedAt time.Time `json:"created_at"`
}

func TopicDir(dataDir, topic string) string { return filepath.Join(dataDir, TopicsDir, topic) }

func PartitionDir(dataDir, topic string, p int) string {
	return filepath.Join(dataDir, TopicsDir, topic, strconv.Itoa(p))
}

type Segment struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	File      string `json:"file"` // base name, e.g. 00000000000000000000.log
	Bytes     int64  `json:"bytes"`
	Path      string `json:"-"`
}

type TopicScan struct {
	Name       string
	Meta       *TopicMeta // nil: data without a residency tag
	Partitions []int
	Segments   []Segment
}

type BrokerScan struct {
	Dir      string
	Identity *Identity // nil: broker.json missing or unreadable
	Topics   []*TopicScan
}

func (s *BrokerScan) HasData() bool {
	for _, t := range s.Topics {
		if len(t.Partitions) > 0 || len(t.Segments) > 0 {
			return true
		}
	}
	return false
}

// Scan reads a broker data directory without opening segment contents. Files that vanish
// while scanning (segments truncated by replication) are skipped.
func Scan(dir string) (*BrokerScan, error) {
	s := &BrokerScan{Dir: dir}
	var id Identity
	if err := fsutil.ReadJSON(filepath.Join(dir, IdentityFile), &id); err == nil {
		s.Identity = &id
	} else if !errors.Is(err, os.ErrNotExist) {
		s.Identity = nil
	}
	topics, err := os.ReadDir(filepath.Join(dir, TopicsDir))
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	for _, te := range topics {
		if !te.IsDir() {
			continue
		}
		ts := &TopicScan{Name: te.Name()}
		tdir := filepath.Join(dir, TopicsDir, te.Name())
		var m TopicMeta
		if err := fsutil.ReadJSON(filepath.Join(tdir, TopicMetaFile), &m); err == nil {
			ts.Meta = &m
		}
		parts, err := os.ReadDir(tdir)
		if err != nil {
			continue
		}
		for _, pe := range parts {
			p, err := strconv.Atoi(pe.Name())
			if !pe.IsDir() || err != nil {
				continue
			}
			ts.Partitions = append(ts.Partitions, p)
			files, err := os.ReadDir(filepath.Join(tdir, pe.Name()))
			if err != nil {
				continue
			}
			for _, fe := range files {
				if fe.IsDir() || !strings.HasSuffix(fe.Name(), SegmentExt) {
					continue
				}
				path := filepath.Join(tdir, pe.Name(), fe.Name())
				// Stat the file itself: on Windows the directory listing reports stale sizes
				// for files another process (the broker) has open for writing.
				info, err := os.Stat(path)
				if err != nil {
					continue
				}
				ts.Segments = append(ts.Segments, Segment{
					Topic: te.Name(), Partition: p, File: fe.Name(), Bytes: info.Size(), Path: path,
				})
			}
		}
		sort.Ints(ts.Partitions)
		sort.Slice(ts.Segments, func(i, j int) bool {
			a, b := ts.Segments[i], ts.Segments[j]
			if a.Partition != b.Partition {
				return a.Partition < b.Partition
			}
			return a.File < b.File
		})
		s.Topics = append(s.Topics, ts)
	}
	sort.Slice(s.Topics, func(i, j int) bool { return s.Topics[i].Name < s.Topics[j].Name })
	return s, nil
}

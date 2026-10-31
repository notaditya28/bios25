package manifest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/fsutil"
	"github.com/notaditya28/hydra-stream/internal/layout"
)

func TestBuildSignVerify(t *testing.T) {
	dir := t.TempDir()
	key, err := LoadOrCreateKey(filepath.Join(dir, "k"))
	if err != nil {
		t.Fatal(err)
	}
	again, _ := LoadOrCreateKey(filepath.Join(dir, "k"))
	if !key.Equal(again) {
		t.Fatal("key not persisted")
	}
	os.MkdirAll(layout.PartitionDir(dir, "t", 0), 0o755)
	fsutil.WriteJSONAtomic(filepath.Join(layout.TopicDir(dir, "t"), layout.TopicMetaFile),
		layout.TopicMeta{TopicConfig: api.TopicConfig{Name: "t", Partitions: 1, Residency: "IN"}})
	os.WriteFile(filepath.Join(layout.PartitionDir(dir, "t", 0), "00000000000000000000.log"), []byte("hello"), 0o644)

	s, err := Build(dir, api.Info{ID: "in-1", Region: "IN"}, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Manifest.Entries) != 1 || s.Manifest.Entries[0].Residency != "IN" || s.Manifest.Entries[0].Bytes != 5 {
		t.Fatalf("entries = %+v", s.Manifest.Entries)
	}
	pub := hex.EncodeToString(key.Public().(ed25519.PublicKey))
	if err := Verify(s, pub); err != nil {
		t.Fatal(err)
	}

	tampered := *s
	tampered.Manifest.Entries = append([]Entry(nil), s.Manifest.Entries...)
	tampered.Manifest.Entries[0].Residency = "global"
	if Verify(&tampered, pub) == nil {
		t.Fatal("tampered manifest verified")
	}
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	if Verify(s, hex.EncodeToString(other)) == nil {
		t.Fatal("verified with the wrong key")
	}
}

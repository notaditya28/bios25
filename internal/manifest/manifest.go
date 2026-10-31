// Package manifest builds a broker's signed manifest: every segment it stores, with its topic,
// residency tag, size and SHA-256, signed with the broker's Ed25519 key. The residency audit
// verifies the signature and checks the manifest against placement rules and the disk.
package manifest

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
	"github.com/notaditya28/hydra-stream/internal/layout"
)

type Entry struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Residency string `json:"residency"` // "" = segment without a topic.json tag
	File      string `json:"file"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"` // of the first Bytes bytes
}

type Manifest struct {
	Broker      string    `json:"broker"`
	Region      string    `json:"region"`
	Zone        string    `json:"zone"`
	GeneratedAt time.Time `json:"generated_at"`
	PublicKey   string    `json:"public_key"` // hex Ed25519 public key
	Entries     []Entry   `json:"entries"`
}

type Signed struct {
	Manifest  Manifest `json:"manifest"`
	Signature string   `json:"signature"` // hex Ed25519 signature over the JSON encoding of Manifest
}

// LoadOrCreateKey loads the broker's signing key, generating one on first start.
func LoadOrCreateKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		seed, err := hex.DecodeString(string(data))
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("bad key file %s", path)
		}
		return ed25519.NewKeyFromSeed(seed), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, []byte(hex.EncodeToString(priv.Seed())), 0o600); err != nil {
		return nil, err
	}
	return priv, nil
}

// HashPrefix returns the SHA-256 of the first n bytes of path.
func HashPrefix(path string, n int64) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if m, err := io.Copy(h, io.LimitReader(f, n)); err != nil || m != n {
		return "", fmt.Errorf("hash %s: read %d of %d bytes: %v", path, m, n, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Build scans dataDir and returns a signed manifest of its segments.
func Build(dataDir string, info api.Info, key ed25519.PrivateKey) (*Signed, error) {
	scan, err := layout.Scan(dataDir)
	if err != nil {
		return nil, err
	}
	m := Manifest{
		Broker: info.ID, Region: info.Region, Zone: info.Zone, GeneratedAt: time.Now().UTC(),
		PublicKey: hex.EncodeToString(key.Public().(ed25519.PublicKey)), Entries: []Entry{},
	}
	for _, t := range scan.Topics {
		res := ""
		if t.Meta != nil {
			res = t.Meta.Residency
		}
		for _, s := range t.Segments {
			sum, err := HashPrefix(s.Path, s.Bytes)
			if err != nil {
				continue // truncated or removed while scanning
			}
			m.Entries = append(m.Entries, Entry{s.Topic, s.Partition, res, s.File, s.Bytes, sum})
		}
	}
	return Sign(m, key)
}

func Sign(m Manifest, key ed25519.PrivateKey) (*Signed, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &Signed{Manifest: m, Signature: hex.EncodeToString(ed25519.Sign(key, data))}, nil
}

// Verify checks the signature with pub (hex). Pass the key you trust, not the one in the manifest.
func Verify(s *Signed, pubHex string) error {
	pub, err := hex.DecodeString(pubHex)
	if err != nil || len(pub) != ed25519.PublicKeySize {
		return errors.New("bad public key")
	}
	sig, err := hex.DecodeString(s.Signature)
	if err != nil {
		return errors.New("bad signature encoding")
	}
	data, err := json.Marshal(s.Manifest)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, data, sig) {
		return errors.New("signature does not match manifest")
	}
	return nil
}

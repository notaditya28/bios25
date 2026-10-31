package policy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

// LoadOrCreateAdminKey loads the administrator's signing key, creating a key pair (private key
// at keyPath, public key at pubPath) on first use. In production this key lives offline or in
// an HSM; brokers only ever get the public half.
func LoadOrCreateAdminKey(keyPath, pubPath string) (ed25519.PrivateKey, error) {
	if data, err := os.ReadFile(keyPath); err == nil {
		seed, err := hex.DecodeString(strings.TrimSpace(string(data)))
		if err != nil || len(seed) != ed25519.SeedSize {
			return nil, fmt.Errorf("bad admin key in %s", keyPath)
		}
		key := ed25519.NewKeyFromSeed(seed)
		if _, err := os.Stat(pubPath); errors.Is(err, os.ErrNotExist) {
			_ = os.WriteFile(pubPath, []byte(hex.EncodeToString(key.Public().(ed25519.PublicKey))+"\n"), 0o644)
		}
		return key, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(key.Seed())+"\n"), 0o600); err != nil {
		return nil, err
	}
	if err := os.WriteFile(pubPath, []byte(hex.EncodeToString(pub)+"\n"), 0o644); err != nil {
		return nil, err
	}
	return key, nil
}

// SignFile reads a policy document (JSON; version 0 = now as unix seconds, zero expiry = ttl
// from now) and signs it.
func SignFile(path string, key ed25519.PrivateKey, ttl time.Duration) (*Signed, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var doc Document
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	now := time.Now().UTC()
	if doc.Version == 0 {
		doc.Version = uint64(now.UnixMilli())
	}
	if doc.IssuedAt.IsZero() {
		doc.IssuedAt = now
	}
	if doc.ExpiresAt.IsZero() && ttl > 0 {
		doc.ExpiresAt = now.Add(ttl)
	}
	return Sign(doc, key)
}

// Package policy is Hydra Stream's jurisdiction policy engine.
//
// A policy says where a class of messages may be STORED (persisted on disk) and where it may
// TRANSIT (pass through a broker's memory on the way somewhere else). The two are separate:
// "store only in IN and EU, never pass through US" is expressible, unlike a single list of
// allowed clusters.
//
// Policies come from an administrator as a signed document (Ed25519) with a version and an
// expiry. Brokers verify the signature against the administrator's public key, refuse older
// versions, and fail closed: a topic whose policy is missing, unsigned or expired can't move
// data at all. Producers may NARROW a policy for their own messages (a stricter class), never
// widen it.
//
// Topic residency strings map to policies:
//
//	"<REGION>"        built-in: store and transit only in REGION
//	"global"          built-in: store and transit anywhere
//	"policy:<class>"  looked up in the installed signed document
package policy

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

const (
	Prefix = "policy:"
	Any    = "*"
)

// Durability floors a policy can impose (see Effective.MinAcks).
const (
	AcksLeader = "leader"
	AcksQuorum = "quorum"
	AcksAll    = "all"
)

func AcksRank(a string) int {
	switch a {
	case AcksLeader:
		return 0
	case AcksAll:
		return 2
	default:
		return 1 // quorum is the default
	}
}

type Policy struct {
	Class          string   `json:"class"`
	Description    string   `json:"description,omitempty"`
	StorageRegions []string `json:"storage_regions"`           // "*" = any region
	TransitRegions []string `json:"transit_regions,omitempty"` // extra regions data may pass through
	MinAcks        string   `json:"min_acks,omitempty"`        // durability floor: leader | quorum | all
}

type Document struct {
	Version   uint64    `json:"version"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Policies  []Policy  `json:"policies"`
}

type Signed struct {
	Document  Document `json:"document"`
	Signature string   `json:"signature"` // hex Ed25519 over the JSON encoding of Document
}

func Sign(doc Document, key ed25519.PrivateKey) (*Signed, error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return &Signed{Document: doc, Signature: hex.EncodeToString(ed25519.Sign(key, data))}, nil
}

func Verify(s *Signed, pub ed25519.PublicKey) error {
	if len(pub) != ed25519.PublicKeySize {
		return errors.New("no policy public key configured")
	}
	sig, err := hex.DecodeString(s.Signature)
	if err != nil {
		return errors.New("bad signature encoding")
	}
	data, err := json.Marshal(s.Document)
	if err != nil {
		return err
	}
	if !ed25519.Verify(pub, data, sig) {
		return errors.New("policy document signature does not match")
	}
	return nil
}

// LoadPublicKey reads a hex public key file.
func LoadPublicKey(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("bad public key in %s", path)
	}
	return ed25519.PublicKey(b), nil
}

// Effective is a resolved rule: what a piece of data may do.
type Effective struct {
	Name    string          `json:"name"`              // residency string or class it came from
	Storage map[string]bool `json:"storage,omitempty"` // nil = any region
	Transit map[string]bool `json:"transit,omitempty"` // nil = any region (storage regions are always transit-allowed)
	MinAcks string          `json:"min_acks,omitempty"`
	Version uint64          `json:"version"` // policy document version (0 = built-in rule)
}

func set(regions []string) map[string]bool {
	if slices.Contains(regions, Any) {
		return nil
	}
	m := map[string]bool{}
	for _, r := range regions {
		m[r] = true
	}
	return m
}

func (e Effective) CanStore(region string) bool {
	return region != "" && (e.Storage == nil || e.Storage[region])
}

// CanTransit: may data pass through (but not be persisted in) region? Invariants kept by every
// constructor: Transit ⊇ Storage, and Storage == nil (store anywhere) implies Transit == nil.
func (e Effective) CanTransit(region string) bool {
	return region != "" && (e.Transit == nil || e.Transit[region])
}

// StorageRegions lists the regions (from all) this rule may store in, sorted.
func (e Effective) StorageRegions(all []string) []string {
	var out []string
	for _, r := range all {
		if e.CanStore(r) {
			out = append(out, r)
		}
	}
	sort.Strings(out)
	return out
}

// Narrow intersects two rules: data must satisfy both. The stricter durability floor wins.
func (e Effective) Narrow(o Effective) Effective {
	inter := func(a, b map[string]bool) map[string]bool {
		switch {
		case a == nil:
			return b
		case b == nil:
			return a
		}
		m := map[string]bool{}
		for r := range a {
			if b[r] {
				m[r] = true
			}
		}
		return m
	}
	n := Effective{Name: e.Name + "∩" + o.Name, Storage: inter(e.Storage, o.Storage), Transit: inter(e.Transit, o.Transit),
		MinAcks: e.MinAcks, Version: max(e.Version, o.Version)}
	if n.Storage != nil && n.Transit != nil { // keep Storage ⊆ Transit
		for r := range n.Storage {
			if !n.Transit[r] {
				delete(n.Storage, r)
			}
		}
	}
	if o.MinAcks != "" && (n.MinAcks == "" || AcksRank(o.MinAcks) > AcksRank(n.MinAcks)) {
		n.MinAcks = o.MinAcks
	}
	return n
}

// Errors (all explicit, all fail closed).
func Unavailable(format string, args ...any) *api.Error {
	e := api.Errorf(api.CodePolicyUnavailable, format, args...)
	e.Reason = "policy_unavailable"
	return e
}

// Engine holds the installed policy document and resolves residency strings.
type Engine struct {
	pub     ed25519.PublicKey
	regions []string
	now     func() time.Time

	mu      sync.RWMutex
	current *Signed
	byClass map[string]Policy
}

func NewEngine(pub ed25519.PublicKey, regions []string) *Engine {
	return &Engine{pub: pub, regions: append([]string(nil), regions...), now: time.Now, byClass: map[string]Policy{}}
}

func (e *Engine) Regions() []string { return append([]string(nil), e.regions...) }

// Install verifies and installs a signed document. Older versions are refused (no rollback);
// re-installing the current version is a no-op.
func (e *Engine) Install(s *Signed) error {
	if err := Verify(s, e.pub); err != nil {
		return err
	}
	d := s.Document
	if !d.ExpiresAt.IsZero() && e.now().After(d.ExpiresAt) {
		return fmt.Errorf("policy document v%d expired at %s", d.Version, d.ExpiresAt.Format(time.RFC3339))
	}
	by := map[string]Policy{}
	for _, p := range d.Policies {
		if p.Class == "" || strings.ContainsAny(p.Class, " /:") {
			return fmt.Errorf("bad class name %q", p.Class)
		}
		if _, dup := by[p.Class]; dup {
			return fmt.Errorf("class %q defined twice", p.Class)
		}
		for _, r := range append(append([]string(nil), p.StorageRegions...), p.TransitRegions...) {
			if r != Any && !slices.Contains(e.regions, r) {
				return fmt.Errorf("class %q names unknown region %q", p.Class, r)
			}
		}
		if len(p.StorageRegions) == 0 {
			return fmt.Errorf("class %q has no storage regions", p.Class)
		}
		if p.MinAcks != "" && p.MinAcks != AcksLeader && p.MinAcks != AcksQuorum && p.MinAcks != AcksAll {
			return fmt.Errorf("class %q: bad min_acks %q", p.Class, p.MinAcks)
		}
		by[p.Class] = p
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.current != nil {
		switch {
		case d.Version < e.current.Document.Version:
			return fmt.Errorf("policy document v%d is older than installed v%d", d.Version, e.current.Document.Version)
		case d.Version == e.current.Document.Version:
			if s.Signature != e.current.Signature {
				return fmt.Errorf("a different document with version %d is already installed", d.Version)
			}
			return nil
		}
	}
	e.current, e.byClass = s, by
	return nil
}

// ForSimulation returns an engine holding d WITHOUT signature checks. It exists only so the
// scenario simulator can evaluate proposed (not yet signed) policy changes with exactly the same
// resolution code the brokers use; brokers never construct engines this way.
func ForSimulation(d Document, regions []string) *Engine {
	e := NewEngine(nil, regions)
	by := map[string]Policy{}
	for _, p := range d.Policies {
		by[p.Class] = p
	}
	e.current, e.byClass = &Signed{Document: d}, by
	return e
}

// Document returns the installed document (zero value if none).
func (e *Engine) Document() Document {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.current == nil {
		return Document{}
	}
	return e.current.Document
}

func (e *Engine) Current() *Signed {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

// Class resolves a policy class from the installed document (fail closed).
func (e *Engine) Class(class string) (Effective, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.current == nil {
		return Effective{}, Unavailable("no signed policy document installed; class %q can't be evaluated", class)
	}
	d := e.current.Document
	if !d.ExpiresAt.IsZero() && e.now().After(d.ExpiresAt) {
		return Effective{}, Unavailable("policy document v%d expired at %s", d.Version, d.ExpiresAt.Format(time.RFC3339))
	}
	p, ok := e.byClass[class]
	if !ok {
		return Effective{}, Unavailable("class %q is not defined in policy document v%d", class, d.Version)
	}
	eff := Effective{Name: Prefix + class, Storage: set(p.StorageRegions), MinAcks: p.MinAcks, Version: d.Version}
	if eff.Storage != nil { // storing anywhere implies transiting anywhere
		eff.Transit = set(append(append([]string(nil), p.StorageRegions...), p.TransitRegions...))
	}
	return eff, nil
}

// Resolve maps a topic residency string to its rule.
func (e *Engine) Resolve(residency string) (Effective, error) {
	switch {
	case residency == api.ResidencyGlobal:
		return Effective{Name: residency}, nil
	case strings.HasPrefix(residency, Prefix):
		return e.Class(strings.TrimPrefix(residency, Prefix))
	case slices.Contains(e.regions, residency):
		m := map[string]bool{residency: true}
		return Effective{Name: residency, Storage: m, Transit: m}, nil
	}
	return Effective{}, Unavailable("unknown residency %q", residency)
}

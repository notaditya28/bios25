package policy

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/notaditya28/hydra-stream/internal/api"
)

var regions = []string{"EU", "IN", "US"}

func doc(version uint64, ps ...Policy) Document {
	return Document{Version: version, IssuedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour), Policies: ps}
}

func keys(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

var financial = Policy{Class: "financial", StorageRegions: []string{"IN", "EU"}, MinAcks: AcksAll}
var euPersonal = Policy{Class: "eu-personal", StorageRegions: []string{"EU"}, TransitRegions: []string{"US"}}

func TestSignedDocumentsAndFailClosed(t *testing.T) {
	pub, priv := keys(t)
	e := NewEngine(pub, regions)

	// No document yet: policy classes fail closed, built-ins still work.
	if _, err := e.Resolve("policy:financial"); api.CodeOf(err) != api.CodePolicyUnavailable {
		t.Fatalf("no document: %v", err)
	}
	if eff, err := e.Resolve("IN"); err != nil || !eff.CanStore("IN") || eff.CanStore("EU") || eff.CanTransit("US") {
		t.Fatalf("built-in IN: %+v %v", eff, err)
	}

	s, _ := Sign(doc(2, financial, euPersonal), priv)
	if err := e.Install(s); err != nil {
		t.Fatal(err)
	}
	// Tampering breaks the signature.
	bad := *s
	bad.Document.Policies = []Policy{{Class: "financial", StorageRegions: []string{"*"}}}
	bad.Document.Version = 3
	if err := e.Install(&bad); err == nil {
		t.Fatal("tampered document installed")
	}
	// A different key is refused.
	_, other := keys(t)
	foreign, _ := Sign(doc(4, financial), other)
	if err := e.Install(foreign); err == nil {
		t.Fatal("document signed by another key installed")
	}
	// No rollback to an older version.
	old, _ := Sign(doc(1, Policy{Class: "financial", StorageRegions: []string{"*"}}), priv)
	if err := e.Install(old); err == nil {
		t.Fatal("older version installed")
	}
	// Expired documents are refused, and an installed one that expires fails closed.
	exp := doc(5, financial)
	exp.ExpiresAt = time.Now().Add(-time.Minute)
	es, _ := Sign(exp, priv)
	if err := e.Install(es); err == nil {
		t.Fatal("expired document installed")
	}
	e.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	if _, err := e.Resolve("policy:financial"); api.CodeOf(err) != api.CodePolicyUnavailable {
		t.Fatalf("expired installed document still used: %v", err)
	}
}

func TestStorageTransitAndNarrowing(t *testing.T) {
	pub, priv := keys(t)
	e := NewEngine(pub, regions)
	s, _ := Sign(doc(1, financial, euPersonal, Policy{Class: "public", StorageRegions: []string{"*"}}), priv)
	if err := e.Install(s); err != nil {
		t.Fatal(err)
	}
	fin, _ := e.Resolve("policy:financial")
	if !fin.CanStore("IN") || !fin.CanStore("EU") || fin.CanStore("US") || fin.CanTransit("US") || fin.MinAcks != AcksAll {
		t.Fatalf("financial: %+v", fin)
	}
	eu, _ := e.Resolve("policy:eu-personal")
	if eu.CanStore("US") || !eu.CanTransit("US") || eu.CanTransit("IN") {
		t.Fatalf("eu-personal: storage EU, transit EU+US; got %+v", eu)
	}
	// A producer can only narrow: global ∩ eu-personal = eu-personal's rule.
	g, _ := e.Resolve(api.ResidencyGlobal)
	n := g.Narrow(eu)
	if n.CanStore("IN") || !n.CanStore("EU") || !n.CanTransit("US") || n.CanTransit("IN") {
		t.Fatalf("global ∩ eu-personal = %+v", n)
	}
	// financial ∩ eu-personal = store only EU, transit only EU (US isn't allowed by financial).
	n = fin.Narrow(eu)
	if !n.CanStore("EU") || n.CanStore("IN") || n.CanTransit("US") || n.MinAcks != AcksAll {
		t.Fatalf("financial ∩ eu-personal = %+v", n)
	}
	// Narrowing can never widen.
	pub2, _ := e.Resolve("policy:public")
	if n2 := fin.Narrow(pub2); n2.CanStore("US") || n2.CanTransit("US") {
		t.Fatalf("financial ∩ public widened: %+v", n2)
	}
}

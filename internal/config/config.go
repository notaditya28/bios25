// Package config loads the cluster topology: regions, and each broker's id, zone and address.
// Everything about where a broker lives comes from here so the same binary runs as local
// processes or in docker-compose.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type Cluster struct {
	Regions []Region `json:"regions"`
	// PolicyPublicKeyFile holds the administrator's Ed25519 public key (hex) that signs policy
	// documents. Relative paths are relative to the config file.
	PolicyPublicKeyFile string `json:"policy_public_key_file,omitempty"`

	dir string
}

// PolicyKeyPath resolves PolicyPublicKeyFile ("" if unset).
func (c *Cluster) PolicyKeyPath() string {
	if c.PolicyPublicKeyFile == "" || filepath.IsAbs(c.PolicyPublicKeyFile) {
		return c.PolicyPublicKeyFile
	}
	return filepath.Join(c.dir, c.PolicyPublicKeyFile)
}

type Region struct {
	Name    string   `json:"name"`
	Brokers []Broker `json:"brokers"`
}

type Broker struct {
	ID   string `json:"id"`
	Zone string `json:"zone"`
	Addr string `json:"addr"` // host:port the broker listens on and peers dial
}

var regionName = regexp.MustCompile(`^[A-Z]{2,8}$`)

func Load(path string) (*Cluster, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Cluster
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	c.dir = filepath.Dir(path)
	return &c, nil
}

func (c *Cluster) Validate() error {
	if len(c.Regions) == 0 {
		return fmt.Errorf("no regions")
	}
	regions, ids, addrs := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, r := range c.Regions {
		if !regionName.MatchString(r.Name) || strings.EqualFold(r.Name, "global") {
			return fmt.Errorf("invalid region name %q (want 2-8 upper-case letters)", r.Name)
		}
		if regions[r.Name] {
			return fmt.Errorf("duplicate region %q", r.Name)
		}
		regions[r.Name] = true
		if len(r.Brokers) == 0 {
			return fmt.Errorf("region %s has no brokers", r.Name)
		}
		zones := map[string]bool{}
		for _, b := range r.Brokers {
			if b.ID == "" || b.Zone == "" || b.Addr == "" {
				return fmt.Errorf("region %s: broker needs id, zone and addr: %+v", r.Name, b)
			}
			if ids[b.ID] {
				return fmt.Errorf("duplicate broker id %q", b.ID)
			}
			if addrs[b.Addr] {
				return fmt.Errorf("duplicate broker addr %q", b.Addr)
			}
			if zones[b.Zone] {
				return fmt.Errorf("region %s: duplicate zone %q", r.Name, b.Zone)
			}
			ids[b.ID], addrs[b.Addr], zones[b.Zone] = true, true, true
		}
	}
	return nil
}

// Find returns the broker with the given id and the region it belongs to.
func (c *Cluster) Find(id string) (Broker, Region, bool) {
	for _, r := range c.Regions {
		for _, b := range r.Brokers {
			if b.ID == id {
				return b, r, true
			}
		}
	}
	return Broker{}, Region{}, false
}

func (c *Cluster) RegionNames() []string {
	names := make([]string, len(c.Regions))
	for i, r := range c.Regions {
		names[i] = r.Name
	}
	return names
}

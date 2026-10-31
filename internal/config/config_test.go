package config

import (
	"strings"
	"testing"
)

func TestLocalConfig(t *testing.T) {
	c, err := Load("../../configs/cluster.local.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(c.RegionNames(), ","); got != "IN,EU,US" {
		t.Fatalf("regions = %s", got)
	}
	b, r, ok := c.Find("eu-2")
	if !ok || r.Name != "EU" || b.Zone != "eu-b" {
		t.Fatalf("Find(eu-2) = %+v %s %v", b, r.Name, ok)
	}
}

func TestValidate(t *testing.T) {
	ok := Broker{ID: "a", Zone: "z1", Addr: ":1"}
	cases := map[string]Cluster{
		"no regions":      {},
		"bad name":        {Regions: []Region{{Name: "in", Brokers: []Broker{ok}}}},
		"global reserved": {Regions: []Region{{Name: "GLOBAL", Brokers: []Broker{ok}}}},
		"dup id": {Regions: []Region{{Name: "IN", Brokers: []Broker{ok}},
			{Name: "EU", Brokers: []Broker{{ID: "a", Zone: "z2", Addr: ":2"}}}}},
		"dup zone": {Regions: []Region{{Name: "IN", Brokers: []Broker{ok, {ID: "b", Zone: "z1", Addr: ":2"}}}}},
		"dup addr": {Regions: []Region{{Name: "IN", Brokers: []Broker{ok, {ID: "b", Zone: "z2", Addr: ":1"}}}}},
	}
	for name, c := range cases {
		if err := c.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

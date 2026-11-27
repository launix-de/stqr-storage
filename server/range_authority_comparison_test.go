// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"reflect"
	"testing"

	"github.com/launix-de/stqr-storage/client"
)

func reflectedRangeAuthority(a, b client.RangeEntry) bool {
	a.PolicyVersion, b.PolicyVersion = 0, 0
	return reflect.DeepEqual(a, b)
}

func TestSameRangeAuthorityMatchesReflectWithoutMutation(t *testing.T) {
	base := client.RangeEntry{Start: client.ObjectID{1}, PrefixBits: 16, PolicyVersion: 7, OwnerEpoch: 8, State: 2, Runtime: client.ID{9}, Locations: []client.Location{{Node: client.ID{10}, Volume: client.ID{11}, Role: 1, Class: 2, Fragment: 3, Flags: 4}}}
	type variant struct {
		name   string
		change func(*client.RangeEntry)
	}
	variants := []variant{
		{"same", func(*client.RangeEntry) {}},
		{"Start", func(a *client.RangeEntry) { a.Start[31]++ }},
		{"PrefixBits", func(a *client.RangeEntry) { a.PrefixBits++ }},
		{"PolicyVersion", func(a *client.RangeEntry) { a.PolicyVersion++ }},
		{"OwnerEpoch", func(a *client.RangeEntry) { a.OwnerEpoch++ }},
		{"State", func(a *client.RangeEntry) { a.State++ }},
		{"Runtime", func(a *client.RangeEntry) { a.Runtime[15]++ }},
		{"Locations", func(a *client.RangeEntry) { a.Locations = append(a.Locations, client.Location{}) }},
		{"Locations.Node", func(a *client.RangeEntry) { a.Locations[0].Node[15]++ }},
		{"Locations.Volume", func(a *client.RangeEntry) { a.Locations[0].Volume[15]++ }},
		{"Locations.Role", func(a *client.RangeEntry) { a.Locations[0].Role++ }},
		{"Locations.Class", func(a *client.RangeEntry) { a.Locations[0].Class++ }},
		{"Locations.Fragment", func(a *client.RangeEntry) { a.Locations[0].Fragment++ }},
		{"Locations.Flags", func(a *client.RangeEntry) { a.Locations[0].Flags++ }},
		{"nil", func(a *client.RangeEntry) { a.Locations = nil }},
		{"empty", func(a *client.RangeEntry) { a.Locations = []client.Location{} }},
	}
	// Fail loudly when the wire types gain fields so the scalar comparator and
	// this exhaustive differential test must be reviewed together.
	for _, typ := range []reflect.Type{reflect.TypeOf(base), reflect.TypeOf(client.Location{})} {
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Name
			if typ == reflect.TypeOf(client.Location{}) {
				name = "Locations." + name
			}
			found := false
			for _, v := range variants {
				found = found || v.name == name
			}
			if !found {
				t.Fatalf("authority field %s needs comparator coverage", name)
			}
		}
	}
	var values []client.RangeEntry
	for _, v := range variants {
		a := base
		a.Locations = append([]client.Location(nil), base.Locations...)
		v.change(&a)
		values = append(values, a)
	}
	for i, a := range values {
		for j, b := range values {
			t.Run(variants[i].name+"/"+variants[j].name, func(t *testing.T) {
				beforeA, beforeB := a, b
				if a.Locations != nil {
					beforeA.Locations = append([]client.Location{}, a.Locations...)
				}
				if b.Locations != nil {
					beforeB.Locations = append([]client.Location{}, b.Locations...)
				}
				if got, want := sameRangeAuthority(a, b), reflectedRangeAuthority(a, b); got != want {
					t.Fatalf("got %v, reflected %v", got, want)
				}
				if !reflect.DeepEqual(a, beforeA) || !reflect.DeepEqual(b, beforeB) {
					t.Fatal("comparison changed caller fields or locations")
				}
			})
		}
	}
}

func TestSameRangeAuthorityAllocatesZero(t *testing.T) {
	a := client.RangeEntry{PolicyVersion: 4, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{2}}, {Node: client.ID{3}}}}
	b := a
	b.PolicyVersion = 99
	b.Locations = append([]client.Location(nil), a.Locations...)
	if n := testing.AllocsPerRun(1000, func() {
		if !sameRangeAuthority(a, b) {
			panic("equal authority rejected")
		}
	}); n != 0 {
		t.Fatalf("authority comparison allocated %v times", n)
	}
}

func BenchmarkSameRangeAuthority(b *testing.B) {
	a := client.RangeEntry{PolicyVersion: 4, Locations: []client.Location{{Node: client.ID{1}, Volume: client.ID{2}}, {Node: client.ID{3}}}}
	other := a
	other.PolicyVersion = 99
	other.Locations = append([]client.Location(nil), a.Locations...)
	for _, mode := range []string{"reflect", "scalar"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				var equal bool
				if mode == "reflect" {
					equal = reflectedRangeAuthority(a, other)
				} else {
					equal = sameRangeAuthority(a, other)
				}
				if !equal {
					b.Fatal("authority differs")
				}
			}
		})
	}
}

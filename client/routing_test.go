// SPDX-License-Identifier: GPL-3.0-only
package client

import "testing"

func TestGoAwayPreservesIndependentRangesAndShortPlacement(t *testing.T) {
	for _, reason := range []uint8{0, 1, 2, 3} {
		t.Run(string(rune('0'+reason)), func(t *testing.T) {
			p := testMapPatch()
			left, right, err := SplitRange(p.Entries[0])
			if err != nil {
				t.Fatal(err)
			}
			right.Locations = []Location{{Node: ID{3}}}
			p.Entries = []RangeEntry{left, right}
			p.Addresses = append(p.Addresses, NodeAddress{Node: ID{3}, Addresses: []string{"http://other:7419"}})
			c := &Client{routers: map[string]ID{"ws://node:7419/api/ws": {2}}}
			if _, err := c.routes.Apply(p); err != nil {
				t.Fatal(err)
			}
			e := Encoder{}
			e.U8(reason)
			e.ID(ID{9})
			e.V(1)
			e.ID(ID{3})
			e.V(1)
			e.Text("http://other:7419")
			e.U64(1)
			e.U64(2)
			if err := c.learnGoAway("ws://node:7419/api/ws", Message{Body: e.B}); err != nil {
				t.Fatal(err)
			}
			_, _, _, _, known := c.routes.Lookup(p.Bucket, ObjectID{})
			if known != (reason == 1 || reason == 2) {
				t.Fatal("incorrect old primary route", known)
			}
			if _, _, _, _, known := c.routes.Lookup(p.Bucket, right.Start); !known {
				t.Fatal("independent primary route discarded")
			}
			if c.routers["ws://other:7419/api/ws"] != (ID{3}) {
				t.Fatal("alternative router not remembered")
			}
			if reason == 0 || reason == 3 {
				// An in-flight old redirect must not resurrect a departed owner.
				if _, err := c.routes.Apply(p); err != nil {
					t.Fatal(err)
				}
				c.rememberRouters(p)
				if _, _, _, _, known := c.routes.Lookup(p.Bucket, ObjectID{}); known {
					t.Fatal("late redirect revived departed owner")
				}
				p.Membership = 2
				if _, err := c.routes.Apply(p); err != nil {
					t.Fatal(err)
				}
				c.rememberRouters(p)
				c.routes.ForgetNode(ID{2}, 1)
				if _, _, _, _, known := c.routes.Lookup(p.Bucket, ObjectID{}); !known {
					t.Fatal("old departure invalidated a newer membership")
				}
			}
			if err := c.learnGoAway("ws://node:7419/api/ws", Message{Body: e.B[:len(e.B)-1]}); err != ErrProtocol {
				t.Fatal("truncated terminal message accepted", err)
			}
		})
	}
}

func TestSeedURLRemainsAliasForItsAdvertisedNode(t *testing.T) {
	p := testMapPatch()
	p.Addresses[0].Addresses = []string{"http://intranet-node:7419"}
	c := &Client{endpoint: "ws://127.0.0.1:17419/api/ws", bucket: p.Bucket, routers: map[string]ID{"ws://127.0.0.1:17419/api/ws": {2}}}
	if _, err := c.routes.Apply(p); err != nil {
		t.Fatal(err)
	}
	c.rememberRouters(p)
	m := Message{Opcode: 0x0100, Body: make([]byte, 80)}
	copy(m.Body, p.Bucket[:])
	endpoint, node, err := c.routeMessage(&m)
	if err != nil || node != (ID{2}) || endpoint != c.endpoint {
		t.Fatal("discarded caller's seed URL", endpoint, err)
	}
	// A genuine address change must still be learned rather than permanently
	// pinning all future traffic for that node to the old seed alias.
	p.Revision++
	p.Addresses[0].Addresses = []string{"http://new-node-address:7419"}
	if _, err := c.routes.Apply(p); err != nil {
		t.Fatal(err)
	}
	c.rememberRouters(p)
	endpoint, _, err = c.routeMessage(&m)
	if err != nil || endpoint != "ws://new-node-address:7419/api/ws" {
		t.Fatal("ignored changed node address", endpoint, err)
	}
}

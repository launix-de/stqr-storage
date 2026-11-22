// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"testing"
	"time"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestDaemonPlacementMaskTransitionsKeepOldAndNewReadable(t *testing.T) {
	for _, class := range []string{"hdd", "ram"} {
		t.Run(class, func(t *testing.T) {
			source, target := startAutomaticDaemonPair(t, false)
			var volumes [2]volume.ID
			for i, n := range []*daemonTestNode{source, target} {
				if class == "ram" {
					op, err := n.node.AddVolume(newID(), AddVolumeRequest{Name: "data", Source: 2, Class: "ram", SizeBytes: 8 << 20})
					if err != nil {
						t.Fatal(err)
					}
					id, _ := parseID(op.VolumeID)
					volumes[i] = id
				} else {
					op := addTestVolume(t, n.node, "data", class, 8<<20)
					id, _ := parseID(op.VolumeID)
					volumes[i] = id
				}
			}
			draft := newTestBucket(t, "pg-transition-"+class)
			draft.description.Policy.StorageStrategy = class
			_, status, _ := adminBucketMessage(t, source.node, client.BucketCreate, bucketPolicyBody(t, draft))
			if status != 201 {
				t.Fatal(status)
			}
			bucket, _ := source.node.Bucket(draft.ID())
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer cancel()
			admin := source.node.cfg.Auth.BootstrapAdmin
			adminSocket, _, err := dialTestSocket(t, source.node.cfg.Node.Advertise, admin.Username, admin.Password, "")
			if err != nil {
				t.Fatal(err)
			}
			defer adminSocket.Close()
			objects, err := client.Dial(ctx, source.node.cfg.Node.Advertise, admin.Username, admin.Password, client.BucketID(bucket.ID()))
			if err != nil {
				t.Fatal(err)
			}
			defer objects.Close()
			payload := bytes.Repeat([]byte{0x73}, 1<<20)
			ids := []client.ObjectID{{1}, {0x81}}
			infos := make(map[client.ObjectID]client.ObjectInfo)
			for _, id := range ids {
				info, err := objects.Put(ctx, id, payload, client.PutOptions{})
				if err != nil {
					t.Fatal(err)
				}
				infos[id] = info
			}
			group := volume.ID(client.NewRequestID())
			mask := func(n *daemonTestNode, v volume.ID) []PlacementNode {
				return []PlacementNode{{NodeID: n.node.cfg.Node.ID, Mode: VolumesWhitelist, Volumes: []string{v.String()}}}
			}
			set := func(expected uint64, nodes []PlacementNode) client.Message {
				t.Helper()
				raw, _ := json.Marshal(nodes)
				e := client.Encoder{}
				e.U16(1)
				e.ID(client.ID(group))
				e.U64(expected)
				e.Text("data group")
				e.Blob(raw)
				m := client.Message{Kind: client.Request, Opcode: placementGroupSet, ID: client.NewRequestID(), Body: e.B}
				if err := adminSocket.WriteMessage(websocket.BinaryMessage, client.EncodeMessage(m)); err != nil {
					t.Fatal(err)
				}
				adminSocket.SetReadDeadline(time.Now().Add(10 * time.Second))
				_, replyWire, err := adminSocket.ReadMessage()
				if err != nil {
					t.Fatal(err)
				}
				reply, err := client.DecodeMessage(replyWire)
				if err != nil || reply.ID != m.ID || reply.Opcode != m.Opcode {
					t.Fatal("uncorrelated PG admin response", err)
				}
				response := client.Decoder{B: reply.Body}
				status := int(response.V())
				if status != 201 && status != 202 && status != 200 {
					t.Fatal("PG set", status)
				}
				return m
			}
			created := set(0, mask(source, volumes[0]))
			assign := func(groupID *volume.ID) {
				t.Helper()
				e := client.Encoder{}
				e.U16(1)
				e.ID(client.ID(bucket.ID()))
				e.U64(bucket.Describe().Policy.Version)
				e.Bool(groupID != nil)
				if groupID != nil {
					e.ID(client.ID(*groupID))
				}
				_, status, _ := adminBucketMessage(t, source.node, bucketSetPlacementGroup, e.B)
				if status != 202 {
					t.Fatal("bucket PG assignment", status)
				}
			}
			settle := func() {
				t.Helper()
				for {
					local, err := source.node.Bucket(bucket.ID())
					if err != nil {
						t.Fatal(err)
					}
					settled := local.Describe().DesiredPolicy == nil
					for _, n := range []*daemonTestNode{source, target} {
						l := n.node.control.ledger
						l.mu.Lock()
						settled = settled && l.slot == nil && l.request == nil
						l.mu.Unlock()
					}
					if settled {
						break
					}
					for _, n := range []*daemonTestNode{source, target} {
						b, err := n.node.Bucket(bucket.ID())
						if err != nil {
							t.Fatal(err)
						}
						if p, ok := n.node.scopes.planPolicyPlacement(b.Describe(), false, nil); ok {
							if err := n.node.scopes.move(ctx, p); err != nil && statusOf(err) != 409 && statusOf(err) != 503 {
								t.Fatal("PG policy handoff", err)
							}
						}
					}
					if ctx.Err() != nil {
						logDaemonRangeState(t, source, target)
						t.Fatal("PG transition did not finish", ctx.Err())
					}
					time.Sleep(25 * time.Millisecond)
				}
			}
			beforePhysical := source.node.volumes[volumes[0].String()].store.Stats().DataPhysical
			assign(&group)
			settle()
			if after := source.node.volumes[volumes[0].String()].store.Stats().DataPhysical; after != beforePhysical {
				t.Fatal("unchanged eligible placement copied payload", beforePhysical, after)
			}
			set(1, mask(target, volumes[1]))
			lookup := func() PlacementGroup {
				t.Helper()
				for _, g := range source.node.placementGroups.Load().object.Groups {
					if g.ID == group.String() {
						return g
					}
				}
				t.Fatal("PG disappeared")
				return PlacementGroup{}
			}
			g := lookup()
			if g.Desired == nil || g.Effective.Nodes[0].NodeID != source.node.cfg.Node.ID || g.Desired.Nodes[0].NodeID != target.node.cfg.Node.ID {
				t.Fatal("desired mask replaced effective before handoff")
			}
			for _, id := range ids {
				got, _, err := objects.Get(ctx, id)
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatal("old effective placement became unreadable", err)
				}
			}
			target = restartDaemonTestNode(t, target)
			for {
				if _, err := source.node.controller.info(ctx, target.node.control.ledger.node); err == nil {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(25 * time.Millisecond)
			}
			settle()
			for lookup().Desired != nil {
				if ctx.Err() != nil {
					t.Fatal("PG effective activation stalled")
				}
				time.Sleep(25 * time.Millisecond)
			}
			for _, id := range ids {
				got, info, err := objects.Get(ctx, id)
				if err != nil || !bytes.Equal(got, payload) || info.Version != infos[id].Version || info.RuntimeID != infos[id].RuntimeID {
					t.Fatal("mask migration changed data/version/runtime", err)
				}
			}
			if source.node.volumes[volumes[0].String()].store.Stats().LogicalBytes != 0 {
				t.Fatal("old PG volume retained physical objects")
			}
			g = lookup()
			if g.Effective.Version != 2 || g.Effective.Nodes[0].NodeID != target.node.cfg.Node.ID {
				t.Fatal("wrong effective mask")
			}
			e := client.Encoder{}
			e.U16(1)
			e.ID(client.ID(group))
			e.U64(g.Revision)
			_, status, _ = adminBucketMessage(t, source.node, placementGroupDelete, e.B)
			if status != 409 {
				t.Fatal("referenced group was deleted", status)
			}
			assign(nil)
			settle()
			_, status, _ = adminBucketMessage(t, source.node, placementGroupDelete, e.B)
			if status != 200 {
				t.Fatal("unused PG deletion", status)
			}
			if _, status := source.node.dispatchAs(created, nil, userPrincipal(t, source.node)); status != 201 {
				t.Fatal("original create receipt lost", status)
			}
			for _, g := range source.node.placementGroups.Load().object.Groups {
				if g.ID == group.String() {
					t.Fatal("create retry resurrected deleted PG")
				}
			}
		})
	}
}

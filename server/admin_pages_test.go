// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func adminPageBody(after string, limit uint32, query string, ids ...string) []byte {
	e := client.Encoder{}
	e.U16(1)
	e.Text(after)
	e.U32(limit)
	e.Text(query)
	e.V(uint64(len(ids)))
	for _, id := range ids {
		e.ID(client.ID(mustBucketID(id)))
	}
	return e.B
}

type adminTestPage struct {
	Users          []userPageEntry   `json:"users"`
	Buckets        []bucketListEntry `json:"buckets"`
	Next           string            `json:"next"`
	Total          int               `json:"total"`
	AllTotal       int               `json:"all_total"`
	Active, Locked int
}

func readAdminTestPage(t *testing.T, s *Server, op uint16, body []byte) adminTestPage {
	t.Helper()
	m := client.Message{Kind: client.Request, Opcode: op, ID: client.NewRequestID(), Body: body}
	wire, status := s.dispatchAs(m, nil, userPrincipal(t, s))
	if status != 200 {
		t.Fatalf("page status %d: %x", status, wire)
	}
	d := client.Decoder{B: wire}
	d.V()
	d.Blob()
	raw := d.Text()
	if d.Done() != nil {
		t.Fatal("invalid page response")
	}
	var result adminTestPage
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func TestAdminCursorPagesSearchAndLookup(t *testing.T) {
	s := testServer(t)
	var accounts accountsImage
	if err := s.readBlob("accounts", &accounts); err != nil {
		t.Fatal(err)
	}
	hash := accounts.Users[0].PasswordHash
	for i := 0; i < 125; i++ {
		accounts.Users = append(accounts.Users, account{ID: volume.ID(client.NewRequestID()).String(), Name: fmt.Sprintf("tenant-user-%03d", i), Role: "USER", PasswordHash: hash})
		b := newTestBucket(t, fmt.Sprintf("tenant-bucket-%03d", i))
		if _, err := s.buckets.Save(b.Describe(), 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.writeBlob("accounts", accounts); err != nil {
		t.Fatal(err)
	}
	for _, op := range []uint16{client.UserListPage, client.BucketListPage} {
		seen := map[string]bool{}
		after := ""
		for pages := 0; ; pages++ {
			page := readAdminTestPage(t, s, op, adminPageBody(after, 17, ""))
			if page.Total != 126 || page.AllTotal != 126 {
				t.Fatal("wrong list totals", page.Total, page.AllTotal)
			}
			names := map[string]string{}
			for _, u := range page.Users {
				names[u.ID] = u.Name
			}
			for _, b := range page.Buckets {
				names[b.ID] = b.Name
				if len(b.Receipts) != 0 {
					t.Fatal("bucket receipts exposed")
				}
			}
			if len(names) > 17 {
				t.Fatal("page limit ignored")
			}
			for id, name := range names {
				if seen[id] || name <= after {
					t.Fatal("overlapping/nonmonotone page", id, name)
				}
				seen[id] = true
			}
			if page.Next == "" {
				break
			}
			if page.Next == after || pages > 20 {
				t.Fatal("cursor did not advance")
			}
			after = page.Next
		}
		if len(seen) != 126 {
			t.Fatal("pagination lost entries", len(seen))
		}
		query := "tenant-user-124"
		if op == client.BucketListPage {
			query = "tenant-bucket-124"
		}
		page := readAdminTestPage(t, s, op, adminPageBody("", 10, query))
		if page.Total != 1 {
			t.Fatal("server search did not find entry beyond first page", query, page.Total)
		}
		id := ""
		if op == client.UserListPage {
			id = page.Users[0].ID
		} else {
			id = page.Buckets[0].ID
		}
		lookup := readAdminTestPage(t, s, op, adminPageBody("", 1, "", id))
		if lookup.Total != 1 || lookup.Next != "" {
			t.Fatal("bounded ID lookup failed")
		}
		if globalAdminMutation(op) {
			t.Fatal("read-only page requires a global mutation")
		}
		for _, limit := range []uint32{0, 1001} {
			m := client.Message{Kind: client.Request, Opcode: op, Body: adminPageBody("", limit, "")}
			if _, status := s.dispatchAs(m, nil, userPrincipal(t, s)); status != 400 {
				t.Fatal("invalid page limit accepted", status)
			}
		}
	}
	wire, _, err := s.executeUsers(client.Message{Kind: client.Request, Opcode: client.UserListPage, Body: adminPageBody("", 1000, "")}, userPrincipal(t, s))
	if err != nil || bytes.Contains(wire, []byte(hash)) || bytes.Contains(wire, []byte("password")) {
		t.Fatal("user page exposed credentials", err)
	}
	nonadmin := principal{User: accounts.Users[1]}
	for _, op := range []uint16{client.UserListPage, client.BucketListPage} {
		if _, status := s.dispatchAs(client.Message{Kind: client.Request, Opcode: op, Body: adminPageBody("", 50, "")}, nil, nonadmin); status != 403 {
			t.Fatal("user accessed admin pagination", status)
		}
	}
}

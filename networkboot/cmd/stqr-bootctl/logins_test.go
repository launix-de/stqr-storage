// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/json"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareLoginsProvisionsHomesWithoutExportingCredentials(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[existing], func(t *testing.T) {
			user, bucket := client.ID{0x12, 0x34, 0x56, 0x78}, client.ID{2}
			created, granted := 0, 0
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				m, err := client.DecodeMessage(raw)
				if err != nil {
					t.Error(err)
					return
				}
				e := client.Encoder{}
				switch m.Opcode {
				case client.UserList:
					e.V(3)
					for _, u := range []struct {
						id       client.ID
						name     string
						disabled bool
					}{{user, "user", false}, {client.ID{3}, "disabled", true}, {client.ID{4}, "boot-00000000000000000000000000000004", false}} {
						e.ID(u.id)
						e.Text(u.name)
						e.U8(0)
						e.Bool(u.disabled)
					}
				case client.BucketList:
					if existing {
						e.Text(`{"buckets":[{"id":"` + bucket.String() + `","name":"user_home","grants":[{"user_id":"` + user.String() + `","permissions":15}]}],"next":""}`)
					} else {
						e.Text(`{"buckets":[],"next":""}`)
					}
				case client.BucketCreate:
					created++
					d := client.Decoder{B: m.Body}
					bucket = d.ID()
					d.U64()
					if d.Text() != "user_home" {
						t.Error("wrong home name")
					}
				case client.BucketGrant:
					granted++
					d := client.Decoder{B: m.Body}
					if d.ID() != bucket || d.ID() != user || d.U32() != 15 || d.Done() != nil {
						t.Error("wrong grant")
					}
				default:
					t.Errorf("unexpected op %d", m.Opcode)
				}
				w.Write(client.EncodeMessage(client.Message{Kind: client.Response, ID: m.ID, Opcode: m.Opcode, Body: client.ResponseBody(200, e.B, "")}))
			}))
			defer host.Close()
			path := filepath.Join(t.TempDir(), "login.json")
			if err := prepareLogins(context.Background(), config{URI: host.URL, Username: "admin-secret", Password: "password-secret"}, path); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var p loginPlan
			if err = json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			if len(p.Accounts) != 1 || p.Accounts[0].Name != "user" || p.Accounts[0].ID != user.String() || p.Accounts[0].Bucket != bucket.String() || p.Accounts[0].UID != 0x52345678 {
				t.Fatalf("wrong plan: %+v", p)
			}
			if strings.Contains(string(raw), "secret") {
				t.Fatal("credentials leaked")
			}
			info, _ := os.Stat(path)
			if info.Mode().Perm() != 0600 {
				t.Fatal(info.Mode())
			}
			want := 1
			if existing {
				want = 0
			}
			if created != want || granted != want {
				t.Fatalf("unexpected mutations: %d %d", created, granted)
			}
		})
	}
}

func TestPrepareLoginsRefusesUnrelatedExistingHome(t *testing.T) {
	user := client.ID{1}
	mutated := false
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		m, _ := client.DecodeMessage(raw)
		e := client.Encoder{}
		switch m.Opcode {
		case client.UserList:
			e.V(1)
			e.ID(user)
			e.Text("user")
			e.U8(0)
			e.Bool(false)
		case client.BucketList:
			e.Text(`{"buckets":[{"id":"02000000000000000000000000000000","name":"user_home","grants":[{"user_id":"03000000000000000000000000000000","permissions":15}]}],"next":""}`)
		default:
			mutated = true
		}
		w.Write(client.EncodeMessage(client.Message{Kind: client.Response, ID: m.ID, Opcode: m.Opcode, Body: client.ResponseBody(200, e.B, "")}))
	}))
	defer host.Close()
	path := filepath.Join(t.TempDir(), "login.json")
	err := prepareLogins(context.Background(), config{URI: host.URL}, path)
	if err == nil || !strings.Contains(err.Error(), "refusing to transfer") || mutated {
		t.Fatalf("unsafe adoption: %v mutations=%v", err, mutated)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("published an unsafe plan")
	}
}

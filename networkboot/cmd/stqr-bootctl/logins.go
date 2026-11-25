// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"regexp"

	"github.com/launix-de/stqr-storage/client"
)

type loginAccount struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	UID    uint32 `json:"uid"`
	Bucket string `json:"bucket"`
}
type loginPlan struct {
	Schema   int            `json:"schema"`
	URI      string         `json:"uri"`
	Accounts []loginAccount `json:"accounts"`
}

var unixLoginName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.-]{0,31}$`)
var bootLoginName = regexp.MustCompile(`^boot-[0-9a-f]{32}$`)

// Identity follows the immutable account ID, never the mutable username or home
// contents. The image installer rejects UID/GID collisions; it never silently
// adopts a local identity. No password or administrator credential enters a plan.
func prepareLogins(ctx context.Context, c config, path string) error {
	raw, err := request(ctx, c, client.UserList, nil)
	if err != nil {
		return err
	}
	d := client.Decoder{B: raw}
	plan := loginPlan{Schema: 1, URI: c.URI, Accounts: []loginAccount{}}
	for n := d.V(); n > 0 && d.Err == nil; n-- {
		id, name := d.ID(), d.Text()
		d.U8()
		disabled := d.Bool()
		if disabled || bootLoginName.MatchString(name) {
			continue
		}
		if !unixLoginName.MatchString(name) || name == "root" {
			fmt.Fprintf(os.Stderr, "Skipping non-Unix STQR login %q\n", name)
			continue
		}
		plan.Accounts = append(plan.Accounts, loginAccount{ID: id.String(), Name: name, UID: 0x40000000 | (binary.BigEndian.Uint32(id[:4]) & 0x3fffffff)})
	}
	if err = d.Done(); err != nil {
		return err
	}
	// Inspect grants before reusing homes: deleting/recreating a username must
	// never transfer the previous identity's files to a new account implicitly.
	type grantEntry struct {
		User        string `json:"user_id"`
		Permissions uint32 `json:"permissions"`
	}
	type homeEntry struct {
		ID     string       `json:"id"`
		Name   string       `json:"name"`
		State  string       `json:"state"`
		Grants []grantEntry `json:"grants"`
	}
	homes := map[string]homeEntry{}
	after := ""
	for {
		e := client.Encoder{}
		e.Text(after)
		e.U32(1000)
		raw, err = request(ctx, c, client.BucketList, e.B)
		if err != nil {
			return err
		}
		bd := client.Decoder{B: raw}
		text := bd.Text()
		if err = bd.Done(); err != nil {
			return err
		}
		var page struct {
			Buckets []homeEntry `json:"buckets"`
			Next    string      `json:"next"`
		}
		if err = json.Unmarshal([]byte(text), &page); err != nil {
			return err
		}
		for _, b := range page.Buckets {
			homes[b.Name] = b
		}
		if page.Next == "" {
			break
		}
		if page.Next <= after {
			return client.ErrProtocol
		}
		after = page.Next
	}
	used := map[uint32]string{}
	for i := range plan.Accounts {
		a := &plan.Accounts[i]
		if other, ok := used[a.UID]; ok {
			return fmt.Errorf("Unix identity collision: %s and %s", other, a.Name)
		}
		used[a.UID] = a.Name
		name := a.Name + "_home"
		if home, ok := homes[name]; ok {
			allowed := false
			for _, g := range home.Grants {
				if g.User == a.ID && g.Permissions&15 == 15 {
					allowed = true
				}
			}
			if !allowed {
				return fmt.Errorf("home %s exists without full permission for the current account ID; refusing to transfer ownership", name)
			}
			a.Bucket = home.ID
		} else {
			bucket, createErr := create(ctx, c, name)
			err = createErr
			a.Bucket = bucket.String()
			if err != nil {
				return fmt.Errorf("create home %s: %w", name, err)
			}
			user, parseErr := parseBootID(a.ID)
			if parseErr != nil {
				return parseErr
			}
			if err = grant(ctx, c, bucket, user, 15); err != nil {
				return fmt.Errorf("grant home %s: %w", name, err)
			}
		}
	}
	raw, err = json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(raw, '\n'))
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

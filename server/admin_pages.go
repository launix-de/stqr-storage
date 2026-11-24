// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
)

type adminPageQuery struct {
	After, Query string
	Limit        int
	IDs          map[string]bool
}

func decodeAdminPage(body []byte) (adminPageQuery, error) {
	d := client.Decoder{B: body}
	q := adminPageQuery{}
	if d.U16() != 1 {
		return q, client.ErrProtocol
	}
	q.After = d.Text()
	q.Limit = int(d.U32())
	q.Query = strings.ToLower(strings.TrimSpace(d.Text()))
	q.IDs = map[string]bool{}
	for i, n := 0, d.Count(1000); i < n; i++ {
		id := d.ID()
		if id == (client.ID{}) || q.IDs[id.String()] {
			return q, ErrBucketInvalid
		}
		q.IDs[id.String()] = true
	}
	if d.Done() != nil || q.Limit < 1 || q.Limit > 1000 || len(q.After) > 255 || len(q.Query) > 256 || !utf8.ValidString(q.Query) || !utf8.ValidString(q.After) {
		return q, ErrBucketInvalid
	}
	return q, nil
}
func (q adminPageQuery) matches(id, text string) bool {
	return (len(q.IDs) == 0 || q.IDs[id]) && (q.Query == "" || strings.Contains(strings.ToLower(text), q.Query))
}

type userPageEntry struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Role     uint8  `json:"role"`
	Disabled bool   `json:"disabled"`
}

func userListPage(body []byte, image accountsImage) ([]byte, int, error) {
	q, err := decodeAdminPage(body)
	if err != nil {
		return nil, 0, err
	}
	users := append([]account(nil), image.Users...)
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	page := []userPageEntry{}
	total := 0
	next := ""
	full := false
	budget := client.MaxMessage - 1024
	for _, u := range users {
		if !q.matches(u.ID, u.Name+" "+u.ID) {
			continue
		}
		total++
		if u.Name <= q.After {
			continue
		}
		if len(page) == q.Limit || full {
			if next == "" {
				next = page[len(page)-1].Name
			}
			continue
		}
		entry := userPageEntry{ID: u.ID, Name: u.Name, Disabled: u.Disabled}
		if u.Role == "ADMIN" {
			entry.Role = 1
		}
		raw, _ := json.Marshal(entry)
		if len(raw)+1 > budget {
			if len(page) == 0 {
				return nil, 0, fail(413, "user entry exceeds response budget")
			}
			if next == "" {
				next = page[len(page)-1].Name
			}
			full = true
			continue
		}
		budget -= len(raw) + 1
		page = append(page, entry)
	}
	return jsonText(struct {
		Users    []userPageEntry `json:"users"`
		Next     string          `json:"next"`
		Total    int             `json:"total"`
		AllTotal int             `json:"all_total"`
	}{page, next, total, len(users)}), 200, nil
}
func (s *Server) executeBucketListPage(body []byte, p principal) ([]byte, int, error) {
	if err := s.requireAdmin(p); err != nil {
		return nil, 0, err
	}
	q, err := decodeAdminPage(body)
	if err != nil {
		return nil, 0, err
	}
	list := s.bucketSnapshots()
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	groups := map[string]string{}
	for _, g := range s.placementGroups.Load().object.Groups {
		groups[g.ID] = g.Name
	}
	page := []bucketListEntry{}
	next := ""
	total, all, active, locked := 0, 0, 0, 0
	budget := client.MaxMessage - 1024
	full := false
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	for _, b := range list {
		if b.DeletionComplete {
			continue
		}
		all++
		if !q.matches(b.ID, b.Name+" "+b.ID+" "+b.Policy.StorageStrategy+" "+groups[b.Policy.PlacementGroup]) {
			continue
		}
		total++
		if b.State == BucketActive {
			active++
		}
		if b.State == BucketLocked {
			locked++
		}
		if b.Name <= q.After {
			continue
		}
		if len(page) == q.Limit || full {
			if next == "" {
				next = page[len(page)-1].Name
			}
			continue
		}
		b.Receipts = nil
		entry := bucketListEntry{BucketDescription: b}
		if !b.Policy.Limits.Unlimited {
			bucket, e := s.Bucket(mustBucketID(b.ID))
			if e != nil {
				return nil, 0, e
			}
			used, e := bucket.administrativeUsage(ctx, p)
			if e != nil {
				entry.UsageError = e.Error()
			} else {
				entry.UsedBytes = &used
			}
		}
		s.addQuotaDiagnostics(&entry)
		raw, e := json.Marshal(entry)
		if e != nil {
			return nil, 0, e
		}
		if len(raw)+1 > budget {
			if len(page) == 0 {
				return nil, 0, fail(413, "bucket entry exceeds response budget")
			}
			next = page[len(page)-1].Name
			full = true
			continue
		}
		budget -= len(raw) + 1
		page = append(page, entry)
	}
	return jsonText(struct {
		Buckets  []bucketListEntry `json:"buckets"`
		Next     string            `json:"next"`
		Total    int               `json:"total"`
		AllTotal int               `json:"all_total"`
		Active   int               `json:"active"`
		Locked   int               `json:"locked"`
	}{page, next, total, all, active, locked}), 200, nil
}

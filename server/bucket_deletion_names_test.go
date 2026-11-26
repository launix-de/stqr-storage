// SPDX-License-Identifier: GPL-3.0-only
package server

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/launix-de/stqr-storage/client"
	"github.com/launix-de/stqr-storage/volume"
)

func TestBucketDeletionReleasesNameAndRemainsTerminal(t *testing.T) {
	for label, name := range map[string]string{"ascii": "photos", "utf8": strings.Repeat("猫", 85)} {
		t.Run(label, func(t *testing.T) {
			c, _ := testBucketCatalog(t)
			if err := c.Initialize(); err != nil {
				t.Fatal(err)
			}
			create := func(name string) *Bucket {
				t.Helper()
				b, err := c.Save(newTestBucket(t, name).Describe(), 0)
				if err != nil {
					t.Fatal(err)
				}
				return b
			}
			prefix := name
			for len(prefix)+len("-deleted00") > 255 {
				_, n := utf8.DecodeLastRuneInString(prefix)
				prefix = prefix[:len(prefix)-n]
			}
			occupied := create(prefix + "-deleted00")
			first := create(name)
			d := first.Describe()
			d.State = BucketDeleting
			deleted, err := c.Save(d, d.Revision)
			if err != nil {
				t.Fatal(err)
			}
			if deleted.ID() != first.ID() || deleted.Describe().Name != prefix+"-deleted01" {
				t.Fatalf("deletion must preserve ID and choose a free name: %+v", deleted.Describe())
			}
			if got, err := c.LookupName(occupied.Describe().Name); err != nil || got.ID() != occupied.ID() {
				t.Fatal("existing suffix bucket changed", got, err)
			}
			replacement := create(name)
			if got, err := c.LookupName(name); err != nil || got.ID() != replacement.ID() {
				t.Fatal("original name did not resolve to replacement", got, err)
			}
			// A new delete request or cleanup update must not append another suffix.
			d = deleted.Describe()
			deleted, err = c.Save(d, d.Revision)
			if err != nil || deleted.Describe().Name != prefix+"-deleted01" {
				t.Fatal(deleted, err)
			}
			for _, state := range []BucketState{BucketActive, BucketLocked} {
				d = deleted.Describe()
				d.State = state
				if _, err := c.Save(d, d.Revision); !errors.Is(err, ErrBucketConflict) {
					t.Fatalf("deleting bucket revived as %s: %v", state, err)
				}
			}
			d = replacement.Describe()
			d.State = BucketDeleting
			second, err := c.Save(d, d.Revision)
			if err != nil || second.Describe().Name != prefix+"-deleted02" {
				t.Fatal(second, err)
			}
		})
	}
}

// Exercise the public admin transport and the same isolated draft validator
// used by clustered changes. Cleanup is held pending so a successful terminal
// check cannot be explained by the bucket having already disappeared.
func TestBucketDeletionAdminRetriesAndResurrection(t *testing.T) {
	s := testServer(t)
	original, _ := createProtocolBucket(t, s, "photos")
	s.mu.Lock()
	if s.bucketJobs == nil {
		s.bucketJobs = make(map[volume.ID]bool)
	}
	s.bucketJobs[original.ID()] = true
	s.mu.Unlock()
	body := client.Encoder{}
	body.ID(client.ID(original.ID()))
	deletion, status, _ := adminBucketMessage(t, s, client.BucketDelete, body.B)
	if status != 202 || original.Describe().Name != "photos-deleted00" || original.Describe().State != BucketDeleting {
		t.Fatal("first delete", status, original.Describe())
	}
	replacement, _ := createProtocolBucket(t, s, "photos")
	admin := s.cfg.Auth.BootstrapAdmin
	status, _ = bucketHTTPReply(t, s, deletion, admin.Username, admin.Password)
	if status != 202 {
		t.Fatal("lost-reply retry", status)
	}
	_, status, _ = adminBucketMessage(t, s, client.BucketDelete, body.B)
	if status != 202 || original.Describe().Name != "photos-deleted00" {
		t.Fatal("new deletion request renamed twice", status, original.Describe())
	}
	for _, state := range []BucketState{BucketActive, BucketLocked} {
		lock := client.Encoder{}
		lock.ID(client.ID(original.ID()))
		lock.Bool(state == BucketLocked)
		description := original.Describe()
		description.State = state
		policy := client.Encoder{B: bucketPolicyBody(t, &Bucket{description: description})}
		policy.U64(description.Policy.Version)
		for _, mutation := range []struct {
			opcode uint16
			body   []byte
		}{{client.BucketLock, lock.B}, {client.BucketSetPolicy, policy.B}} {
			_, status, _ := adminBucketMessage(t, s, mutation.opcode, mutation.body)
			if status != 409 {
				t.Fatalf("HTTP resurrected %s with opcode %x: %d", state, mutation.opcode, status)
			}
			message := client.Message{Kind: client.Request, Opcode: mutation.opcode, ID: client.NewRequestID(), Body: mutation.body}
			draft, err := s.prepareAdminDraft(message, userPrincipal(t, s))
			if draft != nil {
				draft.close()
			}
			if !errors.Is(err, ErrBucketConflict) {
				t.Fatalf("cluster draft resurrected %s with opcode %x: %v", state, mutation.opcode, err)
			}
		}
	}
	if original.Describe().State != BucketDeleting || original.Describe().Name != "photos-deleted00" || original.Describe().DeletionComplete {
		t.Fatal("failed administration changed pending deletion", original.Describe())
	}
	if got, err := s.BucketByName("photos"); err != nil || got.ID() != replacement.ID() || got.Describe().State != BucketActive {
		t.Fatal("replacement was affected by old-ID operations", got, err)
	}
}

func TestBucketDeletionConcurrentTruncatedNamesStayDistinct(t *testing.T) {
	c, _ := testBucketCatalog(t)
	if err := c.Initialize(); err != nil {
		t.Fatal(err)
	}
	prefix := strings.Repeat("x", 245)
	var originals []*Bucket
	for _, ending := range []string{"alpha", "beta"} {
		b, err := c.Save(newTestBucket(t, prefix+ending).Describe(), 0)
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, b)
	}
	start := make(chan struct{})
	results := make(chan error, len(originals))
	for _, b := range originals {
		go func(b *Bucket) {
			<-start
			d := b.Describe()
			d.State = BucketDeleting
			_, err := c.Save(d, d.Revision)
			results <- err
		}(b)
	}
	close(start)
	for range originals {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	for _, b := range originals {
		stored, err := c.Lookup(b.ID())
		if err != nil {
			t.Fatal(err)
		}
		name := stored.Describe().Name
		if name != prefix+"-deleted00" && name != prefix+"-deleted01" || seen[name] {
			t.Fatal("catalog retry reused a deletion name", name)
		}
		seen[name] = true
		if _, err := c.LookupName(b.Describe().Name); !errors.Is(err, ErrBucketNotFound) {
			t.Fatal("original name retained after rename", err)
		}
	}
}

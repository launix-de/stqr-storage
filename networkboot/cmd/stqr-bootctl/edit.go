// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/launix-de/stqr-storage/client"
)

func validateImage(ctx context.Context, conn *client.Client) error {
	raw, _, err := conn.Get(ctx, manifestID)
	if err != nil {
		return err
	}
	var m manifest
	if json.Unmarshal(raw, &m) != nil || m.Schema != 2 || m.RootBucket != conn.BucketID() {
		return errors.New("image must contain its own native root filesystem and boot manifest")
	}
	_, err = client.OpenFilesystem(ctx, conn, client.FSOptions{ReadOnly: true})
	return err
}

// cloneImage asks STQR to link the two object namespaces through its native COW
// mechanism. Only the destination's small boot manifest changes: its root is
// self-contained even for a clone of a clone. No filesystem payload is copied
// through the helper, and an uncertain result never causes automatic deletion.
func cloneImage(ctx context.Context, c config, source, destination string) error {
	src, err := connectImage(ctx, c, source, false)
	if err != nil {
		return err
	}
	srcID := src.BucketID()
	raw, _, err := src.Get(ctx, manifestID)
	if err != nil {
		src.Close()
		return err
	}
	var m manifest
	if json.Unmarshal(raw, &m) != nil || m.Schema != 2 || m.RootBucket != srcID {
		src.Close()
		return errors.New("source must be a native image with its root in the same bucket")
	}
	if _, err = client.OpenFilesystem(ctx, src, client.FSOptions{ReadOnly: true}); err != nil {
		src.Close()
		return err
	}
	src.Close()
	if _, err = resolve(ctx, c, destination); err == nil {
		return errors.New("clone destination already exists")
	}
	var status *apiError
	if !errors.As(err, &status) || status.Status != 404 {
		return err
	}
	// Inherit the source placement group to retain lazy sharing instead of asking
	// the server to detach a clone into a different storage class in the background.
	group, err := imagePlacementGroup(ctx, c, srcID)
	if err != nil {
		return err
	}
	dstID := client.NewRequestID()
	if group == (client.ID{}) {
		_, err = createID(ctx, c, destination, dstID)
	} else {
		e := client.Encoder{}
		e.U16(1)
		e.ID(dstID)
		e.Text(destination)
		e.ID(group)
		e.U8(0)
		e.U64(0)
		e.Bool(true)
		e.U64(0)
		e.U32(0)
		_, err = control(ctx, c, client.BucketCreateInGroup, e.B)
	}
	if err != nil {
		return err
	}
	e := client.Encoder{}
	e.ID(srcID)
	e.ID(dstID)
	if _, err = control(ctx, c, client.BucketClone, e.B); err != nil {
		return fmt.Errorf("clone destination %s retained for inspection: %w", destination, err)
	}
	dst, err := client.Dial(ctx, c.URI, c.Username, c.Password, dstID, "fs")
	if err != nil {
		return err
	}
	defer dst.Close()
	// Read the inherited manifest after the clone decision; an administrator may
	// have updated the source since preflight. Never overwrite with stale content.
	inherited, _, err := dst.Get(ctx, manifestID)
	if err != nil {
		return err
	}
	if json.Unmarshal(inherited, &m) != nil || m.Schema != 2 || m.RootBucket != srcID {
		return errors.New("unexpected cloned image manifest")
	}
	m.RootBucket = dstID
	rebased, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err = dst.CAS(ctx, manifestID, 0, inherited, rebased, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{RequestID: client.NewRequestID()}}); err != nil {
		return err
	}
	return dst.Sync(ctx)
}

func imagePlacementGroup(ctx context.Context, c config, id client.ID) (client.ID, error) {
	after := ""
	for {
		e := client.Encoder{}
		e.Text(after)
		e.U32(100)
		raw, err := request(ctx, c, client.BucketList, e.B)
		if err != nil {
			return client.ID{}, err
		}
		d := client.Decoder{B: raw}
		text := d.Text()
		if d.Done() != nil {
			return client.ID{}, client.ErrProtocol
		}
		var page struct {
			Buckets []struct {
				ID     string `json:"id"`
				Policy struct {
					Group string `json:"placement_group"`
				} `json:"policy"`
			} `json:"buckets"`
			Next string `json:"next"`
		}
		if err = json.Unmarshal([]byte(text), &page); err != nil {
			return client.ID{}, err
		}
		for _, b := range page.Buckets {
			if b.ID == id.String() {
				if b.Policy.Group == "" {
					return client.ID{}, nil
				}
				return parseBootID(b.Policy.Group)
			}
		}
		if page.Next == "" || page.Next <= after {
			return client.ID{}, errors.New("source image missing from bucket catalog")
		}
		after = page.Next
	}
}

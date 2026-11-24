// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/launix-de/stqr-storage/client"
)

// Bound each request below the server's 1 GiB staging budget. The artifact
// remains a single object, and the manifest is committed only after completion.
const artifactTransferBytes uint64 = 64 << 20

type artifactStore interface {
	Stat(context.Context, client.ObjectID) (client.ObjectInfo, error)
	PutFrom(context.Context, client.ObjectID, io.Reader, uint64, client.PutOptions) (client.ObjectInfo, error)
	PutRangeFrom(context.Context, client.ObjectID, uint64, io.Reader, uint64, client.PutOptions) (client.ObjectInfo, error)
	GetRangeToVersion(context.Context, client.ObjectID, uint64, uint64, uint64, io.Writer) (client.ObjectInfo, error)
}

func copyArtifact(ctx context.Context, store artifactStore, id client.ObjectID, size, part uint64, dst io.Writer) (client.ObjectInfo, error) {
	info, err := store.Stat(ctx, id)
	if err != nil {
		return info, err
	}
	if info.Length != size || part == 0 {
		return info, client.ErrProtocol
	}
	for off := uint64(0); off < size; {
		n := min(part, size-off)
		if _, err = store.GetRangeToVersion(ctx, id, off, n, info.Version, dst); err != nil {
			return info, err
		}
		off += n
	}
	return info, nil
}

func uploadArtifact(ctx context.Context, store artifactStore, id client.ObjectID, src io.ReadSeeker, size, part uint64) (client.ObjectInfo, error) {
	if part == 0 {
		return client.ObjectInfo{}, client.ErrRange
	}
	info, err := store.Stat(ctx, id)
	exists := err == nil
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		return info, err
	}
	if exists {
		if info.Length > size {
			return info, fmt.Errorf("existing artifact exceeds source length")
		}
		// A failed upload may leave a prefix. Validate it before resuming; never
		// overwrite a different object or rewrite an already published artifact.
		remote := sha256.New()
		pinned, err := copyArtifact(ctx, store, id, info.Length, part, remote)
		if err != nil {
			return info, err
		}
		info = pinned
		if _, err = src.Seek(0, io.SeekStart); err != nil {
			return info, err
		}
		local := sha256.New()
		if n, err := io.CopyN(local, src, int64(info.Length)); err != nil || uint64(n) != info.Length {
			return info, io.ErrUnexpectedEOF
		}
		if !bytes.Equal(remote.Sum(nil), local.Sum(nil)) {
			return info, fmt.Errorf("existing artifact prefix checksum mismatch")
		}
		if info.Length == size {
			return info, nil
		}
	} else if size == 0 {
		return store.PutFrom(ctx, id, src, 0, client.PutOptions{Absent: true, RequestID: client.NewRequestID()})
	}
	off := info.Length
	if _, err = src.Seek(int64(off), io.SeekStart); err != nil {
		return info, err
	}
	for off < size {
		n := min(part, size-off)
		opt := client.PutOptions{RequestID: client.NewRequestID()}
		version := info.Version
		if !exists {
			opt.Absent = true
		} else {
			opt.ExpectedVersion = &version
		}
		info, err = store.PutRangeFrom(ctx, id, off, src, n, opt)
		if err != nil {
			return info, err
		}
		if info.Length != off+n {
			return info, client.ErrProtocol
		}
		off += n
		exists = true
	}
	return info, nil
}

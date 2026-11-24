// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/launix-de/stqr-storage/client"
	"io"
	"testing"
)

type artifactFixture struct {
	data            []byte
	info            client.ObjectInfo
	exists          bool
	writes, failAt  int
	changeAfterRead bool
}

func (s *artifactFixture) Stat(context.Context, client.ObjectID) (client.ObjectInfo, error) {
	if !s.exists {
		return s.info, client.ErrNotFound
	}
	return s.info, nil
}
func (s *artifactFixture) PutFrom(ctx context.Context, id client.ObjectID, src io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	return s.PutRangeFrom(ctx, id, 0, src, n, opt)
}
func (s *artifactFixture) PutRangeFrom(_ context.Context, _ client.ObjectID, off uint64, src io.Reader, n uint64, opt client.PutOptions) (client.ObjectInfo, error) {
	s.writes++
	if s.failAt == s.writes {
		return s.info, errors.New("interrupted upload")
	}
	if opt.Absent && s.exists || opt.ExpectedVersion != nil && (!s.exists || *opt.ExpectedVersion != s.info.Version) {
		return s.info, client.ErrPrecondition
	}
	if off != uint64(len(s.data)) {
		return s.info, errors.New("unexpected rewrite")
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(src, data); err != nil {
		return s.info, err
	}
	s.data = append(s.data, data...)
	s.info.Version++
	s.info.Length = uint64(len(s.data))
	s.exists = true
	return s.info, nil
}
func (s *artifactFixture) GetRangeToVersion(_ context.Context, _ client.ObjectID, off, n, version uint64, dst io.Writer) (client.ObjectInfo, error) {
	if version != s.info.Version {
		return s.info, client.ErrPrecondition
	}
	_, err := dst.Write(s.data[off : off+n])
	info := s.info
	if s.changeAfterRead {
		s.info.Version++
	}
	return info, err
}
func TestArtifactTransferResumeAndDeduplication(t *testing.T) {
	ctx := context.Background()
	id := client.ObjectID{1}
	data := []byte("full installed root filesystem")
	store := &artifactFixture{failAt: 2}
	if _, err := uploadArtifact(ctx, store, id, bytes.NewReader(data), uint64(len(data)), 4); err == nil {
		t.Fatal("expected interrupted upload")
	}
	if string(store.data) != string(data[:4]) {
		t.Fatal("partial artifact not preserved")
	}
	store.failAt = 0
	if _, err := uploadArtifact(ctx, store, id, bytes.NewReader(data), uint64(len(data)), 4); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(store.data, data) {
		t.Fatal("resumed content differs")
	}
	writes := store.writes
	if _, err := uploadArtifact(ctx, store, id, bytes.NewReader(data), uint64(len(data)), 4); err != nil {
		t.Fatal(err)
	}
	if writes != store.writes {
		t.Fatal("published artifact was rewritten")
	}
	var fetched bytes.Buffer
	if _, err := copyArtifact(ctx, store, id, uint64(len(data)), 4, &fetched); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fetched.Bytes(), data) {
		t.Fatal("ranged download differs")
	}
}
func TestArtifactRejectsForeignPrefixAndMixedVersions(t *testing.T) {
	store := &artifactFixture{exists: true, data: []byte("wrong"), info: client.ObjectInfo{Length: 5, Version: 1}}
	if _, err := uploadArtifact(context.Background(), store, client.ObjectID{}, bytes.NewReader([]byte("right tail")), 10, 2); err == nil {
		t.Fatal("accepted foreign prefix")
	}
	if store.writes != 0 {
		t.Fatal("modified foreign object")
	}
	store.changeAfterRead = true
	if _, err := copyArtifact(context.Background(), store, client.ObjectID{}, 5, 2, io.Discard); !errors.Is(err, client.ErrPrecondition) {
		t.Fatal("mixed versions accepted", err)
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type bootSession struct {
	Mode     string    `json:"mode"`
	URI      string    `json:"uri"`
	Username string    `json:"username"`
	Password string    `json:"password"`
	User     client.ID `json:"user"`
	Lower    client.ID `json:"lower"`
	Upper    client.ID `json:"upper"`
}

// Global control can be briefly fenced while a newly allocated range moves.
// Rejected requests have no effect; retain the same body while waiting.
func control(ctx context.Context, c config, op uint16, body []byte) ([]byte, error) {
	id := client.NewRequestID()
	var reported time.Time
	for attempt := 0; ; attempt++ {
		raw, err := requestWithID(ctx, c, op, body, id)
		var api *apiError
		if !errors.As(err, &api) || api.Status != 503 {
			return raw, err
		}
		if reported.IsZero() || time.Since(reported) >= 10*time.Second {
			fmt.Fprintln(os.Stderr, "Waiting for cluster:", api.Detail)
			reported = time.Now()
		}
		timer := time.NewTimer(time.Duration(min(attempt+1, 8)) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

}

func byID(ctx context.Context, c config, op uint16, id client.ID) error {
	e := client.Encoder{}
	e.ID(id)
	_, err := control(ctx, c, op, e.B)
	return err
}
func grant(ctx context.Context, c config, bucket, user client.ID, permissions uint32) error {
	e := client.Encoder{}
	e.ID(bucket)
	e.ID(user)
	e.U32(permissions)
	_, err := control(ctx, c, client.BucketGrant, e.B)
	return err
}
func newBootSession(ctx context.Context, c config, lowerName string, quota uint64, record string, imageName ...string) (bootSession, error) {
	label := lowerName
	if len(imageName) > 0 && imageName[0] != "" {
		label = imageName[0]
	}
	return newImageSession(ctx, c, lowerName, label, quota, record, false)
}

// A normal boot owns a disposable upper. An editor instead receives a plain
// writable filesystem connection to the image itself: no blockdevice rebuild,
// hidden base bucket, or server-side understanding of the filesystem format.
func newImageSession(ctx context.Context, c config, lowerName, imageName string, quota uint64, record string, edit bool) (s bootSession, err error) {
	s.URI, s.Mode = c.URI, "overlay"
	if edit {
		s.Mode = "edit"
	} else if imageName == "" || len(imageName)+33 > 255 {
		return s, errors.New("invalid image name for session upper")
	}
	s.Lower, err = parseBootID(lowerName)
	if err != nil {
		s.Lower, err = resolve(ctx, c, lowerName)
	}
	if err != nil {
		return
	}
	lower, err := client.Dial(ctx, c.URI, c.Username, c.Password, s.Lower, "fs")
	if err != nil {
		return s, fmt.Errorf("open published rootfs %s: %w", s.Lower, err)
	}
	defer lower.Close()
	if _, err = client.OpenFilesystem(ctx, lower, client.FSOptions{ReadOnly: true}); err != nil {
		return s, fmt.Errorf("read published rootfs %s: %w", s.Lower, err)
	}
	s.User = client.NewRequestID()
	s.Upper = client.NewRequestID()
	if edit {
		s.Upper = s.Lower
	}
	s.Username = "boot-" + s.User.String()
	s.Password = client.NewRequestID().String() + client.NewRequestID().String()
	if record != "" {
		f, e := os.OpenFile(record, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			err = e
			return
		}
		e = json.NewEncoder(f).Encode(map[string]string{"user": s.User.String(), "upper": s.Upper.String(), "lower": s.Lower.String(), "mode": s.Mode})
		if e == nil {
			e = f.Sync()
		}
		closeErr := f.Close()
		if e == nil {
			e = closeErr
		}
		if e != nil {
			err = e
			return
		}
	}
	defer func() {
		if err != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if removeImageSession(cleanupCtx, c, s.User, s.Upper, s.Lower, edit) == nil && record != "" {
				_ = os.Remove(record)
			}
		}
	}()

	e := client.Encoder{}
	e.ID(s.User)
	e.Text(s.Username)
	e.U8(0)
	e.Bool(false)
	e.Text(s.Password)
	if _, err = control(ctx, c, client.UserCreate, e.B); err != nil {
		return
	}
	if edit {
		// Compatibility with images published by older helpers, which globally
		// locked the bucket. Read-only PXE mounts are enforced by their grant and
		// lower mount; the editable image has stable IDs and remains writable.
		unlock := client.Encoder{}
		unlock.ID(s.Lower)
		unlock.Bool(false)
		if _, err = control(ctx, c, client.BucketLock, unlock.B); err != nil {
			return
		}
	} else {
		s.Upper, err = createIDWithQuota(ctx, c, imageName+"-"+s.Upper.String(), s.Upper, quota)
		if err != nil {
			return
		}
		var conn *client.Client
		conn, err = client.Dial(ctx, c.URI, c.Username, c.Password, s.Upper, "fs")
		if err != nil {
			return
		}
		func() {
			defer conn.Close()
			_, err = client.OpenOverlayFilesystem(ctx, lower, conn, client.OverlayOptions{Create: true, LowerBucketID: s.Lower})
			if err == nil {
				err = conn.Sync(ctx)
			}
		}()
		if err != nil {
			return
		}
		if err = grant(ctx, c, s.Lower, s.User, 1); err != nil {
			return
		}
	}
	err = grant(ctx, c, s.Upper, s.User, 15)
	return
}
func removeBootSession(ctx context.Context, c config, user, upper, lower client.ID) error {
	return removeImageSession(ctx, c, user, upper, lower, false)
}
func removeImageSession(ctx context.Context, c config, user, upper, lower client.ID, edit bool) error {
	if upper == (client.ID{}) || lower == (client.ID{}) || user == (client.ID{}) || (upper == lower) != edit {
		return errors.New("session mode does not match bucket identities; refusing cleanup")
	}
	// Claim EOF or gateway death does not prove that a guest has stopped:
	// the mounted filesystem may still own live storage sockets. Existing
	// cluster maintenance admission proves that all required members see an
	// idle image/upper and prevents reconnect while its private user is revoked.
	// An orphan editor account is retained while unrelated image clients exist;
	// the gateway retries instead of disconnecting any of them.
	idle, err := client.Dial(ctx, c.URI, c.Username, c.Password, upper, "fs")
	if err == nil {
		if err = idle.BeginMaintenance(ctx); err != nil {
			idle.Close()
			return fmt.Errorf("temporary upper still active; retained: %w", err)
		}
		defer idle.Close()
	} else if !errors.Is(err, client.ErrNotFound) {
		return err
	}
	// Delete the account while maintenance excludes reconnect. Revoke its
	// stale ACL entry only after releasing the lease: a bucket's configuration
	// cannot change while it is under maintenance (especially editor==image).
	if err := byID(ctx, c, client.UserDelete, user); err != nil {
		var a *apiError
		if !errors.As(err, &a) || a.Status != 404 {
			return err
		}
	}
	if idle != nil {
		if err := idle.Close(); err != nil {
			return err
		}
	}
	e := client.Encoder{}
	e.ID(lower)
	e.ID(user)
	if _, err := control(ctx, c, client.BucketRevoke, e.B); err != nil {
		var a *apiError
		if !errors.As(err, &a) || a.Status != 404 {
			return err
		}
	}
	if edit {
		return nil // The image belongs to the administrator, never to the session.
	}
	// DELETE persists a blocked namespace and schedules batched cluster purge.
	// Clearing object-by-object first is unnecessary and much more expensive.
	err = byID(ctx, c, client.BucketDelete, upper)
	var api *apiError
	if errors.As(err, &api) && api.Status == 404 {
		return nil
	}
	return err

}

// Keep accepted writes bounded by the importer; a Sync separates each published
// tree from the manifest which makes it reachable by booting clients.
// The Go client multiplexes concurrent requests on one connection. Import
// workers share it, then one explicit fence covers the entire completed tree.
type acceptedImport struct{ *client.Client }

func openImport(ctx context.Context, c config, id client.ID) (*acceptedImport, error) {
	conn, err := client.Dial(ctx, c.URI, c.Username, c.Password, id, "fs")
	if err != nil {
		return nil, err
	}
	return &acceptedImport{Client: conn}, nil
}
func (s *acceptedImport) CAS(ctx context.Context, id client.ObjectID, off uint64, old, data []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	opt.Accepted = true
	return s.Client.CAS(ctx, id, off, old, data, opt)
}

var importStateID = client.ObjectID(sha256.Sum256([]byte("STQR networkboot root import v1")))

type rootImportState struct {
	SourceSHA256 string `json:"source_sha256"`
	RootSHA256   string `json:"root_sha256,omitempty"`
}

func publishNativeRoot(ctx context.Context, c config, dir string, id client.ID) (client.ID, error) {
	f, err := os.Open(filepath.Join(dir, "rootfs.tar"))
	if err != nil {
		return id, err
	}
	defer f.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, f); err != nil {
		return id, err
	}
	sourceHash := hex.EncodeToString(hash.Sum(nil))
	if _, err = f.Seek(0, 0); err != nil {
		return id, err
	}
	conn, err := openImport(ctx, c, id)
	if err != nil {
		return id, err
	}
	defer conn.Close()
	stateRaw, _, stateErr := conn.Get(ctx, importStateID)
	var state rootImportState
	if stateErr == nil {
		if json.Unmarshal(stateRaw, &state) != nil || state.SourceSHA256 != sourceHash {
			return id, errors.New("target contains a different rootfs import")
		}
	} else if !errors.Is(stateErr, client.ErrNotFound) {
		return id, stateErr
	}
	root, _, rootErr := conn.Get(ctx, client.FilesystemRootID())
	if rootErr == nil {
		digest := sha256.Sum256(root)
		if state.RootSHA256 != "" && state.RootSHA256 == hex.EncodeToString(digest[:]) {
			return id, nil
		}
		return id, errors.New("target already contains a filesystem without a completed matching import; refusing to replace it")
	}
	if !errors.Is(rootErr, client.ErrNotFound) {
		return id, rootErr
	}
	if stateErr != nil {
		state.SourceSHA256 = sourceHash
		raw, _ := json.Marshal(state)
		if _, err = conn.Client.CAS(ctx, importStateID, 0, nil, raw, client.CASOptions{Scope: client.CASObject, MutationOptions: client.MutationOptions{Absent: true, RequestID: client.NewRequestID()}}); err != nil {
			return id, err
		}
	}
	fmt.Fprintln(os.Stderr, "Importing native root filesystem into", id.String())
	stop := reportImportProgress(ctx, f)
	defer stop()
	if err = client.ImportFilesystemTar(ctx, conn, f); err != nil {
		return id, fmt.Errorf("native import retained in target %s: %w", id, err)
	}
	// Fence all import streams before publishing a bootable image, rather than
	// checkpointing only the separate connection which writes its manifest.
	if err = conn.Sync(ctx); err != nil {
		return id, err
	}
	root, _, err = conn.Get(ctx, client.FilesystemRootID())
	if err != nil {
		return id, err
	}
	digest := sha256.Sum256(root)
	state.RootSHA256 = hex.EncodeToString(digest[:])
	raw, _ := json.Marshal(state)
	_, err = conn.Client.Put(ctx, importStateID, raw, client.PutOptions{RequestID: client.NewRequestID()})
	return id, err
}
func nativeCommand(ctx context.Context, c config, cmd string, args []string, size uint64, record, imageName string) error {
	switch cmd {
	case "boot-session-create", "edit-session-create":
		if len(args) != 1 {
			return errors.New("one root bucket required")
		}
		if imageName == "" {
			imageName = args[0]
		}
		s, err := newImageSession(ctx, c, args[0], imageName, size, record, cmd == "edit-session-create")
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(s)
	case "boot-session-delete", "edit-session-delete":
		if len(args) != 3 {
			return errors.New("user ID, upper bucket ID and lower bucket ID required")
		}
		user, err := parseBootID(args[0])
		if err != nil {
			return err
		}
		upper, err := parseBootID(args[1])
		if err != nil {
			return err
		}
		lower, err := parseBootID(args[2])
		if err != nil {
			return err
		}
		return removeImageSession(ctx, c, user, upper, lower, cmd == "edit-session-delete")
	}
	return errors.New("unknown native command")
}

func parseBootID(s string) (id client.ID, err error) {
	b, e := hex.DecodeString(s)
	if e != nil || len(b) != 16 {
		return id, errors.New("invalid ID")
	}
	copy(id[:], b)
	return id, nil
}

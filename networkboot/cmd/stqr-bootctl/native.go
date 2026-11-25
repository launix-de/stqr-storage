// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type bootSession struct {
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
func newBootSession(ctx context.Context, c config, lowerName string, quota uint64, record string) (s bootSession, err error) {
	s.URI = c.URI
	s.Lower, err = parseBootID(lowerName)
	if err != nil {
		s.Lower, err = resolve(ctx, c, lowerName)
	}
	if err != nil {
		return
	}
	s.User = client.NewRequestID()
	s.Upper = client.NewRequestID()
	s.Username = "boot-" + s.User.String()
	s.Password = client.NewRequestID().String() + client.NewRequestID().String()
	if record != "" {
		f, e := os.OpenFile(record, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			err = e
			return
		}
		e = json.NewEncoder(f).Encode(map[string]string{"user": s.User.String(), "upper": s.Upper.String(), "lower": s.Lower.String()})
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
			if removeBootSession(cleanupCtx, c, s.User, s.Upper, s.Lower) == nil && record != "" {
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
	s.Upper, err = createIDWithQuota(ctx, c, s.Username+"-root", s.Upper, quota)
	if err != nil {
		return
	}
	var conn *client.Client
	conn, err = client.Dial(ctx, c.URI, c.Username, c.Password, s.Upper)
	if err != nil {
		return
	}
	func() {
		defer conn.Close()
		lower, e := client.Dial(ctx, c.URI, c.Username, c.Password, s.Lower)
		if e != nil {
			err = e
			return
		}
		defer lower.Close()
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
	err = grant(ctx, c, s.Upper, s.User, 15)
	return
}
func removeBootSession(ctx context.Context, c config, user, upper, lower client.ID) error {
	e := client.Encoder{}
	e.ID(lower)
	e.ID(user)
	if _, err := control(ctx, c, client.BucketRevoke, e.B); err != nil {
		var a *apiError
		if !errors.As(err, &a) || a.Status != 404 {
			return err
		}
	}

	// Revoke access before attempting storage cleanup. Deletion closes existing
	// connections too; no client may keep writing while maintenance collects it.
	if err := byID(ctx, c, client.UserDelete, user); err != nil {
		var a *apiError
		if !errors.As(err, &a) || a.Status != 404 {
			return err
		}
	}
	// DELETE persists a blocked namespace and schedules batched cluster purge.
	// Clearing object-by-object first is unnecessary and much more expensive.
	err := byID(ctx, c, client.BucketDelete, upper)
	var api *apiError
	if errors.As(err, &api) && api.Status == 404 {
		return nil
	}
	return err

}

// Keep accepted writes bounded by the importer; a Sync separates each published
// tree from the manifest which makes it reachable by booting clients.
type acceptedImport struct {
	*client.Client
	writers   []*client.Client
	available chan *client.Client
}

func openImport(ctx context.Context, c config, id client.ID) (*acceptedImport, error) {
	s := &acceptedImport{available: make(chan *client.Client, 8)}
	for i := 0; i < 8; i++ {
		conn, err := client.Dial(ctx, c.URI, c.Username, c.Password, id)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.writers = append(s.writers, conn)
		s.available <- conn
	}
	s.Client = s.writers[0]
	return s, nil
}
func (s *acceptedImport) Close() {
	for _, c := range s.writers {
		c.Close()
	}
}
func (s *acceptedImport) Sync(ctx context.Context) error {
	for _, c := range s.writers {
		if err := c.Sync(ctx); err != nil {
			return err
		}
	}
	return nil
}
func (s *acceptedImport) CAS(ctx context.Context, id client.ObjectID, off uint64, old, data []byte, opt client.CASOptions) (client.ObjectInfo, error) {
	var writer *client.Client
	select {
	case writer = <-s.available:
	case <-ctx.Done():
		return client.ObjectInfo{}, ctx.Err()
	}
	defer func() { s.available <- writer }()
	opt.Accepted = true
	return writer.CAS(ctx, id, off, old, data, opt)
}
func publishNativeRoot(ctx context.Context, c config, dir string) (client.ID, error) {

	var id client.ID
	f, err := os.Open(filepath.Join(dir, "rootfs.tar"))
	if err != nil {
		return id, err
	}
	defer f.Close()
	id, err = create(ctx, c, "bootroot-"+client.NewRequestID().String())
	if err != nil {
		return id, err
	}
	conn, err := openImport(ctx, c, id)
	if err != nil {
		return id, err
	}
	defer conn.Close()
	fmt.Fprintln(os.Stderr, "Importing native root filesystem into", id.String())
	stopProgress := reportImportProgress(ctx, f)
	defer stopProgress()
	if err = client.ImportFilesystemTar(ctx, conn, f); err != nil {
		return id, fmt.Errorf("native import left release bucket %s for recovery: %w", id.String(), err)
	}
	e := client.Encoder{}
	e.ID(id)
	e.Bool(true)
	_, err = control(ctx, c, client.BucketLock, e.B)
	return id, err
}
func nativeCommand(ctx context.Context, c config, cmd string, args []string, size uint64, record string) error {
	switch cmd {
	case "boot-session-create":
		s, err := newBootSession(ctx, c, args[0], size, record)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(s)
	case "boot-session-delete":
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
		return removeBootSession(ctx, c, user, upper, lower)
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

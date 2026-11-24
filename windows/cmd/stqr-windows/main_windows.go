//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
	winfs "github.com/launix-de/stqr-storage/windows"
	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/term"
)

func run() (result error) {
	flags := flag.NewFlagSet("stqr-windows", flag.ContinueOnError)
	uri := flags.String("uri", "http://127.0.0.1:7419", "STQR HTTP/WebSocket endpoint")
	username := flags.String("username", "", "STQR username")
	bucket := flags.String("bucket", "", "filesystem bucket name or hexadecimal ID")
	ro := flags.Bool("readonly", false, "readonly mount")
	init := flags.Bool("init", false, "initialize an empty filesystem bucket")
	passwordStdin := flags.Bool("password-stdin", false, "read password from redirected standard input")
	uid := flags.Uint64("uid", ^uint64(0), "Unix owner for new files (default: filesystem root owner)")
	gid := flags.Uint64("gid", ^uint64(0), "Unix group for new files (default: filesystem root group)")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: stqr-windows --uri URL --username USER --bucket BUCKET S:\nPassword is requested in the console. WinFsp must be installed. Ctrl+C unmounts.")
		flags.PrintDefaults()
	}
	if e := flags.Parse(os.Args[1:]); e != nil {
		return e
	}
	if *username == "" || *bucket == "" || flags.NArg() != 1 {
		flags.Usage()
		return fmt.Errorf("username, bucket and drive letter required")
	}
	drive := flags.Arg(0)
	if len(drive) != 2 || drive[1] != ':' || !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", rune(drive[0])) {
		return fmt.Errorf("mountpoint must be a drive letter, e.g. S:")
	}
	if *ro && *init {
		return fmt.Errorf("--init requires a writable mount")
	}
	for _, id := range []uint64{*uid, *gid} {
		if id != ^uint64(0) && id > uint64(^uint32(0)) {
			return fmt.Errorf("uid/gid must fit into 32 bits")
		}
	}
	var secret []byte
	var e error
	if *passwordStdin {
		secret, e = readPassword(os.Stdin)
	} else {
		fmt.Fprint(os.Stderr, "Password: ")
		secret, e = term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
	}
	if e != nil {
		return e
	}
	password := string(secret)
	clear(secret)
	if password == "" {
		return fmt.Errorf("empty password")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	var id client.ID
	if decoded, err := hex.DecodeString(*bucket); err == nil && len(decoded) == len(id) {
		copy(id[:], decoded)
	} else {
		id, e = nbd.ResolveBucket(ctx, nbd.Config{URL: *uri, Username: *username, Password: password}, *bucket)
		if e != nil {
			return e
		}
	}
	conn, e := client.Dial(ctx, *uri, *username, password, id)
	if e != nil {
		return e
	}
	defer conn.Close()
	store := client.NewSubscriptionStore(conn, 8<<20)
	defer store.Close()
	options := client.FSOptions{Create: *init, ReadOnly: *ro}
	if *uid != ^uint64(0) {
		options.UID = uint32(*uid)
	}
	if *gid != ^uint64(0) {
		options.GID = uint32(*gid)
	}
	if *init {
		if _, e := conn.Stat(ctx, client.FilesystemRootID()); errors.Is(e, client.ErrNotFound) {
			objects, _, e := conn.ScanObjects(ctx, nil, 1)
			if e != nil {
				return e
			}
			if len(objects) != 0 {
				return fmt.Errorf("--init requires an empty filesystem bucket")
			}
		} else if e != nil {
			return e
		}
	}
	fs, e := client.OpenFilesystem(ctx, store, options)
	if e != nil {
		return e
	}
	adapter := &winfs.Adapter{FS: winfs.New(ctx, fs, *ro)}
	root, e := fs.Stat(ctx, client.FilesystemRootID())
	if e != nil {
		return e
	}
	adapter.FS.UID, adapter.FS.GID = root.UID, root.GID
	if *uid != ^uint64(0) {
		adapter.FS.UID = uint32(*uid)
	}
	if *gid != ^uint64(0) {
		adapter.FS.GID = uint32(*gid)
	}
	host := fuse.NewFileSystemHost(adapter)
	host.SetCapCaseInsensitive(false)
	host.SetCapReaddirPlus(true)
	opts := []string{"-o", "volname=STQR,uid=-1,gid=-1,umask=077,FileInfoTimeout=0,DirInfoTimeout=0,attr_timeout=0,entry_timeout=0,negative_timeout=0"}
	if *ro {
		opts = append(opts, "-o", "ro")
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			host.Unmount()
		case <-done:
		}
	}()
	fmt.Fprintf(os.Stderr, "Mounting bucket %s on %s (Ctrl+C to unmount)\n", *bucket, drive)
	// cgofuse reports a missing WinFsp runtime with a panic. Turn it into a CLI error.
	defer func() {
		if p := recover(); p != nil {
			if text, ok := p.(string); ok && strings.Contains(text, "cgofuse: cannot find winfsp") {
				result = fmt.Errorf("WinFsp runtime not found; install WinFsp before mounting")
			} else {
				panic(p)
			}
		}
	}()
	if !host.Mount(drive, opts) {
		return fmt.Errorf("mount failed; check WinFsp installation and drive availability")
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer syncCancel()
	return conn.Sync(syncCtx)
}
func main() {
	if e := run(); e != nil && !errors.Is(e, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "stqr-windows:", e)
		os.Exit(1)
	}
}

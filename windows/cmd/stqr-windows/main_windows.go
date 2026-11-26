//go:build windows

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
	winfs "github.com/launix-de/stqr-storage/windows"
	"github.com/winfsp/cgofuse/fuse"
	"golang.org/x/term"
)

var errMountUnavailable = errors.New("Windows mount unavailable")
var errFinalSync = errors.New("final filesystem sync failed")

// Cancellation may end a mount normally, but must never hide a failed barrier.
func mountResult(ctx context.Context, err error) error {
	if ctx.Err() != nil && !errors.Is(err, errFinalSync) {
		return nil
	}
	return err
}

func run() error {
	flags := flag.NewFlagSet("stqr-windows", flag.ContinueOnError)
	uri := flags.String("uri", "http://127.0.0.1:7419", "STQR endpoint")
	username := flags.String("username", "", "STQR username")
	bucket := flags.String("bucket", "", "filesystem bucket name or ID")
	ro := flags.Bool("readonly", false, "readonly mount")
	lowerBucket := flags.String("lower-bucket", "", "optional readonly native-overlay base bucket")
	whitelist := flags.String("writable-paths", "", "newline-separated writable overlay paths; directories end in /")
	initialize := flags.Bool("init", false, "initialize an empty filesystem bucket")
	stdin := flags.Bool("password-stdin", false, "read password from redirected standard input")
	uid := flags.Uint64("uid", ^uint64(0), "Unix owner for new files (default: root inode owner)")
	gid := flags.Uint64("gid", ^uint64(0), "Unix group for new files (default: root inode group)")
	install := flags.Bool("install", false, "remember credentials and mount at user login")
	uninstall := flags.String("uninstall", "", "remove saved drive and unmount, e.g. S:")
	saved := flags.String("saved-drive", "", "mount a saved drive, e.g. S:")
	background := flags.Bool("background", false, "run saved drive in background, retrying initial connection")
	flags.Bool("setup", false, "open WinUI configuration")
	flags.Usage = func() {
		fmt.Fprintln(flags.Output(), "Usage: stqr-windows [--setup] | --uri URL --username USER --bucket BUCKET [--install] S: | --uninstall S:\nWithout arguments, opens the setup dialog. WinFsp is required.")
		flags.PrintDefaults()
	}
	if e := flags.Parse(os.Args[1:]); e != nil {
		return e
	}
	installation, e := userInstallation()
	if e != nil {
		return e
	}
	if *uninstall != "" {
		if e = installation.remove(*uninstall); e != nil {
			return e
		}
		return stopDrive(*uninstall)
	}
	if *background && *saved == "" {
		return fmt.Errorf("--background requires --saved-drive")
	}
	var c mountConfig
	if *saved != "" {
		c, e = installation.load(*saved)
		if e != nil {
			return e
		}
	} else {
		if flags.NArg() != 1 || *username == "" || *bucket == "" {
			return fmt.Errorf("username, bucket and drive letter required; use --setup for the dialog")
		}
		c = mountConfig{URI: *uri, Username: *username, Bucket: *bucket, Drive: flags.Arg(0), ReadOnly: *ro, LowerBucket: *lowerBucket}
		if *whitelist != "" {
			for _, line := range strings.Split(*whitelist, "\n") {
				if line = strings.TrimSpace(line); line != "" {
					c.Whitelist = append(c.Whitelist, line)
				}
			}
		}
		for _, v := range []uint64{*uid, *gid} {
			if v != ^uint64(0) && v > uint64(^uint32(0)) {
				return fmt.Errorf("uid/gid must fit into 32 bits")
			}
		}
		if *uid != ^uint64(0) {
			v := uint32(*uid)
			c.UID = &v
		}
		if *gid != ^uint64(0) {
			v := uint32(*gid)
			c.GID = &v
		}
		var secret []byte
		if *stdin {
			secret, e = readPassword(os.Stdin)
		} else {
			fmt.Fprint(os.Stderr, "Password: ")
			secret, e = term.ReadPassword(int(os.Stdin.Fd()))
			fmt.Fprintln(os.Stderr)
		}
		if e != nil {
			return e
		}
		c.Password = string(secret)
		clear(secret)
	}
	if e = c.validate(); e != nil {
		return e
	}
	if *initialize && c.ReadOnly {
		return fmt.Errorf("--init requires a writable mount")
	}
	if *background {
		hideConsole()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		select {
		case <-consoleInterrupt:
			cancel()
		case <-ctx.Done():
		}
	}()
	ctx, release, e := claimDrive(ctx, c.Drive)
	if errors.Is(e, errAlreadyMounted) {
		if *background {
			return nil
		}
		return fmt.Errorf("%w; zuerst über den Dialog trennen", e)
	}
	if e != nil {
		return e
	}
	defer release()
	finished := make(chan struct{})
	_, closeSession, e := watchSessionShutdown(cancel, finished)
	if e != nil {
		return e
	}
	defer closeSession()
	defer close(finished)
	if *background {
		if e = os.MkdirAll(installation.directory, 0700); e != nil {
			return e
		}
		log, e := os.OpenFile(filepath.Join(installation.directory, "mount-"+c.Drive[:1]+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		defer log.Close()
		for {
			e = mount(ctx, c, false, installation, false, false)
			if e == nil {
				return nil
			}
			fmt.Fprintf(log, "%s %v\n", time.Now().Format(time.RFC3339), e)
			if ctx.Err() != nil {
				return mountResult(ctx, e)
			}
			if errors.Is(e, errMountUnavailable) {
				return e
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Second):
			}
		}
	}
	e = mount(ctx, c, *initialize, installation, *install, false)
	return mountResult(ctx, e)
}

func mount(ctx context.Context, c mountConfig, initialize bool, installation installation, persist, forget bool) (result error) {
	if err := ensureWinFsp(); err != nil {
		return err
	}
	conn, e := client.DialBucket(ctx, c.URI, c.Username, c.Password, c.Bucket, "fs")
	if e != nil {
		return e
	}
	defer conn.Close()
	store := client.NewSubscriptionStore(conn, 8<<20)
	store.EnableAcceptedWrites()
	defer store.Close()
	defer func() {
		syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := conn.Sync(syncCtx); err != nil {
			result = errors.Join(result, fmt.Errorf("%w: %w", errFinalSync, err))
		}
	}()
	options := client.FSOptions{Create: initialize, ReadOnly: c.ReadOnly}
	if c.UID != nil {
		options.UID = *c.UID
	}
	if c.GID != nil {
		options.GID = *c.GID
	}
	if initialize {
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
	stores := []*client.SubscriptionStore{store}
	var fs winfs.FilesystemView
	if c.LowerBucket == "" {
		fs, e = client.OpenFilesystem(ctx, store, options)
	} else {
		lowerConn, err := client.DialBucket(ctx, c.URI, c.Username, c.Password, c.LowerBucket, "fs")
		if err != nil {
			return err
		}
		defer lowerConn.Close()
		lowerID := lowerConn.BucketID()
		if lowerID == conn.BucketID() {
			return fmt.Errorf("Basis und beschreibbares Bucket müssen verschieden sein")
		}
		lowerStore := client.NewSubscriptionStore(lowerConn, 8<<20)
		defer lowerStore.Close()
		stores = append(stores, lowerStore)
		fs, e = client.OpenOverlayFilesystem(ctx, lowerStore, store, client.OverlayOptions{Create: initialize, ReadOnly: c.ReadOnly, LowerBucketID: lowerID, Whitelist: c.Whitelist})
	}
	if e != nil {
		return e
	}
	adapter := &winfs.Adapter{FS: winfs.New(ctx, fs, c.ReadOnly)}
	root, e := fs.Stat(ctx, client.FilesystemRootID())
	if e != nil {
		return e
	}
	adapter.FS.UID, adapter.FS.GID = root.UID, root.GID
	if c.UID != nil {
		adapter.FS.UID = *c.UID
	}
	if c.GID != nil {
		adapter.FS.GID = *c.GID
	}
	if persist {
		executable, err := os.Executable()
		if err != nil {
			return err
		}
		if err = installation.save(c, executable); err != nil {
			return err
		}
	} else if forget {
		if err := installation.remove(c.Drive); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	host := fuse.NewFileSystemHost(adapter)
	host.SetCapCaseInsensitive(false)
	host.SetCapReaddirPlus(true)
	host.SetCapDeleteAccess(true)
	stopNotifications := watchWindowsChanges(ctx, stores, adapter.FS, host)
	defer stopNotifications()
	opts := []string{"-o", "volname=STQR,uid=-1,gid=-1,umask=077,FileInfoTimeout=0,DirInfoTimeout=0,attr_timeout=0,entry_timeout=0,negative_timeout=0"}
	if c.ReadOnly {
		opts = append(opts, "-o", "ro")
	}
	done := make(chan struct{})
	defer close(done)
	adapter.OnReady = func() { go waitForUIDrive(ctx, done, c.Drive) }
	go func() {
		select {
		case <-ctx.Done():
			// Cancellation can race the driver's initial mount. Retry until
			// it is actually detached, or Mount has already returned.
			for {
				select {
				case <-done:
					return
				default:
				}
				if host.Unmount() {
					return
				}
				select {
				case <-done:
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
		case <-done:
		}
	}()
	fmt.Fprintf(os.Stderr, "Mounting bucket %s on %s (Ctrl+C to unmount)\n", c.Bucket, c.Drive)
	// cgofuse reports a missing WinFsp runtime with a panic. Turn it into a CLI error.
	defer func() {
		if p := recover(); p != nil {
			if text, ok := p.(string); ok && strings.Contains(text, "cgofuse: cannot find winfsp") {
				result = fmt.Errorf("%w: WinFsp wurde nicht gefunden. Bitte WinFsp über https://winfsp.dev/rel/ installieren und STQR danach erneut öffnen", errMountUnavailable)
			} else {
				panic(p)
			}
		}
	}()
	if !host.Mount(c.Drive, opts) {
		return fmt.Errorf("%w: mount failed; check WinFsp installation and drive availability", errMountUnavailable)
	}
	return nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--ui-rpc" {
		if err := runUIRPC(); err != nil {
			_ = json.NewEncoder(os.Stdout).Encode(uiReply{Version: uiProtocolVersion, Type: "error", Message: err.Error()})
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 1 || hasSetupFlag(os.Args[1:]) || len(os.Args) == 2 && os.Args[1] == "--ui-smoke-test" {
		if err := launchWinUI(); err != nil {
			messageBox(0, "STQR", err.Error(), 0x10)
			os.Exit(1)
		}
		return
	}
	attachCLIConsole()
	if e := run(); e != nil && !errors.Is(e, flag.ErrHelp) {
		if len(os.Args) == 1 || hasSetupFlag(os.Args[1:]) {
			messageBox(0, "STQR", e.Error(), 0x10)
		} else {
			fmt.Fprintln(os.Stderr, "stqr-windows:", e)
		}
		os.Exit(1)
	}
}
func hasSetupFlag(args []string) bool {
	for _, a := range args {
		if strings.TrimLeft(a, "-") == "setup" || strings.TrimLeft(a, "-") == "setup=true" {
			return true
		}
	}
	return false
}

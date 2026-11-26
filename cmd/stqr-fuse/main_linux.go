// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

// stqr-fuse mounts one remote STQR bucket. It runs in the foreground.
package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

type options struct {
	uri, username, password, mountpoint             string
	bucket                                          client.BucketID
	readonlyBucket                                  client.BucketID
	readonlyURI, readonlyUsername, readonlyPassword string
	whitelist                                       []string
	allowOther                                      bool
	create, debug                                   bool
	fsck                                            bool
	passwordStdin                                   bool
	connectTimeout                                  time.Duration
	metadataCacheBytes                              uint64
}

func parseOptions(args []string, out io.Writer) (options, error) {
	var opt options
	var bucket, readonlyBucket, whitelist string
	flags := flag.NewFlagSet("stqr-fuse", flag.ContinueOnError)
	flags.SetOutput(out)
	flags.StringVar(&opt.uri, "uri", "", "STQR node URI (http://host:7419 or ws://host:7419/api/ws)")
	flags.StringVar(&opt.username, "username", "", "STQR username")
	flags.StringVar(&opt.password, "password", "", "STQR password")
	flags.BoolVar(&opt.passwordStdin, "password-stdin", false, "read the STQR password from one line on standard input")
	flags.StringVar(&bucket, "bucket", "", "bucket ID (32 hexadecimal characters)")
	flags.StringVar(&readonlyBucket, "readonly-bucket", "", "readonly bucket ID; with -bucket, use it as the native overlay lower")
	flags.StringVar(&opt.readonlyURI, "readonly-uri", "", "readonly node URI (defaults to -uri)")
	flags.StringVar(&opt.readonlyUsername, "readonly-username", "", "readonly username (defaults to -username)")
	flags.StringVar(&opt.readonlyPassword, "readonly-password", "", "readonly password (defaults to -password)")
	flags.StringVar(&whitelist, "whitelist", "", "overlay writable paths, e.g. conf.json,fop-files/; persisted in the upper")
	flags.BoolVar(&opt.allowOther, "allow-other", false, "allow other local UIDs (e.g. PHP workers); normal file permissions still apply")
	flags.BoolVar(&opt.create, "init", false, "initialize a missing filesystem root; preserve an existing root")
	flags.BoolVar(&opt.debug, "debug", false, "enable FUSE protocol logging")
	flags.BoolVar(&opt.fsck, "fsck", false, "validate the filesystem, delete unreachable bucket objects, then exit; all mounts must be closed")
	flags.Uint64Var(&opt.metadataCacheBytes, "metadata-cache-bytes", 32<<20, "subscription-backed metadata cache budget per bucket in bytes; 0 disables caching")
	flags.DurationVar(&opt.connectTimeout, "connect-timeout", 30*time.Second, "connection and filesystem initialization timeout")
	flags.Usage = func() {
		fmt.Fprintln(out, "Usage: stqr-fuse -uri URI -username USER {-password PASSWORD | -password-stdin} [-readonly-bucket ID] [-bucket ID] [-init] MOUNTPOINT\n       stqr-fuse -uri URI -username USER {-password PASSWORD | -password-stdin} -bucket ID [-readonly-bucket ID] --fsck [MOUNTPOINT]\nOnly -readonly-bucket: readonly; only -bucket: read/write; both: native overlay.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return opt, err
	}
	if (!opt.fsck && flags.NArg() != 1 || opt.fsck && flags.NArg() > 1) || opt.uri == "" || opt.username == "" || opt.password == "" && !opt.passwordStdin {
		return opt, errors.New("URI, username, password and bucket ID are required; supply one mountpoint unless using --fsck (see -help)")
	}
	if opt.passwordStdin && opt.password != "" {
		return opt, errors.New("use -password or -password-stdin, not both")
	}
	if opt.fsck && opt.create {
		return opt, errors.New("--fsck and -init cannot be combined")
	}
	parseBucket := func(raw string) (client.BucketID, error) {
		var id client.BucketID
		if raw == "" {
			return id, nil
		}
		b, err := hex.DecodeString(raw)
		if err != nil || len(b) != len(id) {
			return id, errors.New("bucket IDs must contain 32 hexadecimal characters")
		}
		copy(id[:], b)
		if id == (client.BucketID{}) {
			return id, errors.New("bucket IDs must be nonzero")
		}
		return id, nil
	}
	var err error
	opt.bucket, err = parseBucket(bucket)
	if err != nil {
		return opt, err
	}
	opt.readonlyBucket, err = parseBucket(readonlyBucket)
	if err != nil {
		return opt, err
	}
	if opt.bucket == (client.BucketID{}) && opt.readonlyBucket == (client.BucketID{}) {
		return opt, errors.New("-bucket or -readonly-bucket is required")
	}
	if opt.bucket != (client.BucketID{}) && opt.bucket == opt.readonlyBucket {
		return opt, errors.New("upper and readonly buckets must be different")
	}
	if opt.bucket == (client.BucketID{}) && (opt.create || opt.fsck) {
		return opt, errors.New("-init and --fsck require a writable -bucket")
	}
	whitelistSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "whitelist" {
			whitelistSet = true
		}
	})
	if whitelistSet {
		if opt.bucket == (client.BucketID{}) || opt.readonlyBucket == (client.BucketID{}) {
			return opt, errors.New("--whitelist requires both buckets")
		}
		opt.whitelist, err = client.ParseFilesystemWhitelist(whitelist)
		if err != nil {
			return opt, err
		}
	}
	if opt.readonlyBucket == (client.BucketID{}) && (opt.readonlyURI != "" || opt.readonlyUsername != "" || opt.readonlyPassword != "") {
		return opt, errors.New("readonly connection overrides require -readonly-bucket")
	}
	if opt.readonlyURI == "" {
		opt.readonlyURI = opt.uri
	}
	if opt.readonlyUsername == "" {
		opt.readonlyUsername = opt.username
	}
	if opt.readonlyPassword == "" {
		opt.readonlyPassword = opt.password
	}
	if opt.connectTimeout <= 0 {
		return opt, errors.New("connect-timeout must be positive")
	}
	opt.mountpoint = flags.Arg(0)
	return opt, nil
}

func run(ctx context.Context, opt options, out io.Writer) (resultErr error) {
	if !opt.fsck {
		stat, err := os.Stat(opt.mountpoint)
		if err != nil {
			return fmt.Errorf("mountpoint: %w", err)
		}
		if !stat.IsDir() {
			return errors.New("mountpoint must be a directory")
		}
	}
	startup, cancel := context.WithTimeout(ctx, opt.connectTimeout)
	defer cancel()
	var conn, lower *client.Client
	var err error
	if opt.bucket != (client.BucketID{}) {
		conn, err = client.Dial(startup, opt.uri, opt.username, opt.password, opt.bucket, "fs")
		if err != nil {
			return fmt.Errorf("connect upper: %w", err)
		}
		defer conn.Close()
	}
	if opt.readonlyBucket != (client.BucketID{}) {
		lower, err = client.Dial(startup, opt.readonlyURI, opt.readonlyUsername, opt.readonlyPassword, opt.readonlyBucket, "fs")
		if err != nil {
			return fmt.Errorf("connect readonly: %w", err)
		}
		defer lower.Close()
	}
	if opt.fsck {
		cancel() // Startup timeout must not limit a full bucket traversal.
		var result client.FSCheckResult
		if lower != nil {
			if opt.whitelist != nil {
				if _, err = client.OpenOverlayFilesystem(ctx, lower, conn, client.OverlayOptions{LowerBucketID: opt.readonlyBucket, Whitelist: opt.whitelist}); err != nil {
					return err
				}
			}
			result, err = conn.CheckOverlayFilesystem(ctx, lower, opt.readonlyBucket)
		} else {
			result, err = conn.CheckFilesystem(ctx)
		}
		if err != nil {
			return fmt.Errorf("fsck: %w (confirmed deleted: %d objects, %d payload bytes)", err, result.Deleted, result.DeletedBytes)
		}
		fmt.Fprintf(out, "stqr-fuse: fsck OK; scanned %d objects, reachable %d, deleted %d (%d payload bytes); removed %d whiteouts, pruned %d directory records\n", result.Scanned, result.Reachable, result.Deleted, result.DeletedBytes, result.WhiteoutsRemoved, result.DirectoriesPruned)
		return nil
	}
	var upperStore, lowerStore client.FilesystemStore = conn, lower
	{
		if conn != nil {
			cache := client.NewSubscriptionStore(conn, opt.metadataCacheBytes)
			cache.EnableAcceptedWrites()
			defer func() {
				syncCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if syncErr := cache.Sync(syncCtx); syncErr != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("final sync: %w", syncErr))
				}
				cache.Close()
			}()
			upperStore = cache
		}
		if lower != nil {
			cache := client.NewSubscriptionStore(lower, opt.metadataCacheBytes)
			defer cache.Close()
			lowerStore = cache
		}
	}
	var filesystem client.FilesystemView
	if conn != nil && lower != nil {
		filesystem, err = client.OpenOverlayFilesystem(startup, lowerStore, upperStore, client.OverlayOptions{Create: opt.create, LowerBucketID: opt.readonlyBucket, Whitelist: opt.whitelist})
	} else {
		store := upperStore
		readonly := false
		if lower != nil {
			store = lowerStore
			readonly = true
		}
		filesystem, err = client.OpenFilesystem(startup, store, client.FSOptions{Create: opt.create, ReadOnly: readonly, UID: uint32(os.Getuid()), GID: uint32(os.Getgid())})
	}
	if err != nil {
		return fmt.Errorf("open filesystem (use -init for a new root): %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	mounted, err := client.MountFilesystem(filesystem, opt.mountpoint, client.FSMountOptions{Debug: opt.debug, ReadOnly: conn == nil, AllowOther: opt.allowOther})
	if err != nil {
		return fmt.Errorf("mount: %w", err)
	}
	fmt.Fprintln(out, "stqr-fuse: mounted", opt.mountpoint)
	done := make(chan struct{})
	go func() { mounted.Wait(); close(done) }()
	select {
	case <-done: // Includes external fusermount -u.
		return nil
	case <-ctx.Done():
		if err = mounted.Unmount(); err != nil {
			// Keep serving a busy mount; closing the bucket connection here would
			// break open files. The operator can retry with fusermount -u.
			fmt.Fprintln(out, "stqr-fuse: unmount:", err, "(still serving; unmount externally)")
		}
		<-done
		return nil
	}
}

func main() {
	opt, err := parseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "stqr-fuse:", err)
		os.Exit(2)
	}
	if opt.passwordStdin {
		line, e := bufio.NewReaderSize(io.LimitReader(os.Stdin, 4098), 4098).ReadString('\n')
		if e != nil && !errors.Is(e, io.EOF) || len(line) > 4096 {
			fmt.Fprintln(os.Stderr, "stqr-fuse: cannot read bounded password line")
			os.Exit(2)
		}
		opt.password = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if opt.password == "" {
			fmt.Fprintln(os.Stderr, "stqr-fuse: empty password")
			os.Exit(2)
		}
		if opt.readonlyPassword == "" {
			opt.readonlyPassword = opt.password
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = run(ctx, opt, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "stqr-fuse:", err)
		os.Exit(1)
	}
}

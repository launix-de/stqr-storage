// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

// stqr-fuse mounts one remote STQR bucket. It runs in the foreground.
package main

import (
	"bufio"
	"bytes"
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
	bucket                                          string
	readonlyBucket                                  string
	readonlyURI, readonlyUsername, readonlyPassword string
	whitelist                                       []string
	allowOther                                      bool
	create, debug                                   bool
	fsck                                            bool
	passwordStdin                                   bool
	passwordFile                                    string
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
	flags.StringVar(&opt.passwordFile, "password-file", "", "read the password from FILE (one line; avoids command-line secrets)")
	flags.BoolVar(&opt.passwordStdin, "password-stdin", false, "read the STQR password from one line on standard input")
	flags.StringVar(&bucket, "bucket", "", "bucket name or ID (32 hexadecimal characters)")
	flags.StringVar(&readonlyBucket, "readonly-bucket", "", "readonly bucket name or ID; with --bucket, use it as the native overlay lower")
	flags.StringVar(&opt.readonlyURI, "readonly-uri", "", "readonly node URI (defaults to --uri)")
	flags.StringVar(&opt.readonlyUsername, "readonly-username", "", "readonly username (defaults to --username)")
	flags.StringVar(&opt.readonlyPassword, "readonly-password", "", "readonly password (defaults to --password)")
	flags.StringVar(&whitelist, "whitelist", "", "overlay writable paths, e.g. conf.json,fop-files/; persisted in the upper")
	flags.BoolVar(&opt.allowOther, "allow-other", false, "allow other local UIDs (e.g. PHP workers); normal file permissions still apply")
	flags.BoolVar(&opt.create, "init", false, "initialize a missing filesystem root; preserve an existing root")
	flags.BoolVar(&opt.debug, "debug", false, "enable FUSE protocol logging")
	flags.BoolVar(&opt.fsck, "fsck", false, "validate the filesystem, delete unreachable bucket objects, then exit; all mounts must be closed")
	flags.Uint64Var(&opt.metadataCacheBytes, "metadata-cache-bytes", 32<<20, "subscription-backed metadata cache budget per bucket in bytes; 0 disables caching")
	flags.DurationVar(&opt.connectTimeout, "connect-timeout", 30*time.Second, "connection and filesystem initialization timeout")
	flags.Usage = func() {
		fmt.Fprintln(out, "Usage: stqr-fuse --uri URI --username USER {--password PASSWORD | --password-stdin | --password-file FILE} [--readonly-bucket NAME_OR_ID] [--bucket NAME_OR_ID] [--init] MOUNTPOINT\n       stqr-fuse --uri URI --username USER {--password PASSWORD | --password-stdin | --password-file FILE} --bucket NAME_OR_ID [--readonly-bucket NAME_OR_ID] --fsck [MOUNTPOINT]\nOnly --readonly-bucket: readonly; only --bucket: read/write; both: native overlay.")
		var defaults bytes.Buffer
		flags.SetOutput(&defaults)
		flags.PrintDefaults()
		flags.SetOutput(out)
		fmt.Fprint(out, strings.ReplaceAll(defaults.String(), "  -", "  --"))
	}
	if err := flags.Parse(args); err != nil {
		return opt, err
	}
	if (!opt.fsck && flags.NArg() != 1 || opt.fsck && flags.NArg() > 1) || opt.uri == "" || opt.username == "" || opt.password == "" && !opt.passwordStdin && opt.passwordFile == "" {
		return opt, errors.New("URI, username, password and bucket name or ID are required; supply one mountpoint unless using --fsck (see --help)")
	}
	sources := 0
	if opt.password != "" {
		sources++
	}
	if opt.passwordStdin {
		sources++
	}
	if opt.passwordFile != "" {
		sources++
	}
	if sources > 1 {
		return opt, errors.New("use exactly one of --password, --password-stdin or --password-file")
	}
	if opt.fsck && opt.create {
		return opt, errors.New("--fsck and --init cannot be combined")
	}
	validateSelector := func(raw string) error {
		if raw == "" {
			return nil
		}
		if len(raw) > 260 || strings.ContainsAny(raw, "\r\n\x00") {
			return errors.New("invalid bucket name or ID")
		}
		b, err := hex.DecodeString(raw)
		if err == nil && len(b) == 16 && bytes.Equal(b, make([]byte, 16)) {
			return errors.New("bucket IDs must be nonzero")
		}
		return nil
	}
	var err error
	if err = validateSelector(bucket); err != nil {
		return opt, err
	}
	if err = validateSelector(readonlyBucket); err != nil {
		return opt, err
	}
	opt.bucket, opt.readonlyBucket = bucket, readonlyBucket
	if bucket == "" && readonlyBucket == "" {
		return opt, errors.New("--bucket or --readonly-bucket is required")
	}
	if bucket == "" && (opt.create || opt.fsck) {
		return opt, errors.New("--init and --fsck require a writable --bucket")
	}
	whitelistSet := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "whitelist" {
			whitelistSet = true
		}
	})
	if whitelistSet {
		if bucket == "" || readonlyBucket == "" {
			return opt, errors.New("--whitelist requires both buckets")
		}
		opt.whitelist, err = client.ParseFilesystemWhitelist(whitelist)
		if err != nil {
			return opt, err
		}
	}
	if readonlyBucket == "" && (opt.readonlyURI != "" || opt.readonlyUsername != "" || opt.readonlyPassword != "") {
		return opt, errors.New("readonly connection overrides require --readonly-bucket")
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
	var lowerID client.BucketID
	if opt.bucket != "" {
		conn, err = client.DialBucket(startup, opt.uri, opt.username, opt.password, opt.bucket, "fs")
		if err != nil {
			return fmt.Errorf("connect upper: %w", err)
		}
		defer conn.Close()
	}
	if opt.readonlyBucket != "" {
		lower, err = client.DialBucket(startup, opt.readonlyURI, opt.readonlyUsername, opt.readonlyPassword, opt.readonlyBucket, "fs")
		if err != nil {
			return fmt.Errorf("connect readonly: %w", err)
		}
		defer lower.Close()
		lowerID = lower.BucketID()
		if conn != nil && conn.BucketID() == lowerID {
			return errors.New("upper and readonly buckets must be different")
		}
	}
	if opt.fsck {
		cancel() // Startup timeout must not limit a full bucket traversal.
		var result client.FSCheckResult
		if lower != nil {
			if opt.whitelist != nil {
				if _, err = client.OpenOverlayFilesystem(ctx, lower, conn, client.OverlayOptions{LowerBucketID: lowerID, Whitelist: opt.whitelist}); err != nil {
					return err
				}
			}
			result, err = conn.CheckOverlayFilesystem(ctx, lower, lowerID)
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
		filesystem, err = client.OpenOverlayFilesystem(startup, lowerStore, upperStore, client.OverlayOptions{Create: opt.create, LowerBucketID: lowerID, Whitelist: opt.whitelist})
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
		return fmt.Errorf("open filesystem (use --init for a new root): %w", err)
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
	if opt.passwordFile != "" || opt.passwordStdin {
		var source io.Reader = os.Stdin
		if opt.passwordFile != "" {
			f, e := os.Open(opt.passwordFile)
			if e != nil {
				fmt.Fprintln(os.Stderr, "stqr-fuse: cannot open password file:", e)
				os.Exit(2)
			}
			defer f.Close()
			source = f
		}
		opt.password, err = readPassword(source)
		if err != nil {
			fmt.Fprintln(os.Stderr, "stqr-fuse:", err)
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

// Both credential sources use one bounded line and preserve password spaces.
// Errors never include file contents or the supplied password.
func readPassword(source io.Reader) (string, error) {
	line, err := bufio.NewReaderSize(io.LimitReader(source, 4098), 4098).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) || len(line) > 4096 {
		return "", errors.New("cannot read bounded password line")
	}
	password := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if password == "" {
		return "", errors.New("empty password")
	}
	return password, nil
}

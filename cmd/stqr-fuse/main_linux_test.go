// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	args := []string{"-uri", "http://localhost:7419", "-username", "alice", "-password", "secret", "-bucket", "0123456789abcdef0123456789abcdef", "-init", "/mnt/stqr"}
	opt, err := parseOptions(args, io.Discard)
	if err != nil || opt.bucket.String() != args[7] || !opt.create || opt.mountpoint != "/mnt/stqr" || opt.connectTimeout != 30*time.Second {
		t.Fatalf("unexpected options: %v", err)
	}
	for _, tail := range [][]string{nil, {"/mnt/a", "/mnt/b"}, {"-connect-timeout", "0", "/mnt/a"}} {
		if _, err := parseOptions(append(append([]string{}, args[:8]...), tail...), io.Discard); err == nil {
			t.Fatal("accepted invalid arguments")
		}
	}
	for _, bucket := range []string{"", "xyz", "00000000000000000000000000000000", "01"} {
		bad := append([]string{}, args...)
		bad[7] = bucket
		if _, err := parseOptions(bad, io.Discard); err == nil {
			t.Fatal("accepted invalid bucket")
		}
	}
	if _, err := parseOptions([]string{"-help"}, io.Discard); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
}

func TestRunCancelledBeforeConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := run(ctx, options{mountpoint: t.TempDir(), connectTimeout: time.Second}, io.Discard)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup: %v", err)
	}
}

func TestFsckOptions(t *testing.T) {
	base := []string{"-uri", "http://localhost:7419", "-username", "alice", "-password", "secret", "-bucket", "0123456789abcdef0123456789abcdef", "--fsck"}
	for _, tail := range [][]string{nil, {"/mnt/stqr"}} {
		opt, err := parseOptions(append(append([]string{}, base...), tail...), io.Discard)
		if err != nil || !opt.fsck {
			t.Fatal(err)
		}
	}
	for _, tail := range [][]string{{"-init"}, {"/mnt/a", "/mnt/b"}} {
		if _, err := parseOptions(append(append([]string{}, base...), tail...), io.Discard); err == nil {
			t.Fatal("accepted incompatible fsck arguments")
		}
	}
}

func TestReadonlyAndOverlayOptions(t *testing.T) {
	base := []string{"-uri", "http://localhost:7419", "-username", "alice", "-password", "secret"}
	lower := "11111111111111111111111111111111"
	upper := "22222222222222222222222222222222"
	for _, tail := range [][]string{{"--readonly-bucket", lower, "/mnt/a"}, {"--bucket", upper, "/mnt/a"}, {"--readonly-bucket", lower, "--bucket", upper, "--whitelist=conf.json,fop-files/", "--readonly-uri=http://remote:7419", "--readonly-username=reader", "--readonly-password=other", "--allow-other", "/mnt/a"}} {
		o, err := parseOptions(append(append([]string{}, base...), tail...), io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		if o.readonlyURI == "" || o.readonlyUsername == "" || o.readonlyPassword == "" {
			t.Fatal("readonly defaults missing")
		}
	}
	for _, tail := range [][]string{{"--readonly-bucket", lower, "--init", "/mnt/a"}, {"--readonly-bucket", lower, "--fsck"}, {"--bucket", upper, "--whitelist=x", "/mnt/a"}, {"--readonly-bucket", lower, "--bucket", lower, "/mnt/a"}, {"--readonly-bucket", lower, "--bucket", upper, "--whitelist=../escape", "/mnt/a"}, {"--readonly-bucket", lower, "--bucket", upper, "--whitelist=", "/mnt/a"}} {
		if _, err := parseOptions(append(append([]string{}, base...), tail...), io.Discard); err == nil {
			t.Fatalf("invalid options accepted: %v", tail)
		}
	}
}

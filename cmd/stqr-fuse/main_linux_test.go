// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func TestOptions(t *testing.T) {
	args := []string{"-uri", "http://localhost:7419", "-username", "alice", "-password", "secret", "-bucket", "0123456789abcdef0123456789abcdef", "-init", "/mnt/stqr"}
	opt, err := parseOptions(args, io.Discard)
	if err != nil || opt.bucket != args[7] || !opt.create || opt.mountpoint != "/mnt/stqr" || opt.connectTimeout != 30*time.Second {
		t.Fatalf("unexpected options: %v", err)
	}
	for _, tail := range [][]string{nil, {"/mnt/a", "/mnt/b"}, {"-connect-timeout", "0", "/mnt/a"}} {
		if _, err := parseOptions(append(append([]string{}, args[:8]...), tail...), io.Discard); err == nil {
			t.Fatal("accepted invalid arguments")
		}
	}
	for _, bucket := range []string{"", "bad\nname", "00000000000000000000000000000000", strings.Repeat("x", 261)} {
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

func TestNamedBucketsPasswordFileAndGNUHelp(t *testing.T) {
	base := []string{"--uri=http://localhost:7419", "--username=alice", "--password-file=/run/secrets/stqr", "--bucket=photos", "--readonly-bucket=base", "/mnt/stqr"}
	o, err := parseOptions(base, io.Discard)
	if err != nil || o.bucket != "photos" || o.readonlyBucket != "base" || o.passwordFile != "/run/secrets/stqr" {
		t.Fatal(o, err)
	}
	for _, source := range []string{"--password=literal", "--password-stdin"} {
		args := append(append([]string{}, base[:len(base)-1]...), source, "/mnt/stqr")
		if _, err = parseOptions(args, io.Discard); err == nil {
			t.Fatal("accepted multiple credential sources")
		}
	}
	var help bytes.Buffer
	if _, err = parseOptions([]string{"--help"}, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	if !strings.Contains(help.String(), "--password-file") || strings.Contains(help.String(), "  -uri") || strings.Contains(help.String(), "stqr-fuse -uri") {
		t.Fatal(help.String())
	}
}
func TestBoundedPasswordInput(t *testing.T) {
	for _, raw := range []string{"secret", "secret\n", "secret\r\n", " secret \n"} {
		got, err := readPassword(strings.NewReader(raw))
		want := strings.TrimSuffix(strings.TrimSuffix(raw, "\n"), "\r")
		if err != nil || got != want {
			t.Fatal("password content was changed", err)
		}
	}
	for _, raw := range []string{"", "\n", strings.Repeat("x", 4097)} {
		if _, err := readPassword(strings.NewReader(raw)); err == nil {
			t.Fatal("accepted empty or oversized password")
		}
	}
}

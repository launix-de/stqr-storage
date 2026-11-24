// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/launix-de/stqr-storage/client"
	nbd "github.com/launix-de/stqr-storage/qemu-plugin-nbd"
)

func run() (result error) {
	f := flag.NewFlagSet("stqr-nbd", flag.ContinueOnError)
	cfg := f.String("config", "stqr.yaml", "STQR YAML or private JSON credentials file")
	bucket := f.String("bucket", "", "bucket name")
	bucketID := f.String("bucket-id", "", "bucket ID instead of name")
	size := f.Uint64("size", 0, "virtual bytes (multiple of 512)")
	socket := f.String("socket", "", "private Unix NBD socket")
	def := f.String("default-export", "", "optional export selected by an empty NBD name")
	ro := f.Bool("readonly", false, "readonly exports")
	vendor := f.String("manufacturer", "STQR", "device manufacturer")
	model := f.String("model", "Striped block device", "device model")
	serial := f.String("serial", "", "serial (default device UUID)")
	limit := f.Int("max-connections", 64, "simultaneous NBD connections")
	f.Usage = func() {
		fmt.Fprintln(f.Output(), "Usage: stqr-nbd --config FILE --bucket NAME [OPTIONS] create DEVICE\n       stqr-nbd --config FILE --bucket NAME list|info DEVICE\n       stqr-nbd --config FILE --bucket NAME --socket PATH serve\nAll named devices in the bucket are NBD exports. Stripes are 1 MiB; no data cache.")
		f.PrintDefaults()
	}
	if e := f.Parse(os.Args[1:]); e != nil {
		return e
	}
	args := f.Args()
	if len(args) < 1 {
		f.Usage()
		return fmt.Errorf("command required")
	}
	c, e := nbd.LoadConfig(*cfg)
	if e != nil {
		return e
	}
	ctx := context.Background()
	var id client.ID
	if *bucketID != "" {
		var raw []byte
		raw, e = hex.DecodeString(*bucketID)
		if e == nil && len(raw) != len(id) {
			e = fmt.Errorf("bucket ID must have 32 hexadecimal digits")
		}
		copy(id[:], raw)
	} else {
		id, e = nbd.ResolveBucket(ctx, c, *bucket)
	}
	if e != nil {
		return e
	}
	b, e := nbd.Open(ctx, c.URL, c.Username, c.Password, id, *ro)
	if e != nil {
		return e
	}
	defer func() { result = errors.Join(result, b.Close()) }()
	switch args[0] {
	case "create":
		if *ro || len(args) != 2 {
			return fmt.Errorf("create needs a device name and writable connection")
		}
		m, e := b.Create(ctx, nbd.Metadata{Name: args[1], Size: *size, Manufacturer: *vendor, Model: *model, Serial: *serial})
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	case "list":
		m, e := b.List(ctx)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	case "info":
		if len(args) != 2 {
			return fmt.Errorf("device name required")
		}
		m, e := b.Lookup(ctx, args[1])
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(m)
	case "serve":
		if *socket == "" {
			return fmt.Errorf("--socket required")
		}
		return nbd.Serve(ctx, *socket, b.Catalog, nbd.Options{DefaultExport: *def, ReadOnly: *ro, MaxConnections: *limit})
	}
	return fmt.Errorf("unknown command")
}
func main() {
	if e := run(); e != nil && !errors.Is(e, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "stqr-nbd:", e)
		os.Exit(1)
	}
}

// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/launix-de/stqr-storage/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--version" {
		fmt.Printf("stqr %s (%s)\n", server.SoftwareVersion, server.SoftwareRevision)
		return
	}
	opts, err := server.ParseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "stqr:", err)
		os.Exit(2)
	}
	// Linux shutdown and process supervisors (including PM2) stop us through this
	// path. Cancellation always means SHORT, never a request to drain the node.
	// DRAIN must originate from an authenticated administrative command.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = server.Run(ctx, opts, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "stqr:", err)
		os.Exit(1)
	}
}

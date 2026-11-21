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
	opts, err := server.ParseOptions(os.Args[1:], os.Stderr)
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "stqr:", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err = server.Run(ctx, opts, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "stqr:", err)
		os.Exit(1)
	}
}

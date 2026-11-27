// SPDX-License-Identifier: GPL-3.0-only
//go:build !windows

package main

import "os"

func openExecutableForDigest(path string) (*os.File, error) { return os.Open(path) }

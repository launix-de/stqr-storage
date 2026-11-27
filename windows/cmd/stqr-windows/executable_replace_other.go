// SPDX-License-Identifier: GPL-3.0-only
//go:build !windows

package main

import "os"

func replaceExecutable(source, target string) error { return os.Rename(source, target) }

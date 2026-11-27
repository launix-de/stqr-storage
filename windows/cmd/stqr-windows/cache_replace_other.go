// SPDX-License-Identifier: GPL-3.0-only
//go:build !windows

package main

import "os"

// The verified temporary file is created in the target directory, so this
// publication is a same-filesystem namespace replacement.
func replaceExecutable(source, target string) error { return os.Rename(source, target) }

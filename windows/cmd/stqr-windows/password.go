// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Read one bounded line; do not wait for EOF on an interactive producer.
func readPassword(r io.Reader) ([]byte, error) {
	b := bufio.NewReaderSize(io.LimitReader(r, 4098), 4098)
	line, e := b.ReadString('\n')
	if len(line) > 4096 {
		return nil, fmt.Errorf("password too long")
	}
	if e != nil && e != io.EOF {
		return nil, e
	}
	return []byte(strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")), nil
}

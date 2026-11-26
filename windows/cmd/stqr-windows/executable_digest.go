// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
)

func executableDigest(source string) ([32]byte, error) {
	var digest [32]byte
	f, err := os.Open(source)
	if err != nil {
		return digest, err
	}
	defer f.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(f, (512<<20)+1))
	if err != nil {
		return digest, err
	}
	if n > 512<<20 {
		return digest, fmt.Errorf("executable too large")
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

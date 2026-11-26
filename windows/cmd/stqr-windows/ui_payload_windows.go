// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// The UI is an application resource, not a separately distributed component.
func (i installation) prepareWinUI() (string, error) {
	payload, expected := bundledWinUI()
	if len(payload) == 0 {
		return "", fmt.Errorf("Dieser Entwickler-Build enthält keine Oberfläche. Bitte stqr-windows.exe aus dem vollständigen Windows-Build verwenden")
	}
	return unpackWinUI(i.directory, payload, expected)
}
func unpackWinUI(root string, payload []byte, expected string) (string, error) {
	if hash, err := hex.DecodeString(expected); err != nil || len(hash) != sha256.Size {
		return "", fmt.Errorf("invalid embedded UI digest")
	}
	directory := filepath.Join(root, "runtime", expected)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", err
	}
	target := filepath.Join(directory, "stqr-windows.ui.exe")
	valid := func() bool {
		sum, err := executableDigest(target)
		return err == nil && hex.EncodeToString(sum[:]) == expected
	}
	if valid() {
		return target, nil
	}
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer reader.Close()
	tmp, err := os.CreateTemp(directory, ".ui-*.exe")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, hash), io.LimitReader(reader, (512<<20)+1))
	if err == nil && (n > 512<<20 || hex.EncodeToString(hash.Sum(nil)) != expected) {
		err = fmt.Errorf("embedded UI failed integrity verification")
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err = os.Rename(tmp.Name(), target); err != nil && !valid() {
		return "", err
	}
	return target, nil
}

// SPDX-License-Identifier: GPL-3.0-only
//go:build windows

package main

import "fmt"

// The UI is an application resource, not a separately distributed component.
func (i installation) prepareWinUI() (string, error) {
	payload, expected := bundledWinUI()
	if len(payload) == 0 {
		return "", fmt.Errorf("Dieser Entwickler-Build enthält keine Oberfläche. Bitte stqr-windows.exe aus dem vollständigen Windows-Build verwenden")
	}
	return unpackWinUI(i.directory, payload, expected)
}

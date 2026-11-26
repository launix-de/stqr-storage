// SPDX-License-Identifier: GPL-3.0-only
//go:build windows && !embedded_ui

package main

func bundledWinUI() ([]byte, string) { return nil, "" }

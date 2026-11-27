// SPDX-License-Identifier: GPL-3.0-only
// Package jssdk supplies the dependency-free browser SDK as static ESM assets.
package jssdk

import "embed"

// Files contains the native JavaScript client modules.
//
//go:embed index.mjs codec.mjs
var Files embed.FS

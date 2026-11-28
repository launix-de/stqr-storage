// SPDX-License-Identifier: GPL-3.0-only
//go:build !linux

package server

import "net"

func newSocketOutputPort(net.Conn) socketOutputPort { return nil }

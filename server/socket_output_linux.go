// SPDX-License-Identifier: GPL-3.0-only
//go:build linux

package server

import (
	"net"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

type linuxSocketOutput struct{ raw syscall.RawConn }

func newSocketOutputPort(conn net.Conn) socketOutputPort {
	// Raw writes are valid only on this exact plaintext TCP stream. In
	// particular, never bypass TLS or an opaque connection wrapper.
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	// Linux may cap and double this request. Read the actual kernel accounting
	// below instead of assuming this amount was granted or preallocated.
	_ = tcp.SetWriteBuffer(2 << 20)
	raw, err := tcp.SyscallConn()
	if err != nil {
		return nil
	}
	port := &linuxSocketOutput{raw: raw}
	if _, err := port.budget(); err != nil {
		return nil
	}
	return port
}

func (p *linuxSocketOutput) budget() (socketOutputBudget, error) {
	var info [unix.SK_MEMINFO_VARS]uint32
	size := uint32(unsafe.Sizeof(info))
	var queryErr error
	err := p.raw.Control(func(fd uintptr) {
		_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, fd, unix.SOL_SOCKET, unix.SO_MEMINFO, uintptr(unsafe.Pointer(&info[0])), uintptr(unsafe.Pointer(&size)), 0)
		if errno != 0 {
			queryErr = errno
		}
	})
	if err != nil {
		return socketOutputBudget{}, err
	}
	if queryErr != nil {
		return socketOutputBudget{}, queryErr
	}
	if size < uint32((unix.SK_MEMINFO_WMEM_QUEUED+1)*4) {
		return socketOutputBudget{}, unix.EOPNOTSUPP
	}
	return socketOutputBudget{capacity: int64(info[unix.SK_MEMINFO_SNDBUF]), queued: int64(info[unix.SK_MEMINFO_WMEM_QUEUED])}, nil
}

func (p *linuxSocketOutput) tryWrite(data []byte) (int, error) {
	n := 0
	var sendErr error
	// Control never enters Go's writable-poll wait. MSG_DONTWAIT makes this
	// property explicit even if another component changes descriptor flags.
	err := p.raw.Control(func(fd uintptr) {
		n, sendErr = unix.SendmsgN(int(fd), data, nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
	})
	if err != nil {
		return 0, err
	}
	if sendErr == unix.EAGAIN || sendErr == unix.EWOULDBLOCK || sendErr == unix.EINTR {
		return max(n, 0), nil
	}
	return max(n, 0), sendErr
}

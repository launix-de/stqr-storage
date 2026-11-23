// SPDX-License-Identifier: GPL-3.0-only
package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/launix-de/stqr-storage/client"
)

const nbdOptionMagic uint64 = 0x49484156454f5054
const nbdReplyMagic uint64 = 0x3e889045565a9

func send(w io.Writer, values ...any) error {
	for _, v := range values {
		if err := binary.Write(w, binary.BigEndian, v); err != nil {
			return err
		}
	}
	return nil
}
func optionReply(c net.Conn, opt, kind uint32, body []byte) error {
	if err := send(c, nbdReplyMagic, opt, kind, uint32(len(body))); err != nil {
		return err
	}
	_, err := c.Write(body)
	return err
}
func nbdNegotiate(c net.Conn, size uint64, readonly bool) error {
	flags := uint16(1 | 4 | 8)
	if readonly {
		flags |= 2
	}
	if err := send(c, uint64(0x4e42444d41474943), nbdOptionMagic, uint16(3)); err != nil {
		return err
	}
	var cf uint32
	if err := binary.Read(c, binary.BigEndian, &cf); err != nil {
		return err
	}
	if cf&1 == 0 || cf&^uint32(3) != 0 {
		return errors.New("unsupported client flags")
	}
	for {
		var magic uint64
		var opt, n uint32
		if err := binary.Read(c, binary.BigEndian, &magic); err != nil {
			return err
		}
		if err := binary.Read(c, binary.BigEndian, &opt); err != nil {
			return err
		}
		if err := binary.Read(c, binary.BigEndian, &n); err != nil {
			return err
		}
		if magic != nbdOptionMagic || n > 65536 {
			return errors.New("invalid NBD option")
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c, body); err != nil {
			return err
		}
		switch opt {
		case 1:
			if len(body) != 0 && string(body) != "disk" {
				return errors.New("unknown export")
			}
			if err := send(c, size, flags); err != nil {
				return err
			}
			if cf&2 == 0 {
				_, err := c.Write(make([]byte, 124))
				return err
			}
			return nil
		case 2:
			optionReply(c, opt, 1, nil)
			return io.EOF
		case 6, 7:
			if len(body) < 6 {
				return errors.New("invalid INFO/GO")
			}
			nameLen := binary.BigEndian.Uint32(body)
			if nameLen > uint32(len(body)-6) {
				return errors.New("invalid export name")
			}
			name := string(body[4 : 4+nameLen])
			at := 4 + int(nameLen)
			count := int(binary.BigEndian.Uint16(body[at:]))
			if len(body) != at+2+count*2 {
				return errors.New("invalid info list")
			}
			if name != "" && name != "disk" {
				if err := optionReply(c, opt, 0x80000006, nil); err != nil {
					return err
				}
				continue
			}
			var export [12]byte
			binary.BigEndian.PutUint16(export[:], 0)
			binary.BigEndian.PutUint64(export[2:], size)
			binary.BigEndian.PutUint16(export[10:], flags)
			if err := optionReply(c, opt, 3, export[:]); err != nil {
				return err
			}
			var blocks [14]byte
			binary.BigEndian.PutUint16(blocks[:], 3)
			binary.BigEndian.PutUint32(blocks[2:], 512)
			binary.BigEndian.PutUint32(blocks[6:], 4096)
			binary.BigEndian.PutUint32(blocks[10:], 1<<20)
			if err := optionReply(c, opt, 3, blocks[:]); err != nil {
				return err
			}
			if err := optionReply(c, opt, 1, nil); err != nil {
				return err
			}
			if opt == 7 {
				return nil
			}
		default:
			if err := optionReply(c, opt, 0x80000001, nil); err != nil {
				return err
			}
		}
	}
}
func nbdSession(ctx context.Context, c net.Conn, fs *client.Filesystem, disk client.FSInfo, readonly bool) error {
	if err := nbdNegotiate(c, disk.Size, readonly); err != nil {
		return err
	}
	c.SetDeadline(time.Time{})
	for {
		var req [28]byte
		if _, err := io.ReadFull(c, req[:]); err != nil {
			return err
		}
		if binary.BigEndian.Uint32(req[:]) != 0x25609513 {
			return errors.New("bad NBD request magic")
		}
		flags := binary.BigEndian.Uint16(req[4:])
		cmd := binary.BigEndian.Uint16(req[6:])
		handle := binary.BigEndian.Uint64(req[8:])
		off := binary.BigEndian.Uint64(req[16:])
		length := binary.BigEndian.Uint32(req[24:])
		if length > 32<<20 {
			return errors.New("NBD request exceeds negotiated maximum")
		}
		payload := make([]byte, length)
		if cmd == 1 {
			if _, err := io.ReadFull(c, payload); err != nil {
				return err
			}
		}
		if cmd == 2 {
			return fs.Sync(ctx, disk.ID)
		}
		code := uint32(0)
		var err error
		if flags&^uint16(1) != 0 || (flags != 0 && cmd != 1) {
			code = 22
		}
		if cmd == 0 || cmd == 1 {
			if off > disk.Size || uint64(length) > disk.Size-off || off%512 != 0 || length%512 != 0 {
				code = 22
			}
		}
		if code == 0 {
			switch cmd {
			case 0:
				var n int
				n, err = fs.ReadAt(ctx, disk.ID, payload, int64(off))
				if err == nil && n != int(length) {
					err = io.ErrUnexpectedEOF
				}
			case 1:
				if readonly {
					code = 30
				} else {
					var n int
					n, err = fs.WriteAt(ctx, disk.ID, payload, int64(off))
					if err == nil && n != int(length) {
						err = io.ErrShortWrite
					}
					if err == nil && flags&1 != 0 {
						err = fs.Sync(ctx, disk.ID)
					}
				}
			case 3:
				if off != 0 || length != 0 {
					code = 22
				} else {
					err = fs.Sync(ctx, disk.ID)
				}
			default:
				code = 22
			}
		}
		if err != nil {
			code = 5
			if errors.Is(err, client.ErrFull) {
				code = 28
			}
		}
		if err := send(c, uint32(0x67446698), code, handle); err != nil {
			return err
		}
		if code == 0 && cmd == 0 {
			if _, err := c.Write(payload); err != nil {
				return err
			}
		}
	}
}
func serveNBD(ctx context.Context, path string, fs *client.Filesystem, disk client.FSInfo, readonly bool) error {
	if disk.Size == 0 || disk.Size%512 != 0 || disk.Size > uint64(^uint64(0)>>1) {
		return errors.New("invalid disk size")
	}
	// Never remove a pre-existing socket: it may belong to another live writer.
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer l.Close()
	defer os.Remove(path)
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { <-ctx.Done(); l.Close() }()
	for {
		c, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return fs.Sync(context.Background(), disk.ID)
			}
			return err
		}
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				c.Close()
			case <-done:
			}
		}()
		c.SetDeadline(time.Now().Add(30 * time.Second))
		err = nbdSession(ctx, c, fs, disk, readonly)
		close(done)
		c.Close()
		if err != nil && !errors.Is(err, io.EOF) && ctx.Err() == nil {
			fmt.Fprintln(os.Stderr, "NBD session:", err)
		}
	}
}

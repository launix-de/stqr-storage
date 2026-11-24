// SPDX-License-Identifier: GPL-3.0-only
package nbd

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
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
	if len(body) == 0 {
		return nil
	}
	_, err := c.Write(body)
	return err
}
func nbdNegotiate(ctx context.Context, c net.Conn, catalog *Catalog, opt Options) (Metadata, func(), error) {
	flags := uint16(1 | 4 | 8)
	if opt.ReadOnly {
		flags |= 2
	} else {
		flags |= 32 // NBD_FLAG_SEND_TRIM
	}
	if err := send(c, uint64(0x4e42444d41474943), nbdOptionMagic, uint16(3)); err != nil {
		return Metadata{}, nil, err
	}
	var cf uint32
	if err := binary.Read(c, binary.BigEndian, &cf); err != nil {
		return Metadata{}, nil, err
	}
	if cf&1 == 0 || cf&^uint32(3) != 0 {
		return Metadata{}, nil, errors.New("unsupported client flags")
	}
	lookup := func(name string) (Metadata, error) {
		if name == "" {
			name = opt.DefaultExport
		}
		if name == "" {
			return Metadata{}, client.ErrNotFound
		}
		return catalog.Lookup(ctx, name)
	}
	for {
		var magic uint64
		var opcode, n uint32
		if err := binary.Read(c, binary.BigEndian, &magic); err != nil {
			return Metadata{}, nil, err
		}
		if err := binary.Read(c, binary.BigEndian, &opcode); err != nil {
			return Metadata{}, nil, err
		}
		if err := binary.Read(c, binary.BigEndian, &n); err != nil {
			return Metadata{}, nil, err
		}
		if magic != nbdOptionMagic || n > 65536 {
			return Metadata{}, nil, errors.New("invalid NBD option")
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(c, body); err != nil {
			return Metadata{}, nil, err
		}
		switch opcode {
		case 1:
			m, err := lookup(string(body))
			if err != nil {
				return m, nil, err
			}
			release, err := catalog.claim(m.ID, opt.ReadOnly)
			if err != nil {
				return m, nil, err
			}
			err = send(c, m.Size, flags)
			if err == nil && cf&2 == 0 {
				_, err = c.Write(make([]byte, 124))
			}
			return m, release, err
		case 2:
			optionReply(c, opcode, 1, nil)
			return Metadata{}, nil, io.EOF
		case 3:
			if len(body) != 0 {
				return Metadata{}, nil, client.ErrProtocol
			}
			devices, err := catalog.List(ctx)
			if err != nil {
				return Metadata{}, nil, err
			}
			for _, m := range devices {
				entry := binary.BigEndian.AppendUint32(nil, uint32(len(m.Name)))
				entry = append(entry, []byte(m.Name)...)
				entry = append(entry, []byte(" "+m.Manufacturer+" "+m.Model)...)
				if err = optionReply(c, opcode, 2, entry); err != nil {
					return Metadata{}, nil, err
				}
			}
			if err = optionReply(c, opcode, 1, nil); err != nil {
				return Metadata{}, nil, err
			}
		case 6, 7:
			if len(body) < 6 {
				return Metadata{}, nil, client.ErrProtocol
			}
			nameLen := binary.BigEndian.Uint32(body)
			if nameLen > uint32(len(body)-6) {
				return Metadata{}, nil, client.ErrProtocol
			}
			at := 4 + int(nameLen)
			count := int(binary.BigEndian.Uint16(body[at:]))
			if len(body) != at+2+2*count {
				return Metadata{}, nil, client.ErrProtocol
			}
			m, err := lookup(string(body[4:at]))
			if err != nil {
				if !errors.Is(err, client.ErrNotFound) {
					return m, nil, err
				}
				if err = optionReply(c, opcode, 0x80000006, nil); err != nil {
					return m, nil, err
				}
				continue
			}
			var release func()
			if opcode == 7 {
				release, err = catalog.claim(m.ID, opt.ReadOnly)
				if err != nil {
					if err = optionReply(c, opcode, 0x80000002, nil); err != nil {
						return m, nil, err
					}
					continue
				}
			}
			fail := func(e error) (Metadata, func(), error) { return m, release, e }
			export := binary.BigEndian.AppendUint16(nil, 0)
			export = binary.BigEndian.AppendUint64(export, m.Size)
			export = binary.BigEndian.AppendUint16(export, flags)
			if err = optionReply(c, opcode, 3, export); err != nil {
				return fail(err)
			}
			for _, item := range []struct {
				kind uint16
				text string
			}{{1, m.Name}, {2, m.Manufacturer + " " + m.Model + " " + m.Serial}} {
				data := binary.BigEndian.AppendUint16(nil, item.kind)
				data = append(data, []byte(item.text)...)
				if err = optionReply(c, opcode, 3, data); err != nil {
					return fail(err)
				}
			}
			blocks := binary.BigEndian.AppendUint16(nil, 3)
			for _, v := range []uint32{512, 4096, 1 << 20} {
				blocks = binary.BigEndian.AppendUint32(blocks, v)
			}
			if err = optionReply(c, opcode, 3, blocks); err != nil {
				return fail(err)
			}
			if err = optionReply(c, opcode, 1, nil); err != nil {
				return fail(err)
			}
			if opcode == 7 {
				return m, release, nil
			}
		default:
			if err := optionReply(c, opcode, 0x80000001, nil); err != nil {
				return Metadata{}, nil, err
			}
		}
	}
}
func nbdSession(ctx context.Context, c net.Conn, catalog *Catalog, opt Options) error {
	disk, release, err := nbdNegotiate(ctx, c, catalog, opt)
	if release != nil {
		defer release()
	}
	if err != nil {
		return err
	}
	readonly := opt.ReadOnly
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
		if (cmd == 0 || cmd == 1) && length > 32<<20 {
			return errors.New("NBD request exceeds negotiated maximum")
		}
		var payload []byte
		if cmd == 0 || cmd == 1 {
			payload = make([]byte, length)
		}
		if cmd == 1 {
			if _, err := io.ReadFull(c, payload); err != nil {
				return err
			}
		}
		if cmd == 2 {
			return catalog.Sync(ctx)
		}
		code := uint32(0)
		var err error
		if flags&^uint16(1) != 0 {
			code = 22
		}
		if cmd == 0 || cmd == 1 || cmd == 4 {
			if off > disk.Size || uint64(length) > disk.Size-off || off%512 != 0 || length%512 != 0 {
				code = 22
			}
			if cmd == 4 && length == 0 {
				code = 22
			}
		}
		if code == 0 {
			switch cmd {
			case 0:
				err = catalog.ReadAt(ctx, disk, payload, off)
			case 1:
				if readonly {
					code = 30
				} else {
					err = catalog.WriteAt(ctx, disk, payload, off)
					if err == nil && flags&1 != 0 {
						err = catalog.Sync(ctx)
					}
				}
			case 3:
				if off != 0 || length != 0 {
					code = 22
				} else {
					err = catalog.Sync(ctx)
				}
			case 4: // NBD_CMD_TRIM has no payload.
				if readonly {
					code = 30
				} else {
					err = catalog.Trim(ctx, disk, off, uint64(length))
					if err == nil && flags&1 != 0 {
						err = catalog.Sync(ctx)
					}
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

// Options controls the Unix NBD endpoint. An empty export name is accepted only
// when DefaultExport is configured; named exports always select their own device.
type Options struct {
	DefaultExport  string
	ReadOnly       bool
	MaxConnections int
}

func Serve(ctx context.Context, path string, catalog *Catalog, opt Options) error {
	if opt.MaxConnections <= 0 {
		opt.MaxConnections = 64
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer l.Close()
	defer os.Remove(path)
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	closeControl, err := serveResizeControl(path+".control", catalog, opt.ReadOnly)
	if err != nil {
		return err
	}
	defer closeControl()
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { <-ctx.Done(); l.Close() }()
	var workers sync.WaitGroup
	slots := make(chan struct{}, opt.MaxConnections)
	for {
		c, err := l.Accept()
		if err != nil {
			canceled := ctx.Err() != nil
			stop()
			workers.Wait()
			if canceled {
				return catalog.Sync(context.Background())
			}
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			c.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			defer c.Close()
			done := make(chan struct{})
			defer close(done)
			go func() {
				select {
				case <-ctx.Done():
					c.Close()
				case <-done:
				}
			}()
			c.SetDeadline(time.Now().Add(30 * time.Second))
			if err := nbdSession(ctx, c, catalog, opt); err != nil && !errors.Is(err, io.EOF) && ctx.Err() == nil {
				fmt.Fprintln(os.Stderr, "NBD session:", err)
			}
		}()
	}
}

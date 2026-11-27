// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"crypto/sha256"
	"io"
)

// RestorePatch is one changed logical interval of a fenced migration shadow.
// Intervals must be disjoint and sorted. The complete source contents is published
// atomically with all patches; intermediate objects are never visible.
type RestorePatch struct {
	Offset uint64
	Data   []byte
}

// RestoreDelta patches an existing non-authoritative raw snapshot in place. It
// requires the exact previously imported bytes and unchanged logical length.
// Ordinary client mutations must use PUT_RANGE rather than this trusted import operation.
func (h *Handle) RestoreDelta(k Key, base [32]byte, info Info, patches []RestorePatch, opts Options) error {
	return h.doAcceptedOptions(opts, func(v *Volume) error {
		if opts.RequestID != (ID{}) || opts.Compression != nil && *opts.Compression != Raw {
			return ErrRange
		}
		old, err := v.loadObject(k)
		if err != nil {
			return err
		}
		if err = v.precondition(old, opts); err != nil {
			return err
		}
		if !old.importShadow || !opts.ImportShadow || info.Deleted || old.Info.Deleted || info.Length != old.Info.Length || info.Compression != Raw || old.Info.Compression != Raw {
			return ErrPrecondition
		}
		hash := sha256.New()
		var preimage bytes.Buffer
		var dst io.Writer = hash
		// Optional delta manifests are bounded to 64 MiB. Reuse these verified bytes
		// when updating checksums, rather than reading each checksum block twice.
		if old.Info.Length <= 64<<20 {
			dst = io.MultiWriter(hash, &preimage)
		}
		if _, e := v.copyToLocked(k, 0, old.Info.Length, dst); e != nil {
			return e
		}
		if !bytes.Equal(hash.Sum(nil), base[:]) {
			return ErrPrecondition
		}
		if busy, e := v.pending(k); e != nil {
			return e
		} else if busy {
			return ErrBusy
		}
		var end uint64
		for _, p := range patches {
			if len(p.Data) == 0 || p.Offset < end || p.Offset > info.Length || uint64(len(p.Data)) > info.Length-p.Offset {
				return ErrRange
			}
			end = p.Offset + uint64(len(p.Data))
		}
		t := v.transaction()
		defer t.close()
		o := cloneObject(old)
		dense := true
		var covered uint64
		for _, span := range o.Spans {
			if span.Offset != covered || span.Direct || span.Codec != Raw {
				dense = false
			}
			covered = span.Offset + span.Length
		}
		dense = dense && covered == info.Length
		if dense {
			for i := range o.Spans {
				if err = t.patchRawBatch(&o.Spans[i], patches, preimage.Bytes()); err != nil {
					return err
				}
			}
		} else {
			for _, p := range patches {
				if err = t.writeRange(o, p.Offset, uint64(len(p.Data)), bytes.NewReader(p.Data), Raw); err != nil {
					return err
				}
			}
		}
		o.Info = info
		if err = t.saveObject(k, o); err != nil {
			return err
		}
		if err = t.commitReserved(opts.UseGuaranteed, opts.InFlightAllowance, opts.GuaranteedAllowance); err != nil {
			return err
		}
		v.cache.remove(k)
		return nil
	})
}

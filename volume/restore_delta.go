// SPDX-License-Identifier: GPL-3.0-only
package volume

import "bytes"

// RestorePatch is one changed logical interval of a fenced migration shadow.
// Intervals must be disjoint and sorted. The complete source version is published
// atomically with all patches; intermediate object versions are never visible.
type RestorePatch struct {
	Offset uint64
	Data   []byte
}

// RestoreDelta patches an existing non-authoritative raw snapshot in place. It
// requires the exact previously imported version and unchanged logical length.
// Ordinary client mutations must use PUT_RANGE rather than supplying versions.
func (h *Handle) RestoreDelta(k Key, base uint64, info Info, patches []RestorePatch, opts Options) error {
	return h.do(true, func(v *Volume) error {
		if opts.RequestID != (ID{}) || opts.Compression != nil && *opts.Compression != Raw {
			return ErrRange
		}
		old, err := v.loadObject(k)
		if err != nil {
			return err
		}
		if err = precondition(old, opts); err != nil {
			return err
		}
		if !old.importShadow || !opts.ImportShadow || old.Info.Version != base || info.Version <= base || info.Deleted || old.Info.Deleted || info.Length != old.Info.Length || info.Compression != Raw || old.Info.Compression != Raw {
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
				if err = t.patchRawBatch(&o.Spans[i], patches); err != nil {
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
		v.cache.put(k, o)
		return nil
	})
}

// SPDX-License-Identifier: GPL-3.0-only
package volume

import (
	"bytes"
	"testing"
)

// Models a reference directory whose complete serialized contents grow a little
// per holder, followed by repeated add/remove bursts. It measures storage work,
// not network or an end-to-end filesystem/VM boot.
func BenchmarkWholeReferenceRewrite(b *testing.B) {
	d, err := NewMemory(16 << 20)
	if err != nil {
		b.Fatal(err)
	}
	v, err := Format(d, FormatOptions{Class: RAM})
	if err != nil {
		b.Fatal(err)
	}
	defer v.Close()
	k := testKey(1)
	raw := bytes.Repeat([]byte("r"), 16<<10)
	apply := func(next []byte) {
		h, err := v.Open(true)
		if err != nil {
			b.Fatal(err)
		}
		_, err = h.CAS(k, 0, raw, next, Options{Accepted: true, WholeObject: true})
		h.Close()
		if err != nil {
			b.Fatal(err)
		}
		raw = next
	}
	h, err := v.Open(true)
	if err != nil {
		b.Fatal(err)
	}
	_, err = h.Put(k, raw, Options{Accepted: true})
	h.Close()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 250; i++ {
		apply(append(bytes.Clone(raw), bytes.Repeat([]byte("x"), 81)...))
	}
	base := len(raw)
	b.SetBytes(int64(base))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next := bytes.Clone(raw)
		if len(next) == base {
			next = append(next, bytes.Repeat([]byte("n"), 81)...)
		} else {
			next = next[:base]
		}
		apply(next)
	}
	b.StopTimer()
	o, err := v.loadObject(k)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(len(o.Spans)), "spans/object")
}

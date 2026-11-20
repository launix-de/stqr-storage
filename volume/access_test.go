// SPDX-License-Identifier: GPL-3.0-only
package volume

import "io"

// Single-operation adapters for storage/fault tests. Every call explicitly opens
// and closes the public Handle API; these are not part of the library API.

func testReadAt(v *Volume, k Key, dst []byte, offset uint64) (int, error) {
	h, e := v.Open(false)
	if e != nil {
		return 0, e
	}
	defer h.Close()
	return h.ReadAt(k, dst, offset)
}

func testGetRange(v *Volume, k Key, offset, length uint64) ([]byte, Info, error) {
	h, e := v.Open(false)
	if e != nil {
		return nil, Info{}, e
	}
	defer h.Close()
	return h.GetRange(k, offset, length)
}

func testGet(v *Volume, k Key) ([]byte, Info, error) {
	h, e := v.Open(false)
	if e != nil {
		return nil, Info{}, e
	}
	defer h.Close()
	return h.Get(k)
}

func testExtents(v *Volume, k Key) ([]Extent, error) {
	h, e := v.Open(false)
	if e != nil {
		return nil, e
	}
	defer h.Close()
	return h.Extents(k)
}

func testPut(v *Volume, k Key, data []byte, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.Put(k, data, opts)
}

func testPutReader(v *Volume, k Key, data io.Reader, length uint64, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.PutReader(k, data, length, opts)
}

func testPutRange(v *Volume, k Key, offset uint64, data []byte, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.PutRange(k, offset, data, opts)
}

func testPutRangeReader(v *Volume, k Key, offset uint64, data io.Reader, length uint64, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.PutRangeReader(k, offset, data, length, opts)
}

func testAppend(v *Volume, k Key, data []byte, opts Options) (Result, error) {
	h, e := v.Open(true)
	if e != nil {
		return Result{}, e
	}
	defer h.Close()
	return h.Append(k, data, opts)
}

func testAppendReader(v *Volume, k Key, data io.Reader, length uint64, opts Options) (Result, error) {
	h, e := v.Open(true)
	if e != nil {
		return Result{}, e
	}
	defer h.Close()
	return h.AppendReader(k, data, length, opts)
}

func testTrunc(v *Volume, k Key, length uint64, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.Trunc(k, length, opts)
}

func testDelete(v *Volume, k Key, opts Options) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.Delete(k, opts)
}

func testCAS(v *Volume, k Key, offset uint64, expected, replacement []byte, opts Options) (Info, error) {
	h, e := v.Open(true)
	if e != nil {
		return Info{}, e
	}
	defer h.Close()
	return h.CAS(k, offset, expected, replacement, opts)
}

func testCopyTo(v *Volume, k Key, offset, length uint64, dst io.Writer) (uint64, error) {
	h, e := v.Open(false)
	if e != nil {
		return 0, e
	}
	defer h.Close()
	return h.CopyTo(k, offset, length, dst)
}

func testOperationStatus(v *Volume, id ID) (Result, bool, error) {
	h, e := v.Open(false)
	if e != nil {
		return Result{}, false, e
	}
	defer h.Close()
	return h.OperationStatus(id)
}

func testForgetRequest(v *Volume, id ID) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.ForgetRequest(id)
}

func testScan(v *Volume, bucket ID, start, end ObjectID, fn func(Key, Info) error) error {
	h, e := v.Open(false)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.Scan(bucket, start, end, fn)
}

func testRecompress(v *Volume, k Key, codec Compression) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.Recompress(k, codec)
}

func testRestore(v *Volume, k Key, info Info, source ExtentSource, opts Options) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.Restore(k, info, source, opts)
}

func testRestoreRequest(v *Volume, x RequestRecord) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.RestoreRequest(x)
}

func testScanRequests(v *Volume, fn func(RequestRecord) error) error {
	h, e := v.Open(false)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.ScanRequests(fn)
}

func testRepair(v *Volume, k Key, info Info, source ExtentSource, opts Options) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.Repair(k, info, source, opts)
}

func testSyncThrough(v *Volume, k Key, version uint64) error {
	h, e := v.Open(true)
	if e != nil {
		return e
	}
	defer h.Close()
	return h.SyncThrough(k, version)
}

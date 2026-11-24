# STQR drives on Windows

`stqr-windows` mounts a normal STQR filesystem bucket as a Windows drive using
[WinFsp](https://winfsp.dev/). It speaks the native STQR protocol directly to the
nodes and uses the same STFS inode format as Linux FUSE and `kstqr`.

Install WinFsp on the Windows machine, then run:

```powershell
.\stqr-windows.exe --uri http://storage:7419 --username alice --bucket documents S:
```

The password is requested in the console; it is never a command-line argument.
For automation, `--password-stdin` reads one bounded line from redirected input.
Use `--readonly` for a readonly drive, or `--init` to initialize an empty bucket.
An existing filesystem opens without `--init`. Do not initialize a bucket holding
S3 objects, NBD devices or other application data. `-h` lists the options.
Ctrl+C unmounts the drive and synchronizes pending writes.

File and directory operations, ranged reads/writes, truncation, rename,
symlinks, timestamps, permissions and live capacity reporting are implemented.
New inodes inherit the filesystem root's Unix UID/GID unless `--uid` or `--gid`
overrides them. WinFsp presents the drive to the mounting Windows user; this is
not a Unix-to-Windows identity directory or a full Windows ACL implementation.

The namespace is case-sensitive. Files created on Linux with names that Windows
applications cannot represent may remain inaccessible to those applications.
Native overlays, alternate data streams, Windows security descriptors and xattr
callbacks are not implemented. WinFsp itself does not support Windows hardlink
creation; the adapter returns an explicit unsupported error for that operation.
Existing Linux hardlinks share their inode and content when read from Windows.

The client uses a bounded subscription cache. WinFsp file and directory cache
timeouts are explicitly zero to avoid a second cache hiding remote changes.
Distributed application locks and writable memory-map coherence are not promised.
As with the Linux clients, physical collection of unlinked objects is an offline
operation: disconnect the drive and use the Linux client's `--fsck`.

## Build and verification

From this directory, `make` cross-compiles an amd64 executable into
`bin/stqr-windows.exe`; no C compiler is required. The WinFsp runtime is required
on the Windows machine, not on the build host. The adapter uses cgofuse's Windows
binding with `CGO_ENABLED=0`.

`make all check` tests the portable filesystem layer and builds Windows callback
tests. These cover open handles across rename/unlink, sparse and segmented I/O,
concurrent append, readonly rejection and metadata callbacks. Callback tests can
also run under Wine. A drive mount and application compatibility still require
testing on real Windows with WinFsp; Wine callback tests do not load its driver.

For incremental backups with hardlinks, use Linux FUSE or `kstqr` and
[`rsync --link-dest`](../docs/filesystem-posix.md#incremental-backups).

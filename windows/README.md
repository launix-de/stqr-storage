# STQR drives on Windows

`stqr-windows` mounts a normal STQR filesystem bucket as a Windows drive using
[WinFsp](https://winfsp.dev/). It speaks the native STQR protocol directly to the
nodes and uses the same STFS inode format as Linux FUSE and `kstqr`.

Install WinFsp on the Windows machine, then double-click `stqr-windows.exe`.
Enter the server URI, STQR username/password, bucket and drive letter. Select
**“Dauerhaft einbinden und Passwort merken”** to install the client for your
Windows account and reconnect the drive automatically at **user login**. No
administrator rights are needed for this per-user setup. This is not a system
service or a drive available before anyone has logged in.

The setup copies the executable into `%LOCALAPPDATA%\STQR` using a versioned
filename, registers one `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`
entry per drive, and stores an encrypted profile under `HKCU\Software\STQR\Mounts`.
The profile, including the password, is protected with **Windows DPAPI in user
scope**. No plaintext password is written into files, startup arguments or the
Run entry. The original downloaded executable can be moved after setup; startup
uses the installed copy. An unavailable server is retried in the background.
Startup failures are logged in `%LOCALAPPDATA%\STQR\mount-S.log` (one per drive).

Without that checkbox, the drive is mounted for the current run. The dialog can
initialize an empty bucket; it rejects buckets containing other application data.
Readonly mounts never initialize a filesystem. The bucket and user grants must
already exist on the server.

Open the EXE again and select **“Trennen und Autostart entfernen”** to unmount
and remove the startup entry and saved credentials for the selected drive.
Installed executable versions are kept so removing one drive cannot break
another configured drive. Use `--setup --saved-drive T:` to edit another saved
profile. Removing a profile never deletes its server-side bucket or data.

The command line remains available:

```powershell
# Temporary mount; password requested in the console:
.\stqr-windows.exe --uri http://storage:7419 --username alice --bucket documents S:
# Remember credentials and mount at login:
.\stqr-windows.exe --uri http://storage:7419 --username alice --bucket documents --install S:
# Remove startup/profile and disconnect:
.\stqr-windows.exe --uninstall S:
```

For automation, `--password-stdin` reads one bounded line from redirected input.
Use `--readonly` for a readonly drive, or `--init` to initialize an empty bucket
from the CLI. An existing filesystem opens without `--init`. `-h` lists options.
Ctrl+C unmounts a console mount and synchronizes pending writes. The stop action
and Windows close/logoff notifications request the same cleanup; abrupt process
termination or forced shutdown cannot guarantee a completed flush.

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
concurrent append, readonly rejection and metadata callbacks. The setup tests
cover DPAPI roundtrips and tamper rejection, executable copying, startup quoting,
profile removal and per-drive exclusion/stop events. These tests can run under
Wine; `STQR_TEST_WINDOWS_UI=1` also exercises the native checkbox dialog. A drive mount and application compatibility still require
testing on real Windows with WinFsp; Wine callback tests do not load its driver.

For incremental backups with hardlinks, use Linux FUSE or `kstqr` and
[`rsync --link-dest`](../docs/filesystem-posix.md#incremental-backups).

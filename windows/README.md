# STQR drives on Windows

`stqr-windows` mounts a normal STQR filesystem bucket or native lower/upper overlay as a Windows drive using
[WinFsp](https://winfsp.dev/). It speaks the native STQR protocol directly to the
nodes and uses the same STFS inode format as Linux FUSE and `kstqr`.

Install WinFsp on the Windows machine, download the single `stqr-windows.exe`, then double-click it. The native WinUI 3 frontend is self-contained: users do not
need to install .NET or the Windows App SDK separately.
The application runs without a console window. A WinFsp download button opens
the official runtime download when needed. Enter the server URI, STQR username/password, bucket and drive letter. Select
**“Bei der Windows-Anmeldung verbinden”** to install the client for your
Windows account and reconnect the drive automatically at **user login**. No
administrator rights are needed for this per-user setup. This is not a system
service or a drive available before anyone has logged in.

The setup copies the complete executable into `%LOCALAPPDATA%\STQR` using versioned
application directories, registers one `HKCU\Software\Microsoft\Windows\CurrentVersion\Run`
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

Open the EXE again and select a saved drive. **“Trennen”** unmounts it;
**“Verbindung entfernen”** also removes the startup entry and saved credentials
after confirmation.
Installed application versions are kept so removing one drive cannot break
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
The adapter uses the shared Go filesystem's mutable inode/index/data objects;
repeated overwrites keep their persistent identities. An open handle continues
to address its inode after rename or unlink, including reads and writes until
close. Closing a Windows handle releases only that process's handle; it cannot
assume other mounts have released the inode. On reference-capable backends the
adapter retains a cooperative filesystem reference before publishing an open
handle, and releases it only after that handle's pending operations finish.
Unlink and last-close cleanup use the shared Go filesystem's orphan/reference
protocol; an inode still retained by another handle or mount is preserved.
Crashes, backends without reference support, and cleanup without the required
write/delete grants retain discoverable orphans for a later eligible cleanup or
exclusive filesystem check. The adapter does not sweep unrelated bucket objects
or expose a separate raw PUT/CAS API.

New inodes inherit the filesystem root's Unix UID/GID unless `--uid` or `--gid`
overrides them. Within a setgid directory, new children inherit that directory's
GID and new directories also inherit its setgid bit, matching Linux. WinFsp presents the drive to the mounting Windows user; this is
not a Unix-to-Windows identity directory or a full Windows ACL implementation.

The namespace is case-sensitive. Files created on Linux with names that Windows
applications cannot represent may remain inaccessible to those applications.
Native overlays are available under **Erweiterte Optionen** or with
`--lower-bucket BASE --writable-paths "home/"`. The selected bucket is the upper;
the lower must remain immutable. Rules are persisted in the upper and must match
on later mounts. A readonly overlay never initializes, copies up or cleans up
objects. Both stores are subscribed for notifications. As on Linux, overlay
hardlink/special-node creation and xattr mutation remain unsupported.

Windows extended attributes map to shared `user.*` xattrs. The configured Unix
UID does not grant access to privileged `trusted.*` attributes, which remain
preserved. A zero-length native Windows EA value requests deletion rather than
storing an empty Linux attribute, following WinFsp/Windows EA semantics.
Alternate data streams and Windows security descriptors are not implemented. WinFsp itself does not support Windows hardlink
creation; the adapter returns an explicit unsupported error for that operation.
Existing Linux hardlinks share their inode and content when read from Windows.

The client uses a bounded subscription cache. WinFsp file and directory cache
timeouts are explicitly zero to avoid a second cache hiding remote changes.
Subscription events refresh visited directory snapshots and notify WinFsp about
remote creates, deletes and metadata/content changes, so Explorer can refresh
without reopening the drive. Directory snapshots are bounded to 256 visited
folders. Re-enumerating an existing directory handle also refreshes its view.
Distributed application locks and writable memory-map coherence are not promised.
As with the Linux clients, offline collection remains the fallback for abandoned
references and interrupted cleanup: disconnect all mounts and use the Linux
client's `--fsck`.

## Build and verification

The complete executable requires Windows x64, the .NET 10 SDK and Go:

```powershell
./windows/build.ps1
# Or, with GNU Make on Windows:
make windows-package
```

The build publishes one `windows/bin/stqr-windows.exe`, containing both the
Go/WinFsp client and its WinUI 3 interface (Windows App SDK 2.7). No second EXE
must be downloaded or kept beside it. On the first interactive start, the client
unpacks its included UI resource into a private, content-addressed runtime cache
under `%LOCALAPPDATA%\STQR\runtime`. The resource is verified by SHA256 and
reused on later starts. Background/CLI invocations do not unpack or show the UI.
Versioned installations copy only the complete public executable. WinUI requires
Windows 10 version 1809 or newer; supported deployments also require an OS still
covered by Microsoft support. The XAML compiler requires a Windows host.

On Linux, `make windows` cross-compiles **only a development backend**, named
`windows/bin/stqr-windows-backend.exe`. The distributable `stqr-windows.exe` must
come from the full Windows build, which includes the UI resource. No C compiler is required for the cgofuse Windows binding. The GUI launcher
reports a missing frontend explicitly. Configuration is passed through private
redirected process pipes, never through command-line passwords or plaintext
configuration files. Closing the frontend leaves a mounted drive running; reopen
it to disconnect. Per-user startup copies and versions the complete executable.

The application's manifest is a checked-in resource; `make windows-resources`
regenerates it with LLVM tools. CI publishes the complete executable, executes a real
WinUI startup/validation/layout check and captures its rendered interface.
See [Microsoft's unpackaged WinUI guidance](https://learn.microsoft.com/en-us/windows/apps/package-and-deploy/unpackage-winui-app)
for the self-contained deployment properties.

`make -C windows check` tests the portable filesystem layer and builds Windows callback
tests. These cover open handles across rename/unlink, sparse and segmented I/O,
concurrent append, readonly rejection and metadata callbacks. The setup tests
cover DPAPI roundtrips and tamper rejection, executable copying, startup quoting,
profile removal and per-drive exclusion/stop events. The backend tests can run under
Wine. The WinUI frontend requires real Windows; use `--smoke-test` with
`STQR_UI_SMOKE_DIR` pointing to an existing output directory. A drive mount and application compatibility still require
testing on real Windows with WinFsp; Wine callback tests do not load its driver.

For incremental backups with hardlinks, use Linux FUSE or `kstqr` and
[`rsync --link-dest`](../docs/filesystem-posix.md#incremental-backups).

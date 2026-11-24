# Network boot and bucket-backed virtual disks

These tools keep the STQR storage daemon separate from machine installation,
PXE services and login policy. They provide an image builder/editor, a standard
NBD endpoint for QEMU, a boot gateway and a PAM home-mount module.

The initial automated distribution adapter supports **Ubuntu/Debian x86-64 with
initramfs-tools**. Other distributions need a corresponding initramfs adapter;
the tools fail if the expected installer result/kernel/boot tooling is missing.
This is a development implementation, not a promise that every desktop or
Secure-Boot configuration is already supported.

## Build

Install Go 1.24+, Python 3.12+, QEMU (`qemu-system-x86_64`, `qemu-io`), libguestfs
(`guestfish`), dnsmasq, standalone iPXE BIOS/EFI loaders and GNU tar.
The host compiler/build tools and the guest's matching kernel headers build kstqr. PAM development headers and a C compiler are needed for the PAM
module. `ip` (iproute2) is used to detect the boot network. This directory is its
own Go module, with the parent STQR checkout as its SDK dependency.

```sh
make -C networkboot all pam
```

Run the tools from the directory containing your existing **`stqr.yaml`**.
They read the node's listen address and `auth.bootstrap_admin` credentials from
that private file, without modifying it. No second credentials file is needed.
An optional `--config PATH` selects another YAML or a private credentials JSON.
Builder operations and the default STQR-upper gateway require an administrator
and enough configured storage.
Build/run the updated STQR server too: older nodes do not have clear/clone.
Run `libguestfs-test-tool` if appliance startup fails. On hosts whose `/boot`
kernel is unreadable by the build user, set `SUPERMIN_KERNEL` to a readable copy
and `SUPERMIN_MODULES` to its matching module directory; no host permission
changes are performed by these tools.
No tool changes the host PAM stack, system dnsmasq, host exports or host mounts
as a side effect of building or displaying help.

## Install a template

```sh
networkboot/prepare-system ubuntu-desktop-v1 /images/ubuntu.iso
```

Defaults are a sparse 32-GiB disk, 4 GiB of VM RAM and two vCPUs; optional flags
can override them.

The command creates a new bucket and a sparse named `disk.raw` block device, starts
an exclusive local Unix-socket NBD export and opens the installer in QEMU.
The disk is already stored in STQR; there is no full local VM-disk copy.
Rootfs archives still need staging space in `/var/tmp` (override `--work-dir`).
SDK transfer spools use that work directory too. The tools remember a completed
export by bucket name and retain it after a failed publication. Retry with
`networkboot/prepare-system BUCKET`; no internal object ID or temporary path
is required. After successful publication, cleanup retries skip the import.
Preparation records live in the current user's XDG state directory.
Artifacts transfer in bounded 64-MiB ranges, so large Ubuntu images do not depend
on a whole-file protocol transfer or a large `/tmp` RAM quota. Interrupted artifact
uploads resume only after validating the stored prefix; downloads pin one object
version and verify the full checksum. Complete
the installation, then shut down the VM cleanly. The first installer reboot exits
QEMU so extraction can start. Closing an unfinished installation produces an
error rather than publishing a usable release; the disk remains in the bucket.
To retry an interrupted installation on that existing disk without deleting its
bucket or replacing its device metadata, use:

```sh
networkboot/prepare-system --resume ubuntu-desktop-v1 /images/ubuntu.iso
```

If installation completed but extraction/publication failed, retry that stage
without an installer or another VM boot (also after a failed `edit-image`
publication). A completed export is reused; otherwise the disk is extracted:

```sh
networkboot/prepare-system ubuntu-desktop-v1
```

Resume checks that the device has the requested size and that no release has
already been published. The installer can then repair or reformat the existing
virtual disk. Use `edit-image` for a published template.

The automated flow expects an unencrypted root disk; automatic LUKS unlocking
is not implemented. The default builder firmware is legacy BIOS. `--uefi FIRMWARE` can select a
monolithic OVMF firmware; firmware variables are not persisted in this version,
so the disk needs a working EFI fallback loader for subsequent edits.

After QEMU exits, libguestfs mounts the disk offline, installs the networkboot
initramfs hook, enables broad module inclusion and builds a separate `/boot/initrd.stqr-VERSION` using `mkinitramfs` from
`initramfs-tools-core`. This also handles Ubuntu installations whose
`update-initramfs` command delegates to Dracut: the ordinary VM-boot initrd stays
untouched. The builder exports the installed kernel headers, compiles kstqr with the host
toolchain, installs the module and one-shot mount helpers into the image, and
checks that both the module and STQR boot script are in the network initrd.
Install `linux-headers-VERSION` in the VM if its installer did not include them.
It selects a matching `/boot/vmlinuz-VERSION` and `initrd.img-VERSION` pair;
`--kernel-version` can select explicitly. It then extracts a numeric-owner tar
archive retaining modes, numeric owners, timestamps, xattrs (including file
capabilities), hardlinks, symlinks and FIFOs. The native importer rejects unsupported
POSIX ACLs and device nodes rather than silently dropping their metadata.
Runtime trees and `/home/*` are excluded: homes belong in their own buckets.
The root archive has networkboot fstab configuration; the VM disk's original
fstab is restored so the template can still boot as a normal VM.

The archive is imported into a **new native filesystem release bucket**. The
release bucket is then locked against writes. A schema-2 manifest atomically
publishes its bucket ID and checksummed `vmlinuz`, `initrd.img` and `release.json`
artifacts after import and synchronization succeed. The archive is local staging
only; boot gateways do not download or reconstruct the root tree. A failed
extraction or publication leaves the previous published release unchanged.
Changing the VM disk is deliberately independent from publishing it.

A boot bucket contains named striped block devices and opaque release artifacts.
**Do not run filesystem `--fsck` against a boot bucket**: it is not a native
FUSE filesystem. The builder/editor run the bundle-aware `stqr-bootctl gc BUCKET`
after successful publication. It preserves all device descriptors, their live
stripes, the manifest and current artifact roots, removing obsolete artifacts
and orphan stripes. Stripes are updated in place with ranged writes; no segment
list or immutable per-write stripe chain is stored. Keep physical headroom for
the installation disk and native release trees. Failed imports report their
release bucket ID for explicit cleanup; old release buckets remain available
while existing clients use them. GC of the builder bucket does not delete them.

## Change a template

```sh
# Keep v1 and edit a clone:
networkboot/edit-image ubuntu-desktop-v1 --to-bucket ubuntu-desktop-v2

# Or edit the same bucket:
networkboot/edit-image ubuntu-desktop-v2
```

Install packages or apply updates in the QEMU window, then shut down. Extraction
and atomic publication run again. Existing boot sessions keep their pinned native release bucket. Send SIGHUP to the
gateway to activate the newly published manifest for subsequent boots; versioned
HTTP URLs and immutable root bucket IDs keep already booted systems on their previous release.
A failed reload leaves the active release unchanged.
Use a new bucket for a release you want to retain or roll back to.

## QEMU block-device interface

```sh
networkboot/bin/stqr-bootctl --socket /private/path/disk.sock nbd ubuntu-desktop-v1

qemu-system-x86_64 \
  -blockdev '{"driver":"nbd","server":{"type":"unix","path":"/private/path/disk.sock"},"node-name":"stqr"}' \
  -device virtio-blk-pci,drive=stqr
```

Standard fixed-newstyle NBD is implemented: READ, WRITE, FLUSH, FUA, TRIM and clean
disconnect, with bounds checks and readonly exports. The image builder enables
QEMU `discard=unmap`, so guest `fstrim` can discard unused blocks. Whole 1-MiB
stripes are deleted; boundary ranges are zeroed with ranged writes. Write-zeroes,
structured replies and multi-connection exports are not advertised. QEMU does
not need recompilation; its existing QMP blockdev-add/device_add mechanisms can
hot-plug additional disks. Guest filesystems must be synced/unmounted before
hot removal. This frontend is a userspace block-device adapter, not a new host
kernel block module.

The exporter is implemented in [`qemu-plugin-nbd/`](../qemu-plugin-nbd/README.md).
Each device has one binary descriptor and systematically addressed 1-MiB stripes,
with small I/O dispatched directly through native ranged operations. There is no
stripe list or client payload cache. All named devices in one bucket are separate
NBD exports; the boot helper selects `disk.raw` by default (`--device` overrides).
The standalone `stqr-nbd` CLI supports creating/listing named devices and serving
them on one socket. The exporter uses explicit bucket durability barriers. It holds exclusive bucket maintenance;
other clients cannot open the disk behind the writer. No second writer is
permitted. A failed or lost exclusion session cannot reconnect and resume writes.

## Clear and clone

Server commands `BUCKET_CLEAR` (`0x2028`) and `BUCKET_CLONE` (`0x2029`) are now
available to administrators:

```sh
networkboot/bin/stqr-bootctl create copy
networkboot/bin/stqr-bootctl clone source copy
networkboot/bin/stqr-bootctl clear copy
```

Both buckets must be idle/active; a clone destination must be empty. Cloning
copies object IDs and payloads with bounded memory, not native versions,
permissions or quota policies. Destination policies/grants remain its own.
Clear preserves the bucket identity/configuration while removing all live
objects. Tombstones/receipts remain volume metadata. A persisted operation
receipt prevents replaying a completed clear against subsequently written data.
An interrupted operation is partial and must be inspected/restarted with a new
operation ID; a response is not a multi-object transaction.

NBD, GC, clear and clone use **durable exclusive leases across all admitted
cluster members**. All members must be reachable and upgraded; existing clients
of the affected bucket must disconnect. A read-only preflight checks member
support before the image builder allocates buckets. Native root mounts do not take
exclusive bucket-maintenance leases. Data I/O
continues directly to the committed object owners. Handoffs of a leased bucket
and ordinary membership changes wait until the lease is released; unrelated
bucket data operations continue normally.

Participants persist their fence and sequence high-water mark in the admin
volume. A network timeout never unlocks a live lease. Clean client close releases
it synchronously; loss of the original owner socket, or owner restart, triggers
recovery and release through all members. Passive participant restart preserves
the fence. Explicit externally fenced node exclusion uses the existing recovery
contract and retires leases from the preceding membership epoch.

## Boot gateway

```sh
sudo networkboot/serve ubuntu-desktop-v2
```

The gateway detects the interface on the IPv4 default route, its IPv4 address
and the matching client subnet. It prints the selected network before startup.
If the machine has an ambiguous network configuration, it asks for explicit
`--interface` / `--server` overrides rather than selecting an arbitrary LAN.
Loader paths, the HTTP port and upper quota (default 4 GiB)
already have defaults. No network settings need to be added to `stqr.yaml`.

For a local QEMU PXE test, the gateway can run without LAN DHCP/TFTP:

```sh
sudo networkboot/serve ubuntu-desktop-v2 --no-dhcp
# In a second terminal:
networkboot/test-boot
```

For an unprivileged HTTP-only gateway, also choose a private writable cache with
`--cache PATH`. It must belong to the gateway user and be inaccessible for writes
by other users.

The test launcher waits up to 15 minutes for a valid HTTP boot script before
opening QEMU. Gateway startup downloads and verifies only the kernel, initramfs and release metadata; the `PXE ready` line marks readiness.

QEMU supplies private DHCP/NAT and its iPXE ROM loads the HTTP boot script.
This tests the published release and network root without a guest hard disk or
changes to LAN DHCP. The default is 4 GiB RAM and two vCPUs;
`--server ADDRESS` selects another gateway. It does not exercise physical-client Proxy-DHCP or TFTP.

`--check` resolves the bucket and checks configuration/dependencies without
starting services. `--no-dhcp` uses an existing externally configured PXE setup.
The HTTP port defaults to 8088; dnsmasq provides Proxy-DHCP and TFTP while the
existing DHCP server continues assigning addresses. DNS is disabled (`port=0`).
Standalone loader files are required, not QEMU option ROMs. The example
`systemd/stqr-networkboot.service` runs the supervisor in the foreground and maps
`systemctl reload` to SIGHUP. Installation does not enable it or change host services.

```text
PXE -> iPXE -> HTTP kernel + initramfs (includes matching kstqr.ko)
                 -> temporary boot identity
                     -> one native kstqr overlay mount
                         lower: readonly STFS release bucket
                         upper: temporary STOR/STOM bucket
                     -> switch_root
login -> PAM -> persistent username_home bucket at /home/username
```

The kernel uses our native Go/FUSE-compatible STOR/STOM overlay format:
deletion markers live in directory records, metadata overrides preserve the
original content, and range writes retain references to unchanged lower data.
Overlay uppers cannot be mounted alone. There is no SquashFS, Linux OverlayFS,
NFS root or NBD root upper in this boot path. QEMU's builder disk still uses the
independent NBD frontend.

Each MAC and boot gets a temporary STQR user with **read-only permission on the
pinned lower** and read/write permission on its own upper. The kernel command line
contains a one-use, IP-scoped claim token, never the administrator password. A
one-shot initramfs helper claims boot credentials over HTTP, mounts the native overlay,
removes its private credential file and exits. The kernel retains reconnect
credentials. A separate control socket is held by the upper superblock until
unmount; no resident client helper is needed. Object I/O goes directly to STQR.

On disconnect the gateway revokes the lower grant, deletes the temporary user
(closing authenticated connections), then schedules deletion of the upper.
The cluster persists that intent and purges its data in batches; an extra
object-by-object clear is unnecessary. Unclaimed
sessions expire after an hour; `--max-sessions` defaults to 64. Cleanup failures
or gateway crashes leave password-free, private JSON recovery markers in
`CACHE/uppers`. To retry a recorded cleanup:

```sh
networkboot/bin/stqr-bootctl boot-session-delete USER_ID UPPER_ID LOWER_ID
```

Inspect that the recorded client is stopped before manual recovery. MAC addresses
are selectors, not authentication; claim tokens and boot credentials travel over
HTTP on the trusted boot LAN. Published system images must not contain client or
administrator secrets. Secure-Boot enrollment and cryptographic boot-client
attestation are separate deployment work.

Logout does not clear a running machine's root. Shutdown/reboot releases its
upper; personal homes remain persistent and independent. Machine ID, random seed
and SSH host keys are regenerated in the private upper. Use SIGHUP for release
updates: restarting the gateway deliberately revokes its active boot sessions.
Keep old native release buckets while clients still use them.

## PAM homes

The image builder installs boot support, but does not install/enable PAM homes
or provision Unix accounts automatically. Configure the following inside the
image before publishing it for STQR logins.

Build/install the matching `kstqr` module and `mount.kstqr` on login clients.
Create the STQR user and an existing `username_home` bucket with the user's
read/write grant. Also provide a normal local/NSS Unix account with consistent
UID/GID and `/home/username`; PAM does not create accounts or grant buckets.
The local underlying home must be empty and not a symlink. Homes are mounted
with `nosuid,nodev`: user-controlled storage metadata cannot grant local setuid
privileges or device access. `/home` and helper executable directories must be
root-owned and not writable by other users.

`make install` installs binaries/module but **does not activate PAM rules**.
`DESTDIR`, `PREFIX`, and `PAM_MODULE_DIR` can stage or adapt packaging. The default
PAM directory uses the compiler's Debian-style multiarch triplet; set it for a
distribution with a different module directory.

Install `pam/stqr-home.example.json` as root-owned mode 0600
`/etc/security/stqr-home.json`, set the URI/helper paths, and merge
`pam/pam.example` into the chosen login services. Keep distribution account rules
and arrange mounting before services that need the home. Password authentication
is required; SSH-key/passkey-only logins have no STQR password and fail closed.
The module has auth/setcred/open-session/close-session entrypoints.

The password travels to a fixed privileged helper on stdin, never argv or env.
The helper uses private temporary files in `/run/stqr-home` and removes them
after connection/mount setup. The kernel retains the credentials needed for
reconnect in memory. The module erases its copy after mounting or PAM cleanup.
No administrator password is embedded in the home configuration.

Session IDs are counted per UID under a locked runtime state file. Multiple
logins reuse one mount. The last close attempts a regular unmount; a busy home
is retained, with its state, rather than force/lazy unmounted. Background services
and abruptly killed PAM processes can leave a mounted/stale session requiring
administrative reconciliation; logind-based crash reconciliation and automatic
busy-unmount retry are not implemented yet. Mounting controls activation, not
encryption or protection from local root.

## Validation

```sh
make -C networkboot check
STQR_TEST_NETWORKBOOT=1 go test -race ./server \
  -run '^Test(BucketClearCloneOfflineAndReplay|NetworkbootNBDWithQEMU)$' -count=1
STQR_TEST_KERNEL=1 STQR_KERNEL_OVERLAY=1 STQR_KERNEL_IMAGE=/path/to/vmlinuz \
  go test ./server -run '^TestKSTQRVirtualMachine$' -count=1
```

Tests cover actual QEMU NBD read/write/flush, cloned disk readback, clear replay,
NBD bounds/readonly behavior, NFS RPC reads/write rejection, Proxy-DHCP config,
CLI help, and a PAM ABI harness with a test-only helper/password conversation.
The isolated kernel VM exercises native kstqr lower/upper mounts, copy-up,
whiteouts, chmod, inherited hardlinks, range copy-up, whitelist enforcement,
external-client invalidation, rename, remount, the one-shot credential claim and
control-socket lifetime through unmount. Fixtures use disposable servers/buckets;
no LAN DHCP or host PAM configuration is used. The older NFS fixture is retained
only as a legacy regression test. A full desktop login/PAM home lifecycle remains
a deployment acceptance test with the site's Unix accounts and grants.

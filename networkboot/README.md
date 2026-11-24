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

Install Go 1.24+, Python 3, QEMU (`qemu-system-x86_64`, `qemu-io`), libguestfs
(`guestfish`), dnsmasq, standalone iPXE BIOS/EFI loaders, GNU tar and
squashfs-tools. PAM development headers and a C compiler are needed for the PAM
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
Rootfs archives still need staging space in `/var/tmp` (override `--work-dir`). Complete
the installation, then shut down the VM cleanly. The first installer reboot exits
QEMU so extraction can start. Closing an unfinished installation produces an
error rather than publishing a usable release; the disk remains in the bucket.
To retry an interrupted installation on that existing disk without deleting its
bucket or replacing its device metadata, use:

```sh
networkboot/prepare-system --resume ubuntu-desktop-v1 /images/ubuntu.iso
```

Resume checks that the device has the requested size and that no release has
already been published. The installer can then repair or reformat the existing
virtual disk. Use `edit-image` for a published template.

The automated flow expects an unencrypted root disk; automatic LUKS unlocking
is not implemented. The default builder firmware is legacy BIOS. `--uefi FIRMWARE` can select a
monolithic OVMF firmware; firmware variables are not persisted in this version,
so the disk needs a working EFI fallback loader for subsequent edits.

After QEMU exits, libguestfs mounts the disk offline, installs the networkboot
initramfs hook, enables broad module inclusion and regenerates initramfs images.
It selects a matching `/boot/vmlinuz-VERSION` and `initrd.img-VERSION` pair;
`--kernel-version` can select explicitly. It then extracts a numeric-owner tar
archive retaining ACLs, xattrs, security labels, hardlinks and device metadata.
Runtime trees and `/home/*` are excluded: homes belong in their own buckets.
The root archive has networkboot fstab configuration; the VM disk's original
fstab is restored so the template can still boot as a normal VM.

A fixed manifest object atomically publishes checksummed `rootfs.tar`, `vmlinuz`,
`initrd.img` and `release.json` artifacts only after all uploads succeed. A failed
extraction or publication leaves the previous published release unchanged.
Changing the VM disk is deliberately independent from publishing it.

A boot bucket contains named striped block devices and opaque release artifacts.
**Do not run filesystem `--fsck` against a boot bucket**: it is not a native
FUSE filesystem. The builder/editor run the bundle-aware `stqr-bootctl gc BUCKET`
after successful publication. It preserves all device descriptors, their live
stripes, the manifest and current artifact roots, removing obsolete artifacts
and orphan stripes. Stripes are updated in place with ranged writes; no segment
list or immutable per-write stripe chain is stored. Keep physical headroom for
installation writes and rootfs archives.

## Change a template

```sh
# Keep v1 and edit a clone:
networkboot/edit-image ubuntu-desktop-v1 --to-bucket ubuntu-desktop-v2

# Or edit the same bucket:
networkboot/edit-image ubuntu-desktop-v2
```

Install packages or apply updates in the QEMU window, then shut down. Extraction
and atomic publication run again. Existing boot sessions keep their pinned local release. Send SIGHUP to the
gateway to activate the newly published manifest for subsequent boots; versioned
HTTP URLs and NFS handles keep already booted systems on their previous release.
A failed reload leaves the active release unchanged.
Use a new bucket for a release you want to retain or roll back to.

## QEMU block-device interface

```sh
networkboot/bin/stqr-bootctl --socket /private/path/disk.sock nbd ubuntu-desktop-v1

qemu-system-x86_64 \
  -blockdev '{"driver":"nbd","server":{"type":"unix","path":"/private/path/disk.sock"},"node-name":"stqr"}' \
  -device virtio-blk-pci,drive=stqr
```

Standard fixed-newstyle NBD is implemented: READ, WRITE, FLUSH, FUA and clean
disconnect, with bounds checks and readonly exports. Trim, write-zeroes,
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
support before the builder or STQR-upper gateway allocates buckets. Data I/O
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
Loader paths, ports, `--upper` (default `stqr`) and upper size (default 4 GiB)
already have defaults. No network settings need to be added to `stqr.yaml`.

`--check` resolves the bucket and checks configuration/dependencies without
starting services. `--no-dhcp` uses an existing externally configured PXE setup.
Ports default to HTTP 8088 and NFS/MOUNT TCP 2049; dnsmasq provides Proxy-DHCP
and TFTP while the existing DHCP server continues assigning addresses. DNS is
disabled (`port=0`). Standalone loader files are required, not QEMU option ROMs.
The example `systemd/stqr-networkboot.service` runs the supervisor in the
foreground and maps `systemctl reload` to SIGHUP. It is not enabled by installation.
The supervisor stops its own children on shutdown or failure; it never edits the
system dnsmasq configuration or starts a system-wide kernel NFS server.

Artifacts are checksum-verified and extracted into a root-owned local cache.
The gateway converts the root into a readonly SquashFS image, preserving Linux
metadata. Its userspace NFSv3 server exports that image to the configured client
CIDR. Boot clients use:

```text
PXE -> iPXE -> HTTP kernel + initramfs
                 -> NFS /release-digest/rootfs.squashfs
                     -> loop-mounted SquashFS lower
                         -> private ext4 upper on STQR/NBD -> switch_root
```

The default writable root is a **private temporary bucket per MAC and boot**.
The gateway allocates a sparse disk, starts a private NBD export and gives that
client an IP-scoped TCP endpoint. The initramfs connector attaches `/dev/nbd0`,
formats ext4 and uses it as the Linux overlay upper/work filesystem. The static
connector waits for kernel activation before returning; a minimal process remains
blocked in the kernel's NBD handler for the connection lifetime. No QEMU
recompilation or proprietary QEMU driver is needed.

When that NBD connection ends, the gateway checkpoints/stops the export, clears
and deletes the temporary bucket. Pending clients that never attach expire after
one hour. `--max-sessions` bounds concurrent allocations (default 64). The NBD
ports are dynamically allocated TCP ports on the gateway address. Cleanup failures and gateway crashes leave private recovery markers in
`CACHE/uppers`; inspect those before manually clearing/deleting their buckets.
MACs are selectors, not authentication; only the IP that requested a boot may
connect to that session port. This is intended for a trusted boot LAN.

`--upper ram` selects the simpler per-boot client tmpfs upper and does not need
exclusive bucket maintenance. STQR uppers and the image-builder writer require
all admitted members to be available for lease acquisition. A read-only preflight
checks support before allocating anything. Logout alone does not clear a running
system; shutdown/reboot disconnects its root upper. Personal home buckets remain
persistent and independent. Machine ID, random seed and SSH host keys are
regenerated in the private upper.

Published images are administrator-trusted and readable by the allowed boot
network; do not put client secrets into a shared system image. The gateway has
no per-client cryptographic boot authentication or Secure-Boot enrollment.
For provisioning, a MAC may select a boot configuration but is not a credential.

Old exported release images remain in the local cache. Their NFS handles derive
from stable release paths and a fixed namespace, so a gateway restart serving a
new release can still resolve the old cached release handles. Keep the cache
intact while clients use those releases. Prefer SIGHUP for updates: restarting the
gateway disconnects active temporary NBD uppers even though the readonly release
handles remain reconstructible. No automatic live-kernel update or
migration of persistent native STQR uppers is performed.

## PAM homes

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
STQR_TEST_NBD_UPPER=1 STQR_KERNEL_IMAGE=/path/to/vmlinuz STQR_KERNEL_VERSION=matching-release \
  python3 networkboot/tests/networkboot_vm.py
```

Tests cover actual QEMU NBD read/write/flush, cloned disk readback, clear replay,
NBD bounds/readonly behavior, NFS RPC reads/write rejection, Proxy-DHCP config,
CLI help, and a PAM ABI harness with a test-only helper/password conversation.
The isolated kernel VM boots from NFS/SquashFS and switches root with either a
RAM upper or a kernel-NBD/ext4 upper. The per-MAC session test separately exercises
the STQR-backed TCP export and automatic clear/delete on disconnect. No production bucket, LAN DHCP or host PAM configuration is used.
A full interactive Ubuntu installation and real desktop/PAM home lifecycle have
not been run automatically; those remain deployment acceptance tests.

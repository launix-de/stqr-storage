# Network boot with STQR

This directory contains the tools for installing a Linux template in QEMU,
finishing an interrupted installation, and booting diskless machines from STQR over
PXE. A separate gateway provides the boot services; the storage nodes continue
running the normal STQR daemon.

The commands below run **from the STQR repository root**, beside your private
`stqr.yaml`. They use its connection settings and administrator credentials.
`ubuntu` is an example **bucket name**; replace it with your template's name.
You do not need object IDs or temporary-directory names for the normal workflow.

## Workflow at a glance

```sh
# Create a template: complete the installation in the QEMU window.
./networkboot/prepare-system ubuntu /path/to/ubuntu.iso

# Start a gateway for a local diskless QEMU test.
sudo ./networkboot/serve ubuntu --no-dhcp

# In another terminal:
./networkboot/test-boot

# For physical PXE clients, run the gateway with Proxy-DHCP/TFTP instead.
# Stop the test gateway first; both use the same HTTP port.
sudo ./networkboot/serve ubuntu
```

Initial preparation publishes automatically after the installer VM exits. Subsequent edits mount the published filesystem directly and save in place. Wait for
`Published ubuntu` and `Cleanup complete` before starting or reloading the gateway.
These are distinct storage roles:

| Role | Contents and lifetime |
| --- | --- |
| Image bucket (`ubuntu`) | The STFS root filesystem, boot artifacts and manifest, all in the named bucket. PXE mounts it read-only; the editor mounts it writable. Its manifest refers to this bucket itself. It is not temporary. |
| Installation disk (`ubuntu-bd`) | Temporary NBD disk used only for installation/extraction. Its canonical ID is recorded for retries; renaming it or reusing its label cannot redirect installation or cleanup. It is deleted after successful image publication. |
| Per-boot upper bucket | Automatically created STOR/STOM overlay for one boot session. The hard logical quota defaults to 16 GiB (`serve --upper-gib`); space is consumed as written. Root writes are temporary and the upper is scheduled for deletion when the client disconnects. |
| Personal home (`alice_home`) | Automatically provisioned persistent filesystem bucket, mounted at login by PAM. |

Changing files on a PXE client's temporary root does **not** update the template.
The installation disk is not retained after successful publication. Build a new named image for a different system installation. Logging out does not reset a
running client's root; shutting down/rebooting ends that boot session.

## 1. Prepare the host and storage

The automated adapter currently supports **Ubuntu/Debian-style x86-64 systems
with initramfs-tools**. The native kernel client targets Linux 7.0; other kernel
interfaces and distributions need compatibility work and testing. The builder
checks for the installed kernel, matching headers and boot tooling rather than
assuming any installer ISO will work.

On the build/gateway host, install:

- Go 1.24+, Python 3.12+, a C compiler and make;
- QEMU (`qemu-system-x86_64`, `qemu-io`) and libguestfs (`guestfish`);
- GNU tar, `sort`, `lsinitramfs` and `ip` from iproute2;
- dnsmasq and standalone iPXE loaders for physical LAN boot;
- PAM development headers (`libpam0g-dev`) for automatic guest logins.

Build from the repository root:

```sh
make networkboot
```

The builder packages matching kstqr sources with its helpers. It builds the
module against the **guest's** exported headers; the host does not need to boot
that guest kernel or load kstqr itself. QEMU uses its existing NBD driver and
does not need recompilation. See [kstqr](../docs/kstqr.md) and
[the NBD frontend](../qemu-plugin-nbd/README.md) for their separate interfaces.

Configure storage volumes on the STQR nodes before installing an OS. Allow room
for the temporary VM disk, the named native filesystem image, and
rebuild/metadata overhead. The host also needs local export space, by default
in `/var/tmp`; `--work-dir PATH` selects another disk. SDK spools use this work
directory too. The template bucket itself is created by `prepare-system`.

All admitted cluster members must be reachable and support exclusive maintenance
for builder/editor, clone and cleanup operations. Other users must disconnect
from the temporary installation disk. Normal PXE clients read the named filesystem image read-only and write only to their own private upper buckets.

`stqr.yaml` stays private and is **not copied into the image**. An optional
`--config PATH` selects another node YAML or private credentials JSON. Run retry
commands as the same host user with the same configuration path so they find
that user's preparation record.

If libguestfs cannot start, run `libguestfs-test-tool`. If the host's `/boot`
kernel is unreadable, provide a readable `SUPERMIN_KERNEL` and its matching
`SUPERMIN_MODULES` directory. Building the tools does not change host PAM,
start dnsmasq, or mount any host filesystem.

## 2. Install a new image

```sh
./networkboot/prepare-system ubuntu /path/to/ubuntu.iso
```

The tool creates the temporary `ubuntu-bd` disk, starts an exclusive NBD export and opens the installer
in QEMU. Defaults are a sparse **32-GiB disk**, **4 GiB RAM**, **two vCPUs** and
legacy BIOS. The installer sees an ordinary writable virtual disk (usually
`/dev/vda`). Partition and format that virtual disk normally, for example with
ext4. The disk data is already stored in STQR, not in a local qcow2 copy.

Complete the installation. Use the installer's shutdown/reboot action; the
initial installer VM exits on reboot so preparation can continue. For later
editing, shut the installed OS down cleanly from inside the VM. Closing the
window while installation or writes are still in progress is not a substitute
for completing the installation and syncing its disk.

The installed system needs `mkinitramfs` from `initramfs-tools-core`, and headers
for the kernel that will be published. If they are missing, the disk is retained:
resume the recorded installation with `./networkboot/prepare-system --resume ubuntu /path/to/ubuntu.iso`, then finish installing the required packages
inside that VM. For a running installed Ubuntu kernel, for example:

```sh
# Inside the installed VM, not in the host or the ISO's live environment:
sudo apt-get update
sudo apt-get install initramfs-tools-core "linux-headers-$(uname -r)"
sudo poweroff
```

If an update installed a newer kernel, ensure that newer version has matching
headers too; the exporter selects the newest matching kernel/initrd pair.
`--kernel-version VERSION` can select a specific installed pair.

Unencrypted root disks are supported. Automatic LUKS unlocking is not implemented.
`--uefi FIRMWARE` selects a monolithic OVMF image when installing; use the same
firmware when editing. VM firmware variables are not persisted, so the installed
disk needs a working EFI fallback loader for subsequent VM boots. Secure Boot
module signing/enrollment is a separate deployment task.

After the VM exits, the tool automatically:

1. Mounts the installed disk offline through libguestfs.
2. Builds kstqr for the selected guest kernel and installs the one-shot helpers.
3. Builds and verifies a separate `/boot/initrd.stqr-VERSION`; the ordinary VM
   initrd remains available for later editing.
4. Exports the root tree, preserving numeric ownership, modes, timestamps,
   xattrs/capabilities, symlinks, hardlinks and FIFOs. Runtime trees and `/home/*`
   are excluded. Unsupported ACLs and device nodes cause an error.
5. Imports the root into the requested image bucket, explicitly syncs the import stream, and stores the checksummed kernel/initramfs and manifest in that same bucket. PXE users receive read-only grants; the filesystem remains available for an authorized editor.
6. Deletes only the recorded temporary installation-disk bucket. No second permanent rootfs bucket is created; filesystem-aware `gc` retains both the reachable tree and the manifest/artifact objects.

The published root uses a network-root fstab. The original disk fstab is restored
on the temporary builder disk, allowing an unfinished build to be resumed. The root is
served directly by kstqr; there is no SquashFS conversion or NFS root export.

### Continue after an interruption

If installation completed but extraction, publication or cleanup failed:

```sh
./networkboot/prepare-system ubuntu
```

Without an ISO, this finishes an existing installation. It reuses a completed
export when available; otherwise it extracts the existing disk. It never opens
the installer or creates a replacement disk. A recorded successful publication
retries cleanup without importing the root again. Work records live in the
current user's XDG state directory; internal staging paths are managed by the tool.
The installation record pins the original disk's canonical ID for resume, NBD
access and deletion even if its visible name changes. Missing or older name-only
records stop for manual recovery; a familiar bucket name cannot prove ownership.

If the **installation itself** is incomplete and has not been published:

```sh
./networkboot/prepare-system --resume ubuntu /path/to/ubuntu.iso
```

This opens the installer against the existing disk, after checking its size. The installation bucket is `ubuntu-bd`.
Use the same `--disk-gib` if you overrode the default. Use `edit-image` for an
already installed system. A failed extraction/publication leaves the previous
published release in place.

## 3. Edit a published filesystem image

```sh
./networkboot/edit-image ubuntu
# Create a separate copy-before-write image, then edit that copy:
./networkboot/edit-image ubuntu-next --copy-from=ubuntu
```

The editor starts a loopback-only private boot gateway and boots QEMU with `ubuntu` mounted
writable directly as its native filesystem. Its generated URLs use the host alias
inside QEMU user networking; the writable editor endpoint is not exposed on the LAN. No installation blockdevice is
required or recreated. Shut down the guest normally to flush its changes.
The image and its stable object IDs remain in place; only the editor's temporary
login is removed afterward. A failed cleanup retains a private recovery record.

`--copy-from` requires a new destination name. It uses STQR's server-side COW
clone, including for a clone of a clone. The source remains unchanged and the
new image's manifest points to the new bucket itself. There is no rootfs export
or client-side payload copy. `edit-image ubuntu --to-bucket ubuntu-next` is a
compatibility spelling for the same operation.

The editor uses the image's published kernel and initramfs. It patches only its
private cached mount helper to select the writable mount; this does not replace
the published artifacts or automatically select a newly installed guest kernel.

## 4. Test diskless boot locally

First publish the template successfully. Start an HTTP-only gateway in one
terminal:

```sh
sudo ./networkboot/serve ubuntu --no-dhcp
```

Wait for `PXE ready`. In another terminal:

```sh
./networkboot/test-boot
```

To compare with a directly writable root filesystem without a client overlay:

```sh
./networkboot/test-boot ubuntu --no-overlay
```

This starts a private gateway and boots a separate server-side COW copy, leaving
`ubuntu` unchanged. The `test-plain-…` bucket is retained after shutdown for
inspection and can then be deleted in the dashboard.

The launcher waits for a valid boot script before opening a diskless QEMU VM.
Its private DHCP/NAT network obtains the boot script from the host gateway;
there is no guest hard disk. This test does not change LAN DHCP or exercise
physical-client Proxy-DHCP/TFTP. Defaults are 4 GiB RAM and two vCPUs.
To use a gateway on another host: `./networkboot/test-boot --server GATEWAY_IP`.

Verify that Ubuntu boots and that a file created outside `/home` is writable.
Shut the client down cleanly and boot a new client: that root-only test file
should be absent. Once PAM homes are configured, also verify that a home file
survives logout and a subsequent client reboot. Until then, use the installed
system's local account; an STQR username alone does not create a Unix account
or make a desktop session ready to use.

An unprivileged HTTP-only gateway can use `--no-dhcp --cache PATH` with a private
writable cache owned by that user. The normal LAN gateway needs root for
Proxy-DHCP/TFTP.

## 5. Enable physical PXE clients on the LAN

Stop the HTTP-only test gateway before starting the LAN gateway on the same
port. Existing mounted clients continue using their native storage connections;
the replacement gateway is needed for new boot sessions and deferred cleanup.

```sh
sudo ./networkboot/serve ubuntu
```

The gateway detects the IPv4 default-route interface, server address and subnet,
prints its choice, and runs dnsmasq in **Proxy-DHCP mode** with TFTP. Your existing
DHCP server continues assigning addresses; the gateway does not run DNS or
replace it. No extra network entries are required in `stqr.yaml`.

Check the following before booting a physical client:

1. Keep the gateway and storage nodes running, with stable addresses. Every
   address advertised by STQR range owners must be reachable by the clients;
   an advertised loopback address is not a usable remote storage endpoint.
2. Connect the gateway and clients to the same boot LAN/broadcast domain for
   this Proxy-DHCP setup. Routed VLANs need deployment-specific DHCP relay and
   PXE configuration; they are not configured automatically.
3. Install standalone iPXE files at the default locations
   `/usr/lib/ipxe/undionly.kpxe` (BIOS) and `/usr/lib/ipxe/ipxe.efi` (x86-64 UEFI),
   or select them with `--bios-loader` / `--efi-loader`. QEMU option ROM files
   are not standalone TFTP loaders.
4. Allow the required traffic shown below. Remove conflicting boot offers from
   another PXE service, or deliberately integrate with that existing service.
5. Enable the NIC's network/PXE boot in the client firmware, select BIOS or
   x86-64 UEFI as appropriate, and select network boot in its boot menu.
   Secure Boot is not automatically provisioned by these tools.
6. Wait for iPXE, the kernel/initramfs download, and then the Linux system.
   No per-client disk installation or MAC registration is required.

| Connection | Purpose |
| --- | --- |
| DHCP/Proxy-DHCP UDP 67/68 and PXE UDP 4011 | Address assignment by existing DHCP and boot offers from the gateway |
| TFTP UDP 69 and its transfer traffic | Initial standalone iPXE loader |
| Gateway TCP 8088 by default | Boot script, kernel/initramfs and one-use credential claim |
| STQR TCP 7419 by default on advertised nodes | Native kernel filesystem data and subscription connections |

If automatic interface detection is ambiguous, select the boot interface and
address explicitly. Other advanced options have defaults:

```sh
sudo ./networkboot/serve ubuntu --interface enp1s0 --server 192.0.2.10
```

`--check` prints the chosen configuration and checks dependencies/bucket access
without starting services. With an existing external PXE infrastructure, run
`serve ... --no-dhcp` and arrange for its clients to chain to
`http://GATEWAY_IP:8088/boot.ipxe`.

### Optional: run the gateway as a service

From the repository root, install the tools, private gateway configuration and
example unit:

```sh
sudo make -C networkboot install
sudo install -D -m 0600 stqr.yaml /etc/stqr/stqr.yaml
sudo install -m 0644 networkboot/systemd/stqr-networkboot.service \
  /etc/systemd/system/stqr-networkboot.service
sudoedit /etc/systemd/system/stqr-networkboot.service
```

In the unit, set `ExecStart=/usr/sbin/stqr-networkboot ubuntu` for this example.
Its working directory is `/etc/stqr`, so it finds that private `stqr.yaml`.
Stop any manually running gateway on the same ports before enabling the service:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now stqr-networkboot
sudo systemctl status stqr-networkboot
journalctl -u stqr-networkboot -f
```

Installation alone does not enable the service, alter system dnsmasq, or configure
host PAM. The service's configuration stays on the gateway; do not put its
administrator credentials into the template.

## 6. Activate updates and roll back

For an update to the same template, finish `./networkboot/edit-image ubuntu`,
then reboot clients which should use the edited rootfs. To reload changed boot artifacts, reload the gateway service:

```sh
sudo systemctl reload stqr-networkboot
```

Reload fetches and verifies the new manifest/kernel/initramfs before switching
new boot sessions. Existing sessions retain their selected artifact URLs. Direct edits change the
shared filesystem image in place; they are not a versioned root snapshot. A failed reload
keeps the old active release. Reload does not update the root of a running client;
reboot that client when it should use the new system.

A foreground gateway also accepts SIGHUP. Gateway restart does not prove that
mounted clients are idle and does not revoke their working storage connections.
To promote a separately prepared `ubuntu-next`, change the service's bucket name and restart during a
maintenance window. To roll back, select the retained known working template in
the same way. There is no automatic rollback CLI.

Keep old native release buckets while boot sessions still reference them. The
builder's normal cleanup handles its disk/artifacts; it does not delete older
native release buckets. Failed native imports may also retain a release bucket
for administrative cleanup; their IDs are diagnostics, not normal CLI inputs.

## 7. Configure STQR logins and persistent homes in the template

`prepare-system` configures STQR logins in the
prepared Debian/Ubuntu image. They query the STQR user list with the host's admin
configuration, skip disabled and temporary boot-session accounts, provision a
`username_home` bucket and grant its user filesystem access. Existing homes must
already grant the current stable user ID full access; preparation refuses to
transfer an old user's files merely because a username was reused.

The image receives Unix accounts with stable UID/GID values derived from their
STQR account IDs. Identity collisions and existing template-account conflicts
fail preparation instead of silently taking over local accounts. Usernames must
fit the supported Unix name syntax (32 characters); other STQR names and `root`
are reported and skipped. Template users remain available for local rescue but
are hidden from the graphical account list. STQR administrator status does not
grant Unix root/sudo access.

The builder installs `pam_stqr`, `stqr-home`, the compatible boot client and a
credential-free login manifest. The module is enabled in `common-auth` and before
`pam_systemd` in `common-session`. Managed users must authenticate against STQR;
they cannot fall back to a local password. Existing local accounts retain their
usual authentication. No host PAM files are changed. The target needs Python 3,
libpam, `pam_succeed_if`, the Debian/Ubuntu common PAM layout, and a compatible
architecture/libc ABI for the host-built helpers.

At PXE boot, the same installer applies the payload to the private upper before
PID1 starts. This also permits an initrd-only login hotpatch while retaining an
existing read-only lower. A loopback daemon address in the build configuration
is replaced with `stqr.server`; explicitly configured remote addresses are kept.
No administrator credentials or user passwords enter the image or initrd. Home
bucket IDs are pinned in root-owned configuration, preventing a renamed/replaced
bucket from silently becoming a different home.

Prepare/publish the image again to synchronize newly created users. Password
changes and disabling/deleting users take effect during STQR authentication;
there is no password copy in the template. Key-only SSH cannot supply the STQR
password needed to mount a home.

### Home mount lifetime

The password is passed to the fixed privileged helper on stdin, not in argv or
the environment. Temporary credential files are private and removed after setup;
the kernel retains reconnect credentials in memory. No administrator password is
needed in the home configuration. Helper files/directories must be root-owned
and not writable by other users; the JSON must be root-owned mode 0600.

Homes use `nosuid,nodev`. Multiple logins of one UID reuse a mount, and the last
PAM session close attempts a regular unmount. A busy home is retained instead
of being force/lazy unmounted. Background services or a killed PAM process can
leave session state requiring administrator reconciliation; automatic busy-home
retry and logind crash reconciliation are not implemented. Keep `/home` root-owned
and non-writable by ordinary users. A mount is not encryption or isolation from
local root.

`make -C networkboot install` installs helpers and the PAM module, but does not
activate PAM rules. Packaging supports `DESTDIR`, `PREFIX` and `PAM_MODULE_DIR`.
The builder separately installs the guest's kstqr kernel module on publication.

## Boot architecture and session lifetime

```text
PXE -> iPXE -> HTTP kernel + initramfs (includes matching kstqr.ko)
                 -> temporary boot identity
                     -> one native kstqr overlay mount
                         lower: readonly STFS release bucket
                         upper: temporary STOR/STOM bucket
                     -> switch_root
login -> PAM -> persistent username_home bucket at /home/username
```

Image preparation installs helpers, the matching module, account tables and PAM
configuration once. A small hash manifest binds that completed preparation to
the initramfs payload. Matching boots read only the manifest from the lower and
skip helper copying and Python account installation. For stale or missing markers, changed helper/module payloads are copied only
into a root-only executable tmpfs below `/run`, made readonly, and file-bound
onto their runtime paths. Existing backing helper bytes remain untouched; a
missing path needs only an empty placeholder. The `/run` child and binds survive
initramfs-tools moving `/run` into the final root. The dedicated child is
executable even though `/run` itself is mounted `noexec`.

Changed login plans/installers still refresh small account/config files before
PID1. Runtime RAM bindings never advance the persistent prepared marker: a
reused upper or edited image must bind them again on its next boot. Payload hashes
are calculated from initramfs RAM, not by reading all helpers back from STQR.

A static helper substitutes the numeric per-boot server address into the small
credential-free home configuration; no Python interpreter or persistence fence
is needed for this runtime update. The shared lower stays unchanged. Since this
PXE session supplies its own network configuration and identity, cloud metadata
initialization and local GRUB bookkeeping are disabled in the private upper.

The kernel uses our native Go/FUSE-compatible STOR/STOM overlay format:
deletion markers live in directory records, metadata overrides preserve the
original content, and range writes retain references to unchanged lower data.
Overlay uppers cannot be mounted alone. Creating hardlinks through an overlay
is unsupported, as is copy-up of multiply linked lower regular files; unchanged
lower hardlinks remain readable. Maintain the system through the builder VM.
There is no SquashFS, Linux OverlayFS,
NFS root or NBD root upper in this boot path. QEMU's builder disk still uses the
independent NBD frontend.

Each MAC and boot gets a temporary STQR user with **read-only permission on the
pinned lower** and read/write permission on its own upper. The kernel command line
contains a one-use, IP-scoped claim token, never the administrator password. A
one-shot initramfs helper claims boot credentials over HTTP, mounts the native overlay,
removes its private credential file and exits. The kernel retains reconnect
credentials. A separate control socket is held by the upper superblock until
unmount; no resident client helper is needed. Object I/O goes directly to STQR.

After a claim disconnect, the helper first acquires cluster-wide maintenance to
prove that the image/upper has no active storage clients. It then deletes the
temporary user, releases maintenance, revokes the lower ACL entry and schedules
deletion of a disposable upper. Editor cleanup retains the image itself. Busy,
unavailable or uncertain participants retain the private recovery record and
credentials remain valid for a still-mounted guest. The cluster persists deletion
intent and purges data in batches; an extra object-by-object clear is unnecessary. Unclaimed
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
updates. A restarted gateway retries only its recorded private session identities,
and retains sessions while cluster-wide inactivity cannot be proved.
Keep old native release buckets while clients still use them.

## Storage and administration reference

### QEMU block-device interface

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

### Clear and clone

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

NBD, GC and clear use **durable exclusive leases across all admitted
cluster members**. All members must be reachable and upgraded; existing clients
of the affected bucket must disconnect. Clone instead publishes immediate
object-wise COW connections through an ALL-member catalog decision; existing
clients may remain connected. Flush or stop application writers when an
application-consistent clone is required. A read-only preflight checks member
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

## Publication progress and retries

`stqr-bootctl publish` waits for transient cluster-control barriers using the same
operation ID. It prints the reason while waiting; Ctrl+C cancels the wait.
During native import it reports how much of `rootfs.tar` has been read. This is
input progress, not confirmation that the new release is already published.
Wait for publication to finish before booting the new release.

A local lock in the staging directory prevents two publishers from importing the
same prepared image simultaneously. Repeating the command while one is running
reports that publisher instead of creating another release bucket. Failed imports
retain their partial release bucket for inspection; the prepared archive and
existing published release remain available.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| `required control participant is offline`, maintenance unavailable, or an operation waiting for a peer | Restore connectivity to every admitted cluster member. Retry `prepare-system ubuntu` after the cluster is stable. |
| Missing headers or `mkinitramfs` | Use `edit-image ubuntu` to install the target kernel's headers and `initramfs-tools-core`, then shut the VM down. |
| Image format/manifest mismatch | The named bucket must be the FS image itself and contain a self-referencing boot manifest. Migrate existing split BD/root images before using these tools. |
| iPXE download fails or `test-boot` keeps waiting | Check the gateway terminal/service journal for `PXE ready`, its selected address and HTTP port. Verify client routing/firewall access. |
| Physical firmware never finds the boot offer | Check NIC PXE settings, broadcast/VLAN layout, standalone loader files, competing PXE services and Proxy-DHCP/TFTP traffic. A QEMU HTTP-only success does not test these. |
| STQR login works but the desktop/home does not | Check the Unix/NSS account, numeric ownership, bucket grant, client-reachable URI and actual service's PAM ordering. Reprepare the image to synchronize accounts; inspect the guest PAM/helper errors if a home mount fails. |

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

### Automatic cleanup of temporary boots

The gateway records each allocated temporary user and upper privately before
provisioning it. Claim EOF and unclaimed-session expiry trigger cleanup; startup
and a periodic retry also process leftover records from failed allocations,
interrupted cleanup, or a previous gateway process. Records contain IDs and
mount mode, never passwords. No bucket is selected for deletion merely because
its name looks temporary.

Before revoking a normal boot user, STQR must grant exclusive maintenance on
its upper across the cluster. An upper with a live storage connection is kept,
even if its gateway died. Once the VM disconnects, a subsequent retry removes
its temporary account and bucket. Editor records revoke only the temporary
editor user; their image bucket is never deleted. The same maintenance guard
retains an orphan editor account while any client still has its image open;
automatic retries finish that cleanup once the image becomes idle. Stop the gateway and keep its
private cache if a member is unavailable; remaining records allow later retry.

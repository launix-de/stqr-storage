# Local validation, 2026-11-23

All services and storage used disposable fixtures. No production bucket, host
PAM stack, host kernel mount or LAN DHCP configuration was changed.

- Go race tests: SDK, clear/clone replay, maintenance, accepted-write barriers,
  filesystem checks and the new networkboot integration fixtures passed.
- `qemu-io` wrote, flushed and verified a real STQR-backed NBD disk. A cloned
  bucket read back the same sectors. The exclusive path was checked to actually
  negotiate accepted writes, with durable barriers at flush/FUA.
- Actual libguestfs appliance negotiation against the same NBD endpoint passed.
  Its host appliance used a readable Linux 7.0.0-38 kernel copy via
  `SUPERMIN_KERNEL`, with matching `SUPERMIN_MODULES`; default `/boot` permissions
  otherwise prevented appliance generation for the unprivileged build user.
- Publish/fetch checksummed artifacts round-tripped, including an artifact over
  1 MiB. The bundle-aware collector retained the current artifacts and disk.
- Per-MAC session allocation exposed only a loopback-scoped test TCP endpoint;
  QEMU read/write/flush passed, and disconnect cleared/deleted its private bucket.
- The real gateway started in an isolated user namespace, with DHCP disabled and
  loopback HTTP/NFS endpoints. Publishing a second manifest and sending SIGHUP
  updated the next boot script. Both old and new versioned kernel URLs continued
  returning the correct bytes.
- A separate QEMU VM booted Linux 7.0.0-38 from the userspace NFS server, mounted
  its SquashFS lower and switched root with a private upper. Both tmpfs and
  kernel-NBD/ext4 upper paths passed; the latter used the new static connector
  against a standard QEMU NBD server, complementing the STQR/QEMU protocol test.
- PAM compiled with `-Wall -Wextra -Werror`. A real libpam conversation/module
  harness exercised password success/failure, open/close callbacks and rejection
  of option-like usernames, using a test-only helper. No host PAM rule was added.
- dnsmasq accepted the generated proxy configuration in `--test` mode. Python
  workflow/help tests, shell syntax checks, `go vet`, builds and staged `make
  install DESTDIR=...` passed.

Not exercised: a full interactive Ubuntu installer run, physical firmware PXE /
UEFI / Secure Boot interoperability, or a real desktop login with a kernel home
mount. The current distribution adapter is Ubuntu/Debian initramfs-tools;
exclusive block writers and bucket clear/clone still require single-node
membership. These are explicit acceptance boundaries, not passing test claims.

## Named striped devices, 2026-11-24

The NBD adapter now lives in `qemu-plugin-nbd/`. The new format was tested with
disposable server/volume fixtures:

- A sparse 32-GiB device initially contains exactly one metadata object. Two
  named devices remain independent; duplicate creation and duplicate writers
  are rejected. Export LIST and unknown-export negotiation passed.
- Small writes call PutRange directly without full-stripe reads or writes.
  Boundary splits, sparse zero padding, full-stripe Put, repeated uncached reads
  and bounds rejection passed under the race detector.
- The collector retains actual live stripes of a 1-PiB virtual device without
  constructing a stripe list, removes orphan/out-of-range objects, preserves
  opaque artifact roots and rejects the old filesystem-backed disk format.
- Real QEMU wrote/flushed/read the named `data` export across a stripe boundary
  while `disk.raw` stayed connected. NBD disconnect flushed/released the writer;
  cloned disk readback, publish/fetch/GC and gateway/session fixtures passed.
- Native block-lease ranged writes and Sync passed; a second bucket connection
  was excluded, and even an administrator could not write/delete through a
  readonly block lease. Existing filesystem-maintenance exclusion tests passed.
- Root SDK/server tests, focused race tests, standalone/static connector builds,
  networkboot race/Python/shell checks and CLI help passed. The PAM test was
  skipped in this run because development headers were not configured; PAM code
  was not changed.

This run did not repeat the interactive installer or guest kernel boot tests.
Cluster-wide exclusion, Trim and Resize remain unimplemented.

## Cluster maintenance, 2026-11-24

This supersedes the earlier single-node-only exclusion boundary. Disposable
production daemon fixtures now pass:

- Two-owner ranged read/write and Sync under one ALL-member lease, with normal
  clients excluded and readonly mutation denied on both nodes.
- Passive participant restart retaining its durable fence; the owning client
  reauthenticates its auxiliary socket and reads the preserved HDD data.
- Abrupt original-socket loss followed by ALL orphan release, with no SDK
  reconnect of the old owner. Retired prepares cannot resurrect a lease, and
  delayed release cannot clear the next lease.
- Clear/clone through either node, with globally committed receipts protecting
  later writes against clear replay. A production one-node daemon also retains
  those receipts across restart.
- Real QEMU across a two-node deployment: named disk creation, stripe-boundary
  write, flush/readback, publish/GC/fetch, clone, clear and delete.
- The existing single-node QEMU/gateway/session fixture still passes. CLI
  operational failures exit without Python tracebacks. Full Go tests and focused
  race tests passed; Python, shell checks and helper builds passed.

An interactive Ubuntu installer and physical PXE firmware remain outside this
fixture. The scripts do not hide those untested end-to-end steps.

## 2026-11-24: installed Ubuntu 26.04.1 image

A user-installed Ubuntu 26.04.1 image (Linux 7.0.0-44-generic) was inspected and
published on the two-node deployment. The 12,254,074,880-byte root archive and
other artifacts round-tripped with full SHA-256 verification using bounded
64-MiB ranged transfers. Failed export/upload staging is retained for retry.

Real QEMU iPXE boot was exercised over private DHCP/NAT, with HTTP kernel/initrd
loading and a userspace NFS/SquashFS lower. Both a RAM upper and a temporary
STQR/NBD upper booted the actual installed root, verified its package database
and GNOME binary, and wrote/read a file through the overlay. A further boot
started systemd to the multi-user target and powered off without NBD I/O errors.
All temporary STQR upper buckets from these tests were deleted after disconnect.

Ubuntu's update-initramfs shim uses Dracut, so it does not execute our
initramfs-tools hook. The builder now explicitly creates a separate network
initrd with mkinitramfs and verifies that it contains scripts/stqr. The ordinary
VM initrd remains separate. Root-storage NBD uses generic netlink, allowing the
connector to exit after the kernel confirms capacity. This avoids legacy ioctl
handlers being interrupted by systemd's global SIGSTOP/SIGCONT shutdown pass.
Readonly lower/NFS mounts can remain busy until kernel poweroff; no shutdown
journal abort or NBD write error occurred in the final test.

The boot tests did not exercise graphical desktop login, PAM home mounts,
physical LAN Proxy-DHCP/TFTP, Secure Boot or UEFI firmware. The template does not
automatically install/enable STQR PAM homes or provision Unix accounts.

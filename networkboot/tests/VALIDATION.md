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

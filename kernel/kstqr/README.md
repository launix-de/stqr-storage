# kstqr

Standalone Linux kernel filesystem client for STQR, using the native C protocol
core. The server may run on the same host. The mount helper exits after mounting;
no resident client daemon, Go runtime or FUSE process is required.

Build: `make` and `make helper` (matching Linux 7.0 headers required).

See [the kernel client guide](../../docs/kstqr.md) for credentials, mount syntax,
implemented filesystem operations, current limits and isolated QEMU tests.

`-o bd` exports an existing named block-device bucket as real Linux block nodes;
`-o bd,device=NAME` selects one device and exposes it as `bd`. It shares the NBD
stripe format and exclusive object lease, without running an NBD process. See the
[block-device and fstab instructions](../../docs/kstqr.md#native-block-device-buckets--o-bd).

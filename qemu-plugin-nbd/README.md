# STQR named block devices over NBD

`stqr-nbd` opens one authenticated STQR bucket and exposes its named virtual
block devices through a private Unix NBD socket. QEMU's existing NBD driver
connects without rebuilding QEMU. QMP `blockdev-add`/`device_add` can hot-plug
devices; sync and unmount the guest filesystem before removal.

## Build and run

```sh
make -C qemu-plugin-nbd
# Private credentials file, chmod 600:
# {"uri":"http://localhost:7419","username":"vm-user","password":"..."}

qemu-plugin-nbd/bin/stqr-nbd --config /private/credentials.json \
  --bucket vm-disks --size 34359738368 create system
qemu-plugin-nbd/bin/stqr-nbd --config /private/credentials.json \
  --bucket vm-disks --size 68719476736 --manufacturer STQR --model Data create data
qemu-plugin-nbd/bin/stqr-nbd --config /private/credentials.json --bucket vm-disks list
qemu-plugin-nbd/bin/stqr-nbd --config /private/credentials.json \
  --bucket vm-disks --socket /private/vm-disks.sock serve

qemu-system-x86_64 \
  -blockdev '{"driver":"nbd","server":{"type":"unix","path":"/private/vm-disks.sock"},"export":"system","node-name":"system","discard":"unmap"}' \
  -device virtio-blk-pci,drive=system \
  -blockdev '{"driver":"nbd","server":{"type":"unix","path":"/private/vm-disks.sock"},"export":"data","node-name":"data","discard":"unmap"}' \
  -device virtio-blk-pci,drive=data
```

`--config` is optional: by default the tool reads the existing `./stqr.yaml`
(node listen address and bootstrap administrator credentials). The private JSON
shown above can be used for a dedicated non-administrator.

Create the bucket and grant the user READ+WRITE first. Names resolve through
BUCKET_INFO; `--bucket-id` accepts a hexadecimal bucket ID instead. Flags precede
the command. `--readonly` needs READ only. `--default-export NAME` optionally
supports clients that request an empty export name. `-h` lists all flags.
Credentials stay in the private exporter process; QEMU receives only the socket.
Manufacturer/model/serial are metadata and NBD descriptions. Configure QEMU's
guest-facing hardware properties separately when a guest needs a specific vendor.

## Storage format

Each device has exactly one binary metadata object: `STNBD001`, version u16=1,
stripe size u32=1048576, virtual byte size u64, generation ID (16 bytes), followed
by native Text fields for name, manufacturer, model and serial. Integers are
big endian. Device sizes must be positive multiples of 512 bytes.

Metadata addresses have eight zero bytes followed by the first 24 bytes of
SHA-256(`"STQR NBD metadata v1\0" + name`). A sorted object scan lists descriptors
without reading stripe payloads. Creation uses absent-object CAS.

Stripe addresses are computed from device generation and stripe number; **there
is no stripe/segment list**. Let H be SHA-256(`"STQR NBD stripe v1\0" + generation
+ stripe-number-u64-BE`). The address is H[0:16], generation[0:8], stripe number
u64-BE. If the first eight bytes are all zero, byte zero becomes 1. The hashed
high bits distribute placement; the low bits allow offline orphan collection.
The full generation participates in the hash. Bucket cloning preserves IDs.

Stripes cover 1 MiB each. Missing stripes and unwritten tails read as zeros.
Full-stripe I/O uses Get/Put; smaller I/O uses GetRange/PutRange, split at stripe
boundaries. Partial writes do not fetch/rewrite the whole stripe. A new sparse
device writes only its descriptor. There is no client payload cache; repeated
reads go to STQR. Guest operating systems can use their normal caches.

## Durability and exclusion

Writes use native accepted operations. NBD FLUSH, FUA and clean disconnect use
explicit bucket Sync barriers. Ordinary writes need a later FLUSH/FUA for durable
completion; force-killing an exporter does not substitute for a flush.

One exporter holds a socket-scoped exclusive block lease (`0x011c`), with
READ+WRITE or READ-only authorization. It admits one writer per named device;
different devices run concurrently on the shared connection. Multiple readonly
connections are supported by a readonly exporter. The NBD multi-connection flag
for concurrent writers to the same device is not advertised. After connection
loss, the lease cannot reconnect and silently resume writes.

Leases span **all admitted cluster members**, including a one-node deployment.
Every participant persists its fence before acquisition succeeds. Other bucket
clients must disconnect first. Object reads/writes and Sync still go directly to
their committed owners through authenticated auxiliary sockets carrying the
lease grant. The original owning socket remains pinned; losing it stops the SDK
from continuing and triggers orphan cleanup. A passive participant restart keeps
the fence. A network timeout alone never releases ownership.

Internal range migration remains available while a native block lease is held.
Local capacity exhaustion immediately wakes the range worker; the native client
waits through retryable responses and continues the same request after handoff.
Placement classes/groups still apply, and genuine cluster exhaustion returns a
capacity error. Private clear/clone and filesystem-maintenance leases retain
their placement fence so an offline inventory cannot change underneath them.

Writable exports advertise NBD TRIM. Enable `discard=unmap` on QEMU's NBD block
node (the networkboot builder does this automatically), then run `fstrim` inside
the guest. A discarded complete 1-MiB stripe is deleted as a native object,
returning its payload allocation to STQR. Partial stripes are zeroed through
PUT_RANGE; adjacent data survives. Missing stripes and sparse tails remain
unallocated. Repeated partial discards do not accumulate a bitmap: only a single
request covering a complete stripe deletes that stripe. FLUSH/FUA also persist
discard operations. Readonly exports reject TRIM.

Native READ+WRITE block grants cover deleting stripes during an exclusive RW
lease. Ordinary object DELETE still requires its separate DELETE grant. TRIM
releases space inside STQR volumes; it does not shrink the configured volume file
or promise a physical SSD discard through the host storage stack. WRITE_ZEROES,
online resize and structured allocation-status replies remain unimplemented.

Clean close sends `BUCKET_MAINTENANCE_END` (`0x011d`) before disconnecting.
Journal epochs, monotonically increasing sequences and fresh server-generated
bearer tokens reject delayed prepares/releases and stale auxiliary connections.
Handoffs of this bucket and ordinary membership changes are blocked while leased;
unrelated bucket operations remain available. All nodes and the exporter must
use the upgraded protocol. Device resize, discard/trim, write-zeroes and snapshots
remain unimplemented.

The older filesystem-backed disk format is rejected rather than reformatted.
Export old disks with the previous version and copy into a fresh striped bucket.
A block-device bucket is not a native FUSE filesystem: do not mount it with FUSE
or run filesystem `--fsck` on it. Networkboot's bundle-aware collector preserves
all named devices and current boot artifacts. The static Linux NBD connector
used by the networkboot initramfs lives in `linux-client/`.

Run `make -C qemu-plugin-nbd check` for range/sparse/format/NBD tests. The real
STQR/QEMU integration is `STQR_TEST_NETWORKBOOT=1 go test ./server -run
'^TestNetworkbootNBDWithQEMU$' -count=1` after `make -C networkboot`.

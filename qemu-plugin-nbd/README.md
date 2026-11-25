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

## Growing devices

In the dashboard, open **Buckets → Disks** on the desired bucket. The device
list shows each disk's virtual capacity and identity, supports search and loads
additional pages while scrolling. **Vergrößern** opens the same size controls as
the rest of the dashboard: unit input, a discrete slider, quick increments and a
before/after preview. Readonly exporters have the action disabled.

The dashboard also works when the exporter runs on another machine. An updated
exporter registers a bounded native control handler on its original STQR lease
socket; the cluster forwards the administrator's request to that owner. It does
not open the private Unix socket remotely, release the lease or permit a second
writer. Without an exporter, the dashboard acquires its own ordinary exclusive
block lease for the operation. An old/unreachable lease owner remains fenced and
produces an explicit error rather than being bypassed.

Grow a device while its exporter is stopped, using the usual bucket credentials:

```sh
stqr-nbd --config /private/credentials.json --bucket vm-disks \
  --size 68719476736 resize system
```

With an exporter running, send the request to that process instead. It keeps its
existing exclusive cluster lease and uses no additional STQR connection:

```sh
stqr-nbd --socket /private/vm-disks.sock --size 68719476736 resize system
```

The exporter creates `/private/vm-disks.sock.control`, a Unix control socket with
mode 0600. Keep both sockets in a private directory. Readonly exporters reject
resize requests. A stale socket is an error; the command never falls back to a
second connection or removes another process's socket.

Sizes are absolute bytes, positive multiples of 512, at most `2^63-1`. Only
growth is supported. Resize atomically compares/replaces the existing descriptor
and flushes it before success. Device generation, serial, existing stripe IDs and
data remain unchanged; the extension stays sparse and reads as zeros. No disk
format change or stripe rewrite is involved. After a lost response or flush
error, inspect the size or retry the same size; the retry flushes again.

An established NBD connection retains its negotiated size. Reconnect the NBD
client to discover the extension; this does not restart the exporter or affect
other disks. This does not resize an attached guest's partition/filesystem or
provide automatic live capacity notifications to QEMU/the Linux NBD client.

## Storage format

Ordinary writes retain the device generation and its deterministic stripe object
IDs. Full-stripe requests use native PUT; partial requests use native PUT_RANGE.
Descriptor growth uses CAS at the existing metadata ID. Repeated overwrites do
not create a new stripe index, device identity or per-write object graph. Native
storage decides whether existing physical allocations can hold the replacement.

TRIM deletes complete stored stripes and zeroes only stored portions at the
edges. With WRITE but no DELETE permission, a complete stripe is replaced by an
empty object instead. The distinction affects live object counts, not the
zero-filled read result. FLUSH/FUA uses the ordinary native durability barrier.

`Catalog.Collect` is an explicit offline collection operation: the caller must
hold exclusive maintenance and stop exporters before invoking it. It retains
named device descriptors, their current stripes and the supplied opaque object
roots, and removes orphan generations. It does not inspect a guest filesystem
or discover application snapshots. A caller must preserve those references
explicitly; collection must not run concurrently with open exports or writers.
The catalog tracks writable and readonly local exports and rejects collection
until the last one closes. It excludes new local export claims during the sweep.
The native exclusive lease remains necessary for other catalogs and processes;
the local reference table does not replace that fence.

Each device has exactly one binary metadata object: `STNBD001`, version u16=1,
stripe size u32=1048576, virtual byte size u64, generation ID (16 bytes), followed
by native Text fields for name, manufacturer, model and serial. Integers are
little endian, as in the shared native codec. Device sizes must be positive
multiples of 512 bytes.

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

One exporter holds a socket-scoped generic exclusive object lease (`0x011c`), with
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

Internal range migration remains available while a generic object lease is held.
Local capacity exhaustion immediately wakes the range worker; the native client
waits through retryable responses and continues the same request after handoff.
Placement classes/groups still apply, and genuine cluster exhaustion returns a
capacity error. Private clear/clone and filesystem-maintenance leases retain
their placement fence so an offline inventory cannot change underneath them.

Writable exports advertise NBD TRIM. Enable `discard=unmap` on QEMU's NBD block
node (the networkboot builder does this automatically), then run `fstrim` inside
the guest. With DELETE permission a discarded complete 1-MiB stripe is deleted
as a native object. With WRITE permission alone, the client replaces its payload
with an empty object instead; its small object header remains. Both forms release
the stripe payload allocation. Partial stripes are zeroed through
PUT_RANGE; adjacent data survives. Missing stripes and sparse tails remain
unallocated. Repeated partial discards do not accumulate a bitmap: only a single
request covering a complete stripe deletes that stripe. FLUSH/FUA also persist
discard operations. Readonly exports reject TRIM.

Exclusive object leases preserve the ordinary per-operation grants. Native
DELETE always requires DELETE permission; the WRITE-only fallback belongs to the
NBD client, and the server never recognizes disk/stripe metadata. TRIM
releases space inside STQR volumes; it does not shrink the configured volume file
or promise a physical SSD discard through the host storage stack. WRITE_ZEROES,
automatic live NBD capacity notifications and structured allocation-status replies
remain unimplemented.

Clean close sends `BUCKET_MAINTENANCE_END` (`0x011d`) before disconnecting.
Journal epochs, monotonically increasing sequences and fresh server-generated
bearer tokens reject delayed prepares/releases and stale auxiliary connections.
Handoffs of this bucket and ordinary membership changes are blocked while leased;
unrelated bucket operations remain available. All nodes and the exporter must
use the upgraded protocol. Device shrinking, write-zeroes and snapshots remain
unimplemented.

The older filesystem-backed disk format is rejected rather than reformatted.
Export old disks with the previous version and copy into a fresh striped bucket.
A block-device bucket is not a native FUSE filesystem: do not mount it with FUSE
or run filesystem `--fsck` on it. Networkboot's bundle-aware collector preserves
all named devices and current boot artifacts. The static Linux NBD connector
used by the networkboot initramfs lives in `linux-client/`. It waits for the
expected block capacity before returning. The boot hook passes `--root-storage`
to attach through generic netlink: the kernel owns the connection after setup,
with no userspace handler for systemd to interrupt during shutdown. This mode
requires a kernel with NBD generic-netlink support. Ordinary attaches keep the
legacy ioctl path for compatibility.


Run `make -C qemu-plugin-nbd check` for range/sparse/format/NBD tests. The real
STQR/QEMU integration is `STQR_TEST_NETWORKBOOT=1 go test ./server -run
'^TestNetworkbootNBDWithQEMU$' -count=1` after `make -C networkboot`.

The dashboard is another client of this format. A connected exporter handles
opaque management requests on its original lease socket. Without an exporter,
the browser lists ordinary native objects, reads descriptors and conditionally
changes their eight-byte size field with ranged CAS. Each CAS compares the old
descriptor bytes under the volume lock; there are no object content versions.
A concurrently acquired exclusive lease rejects the offline object request.
The server never opens a disk catalog or decodes a descriptor for administration.

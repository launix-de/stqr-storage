# STQR

**An easy to configure storage management system. Start with one disk. Grow on your own terms.**

### Physical view: start small, add capacity

```text
Nodes
└── Node 1
    └── Volume 1 · 8 TB
```

```text
Nodes
├── Node 1
│   ├── Volume 1 ·  8 TB
│   ├── Volume 2 ·  8 TB
│   ├── Volume 3 · 16 TB
│   └── Volume 4 · 16 TB
└── Node 2
    ├── Volume 1 ·  8 TB
    ├── Volume 2 ·  8 TB
    ├── Volume 3 · 16 TB
    └── Volume 4 · 16 TB
```

### Logical view: buckets and their clients

```text
Buckets                         Connected clients
├── videos ────────────────────► FUSE
├── database1 ─────────────────► memcp
├── database2 ─────────────────► memcp
├── webserver-scripts ─────────► FUSE
├── webserver1 ────────────────► FUSE + overlay
├── webserver2 ────────────────► FUSE + overlay
└── vm-disks ──────────────────► QEMU
```

Volumes provide physical capacity; buckets give applications a logical namespace
independent of disk and node boundaries. FUSE, native clients and S3 expose that
storage in different ways. The QEMU connection illustrates the planned block-device
integration. See [implementation status](#implementation-status) for current capabilities.

STQR is built around a simple idea: adding storage should not require redesigning
how your applications access their data. Start the daemon, select a disk, create
a bucket and user, and mount it. The architecture is designed to carry that same
storage concept from one node and one disk to a petabyte-scale cluster, adding
drives and nodes as your needs grow.

Use it for an expanding media library, shared application files or application
storage. Access data through a Linux FUSE mount, the native Go API or an
S3-compatible endpoint built into the daemon. Add volumes and nodes to extend
capacity while applications keep their bucket and object addresses. The name
is a nod to *store* and *SPQR*.

## Why STQR exists

A full disk often becomes an infrastructure project. Depending on the setup,
expanding a conventional Linux filesystem can involve moving data, changing
partitions, extending logical volumes, unmounting filesystems or scheduling
downtime. Some setups support online growth, but someone still has to plan and
manage the storage underneath.

Adding a second server introduces another question: how do both machines see the
same files? Databases already provide shared access to relational data. For files,
the step from a local disk to shared, expandable storage is often much larger.

A system such as Ceph addresses these needs, but adopting a full storage cluster
also introduces deployment and management complexity. That is a substantial step
for a NAS or web application that has simply outgrown its first disk.

STQR aims to close this gap with one continuous storage model. Begin with the
hardware you have, add capacity when you need it, and keep the application-facing
interface as the installation grows. A three-node deployment is not a prerequisite
for getting started.

## Simple to start, room to grow

- **A short path to a mounted filesystem.** Start the daemon, select a disk in the
  dashboard, create a bucket and user, then mount the bucket with `stqr-fuse`.
- **No three-node starting requirement.** One machine and one disk are a valid
  starting point. Add disks or nodes as capacity needs grow; redundancy is a
  separate part of the roadmap.
- **Automatic initial setup.** The daemon generates its configuration, identity
  and bootstrap credentials on first start. There is no configuration file to
  assemble before you can log in and set up storage.
- **Placement under your control.** Choose storage classes and placement groups
  for buckets. Capacity-driven range migration distributes data across eligible
  volumes; volume and node drains move data before hardware is removed.
- **Several ways to use the same storage.** Mount files with FUSE, use native
  object operations or connect an S3 client to the daemon's HTTP endpoint.
  Each frontend has its own data format; use a dedicated bucket for each.
  Block-device access is part of the planned expansion.

The zero-config starting experience uses generated defaults and dashboard setup;
advanced configuration remains available as your requirements grow.

## What would you put on it?

| Use case | What STQR is designed to provide |
| --- | --- |
| **Growing media collections and NAS** | A mounted namespace backed by storage that can grow across disks and nodes |
| **VM-host clusters** | A path toward shared virtual-machine storage, with block-device frontends and replication on the roadmap |
| **Database clusters, including memcp** | Native object access, partial updates, append and CAS, with application-level coordination kept in the database |
| **S3 object storage** | An integrated S3-compatible endpoint for existing SDKs, upload tools and applications |
| **PHP and web-server clusters** | A shared view of application scripts, with optional private writable overlays for configuration and local files |

Choose FUSE for existing file-based applications, S3 for applications using an
S3 SDK, or the native API when your application manages its own object layout.
These access paths use the same bucket administration and storage backend,
from a local daemon to a cluster.

## A concrete use case: shared PHP code, private changes

Several PHP workers or hosts can mount the same code bucket readonly. Each can
add a private upper bucket for configuration and writable application files.
The upper can live on a local STQR server while the shared lower lives on a
remote server. Each application instance gets the same base scripts and its own
writable view, without copying the entire code tree.

```text
                     shared code bucket
                       /           \
                 readonly         readonly
                    |                 |
             private upper A   private upper B
                    |                 |
               PHP mount A       PHP mount B
```

The overlay stores deletion markers in directory metadata. A permissions-only
change keeps a reference to the original content; a partial write stores the
changed ranges and references the remaining lower data. This avoids copying a
whole large file just to change a few bytes.

A whitelist such as `conf.json,files/` restricts writes to that file and
directory tree. It is persisted in the upper bucket so a remount retains the
same policy. Reads can still reach the rest of the shared code. This is a
filesystem policy: users with direct writable SDK credentials can access the
bucket outside FUSE.

The integration tests exercise multiple mounts and private uppers, PHP OPcache,
readonly mmap, local `flock`/`LOCK_EX`, permissions, remounts and cleanup.

## Implementation status

STQR implements storage across multiple nodes, an administration dashboard,
a native Go client, Linux FUSE with overlays and an integrated S3 endpoint.
The cluster distributes object ranges across primary owners, routes clients to
those owners and migrates ranges as capacity and placement requirements change.

| Component | Available functionality |
| --- | --- |
| Cluster | Membership and shared administration, client routing, capacity-driven range splits and migration, recovery of interrupted handoffs, volume and node drain |
| Object API | `Get`, `Put`, ranged access, atomic `Append`, `Trunc`, `Delete`, `Stat`, whole-object and ranged CAS, streaming transfers and retry deduplication |
| Storage | File and Linux raw-device volumes, volatile RAM volumes, checksums, crash recovery and raw/gzip/LZ4/xz storage |
| Placement | Bucket storage classes and placement groups, automatic capacity distribution and migration for policy changes |
| Dashboard | Users, bucket grants, S3 access keys, logical/physical quotas and protected guarantees, node and volume management, live statistics and maintenance operations |
| Go client | Authenticated, bucket-bound connections, direct routing to range owners, reconnects and request IDs for retry deduplication |
| S3 | GET/HEAD/PUT/DELETE, range and conditional requests, listing, multipart uploads, server-side copy, bulk delete, checksums, SigV4 and presigned URLs |
| Linux FUSE | Read/write and readonly mounts, permissions, sparse files, rename, symlinks, hardlinks for incremental backups, extended attributes and offline `--fsck` |
| Linux kernel client | Standalone `kstqr` mounts using the native filesystem format; see [kernel documentation](kernel/kstqr/README.md) |
| Windows client | WinFsp drive-letter client sharing the Linux inode format; build and limits in [windows/](windows/README.md) |
| Native overlays | Partial-content copy-up, metadata overrides, whiteouts and persisted write whitelists |

Disk replication uses **one primary and required full replicas per range**.
PG settings select a fixed replica count or EVERYWHERE within the PG's node
mask. Writes require ALL copies; a missing required node never silently reduces
durability. Prepared writes stay invisible, retries retain their original result,
and reads can use a committed replica while the primary is offline. Independent
buckets remain available. Online replica additions, count reductions and codec
changes preserve object bytes and retry receipts. Logical quotas count data once;
physical quotas include every required copy and staging allocation.

RAM replica restore and automatic relocation of existing multi-copy groups are
still pending; multi-copy RAM writes return 501. No automatic writer failover is
performed. Read-cache tiers and erasure coding remain on the roadmap. Local
daemon, fault-injection and opt-in SSH tests cover replication and recovery;
large deployments still need field validation and reproducible benchmarks.
The replication storage records require volume format 4. Formats 2 and 3 remain
readable and upgrade on the next write; older binaries must not reopen an
upgraded volume. See [replication notes](docs/replication.md).

The native store offers atomic operations per object. Applications and frontends
build their own coordination above that interface; STQR does not provide a
multi-object database transaction engine. Persistent volumes use a packed format
with B+-tree metadata and redo recovery, and acknowledge disk writes after the
required local persistence steps. RAM volumes remain volatile. APIs and on-disk
formats may change as development continues.

## Getting started

Build with **Go 1.24 or later**. FUSE additionally requires Linux, `/dev/fuse`
and a suitable fusermount helper. The Go FUSE adapter talks directly to the
kernel; no C libfuse linkage is required.

```sh
git clone https://github.com/launix-de/stqr-storage.git
cd stqr-storage
go build -o stqr .
go build -o stqr-fuse ./cmd/stqr-fuse
./stqr
```

Open <http://localhost:7419>. On first start, the daemon creates `stqr.yaml`;
the initial credentials are under `auth.bootstrap_admin`. Keep this file private.
Use the dashboard to add a data volume, create a bucket and grant a user access.
The reserved admin volume stores accounts and bucket descriptions separately
from your application data.

By default, access is restricted to loopback clients. The daemon currently
serves plain HTTP/WebSocket, including Basic authentication. Use a protected
network, an SSH tunnel or a TLS reverse proxy for remote access. S3 can be exposed
through an Apache or nginx TLS endpoint using the same HTTP server. The
[server guide](docs/server.md) describes configuration and access controls.

### Mount a bucket

Use the bucket's **32-character hexadecimal ID**, not its display name. Replace
the example values below with a bucket and credentials created in the dashboard.

```sh
mkdir -p ./mnt
./stqr-fuse --uri http://localhost:7419 \
  --username alice --password 'PASSWORD' \
  --bucket BUCKET_ID --init ./mnt
```

`--init` creates a missing filesystem root and preserves an existing one. The
bucket must already exist. The mount process runs in the foreground; unmount
from another terminal with `fusermount3 -u ./mnt` (or `fusermount -u ./mnt`).

There are three mount modes:

| Options | Mode |
| --- | --- |
| `--bucket ID` | Normal read/write filesystem |
| `--readonly-bucket ID` | Readonly filesystem |
| Both | Native overlay: readonly lower plus writable upper |

For an overlay, first populate a normal filesystem bucket as the lower and
create a separate empty upper bucket. Then mount with:

```sh
./stqr-fuse --uri http://localhost:7419 \
  --username alice --password 'PASSWORD' \
  --readonly-bucket LOWER_ID --bucket UPPER_ID \
  --whitelist=conf.json,fop-files/ --init ./mnt
```

An initialized overlay upper must be mounted with its matching lower. Separate
`--readonly-uri`, `--readonly-username` and `--readonly-password` options select
another server or account for the lower. `--allow-other` permits other local
users, such as PHP workers, where the host's FUSE configuration allows it;
normal Unix permissions still apply. See `./stqr-fuse -h` and the
[client and FUSE guide](docs/client-api.md) for details.

### Connect an S3 application

Create a dedicated S3 bucket and grant a user access. In the dashboard's user
administration, generate an S3 access key and secret for that user. Existing
bucket grants control read, write and delete permissions; keys can be revoked
without creating a separate S3 account system.

Configure your SDK or tool with:

| Setting | Value |
| --- | --- |
| Endpoint | `http://localhost:7419/s3` |
| Addressing | Path style |
| Bucket | The bucket's display name |
| Region | `eu-west-1` |
| Credentials | The user's S3 access key and secret |

The endpoint runs inside the daemon. It supports ordinary SDK workflows,
including multipart uploads, server-side copy and presigned URLs. S3 metadata
and content are stored together in native objects; replacing or deleting an
object reclaims its previous allocation without FUSE fsck.

Buckets are managed through STQR administration. Public ACLs, object versioning,
nonempty tags, general lifecycle policies and S3 server-side encryption headers
are outside the supported subset. The [S3 guide](docs/s3.md) documents the exact
operations, upload cleanup and reverse-proxy configuration. Preserve the signed
host, path and query when putting the endpoint behind TLS.

### Use the Go API

A connection belongs to exactly one bucket. Its constructor takes the node URL,
username, password and bucket ID; subsequent operations need only the object ID.
Object addresses are native 32-byte values. Applications choose those addresses;
they need not be hashes of the payload.

```go
conn, err := client.Dial(ctx, nodeURL, username, password, bucketID)
if err != nil {
    return err
}
defer conn.Close()

_, err = conn.Put(ctx, objectID, []byte("hello"), client.PutOptions{
    RequestID: client.NewRequestID(),
})
```

Import `github.com/launix-de/stqr-storage/client`. Preserve the request ID and
operation arguments when retrying an unknown outcome. Use streaming methods for
large objects; the convenience buffered read is limited to 16 MiB. More examples
and the retry contract are in the [client guide](docs/client-api.md).

## Planning your deployment

For shared scripts and overlays, use a stable base and give each application
instance its own writable bucket. For native applications, choose the object API
when you want to manage indexing and coordination yourself. Keep filesystem,
S3 and application-native data in separate buckets: they share a storage engine,
not an interchangeable on-disk namespace.

In a cluster, distinguish capacity from availability. An unavailable primary can
stall access to its ranges; known independent ranges can continue through their
own connections. Some shared administration changes require all participating
members. Use the drain operation to evacuate a volume or node before removing it;
a normal shutdown preserves its placement for restart. Persistent volumes provide
local durability, while RAM volumes deliberately lose their contents on restart.

The current FUSE implementation has these operational constraints:

- **Space reclamation is offline.** Unlinked files and superseded chunks can
  leave unreachable objects. Close every mount using the writable bucket, then
  run the same connection options with `--fsck` instead of mounting. For an
  overlay, supply both bucket options. Use dedicated filesystem buckets: fsck
  treats objects outside the filesystem graph as garbage.
- **An overlay's referenced base content must remain stable.** Changing or
  removing a lower inode used by copy-up can produce `ESTALE`. Upper fsck
  removes redundant whiteouts after lower maintenance, but does not rebase
  content or reconnect replaced lower directories. Lower fsck does not track
  offline uppers' dependencies; it can collect data an upper still needs.
  Keep the lower quiescent during upper reconciliation.
- **Multi-mount has explicit coordination limits.** Object subscriptions invalidate
  the native cache. Distributed POSIX locks and writable mmap coherence remain
  separate concerns. Normal Linux filesystem buckets support symlinks and
  hardlinks; native overlays have additional limits described in the
  [filesystem guide](docs/filesystem-posix.md).
- **Native overlays target modest writable namespaces.** Namespace changes
  currently rebuild a sparse manifest, bounded to 64 MiB. This path needs more
  work for very large or intensely modified directory trees.

## How the architecture supports growth

The distributed design separates an object's stable address from its physical
location. Clients route directly using a versioned range map. The planner splits
ranges and migrates them to eligible volumes as capacity and bucket policies
change. Handoffs copy data, catch up writes and commit ownership before reclaiming
the old copy; interrupted transitions have a durable recovery path. Adding
hardware is a storage operation while applications keep their object addresses
and access interface.

That mechanism is the foundation for a broader vision: continuous growth from
one disk to a petabyte-scale installation, with workload-specific placement
instead of a fixed cluster layout. HDD, SSD and RAM storage are available today.
The roadmap extends them with selectable redundancy, read-cache tiers, erasure
coding and block-device integration for workloads such as VM hosts. Replica reads
are intended to serve locally committed data during an in-progress mutation,
keeping hot objects readable
without waiting for the entire write to finish. Applications that need stronger
coordination can provide it above the object interface; acknowledged writes retain
their intended durability contract.

The [architecture specification](STQR_Architekturspezifikation.md), written in
German, describes these mechanisms, their trade-offs and the remaining open
questions. Comparative performance claims await reproducible benchmarks.

## Development and testing

```sh
go test ./...
go test -race ./...
go vet ./...
```

Tests cover the protocol, authorization, retries, volume recovery, filesystem
namespace operations and overlay cleanup. Cluster integration tests use separate
daemons and temporary volumes to exercise admission, capacity expansion, routed
reads and writes, migration, drain and restart recovery. S3 tests cover SDK
compatibility, authentication, checksums, multipart recovery and reclamation.
Real kernel mounts are opt-in:

```sh
go build -race -o stqr-fuse ./cmd/stqr-fuse
STQR_TEST_FUSE=1 STQR_FUSE_BINARY="$PWD/stqr-fuse" go test -race ./... -count=1
```

These tests require a host that permits FUSE mounts. The PHP integration also
needs PHP CLI with OPcache and Python 3. Fixtures use temporary volumes and buckets.
Bug reports with a reproducer, recovery tests and measured workload results are
welcome; larger changes should discuss the relevant storage or protocol contract.

Further reading: [server and administration](docs/server.md),
[client API and FUSE](docs/client-api.md), [S3 access](docs/s3.md),
[volume format](docs/volume-format.md),
and the [architecture specification](STQR_Architekturspezifikation.md).

## License

STQR is licensed under the [GNU General Public License v3.0](LICENSE.md)
(`GPL-3.0-only`).

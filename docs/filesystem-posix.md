# Inode extensions for Linux applications

Normal STQR filesystem buckets now support symbolic links, hardlinks, FIFO and
Unix-socket filesystem nodes, plus `user.*` and `trusted.*` extended attributes.
The Go filesystem API, Linux FUSE adapter and standalone `kstqr` kernel client
share the same representation. Native overlay views expose lower/upper symlink
and xattr reads, but do not yet implement creation of these nodes, hardlinks or
xattr mutations through an overlay. Copy-up of multiply linked lower files is
rejected. If a later base change introduces an inherited name aliasing an inode
already published under a different upper name, directory lookup fails with
`EOPNOTSUPP`; it never exposes the upper content through that protected alias.
This prevents hardlinks from bypassing a path-based overlay whitelist.

## Incremental backups

Normal filesystem buckets support `rsync -aH --link-dest=...` in both Linux
clients. Keep all snapshot directories in the same mounted bucket. For example,
with an existing `/mnt/backup/day1`:

```sh
rsync -aH --link-dest=/mnt/backup/day1 /source/ /mnt/backup/day2/
```

Unchanged files share their inode with the previous snapshot. Changed files get
their own inode, and pruning an old snapshot preserves links in the remaining
snapshots. Treat completed snapshots as immutable: writing an existing hardlink
modifies every name for that inode. Use fresh destination directories and avoid
`--inplace`. ACL preservation with `-A` is not supported. Native overlays do not
support hardlink creation.

The opt-in `TestFilesystemRsyncLinkDest` runs real rsync through a FUSE mount,
including segmented files, source hardlinks, content and mode changes, pruning
and offline object collection (`STQR_TEST_FUSE=1`). Kernel link/unlink semantics
are covered by the kernel smoke test; an rsync run in a kernel VM is separate.

## Format and upgrade

**Upgrade all mounts before enabling the new writer.** Writers use
**STFS v7**, **STIX v3** and **STFT v3** over `stqr.v2`. Data and index block IDs
remain stable during ordinary writes. Inode intents publish exact before/after
metadata bytes and replay bounded in-place payload edits; they are temporary
recovery state, not object revisions. Format 7 resolves the conflicting format-6
layouts used by two development branches. Known old layouts remain decoder-only;
legacy lower inode references must be converted before a new-format write.

Native volumes accept known formats 2/3/4/5 and write format 5. This compatibility
is exercised against every format-checkpoint tag, including repeated restarts.
Do not downgrade volumes or mix filesystem writers with incompatible codecs.

An overlay lower is read-only for the lifetime of its inherited references.
Create a COW bucket when the source must remain writable. New copy-ups record a
lower data fingerprint; violating the read-only contract reports `ESTALE` rather
than silently exposing changed contents. Link-count-only updates do not change
that fingerprint. Bucket COW preserves the actual object bytes recursively.

Extended inode metadata contains:

```text
links:u32
attribute_count:u32
(name:Text, value:Blob)*
```

Names are unique, sorted by UTF-8 bytes and at most 255 bytes. Attribute values
are binary, including empty values. At most 128 attributes are stored, with a
64-KiB per-value limit and a 256-KiB encoded attribute-section limit. All inode
objects remain bounded to 1 MiB. Go setters conservatively account for codec
overhead before attempting publication.

The `kind` field adds:

| Kind | Node | Content |
| --- | --- | --- |
| 1 | Regular file | Existing inline/indexed blob |
| 2 | Directory | Existing STDR entries |
| 3 | Symbolic link | Inline, nonempty target up to 4095 bytes; no NUL |
| 4 | FIFO | Empty |
| 5 | Unix socket | Empty |

Symlink targets may be relative, absolute or nonexistent; VFS resolves them.
Fsck preserves the symlink itself without following its target. Symlink loops
are therefore different from invalid directory graph cycles.

FIFO and Unix socket communication is **local to a mount**. Persistent objects
represent their names and types, not pipe buffers or active socket connections.
Two independent mounts, even on the same host, do not share an IPC endpoint.
Bind mounts of the same existing mount retain its underlying inode identity.

Character/block device nodes are not supported. POSIX ACLs, `security.*` xattrs
and capability xattrs are not implemented. Filesystem xattr operations support
regular files/directories; the adapters require privilege for `trusted.*` and
hide trusted names from unprivileged listings. These local VFS checks do not
replace the server's bucket grants or isolate direct SDK users with write access.

## Link semantics and recovery

Multiple directory entries point to the same inode ID for a hardlink. Content,
permissions, timestamps and xattrs therefore remain shared. Links to directories
and relinking an inode whose final name has been removed are rejected.

Adding/removing a link atomically changes both the directory and the inode's
persistent link count using STFT/STFL intents. Rename replacement decrements the
replaced inode's count in the same intent as the namespace change. Final unlink
publishes count zero; existing open handles remain usable until close, and
unreachable objects are retained until offline fsck.

Directory edits also calculate the directory's own link count from its immediate
subdirectories. Legacy directory metadata can report one (unknown), rather than
incorrectly claiming the leaf-directory count of two. Newly created and edited
directories store their actual count.

Fsck counts incoming references before sweeping. A regular/symlink/special inode's
stored link count must match its names in the normal filesystem namespace, and
a directory must have exactly one parent (the root is counted once). Mismatches
abort before deletion; this version does not guess a repair. This validation
applies to normal buckets, not the merged namespace of native overlay buckets.

Link-count updates compare and replace actual inode bytes under the volume
lock. An overlay's metadata-only copy-up retains references to its read-only lower;
a lower hardlink-count update does not change the data fingerprint.
Referenced lower objects must remain available; lower fsck cannot discover
an offline upper's dependencies automatically.

## Verification

Go tests cover link sharing, rename replacement, directory counts, xattr flags,
concurrent link/unlink, fsck validation and failures before/after every CAS in
link/unlink publication. Real FUSE tests use Linux symlink/link/xattr calls,
Unix stream sockets and FIFO I/O, plus Git init/commit/gc/local-clone/fsck. The QEMU kernel tests exchange these metadata
types with the Go client and finish by running Go fsck over the kernel's graph.

```sh
STQR_TEST_FUSE=1 go test -race ./client -count=1
STQR_TEST_KERNEL=1 STQR_KERNEL_IMAGE=/path/to/matching/vmlinuz \
  go test -race ./server -run '^TestKSTQRVirtualMachine$' -count=1
```

The kernel client supports shared writable mmap, including explicit sync and
background writeback; see [kstqr semantics and tests](kstqr.md). The FUSE client
still uses direct I/O for writable opens and does not provide shared writable
mmap. Full home-application compatibility and POSIX ACLs remain separate work.

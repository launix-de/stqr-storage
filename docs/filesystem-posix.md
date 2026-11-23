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

## Format and upgrade

**Upgrade every filesystem client before using these changes.** The readers
accept existing STFS inode versions 1 through 4, while the extended metadata uses **STFS v5**. Older clients reject v5; there is no transparent downgrade.
The object protocol, bucket IDs, data/index addressing and volume format do not
change. Perform an offline fsck with the upgraded client before returning a
previously shared bucket to service.

STFS v5 retains the v4 header, content layout and suffix, then appends:

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

A link-count update changes the native inode version. Consequently adding or
removing a lower hardlink, including final unlink, invalidates an overlay's
version-pinned reference to that lower inode, even if the payload still exists.
This follows the existing stable-base requirement; content-version rebasing is
not introduced by this format extension.

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

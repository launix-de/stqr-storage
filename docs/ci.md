# Continuous integration

CI runs on pull requests, pushes to master and manual dispatch. It uses disposable
files and localhost listeners; cluster tests start several nodes on different
ports. No SSH host, production cluster, PM2 service or stored service credentials
are needed.

| Job | Activated checks |
| --- | --- |
| Go / root | `go vet ./...`; race suite for the client, volume, NBD and CLI packages, including crash/fault injection and parser fuzz seeds |
| Go / server shards 0–3 | Complete automatically discovered server tests/examples/fuzz seeds, including operations, localhost clusters, quotas and migrations, partitioned into disjoint processes with race detector; avoids one long bcrypt-heavy package exceeding its job budget |
| Go / networkboot | Independent Go module with race detector, static helpers, PAM build and real private PAM harness, Python workflows and shell syntax |
| Go / windows | Portable filesystem and settings tests with race detector on Linux |
| Debian | Real `.deb` build/extraction, Lintian, embedded version, isolated upgrade/abort/failure hooks; no host installation |
| Dashboard | JavaScript tests and all existing browser acceptance tests with a pinned Playwright Chromium |
| Linux integration | C library/parser and live client with ASan/UBSan, actual FUSE mounts/PHP/rsync, QEMU NBD and localhost cluster interoperability |
| Windows | Native Windows callbacks, registry/DPAPI setup, lifecycle tests and executable build; no driver mount is claimed |
| Tagged formats | Every tag produces data with its own code; current server and current format readers verify the same files, then verify again after restart |

Full kernel-module VM tests, physical PXE/installer tests, actual Windows drive
mounts, SSH/PM2 deployment tests, optional external AWS/rclone/PHP SDK tests and
throughput benchmarks remain separate environment-dependent acceptance checks.
The ordinary suite already tests clusters through localhost; cluster CI does not
need an external machine. Fuzz seeds run normally; long fuzz campaigns are not
represented as passing merely because the seed corpus passed.

## Retained deployment and release checkpoints

Keep a checkpoint for deployed or released data that current readers must still
support. The retained deployed-master checkpoint is the baseline during alpha
development; intermediate source edits do not require additional tags. Never
move an existing retained tag. A checkpoint must include a working
`server/format_compat_test.go` producer. Future fixture/manifest changes must
preserve the current reader's ability to verify its older manifests.

When format-owning files change, CI requires a retained checkpoint to be an
ancestor of the PR/push base and tests every retained tag. Changes between that
checkpoint and the base do not create a new checkpoint obligation: compatibility
is decided by reading the generated data, not by identical source files.
`FORMAT_PATHS` lists format owners; extend it when adding an owner. Manual runs
can select a comparison base with `--base COMMIT`.

`tools/compat/check.py` enumerates **all** Git tags, including annotated and
lightweight tags. For each tag it:

1. Exports the immutable tagged source and compiles its fixture/server with the
   Go toolchain recorded in that version's `go.mod`.
2. Starts the actual tagged server implementation on localhost. Its tagged SDK
   writes persistent data and a manifest of independently expected byte hashes,
   sizes, identities and metadata, then closes all connections and the server.
3. Starts the current server implementation using that exact configuration,
   system graph, journals and volume files. The **current** SDK/filesystem/S3/
   block-device readers check every recorded datum. Successful APPEND replies
   must remain idempotent. Bucket/PG policies and native volume checksums are
   checked too.
4. Stops that process and runs the current verifier again against the resulting
   disk state, checking that upgrade/recovery changes survive another restart.

Current producer cases cover raw/gzip/lz4/xz, large and sparse native objects,
retained APPEND requests and deletes; inline/segmented/sparse files, modes,
ownership, hardlinks, symlinks and xattrs; named striped block devices with
cross-stripe writes and zero holes; and S3 payloads, listings and metadata.
Extend the producer before tagging a newly introduced persistent structure.
Regular operation/cluster tests remain necessary: this is compatibility testing
of the generated corpus, not a claim to enumerate every possible disk state.

Missing producers, unknown manifest versions/case types, empty tag sets and any
build/start/read/hash/metadata failure fail CI. No historical tag is silently
skipped. Tags predating this harness require an explicit historical adapter.
Per-tag build/seed/verify logs and manifests are retained as CI artifacts; fixture
disks and test credentials are not uploaded.

For a local bootstrap smoke test only:

```sh
python3 tools/compat/check.py --tag HEAD
```

This requires the fixture to be committed because the producer comes from Git,
not from uncommitted working files. Normal CI does not use this override and
requires at least one pushed checkpoint/release tag.

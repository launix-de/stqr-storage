# Gates: Storage release safety

OWNS: GATES.md, volume/**, server/**, client/c/**, kernel/kstqr/**, qemu-plugin-nbd/**, docs/storage-release-safety.md

Scope: Investigate the injected cyclic tree and production tree writers; bound client-controlled volume-lock lifetime; require read and write for CAS; verify sparse BD reads, RAM without placement, and backend-outage retry. Record FS reclamation and equal-size in-place writes for all clients in a separate issue without implementing them.

- [x] G1: Corrupt trees return ErrCorrupt and normal split/merge/reopen operations retain an acyclic tree.
  CHECK: go test ./volume -run 'TestStorageSafetyTree' -count=1 -v
  EXPECT: /^ok[ \t]+github\.com\/launix-de\/stqr-storage\//m
  EVIDENCE: automatic-evidence=v1; definition-sha256=ee1a3a610e79e210aa07a68b296688876224816ca5852636e6e25243c289b817; exit=0; EXPECT=matched; output-sha256=db78dc67a0504f9a0d662f58038626804b7dd4fa8822e5e44354f5116fbdf590; output-bytes=1412; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

- [x] G2: Slow or stalled client transfers cannot retain a volume handle; CAS requires both read and write; RAM without placement returns safely.
  CHECK: go test ./server -run 'TestStorageSafety' -count=1 -v -timeout=3m
  EXPECT: /^ok[ \t]+github\.com\/launix-de\/stqr-storage\//m
  EVIDENCE: automatic-evidence=v1; definition-sha256=6f1f10a5645b8fcb80332e3649f9d68b0192cf0fcc357c9f8afde7bc92354d5f; exit=0; EXPECT=matched; output-sha256=c1ba513fef2ffc4e4e5aac968b757310ffaf1044347dfa78c1b6836f89883dab; output-bytes=826; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

- [x] G3: Sparse block reads zero-fill only valid disk ranges and preserve populated prefixes through the real server.
  CHECK: go test ./qemu-plugin-nbd ./server -run 'TestStorageSafetySparse|Test.*Sparse' -count=1 -v -timeout=3m
  EXPECT: /^ok[ \t]+github\.com\/launix-de\/stqr-storage\//m
  EVIDENCE: automatic-evidence=v1; definition-sha256=930cf636c24444ba191519cc27c5f0e098ceb7e6581a5968c26b985532f3c3c0; exit=0; EXPECT=matched; output-sha256=16dac3253ec6e64bc89debb1fb3ed77358d9e899fba7f8dfc85285abc0442d74; output-bytes=955; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

- [x] G4: The kernel filesystem waits through a temporary backend outage and replays an uncertain mutation with its original identity.
  CHECK: env STQR_TEST_KERNEL=1 STQR_KERNEL_OUTAGE=1 STQR_KERNEL_IMAGE=/tmp/stqr-kernel-vm/kernel/boot/vmlinuz-7.0.0-38-generic STQR_KERNEL_HEADERS=/lib/modules/7.0.0-38-generic/build go test ./server -run '^TestKSTQRVirtualMachine$' -count=1 -v -timeout=10m
  EXPECT: /^--- PASS: TestKSTQRVirtualMachine/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=7a3e16b6225eb1cd56344f691a83f58bab073b68038792a0c474de8d5d717a19; exit=0; EXPECT=matched; output-sha256=9756e9ba3bf8ce767eabd9684c5e2222055723ace404d07b82fbee8d24a05621; output-bytes=134; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

- [x] G5: Volume, client, NBD and server regression suites pass with the race detector.
  CHECK: env GOMAXPROCS=4 go test -race ./volume ./client ./qemu-plugin-nbd ./server -parallel=2 -count=1 -timeout=25m
  EXPECT: /ok[ \t]+github\.com\/launix-de\/stqr-storage\/server/
  EVIDENCE: automatic-evidence=v1; definition-sha256=0fc84a4a88f69f4f82a1081805b63dad897756c79b50666a8dbd5f2df438722b; exit=0; EXPECT=matched; output-sha256=4191de58fc1f4e130ecd53e7069460c3bd4da2a3437fc9056dc15fb64d3a3fd3; output-bytes=225; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

- [x] G6: The investigation documents actual cycle provenance, all external lock paths, code and test limitations; a separate FS/in-place issue covers every implemented client and remains unimplemented.
  EVIDENCE: Reviewed docs/storage-release-safety.md against tree writers/readers, native/S3/peer/COW transfer boundaries and the corrected tests. Cycle provenance is direct test injection, not an established production writer defect. Issue https://github.com/launix-de/stqr-storage/issues/32 is English and remains backlog only. Hardware-stall, server-incarnation and BD lease-fencing limits are explicit.

- [x] G7: Native kernel filesystem and block lifecycle, sparse tails, interoperability and clean unload regressions pass in disposable VMs.
  CHECK: env STQR_TEST_KERNEL=1 STQR_KERNEL_IMAGE=/tmp/stqr-kernel-vm/kernel/boot/vmlinuz-7.0.0-38-generic STQR_KERNEL_HEADERS=/lib/modules/7.0.0-38-generic/build go test ./server -run '^TestKSTQR(Block)?VirtualMachine$' -count=1 -v -timeout=10m
  EXPECT: /^--- PASS: TestKSTQRBlockVirtualMachine/m
  EVIDENCE: automatic-evidence=v1; definition-sha256=be96b1d2f826536e6de38ade23396375c3ef57a8fbc2e608c84aa4dc0f43b635; exit=0; EXPECT=matched; output-sha256=e195c774365cae2a9a7ed31befb0b9fc19ae32e59f3eb14b70bfb3034e389639; output-bytes=507; shell=/bin/sh; cwd=/home/carli/projekte/stqr/.worktrees/storage-release-safety; path=859c708778a7/24 entries

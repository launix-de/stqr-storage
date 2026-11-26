# SPDX-License-Identifier: GPL-3.0-only
.DEFAULT_GOAL := all

GO ?= go
GOFLAGS ?=
export GO GOFLAGS

# KDIR may point to headers for a different target kernel.
KDIR ?= /lib/modules/$(shell uname -r)/build
VERSION ?=
DEB_OUT ?= dist

.PHONY: all server fuse stqr stqr-fuse c-client c-integration kernel \
	kernel-module kernel-helper nbd pam networkboot windows windows-resources \
	deb clean help

all: server fuse c-client kernel nbd networkboot windows

server: stqr
stqr:
	$(GO) build -buildvcs=false -o stqr .

fuse: stqr-fuse
stqr-fuse:
	$(GO) build -buildvcs=false -o stqr-fuse ./cmd/stqr-fuse

c-client:
	$(MAKE) -C client/c all

# Build the C test executable without connecting to a running daemon.
c-integration: c-client
	$(MAKE) -C client/c tests/integration

kernel: kernel-module kernel-helper
kernel-module:
	$(MAKE) -C kernel/kstqr all KDIR="$(KDIR)"

kernel-helper:
	$(MAKE) -C kernel/kstqr helper

nbd:
	$(MAKE) -C qemu-plugin-nbd all

pam:
	$(MAKE) -C networkboot pam

# Networkboot's standalone Makefile also builds NBD/PAM. Complete the shared
# outputs first so parallel root targets cannot write those files concurrently.
networkboot: nbd pam
	$(MAKE) -C networkboot all

windows: windows/cmd/stqr-windows/resource_windows_amd64.syso
	$(MAKE) -C windows all

windows-resources: windows/cmd/stqr-windows/resource_windows_amd64.syso
windows/cmd/stqr-windows/resource_windows_amd64.syso: windows/cmd/stqr-windows/app.rc windows/cmd/stqr-windows/app.manifest
	$(MAKE) -C windows resources

# Packaging is explicit; all builds binaries without installing or loading them.
deb:
	./tools/build-deb.sh "$(VERSION)" "$(DEB_OUT)"

clean:
	$(RM) stqr stqr-fuse
	$(MAKE) -C client/c clean
	$(MAKE) -C networkboot clean
	$(MAKE) -C qemu-plugin-nbd clean
	$(MAKE) -C windows clean
	$(RM) kernel/kstqr/mount.kstqr kernel/kstqr/*.o kernel/kstqr/*.ko \
		kernel/kstqr/*.mod kernel/kstqr/*.mod.c kernel/kstqr/.module-common.o \
		kernel/kstqr/Module.symvers \
		kernel/kstqr/modules.order kernel/kstqr/.*.cmd kernel/kstqr/.*.d
	rm -rf kernel/kstqr/.tmp_versions

help:
	@printf '%s\n' \
		'make all                Build every production component (default; Linux build host)' \
		'make server             Build ./stqr' \
		'make fuse               Build ./stqr-fuse (GOFLAGS=-race for race instrumentation)' \
		'make c-client           Build static/shared C SDK libraries' \
		'make c-integration      Build the C integration test executable' \
		'make kernel             Build the Linux module and mount helper (KDIR=... supported)' \
		'make kernel-module      Build kernel/kstqr/kstqr.ko' \
		'make kernel-helper      Build kernel/kstqr/mount.kstqr' \
		'make nbd                Build the NBD server and native boot client' \
		'make pam                Build networkboot/bin/pam_stqr.so' \
		'make networkboot        Build networkboot tools, boot helpers, PAM and source archive' \
		'make windows            Cross-compile windows/bin/stqr-windows.exe (amd64)' \
		'make windows-resources  Update the Windows resource when its sources changed (LLVM)' \
		'make deb VERSION=...    Build a Debian package (DEB_OUT=dist by default)' \
		'make clean              Remove generated build artifacts; preserve packages in dist/'

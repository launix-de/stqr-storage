# SPDX-License-Identifier: GPL-3.0-only
GO ?= go
CC ?= cc
PAM_CFLAGS ?=
PAM_LIBS ?= -l:libpam.so.0
PREFIX ?= /usr
PAM_MODULE_DIR ?= $(PREFIX)/lib/$(shell $(CC) -dumpmachine)/security
DESTDIR ?=
.PHONY: all pam check clean install
all: pam
	mkdir -p bin
	CGO_ENABLED=0 $(GO) build -buildvcs=false -o bin/stqr-bootctl ./cmd/stqr-bootctl
	CGO_ENABLED=0 $(GO) build -buildvcs=false -o bin/stqr-boot-nfs ./cmd/stqr-boot-nfs
	$(MAKE) -C ../qemu-plugin-nbd all
	install -m 0755 ../qemu-plugin-nbd/bin/stqr-nbd-client bin/
	$(CC) -std=c11 -Wall -Wextra -Werror -O2 -static -o bin/stqr-boot-root linux-client/boot-root.c
	$(CC) -std=c11 -Wall -Wextra -Werror -O2 -static -o bin/mount.kstqr ../kernel/kstqr/mount.c
	python3 -c "from common import source_archive; source_archive('bin/kstqr-source.tar')"
pam:
	mkdir -p bin
	$(CC) -std=c11 -Wall -Wextra -Werror -fPIC -shared -O2 $(PAM_CFLAGS) -o bin/pam_stqr.so pam/pam_stqr.c $(PAM_LIBS)
check:
	$(MAKE) -C ../qemu-plugin-nbd check
	$(GO) test -race ./...
	python3 -m unittest discover -s tests -v
	sh -n initramfs/hooks/stqr initramfs/scripts/stqr
install: all pam
	install -d $(DESTDIR)$(PREFIX)/libexec $(DESTDIR)$(PAM_MODULE_DIR) $(DESTDIR)$(PREFIX)/sbin
	install -m 0755 bin/stqr-bootctl pam/stqr-home $(DESTDIR)$(PREFIX)/libexec/
	install -m 0755 bin/pam_stqr.so $(DESTDIR)$(PAM_MODULE_DIR)/
	install -d $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/bin
	install -m 0755 prepare-system edit-image serve test-boot $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/
	install -m 0644 common.py firstboot.py sessions.py native_sessions.py $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/
	cp -R initramfs $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/
	install -d $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/pam
	install -m 0755 pam/stqr-home pam/install-login $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/pam/
	install -m 0644 bin/pam_stqr.so $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/bin/
	install -m 0755 bin/stqr-bootctl bin/stqr-boot-nfs bin/stqr-nbd-client bin/stqr-boot-root bin/mount.kstqr $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/bin/
	install -m 0644 bin/kstqr-source.tar $(DESTDIR)$(PREFIX)/libexec/stqr-networkboot/bin/
	ln -sf ../libexec/stqr-networkboot/prepare-system $(DESTDIR)$(PREFIX)/sbin/stqr-prepare-system
	ln -sf ../libexec/stqr-networkboot/edit-image $(DESTDIR)$(PREFIX)/sbin/stqr-edit-image
	ln -sf ../libexec/stqr-networkboot/serve $(DESTDIR)$(PREFIX)/sbin/stqr-networkboot
	ln -sf ../libexec/stqr-networkboot/test-boot $(DESTDIR)$(PREFIX)/sbin/stqr-test-boot

clean:
	rm -rf bin __pycache__ tests/__pycache__

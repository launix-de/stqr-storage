# SPDX-License-Identifier: GPL-3.0-only
ifneq ($(KERNELRELEASE),)
obj-m += kstqr.o
kstqr-y := vfs.o protocol.o port.o cache.o block.o
ccflags-y += -I$(src)/../../client/c/include -I$(src)/../../client/c/src
else
KDIR ?= /lib/modules/$(shell uname -r)/build
all:
	$(MAKE) -C $(KDIR) M=$(CURDIR) modules
helper: mount.kstqr
mount.kstqr: mount.c
	$(CC) -std=c11 -Wall -Wextra -Werror -O2 -g -o $@ $<
clean:
	$(MAKE) -C $(KDIR) M=$(CURDIR) clean
.PHONY: all helper clean
endif

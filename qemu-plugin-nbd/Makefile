# SPDX-License-Identifier: GPL-3.0-only
CC ?= cc
PREFIX ?= /usr
DESTDIR ?=
.PHONY: all check clean install
all:
	mkdir -p bin
	CGO_ENABLED=0 go build -buildvcs=false -o bin/stqr-nbd ./cmd/stqr-nbd
	$(CC) -std=c11 -Wall -Wextra -Werror -static -O2 -o bin/stqr-nbd-client linux-client/main.c
check:
	go test -race ./...
install: all
	install -d $(DESTDIR)$(PREFIX)/bin $(DESTDIR)$(PREFIX)/libexec
	install -m 0755 bin/stqr-nbd $(DESTDIR)$(PREFIX)/bin/
	install -m 0755 bin/stqr-nbd-client $(DESTDIR)$(PREFIX)/libexec/
clean:
	rm -rf bin

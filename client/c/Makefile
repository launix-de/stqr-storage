# SPDX-License-Identifier: GPL-3.0-only
CC ?= cc
AR ?= ar
CFLAGS ?= -O2 -g
CPPFLAGS += -Iinclude -Isrc
WARN = -std=c11 -Wall -Wextra -Werror -Wpedantic
LDLIBS += -lcrypto
SOURCES = src/core.c src/posix.c
OBJECTS = $(SOURCES:.c=.o)
all: libstqr.a libstqr.so
%.o: %.c include/stqr.h src/port.h
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARN) -fPIC -c $< -o $@
libstqr.a: $(OBJECTS)
	$(AR) rcs $@ $^
libstqr.so: $(OBJECTS)
	$(CC) -shared -Wl,-soname,libstqr.so -o $@ $^ $(LDLIBS)
tests/integration: tests/integration.c libstqr.a
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARN) -o $@ $< libstqr.a $(LDLIBS) -pthread
check: tests/integration
	./tests/integration
clean:
	rm -f $(OBJECTS) libstqr.a libstqr.so tests/integration
.PHONY: all check clean

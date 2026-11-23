/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <linux/nbd.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
static void fail(const char *what) {
  perror(what);
  exit(1);
}
static void readall(int fd, void *p, size_t n) {
  while (n) {
    ssize_t r = read(fd, p, n);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0) {
      errno = EIO;
      fail("NBD read");
    }
    p = (char *)p + r;
    n -= (size_t)r;
  }
}
static void writeall(int fd, const void *p, size_t n) {
  while (n) {
    ssize_t r = write(fd, p, n);
    if (r < 0 && errno == EINTR)
      continue;
    if (r <= 0)
      fail("NBD write");
    p = (const char *)p + r;
    n -= (size_t)r;
  }
}
int main(int argc, char **argv) {
  if (argc != 4) {
    fputs("Usage: stqr-nbd-client IPv4 PORT /dev/nbdN\n", stderr);
    return 2;
  }
  char *end;
  long port = strtol(argv[2], &end, 10);
  if (*end || port < 1 || port > 65535)
    return 2;
  struct sockaddr_in address = {.sin_family = AF_INET,
                                .sin_port = htons((uint16_t)port)};
  if (inet_pton(AF_INET, argv[1], &address.sin_addr) != 1)
    return 2;
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0)
    fail("socket");
  if (connect(sock, (struct sockaddr *)&address, sizeof(address)))
    fail("connect");
  uint64_t magic, options, size;
  uint16_t hello, flags;
  readall(sock, &magic, 8);
  readall(sock, &options, 8);
  readall(sock, &hello, 2);
  if (be64toh(magic) != UINT64_C(0x4e42444d41474943) ||
      be64toh(options) != UINT64_C(0x49484156454f5054) ||
      (be16toh(hello) & 3) != 3) {
    fputs("unsupported NBD handshake\n", stderr);
    return 1;
  }
  uint32_t cf = htobe32(3), op = htobe32(1), length = 0;
  writeall(sock, &cf, 4);
  writeall(sock, &options, 8);
  writeall(sock, &op, 4);
  writeall(sock, &length, 4);
  readall(sock, &size, 8);
  readall(sock, &flags, 2);
  size = be64toh(size);
  flags = be16toh(flags);
  if (!size || size % 4096 || size / 4096 > ~0UL || flags & 2) {
    fputs("invalid writable NBD export\n", stderr);
    return 1;
  }
  int dev = open(argv[3], O_RDWR | O_CLOEXEC);
  if (dev < 0)
    fail("open NBD device");
  if (ioctl(dev, NBD_SET_SOCK, sock) || ioctl(dev, NBD_SET_BLKSIZE, 4096) ||
      ioctl(dev, NBD_SET_SIZE_BLOCKS, (unsigned long)(size / 4096)) ||
      ioctl(dev, NBD_SET_FLAGS, (unsigned long)flags))
    fail("configure NBD");
  pid_t pid = fork();
  if (pid < 0)
    fail("fork");
  if (pid) {
    char path[256], data[64];
    const char *name = strrchr(argv[3], '/');
    name = name ? name + 1 : argv[3];
    snprintf(path, sizeof(path), "/sys/block/%s/pid", name);
    for (unsigned n = 0; n < 500; n++) {
      int f = open(path, O_RDONLY | O_CLOEXEC);
      if (f >= 0) {
        ssize_t count = read(f, data, sizeof(data) - 1);
        close(f);
        if (count > 0) {
          data[count] = 0;
          if (strtol(data, NULL, 10) > 0) {
            close(dev);
            close(sock);
            return 0;
          }
        }
      }
      int status;
      if (waitpid(pid, &status, WNOHANG) == pid) {
        fputs("NBD handler exited before activation\n", stderr);
        return 1;
      }
      usleep(10000);
    }
    fputs("NBD activation timed out\n", stderr);
    kill(pid, SIGTERM);
    return 1;
  }
  if (setsid() < 0)
    fail("setsid");
  /* /run is moved into the real root; do not pin the disposable initramfs. */
  if (chdir("/run") || chroot(".") || chdir("/"))
    fail("NBD runtime root");
  signal(SIGHUP, SIG_IGN);
  int result = ioctl(dev, NBD_DO_IT);
  if (result < 0)
    perror("NBD_DO_IT");
  ioctl(dev, NBD_CLEAR_QUE);
  ioctl(dev, NBD_CLEAR_SOCK);
  close(dev);
  close(sock);
  return result < 0 ? 1 : 0;
}

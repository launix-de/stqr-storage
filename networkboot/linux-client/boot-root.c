/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

static void fail(const char *s) {
  perror(s);
  exit(1);
}
static int full(int fd, const char *p, size_t n) {
  while (n) {
    ssize_t k = write(fd, p, n);
    if (k < 0 && errno == EINTR)
      continue;
    if (k <= 0)
      return -1;
    p += k;
    n -= k;
  }
  return 0;
}
static int hex(const char *p, size_t n) {
  if (strlen(p) != n)
    return 0;
  for (size_t i = 0; i < n; i++)
    if (!strchr("0123456789abcdef", p[i]))
      return 0;
  return 1;
}
static void mounted(const char *uri, const char *upper, const char *lower,
                    int lifetime, const char *target, int edit) {
  char life[32];
  snprintf(life, sizeof life, "%d", lifetime);
  pid_t child = fork();
  if (child < 0)
    fail("fork");
  if (!child) {
    if (edit)
      execl("/bin/mount.kstqr", "mount.kstqr", "--uri", uri, "--bucket", upper,
            "--credentials", "/run/stqr/credentials", "--boot-lifetime-fd", life,
            target, NULL);
    else
      execl("/bin/mount.kstqr", "mount.kstqr", "--uri", uri, "--bucket", upper,
          "--readonly-bucket", lower, "--credentials", "/run/stqr/credentials",
          "--boot-lifetime-fd", life, target, NULL);
    fail("mount.kstqr");
  }
  int status;
  while (waitpid(child, &status, 0) < 0)
    if (errno != EINTR)
      fail("waitpid");
  if (!WIFEXITED(status) || WEXITSTATUS(status)) {
    unlink("/run/stqr/credentials");
    errno = EIO;
    fail("kstqr native root mount");
  }
}

int main(int argc, char **argv) {
  if (argc != 5) {
    fputs("Usage: stqr-boot-root SERVER PORT TOKEN ROOTMOUNT\n", stderr);
    return 2;
  }
  char *end;
  long port = strtol(argv[2], &end, 10);
  struct sockaddr_in addr = {.sin_family = AF_INET};
  if (*end || port < 1 || port > 65535 ||
      inet_pton(AF_INET, argv[1], &addr.sin_addr) != 1 || !hex(argv[3], 64)) {
    errno = EINVAL;
    fail("boot endpoint");
  }
  addr.sin_port = htons((unsigned short)port);
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    fail("socket");
  struct timeval timeout = {.tv_sec = 30};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
  setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof timeout);
  int yes = 1, idle = 60, interval = 10, count = 6;
  setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof yes);
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof idle);
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof interval);
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof count);
  if (connect(fd, (struct sockaddr *)&addr, sizeof addr))
    fail("claim connect");
  char request[512];
  int len = snprintf(request, sizeof request,
                     "GET /claim/%s HTTP/1.1\r\nHost: %s:%ld\r\nConnection: "
                     "keep-alive\r\n\r\n",
                     argv[3], argv[1], port);
  if (full(fd, request, len))
    fail("claim write");
  char header[4096];
  size_t used = 0;
  while (used < sizeof(header) - 1) {
    ssize_t k = read(fd, header + used, 1);
    if (k < 0 && errno == EINTR)
      continue;
    if (k != 1)
      fail("claim header");
    header[++used] = 0;
    if (used >= 4 && !memcmp(header + used - 4, "\r\n\r\n", 4))
      break;
  }
  if (!strstr(header, "\r\n\r\n") || strncmp(header, "HTTP/1.1 200 ", 13)) {
    errno = EPROTO;
    fail("claim rejected");
  }
  char *length = strcasestr(header, "\r\nContent-Length: ");
  if (!length) {
    errno = EPROTO;
    fail("claim length");
  }
  long size = strtol(length + 18, &end, 10);
  if (strncmp(end, "\r\n", 2) || size < 1 || size > 8191) {
    errno = EPROTO;
    fail("claim size");
  }
  char body[8192];
  used = 0;
  while (used < (size_t)size) {
    ssize_t n = read(fd, body + used, size - used);
    if (n < 0 && errno == EINTR)
      continue;
    if (n <= 0)
      fail("claim body");
    used += n;
  }
  body[used] = 0;
  char *cursor = body, *line = strsep(&cursor, "\n"), *uri = NULL, *user = NULL,
       *pass = NULL, *lower = NULL, *upper = NULL, *mode = "overlay";
  if (!line || strcmp(line, "STQR-BOOT-1")) {
    errno = EPROTO;
    fail("claim format");
  }
  while ((line = strsep(&cursor, "\n")) && *line) {
    char *value = strchr(line, '=');
    if (!value || strchr(line, '\r')) {
      errno = EPROTO;
      fail("claim field");
    }
    *value++ = 0;
    if (!strcmp(line, "uri"))
      uri = value;
    else if (!strcmp(line, "username"))
      user = value;
    else if (!strcmp(line, "password"))
      pass = value;
    else if (!strcmp(line, "lower"))
      lower = value;
    else if (!strcmp(line, "upper"))
      upper = value;
    else if (!strcmp(line, "mode"))
      mode = value;
  }
  if (!uri || strncmp(uri, "http://", 7) || !user || !*user || !pass ||
      !*pass || !lower || !hex(lower, 32) || !upper || !hex(upper, 32)) {
    errno = EPROTO;
    fail("claim credentials");
  }
  int edit = !strcmp(mode, "edit");
  if ((!edit && strcmp(mode, "overlay")) || (!strcmp(upper, lower)) != edit) {
    errno = EPROTO;
    fail("claim mount mode");
  }
  mkdir("/run/stqr", 0700);
  int cred = open("/run/stqr/credentials",
                  O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
  if (cred < 0)
    fail("credentials");
  if (dprintf(cred, "username=%s\npassword=%s\n", user, pass) < 0)
    fail("credentials write");
  close(cred);
  mounted(uri, upper, lower, fd, argv[4], edit);
  unlink("/run/stqr/credentials");
  explicit_bzero(body, sizeof body);
  close(fd);
  return 0;
}

/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "outage line %d: %s (%s)\n", __LINE__, #x,               \
              strerror(errno));                                                \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static double now(void) {
  struct timespec t;
  CHECK(!clock_gettime(CLOCK_MONOTONIC, &t));
  return t.tv_sec + t.tv_nsec / 1e9;
}
static void control(const char *base, const char *path) {
  char url[512];
  snprintf(url, sizeof(url), "%s/%s", base, path);
  pid_t p = fork();
  CHECK(p >= 0);
  if (!p) {
    execl("/bin/busybox", "wget", "-q", "-O", "/dev/null", url, NULL);
    _exit(127);
  }
  int st;
  CHECK(waitpid(p, &st, 0) == p && WIFEXITED(st) && WEXITSTATUS(st) == 0);
}
static void mixed_busy(const char *base) {
  int fd = open("/mnt/busy-mutable", O_RDWR);
  CHECK(fd >= 0);
  CHECK(!fsync(fd));
  control(base, "busy-stat");
  CHECK(pwrite(fd, "S", 1, 32768) == 1);
  CHECK(!fsync(fd));
  char byte;
  CHECK(pread(fd, &byte, 1, 32768) == 1 && byte == 'S');
  CHECK(!close(fd));
  control(base, "check-busy-stat");
  control(base, "busy-delete");
  CHECK(!unlink("/mnt/busy-mutable"));
  control(base, "check-busy-delete");
  puts("KSTQR_MIXED_BUSY_PASS");
}
int main(int argc, char **argv) {
  CHECK(argc == 2 || argc == 3);
  if (argc == 3) {
    CHECK(!strcmp(argv[2], "mixed-busy"));
    mixed_busy(argv[1]);
    return 0;
  }
  control(argv[1], "busy-read");
  int busy = open("/mnt/busy-cold", O_RDONLY);
  CHECK(busy >= 0);
  char byte;
  CHECK(pread(busy, &byte, 1, 32768) == 1 && byte == 'B');
  CHECK(!close(busy));
  control(argv[1], "check-busy");
  int fd = open("/mnt/outage-cold", O_RDONLY);
  CHECK(fd >= 0);
  volatile unsigned char *p = mmap(NULL, 65536, PROT_READ, MAP_SHARED, fd, 0);
  CHECK(p != MAP_FAILED);
  int fds[12];
  for (int i = 0; i < 12; i++) {
    char name[128];
    snprintf(name, sizeof(name), "/mnt/parallel-cold-%d", i);
    fds[i] = open(name, O_RDONLY);
    CHECK(fds[i] >= 0);
  }
  /* Make open-reference mutations durable before intentionally losing the
   * socket. This scenario tests read recovery, not lost unsynced writes. */
  CHECK(!fsync(fd));
  control(argv[1], "offline");
  pid_t children[13];
  for (int i = 0; i < 12; i++) {
    children[i] = fork();
    CHECK(children[i] >= 0);
    if (!children[i]) {
      char value;
      _exit(pread(fds[i], &value, 1, 0) == 1 && value == 'P' ? 0 : 1);
    }
  }
  children[12] = fork();
  CHECK(children[12] >= 0);
  if (!children[12]) {
    struct statfs space;
    _exit(statfs("/mnt", &space) == 0 ? 0 : 1);
  }
  double started = now();
  CHECK(p[32768] == 'R');
  CHECK(now() - started >= 1.0);
  for (int i = 0; i < 13; i++) {
    int status;
    CHECK(waitpid(children[i], &status, 0) == children[i]);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    if (i < 12)
      CHECK(!close(fds[i]));
  }
  control(argv[1], "check-recovery");
  CHECK(!munmap((void *)p, 65536));
  CHECK(!close(fd));
  int append = open("/mnt/append-once", O_RDWR | O_APPEND);
  CHECK(append >= 0);
  CHECK(!fsync(append));
  control(argv[1], "lose-cas-reply");
  started = now();
  CHECK(write(append, "once", 4) == 4);
  CHECK(now() - started >= 1.0);
  /* The CAS can recover visibility by its exact after-image. Its lost reply
   * cannot certify all older uncheckpointed requests on that connection;
   * explicit fsync must retain that uncertainty rather than invent durability. */
  errno = 0;
  CHECK(fsync(append) == -1 && errno == ESTALE);
  struct stat info;
  CHECK(!fstat(append, &info));
  CHECK(info.st_size == 4);
  char text[4];
  CHECK(pread(append, text, 4, 0) == 4 && !memcmp(text, "once", 4));
  CHECK(!close(append));
  puts("KSTQR_OUTAGE_PASS");
  return 0;
}

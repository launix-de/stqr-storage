/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "overlay quiet line %d: %s (%s)\n", __LINE__, #x,        \
              strerror(errno));                                                \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static unsigned long long calls(void) {
  char line[4096];
  int selected = 0;
  unsigned long long total = 0;
  FILE *f = fopen("/proc/self/mountstats", "r");
  CHECK(f);
  while (fgets(line, sizeof(line), f)) {
    if (!strncmp(line, "device ", 7))
      selected = strstr(line, " mounted on /mnt with fstype kstqr") != NULL;
    if (selected) {
      char *m = strstr(line, "cache_misses="), *g = strstr(line, "rpc_get=");
      if (m && g)
        total += strtoull(m + 13, NULL, 10) + strtoull(g + 8, NULL, 10);
    }
  }
  CHECK(!fclose(f));
  return total;
}
static void trace_write(const char *name, const char *value) {
  char path[256];
  snprintf(path, sizeof(path), "/sys/kernel/tracing/%s", name);
  int fd = open(path, O_WRONLY | O_TRUNC);
  CHECK(fd >= 0);
  CHECK(write(fd, value, strlen(value)) == (ssize_t)strlen(value));
  CHECK(!close(fd));
}
static void trace_begin(void) {
  CHECK(!mount("tracefs", "/sys/kernel/tracing", "tracefs", 0, NULL));
  trace_write("tracing_on", "0");
  trace_write("current_tracer", "nop");
  trace_write("trace", "");
  trace_write("set_graph_function", "ks_invalidation_worker");
  trace_write("current_tracer", "function_graph");
  trace_write("tracing_on", "1");
}
static void trace_end(void) {
  char line[4096];
  int observed = 0;
  trace_write("tracing_on", "0");
  FILE *f = fopen("/sys/kernel/tracing/trace", "r");
  CHECK(f);
  while (fgets(line, sizeof(line), f)) {
    if (strstr(line, "ks_invalidation_worker"))
      observed = 1;
    /* Observe real nested calls, not a synthetic counter maintained by the
     * implementation under test. Cache-pressure invalidation must never issue
     * an RPC or resubscribe the entry it just evicted. */
    if (strstr(line, "ks_cache_get") || strstr(line, "stqr_subscribe") ||
        strstr(line, "stqr_unsubscribe") || strstr(line, "stqr_get") ||
        strstr(line, "stqr_cas")) {
      fprintf(stderr, "invalidation performed native I/O: %s", line);
      CHECK(0);
    }
  }
  CHECK(!fclose(f));
  CHECK(observed);
  trace_write("current_tracer", "nop");
  CHECK(!umount("/sys/kernel/tracing"));
  puts("lower eviction invalidation trace contains no native I/O");
}
static void lower_pressure(void) {
  enum { count = 32 };
  char *mapped[count];
  trace_begin();
  alarm(20);
  for (unsigned i = 0; i < count; i++) {
    char path[128];
    snprintf(path, sizeof(path), "/mnt/lower-cache-pressure-%u", i);
    int fd = open(path, O_RDONLY);
    CHECK(fd >= 0);
    mapped[i] = mmap(NULL, 4096, PROT_READ, MAP_SHARED, fd, 0);
    CHECK(mapped[i] != MAP_FAILED);
    CHECK(mapped[i][0] == 'L');
    CHECK(!close(fd));
  }
  sleep(2);
  unsigned long long before = calls();
  sleep(1);
  unsigned long long after = calls();
  printf("lower eviction idle calls: before=%llu after=%llu\n", before, after);
  fflush(stdout);
  CHECK(after == before);
  trace_end();
  for (unsigned i = 0; i < count; i++)
    CHECK(!munmap(mapped[i], 4096));
  alarm(0);
}
int main(int argc, char **argv) {
  if (argc == 2 && !strcmp(argv[1], "--lower-pressure")) {
    lower_pressure();
    return 0;
  }
  CHECK(argc == 2 && !strcmp(argv[1], "--overlay-updates"));
  char initial[4096];
  memset(initial, 'x', sizeof(initial));
  int original = open("/mnt/mmap-evicted", O_CREAT | O_TRUNC | O_RDWR, 0600);
  CHECK(original >= 0);
  CHECK(write(original, initial, sizeof(initial)) == sizeof(initial));
  CHECK(!fsync(original));
  char *mapping =
      mmap(NULL, sizeof(initial), PROT_READ, MAP_SHARED, original, 0);
  CHECK(mapping != MAP_FAILED);
  CHECK(mapping[0] == 'x');
  for (unsigned i = 0; i < 48; i++) {
    char path[128];
    snprintf(path, sizeof(path), "/mnt/quiet-cache-%u", i);
    int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
    CHECK(fd >= 0);
    CHECK(write(fd, "q", 1) == 1);
    CHECK(!close(fd));
  }
  for (unsigned i = 0; i < 48; i++) {
    char path[128];
    struct stat st;
    snprintf(path, sizeof(path), "/mnt/quiet-cache-%u", i);
    CHECK(!stat(path, &st));
  }
  sleep(2);
  unsigned long long before = calls();
  sleep(1);
  unsigned long long after = calls();
  printf("overlay idle cache/network calls: before=%llu after=%llu\n", before,
         after);
  fflush(stdout);
  CHECK(after == before);
  /* sendfile/splice bypass read_iter. They must perform the same foreground
   * validation before exposing page-cache bytes after an external write. */
  int other = open("/other/mmap-evicted", O_WRONLY);
  CHECK(other >= 0);
  CHECK(pwrite(other, "next", 4, 0) == 4);
  CHECK(!fsync(other));
  CHECK(!close(other));
  char received[4] = {0};
  for (unsigned i = 0; i < 100; i++) {
    int descriptors[2];
    loff_t position = 0;
    CHECK(!pipe(descriptors));
    CHECK(splice(original, &position, descriptors[1], NULL, 4, 0) == 4);
    CHECK(read(descriptors[0], received, sizeof(received)) == sizeof(received));
    CHECK(!close(descriptors[0]));
    CHECK(!close(descriptors[1]));
    if (!memcmp(received, "next", 4))
      break;
    usleep(10000);
  }
  CHECK(!memcmp(received, "next", 4));
  puts("kstqr: splice observes remote writes after lazy invalidation");
  other = open("/other/mmap-evicted", O_WRONLY | O_TRUNC);
  CHECK(other >= 0);
  CHECK(!close(other));
  pid_t child = fork();
  CHECK(child >= 0);
  if (!child) {
    volatile char value = mapping[0];
    (void)value;
    _exit(0);
  }
  int status;
  CHECK(waitpid(child, &status, 0) == child);
  CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGBUS);
  CHECK(!munmap(mapping, sizeof(initial)));
  CHECK(!close(original));
  puts("kstqr: evicted overlay metadata stays idle and mmap observes remote "
       "truncation");
  return 0;
}

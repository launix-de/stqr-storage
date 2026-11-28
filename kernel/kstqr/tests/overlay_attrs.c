/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "overlay attrs line%d: %s (%s)\n", __LINE__, #x,         \
              strerror(errno));                                                \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static void trace_write(const char *n, const char *v) {
  char p[256];
  snprintf(p, sizeof(p), "/sys/kernel/tracing/%s", n);
  int f = open(p, O_WRONLY | O_TRUNC);
  CHECK(f >= 0);
  CHECK(write(f, v, strlen(v)) == (ssize_t)strlen(v));
  CHECK(!close(f));
}
static unsigned long long gets(void) {
  FILE *f = fopen("/proc/self/mountstats", "r");
  CHECK(f);
  char *l = NULL;
  size_t n = 0;
  int selected = 0;
  unsigned long long total = 0;
  while (getline(&l, &n, f) > 0) {
    if (!strncmp(l, "device ", 7))
      selected = strstr(l, "mounted on /mnt with fstype kstqr") != NULL;
    if (selected) {
      char *p = strstr(l, "rpc_get=");
      if (p)
        total += strtoull(p + 8, NULL, 10);
    }
  }
  free(l);
  CHECK(!fclose(f));
  return total;
}
static double now(void) {
  struct timespec t;
  CHECK(!clock_gettime(CLOCK_MONOTONIC, &t));
  return t.tv_sec + t.tv_nsec / 1e9;
}
static void observe(void) {
  struct stat s;
  CHECK(!stat("/mnt/live-directory", &s));
  CHECK(S_ISDIR(s.st_mode));
  CHECK((s.st_mode & 0777) == 0755);
}
static unsigned trace_count(void) {
  trace_write("tracing_on", "0");
  FILE *f = fopen("/sys/kernel/tracing/trace", "r");
  CHECK(f);
  char *l = NULL;
  size_t n = 0;
  unsigned count = 0;
  while (getline(&l, &n, f) > 0)
    if (strstr(l, "ov_entries_info"))
      count++;
  free(l);
  CHECK(!fclose(f));
  return count;
}
int main(int argc, char **argv) {
  CHECK(argc == 2);
  CHECK(!mount("tracefs", "/sys/kernel/tracing", "tracefs", 0, NULL));
  observe();
  for (unsigned i = 0; i < 3; i++) {
    char p[64];
    snprintf(p, sizeof(p), "/mnt/prewarm-%u", i);
    CHECK(!mkdir(p, 0700));
    CHECK(!rmdir(p));
    observe();
  }
  trace_write("tracing_on", "0");
  trace_write("current_tracer", "nop");
  trace_write("set_ftrace_filter", "ov_entries_info");
  trace_write("current_tracer", "function");
  for (unsigned phase = 0; phase < 2; phase++) {
    unsigned long long calls = 0;
    unsigned merges = 0;
    double elapsed = 0;
    for (unsigned i = 0; i < 32; i++) {
      if (phase) {
        char p[64];
        snprintf(p, sizeof(p), "/mnt/unrelated-%u", i);
        CHECK(!mkdir(p, 0700));
        CHECK(!rmdir(p));
        usleep(1000);
      } else
        for (unsigned warm = 0; warm < 4; warm++)
          observe();
      trace_write("trace", "");
      trace_write("tracing_on", "1");
      unsigned long long before = gets();
      double started = now();
      observe();
      elapsed += now() - started;
      calls += gets() - before;
      merges += trace_count();
    }
    printf("overlay-attrs phase=%s iterations=32 stat_only_get=%llu "
           "stat_only_materializations=%u stat_only_seconds=%.6f\n",
           phase == 0 ? "quiet" : "churn-stat", calls, merges, elapsed);
    fflush(stdout);
    CHECK(merges == 0);
  }
  trace_write("current_tracer", "nop");
  /* Lower metadata and names remain dependencies even when the upper row is
   * unchanged. Exercise a real other-client update through the fixture. */
  char command[512];
  CHECK(snprintf(command, sizeof(command),
                 "wget -qO- %s/__kstqr_test/native-directory", argv[1]) > 0);
  CHECK(!system(command));
  struct stat st;
  unsigned retry;
  for (retry = 0; retry < 100; retry++) {
    CHECK(!stat("/mnt/live-directory", &st));
    if ((st.st_mode & 0777) == 0700)
      break;
    usleep(10000);
  }
  CHECK(retry < 100);
  CHECK(!stat("/mnt/live-directory/new-from-go", &st));
  CHECK(!chmod("/mnt/live-directory", 0711));
  CHECK(!stat("/mnt/live-directory", &st));
  CHECK((st.st_mode & 0777) == 0711);
  off_t size = st.st_size;
  int fd = open("/mnt/live-directory/upper-entry", O_CREAT | O_EXCL | O_WRONLY,
                0600);
  CHECK(fd >= 0);
  CHECK(!close(fd));
  CHECK(!stat("/mnt/live-directory", &st));
  CHECK(st.st_size > size);
  CHECK(!unlink("/mnt/live-directory/upper-entry"));
  CHECK(!stat("/mnt/live-directory", &st));
  CHECK(st.st_size == size);
  CHECK(!rename("/mnt/live-directory", "/mnt/renamed-directory"));
  errno = 0;
  CHECK(stat("/mnt/live-directory", &st) < 0 && errno == ENOENT);
  CHECK(!stat("/mnt/renamed-directory", &st));
  CHECK((st.st_mode & 0777) == 0711);
  CHECK(!rename("/mnt/renamed-directory", "/mnt/live-directory"));
  puts("overlay-attrs lower metadata/name, own-row size/mode and rename guards "
       "passed");
  puts("KSTQR_OVERLAY_ATTRS_PASS");
  return 0;
}

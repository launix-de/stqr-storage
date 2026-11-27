/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/mount.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "negative-cache: line%d %s errno=%d\n", __LINE__, #x,    \
              errno);                                                          \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static void trace_write(const char *name, const char *value) {
  char path[256];
  snprintf(path, sizeof(path), "/sys/kernel/tracing/%s", name);
  int fd = open(path, O_WRONLY | O_TRUNC);
  CHECK(fd >= 0);
  CHECK(write(fd, value, strlen(value)) == (ssize_t)strlen(value));
  CHECK(!close(fd));
}
int main(void) {
  enum { N = 32 };
  volatile unsigned char *maps[N];
  CHECK(!mount("tracefs", "/sys/kernel/tracing", "tracefs", 0, NULL));
  trace_write("tracing_on", "0");
  trace_write("current_tracer", "nop");
  trace_write("buffer_size_kb", "2048");
  trace_write("max_graph_depth", "3");
  trace_write("trace", "");
  trace_write("set_graph_function", "remove_entry");
  trace_write("current_tracer", "function_graph");
  trace_write("tracing_on", "1");
  alarm(20);
  for (unsigned i = 0; i < N; i++) {
    char path[128];
    snprintf(path, sizeof(path), "/mnt/lower-cache-pressure-%u", i);
    int fd = open(path, O_RDONLY);
    CHECK(fd >= 0);
    maps[i] = mmap(NULL, 4096, PROT_READ | PROT_EXEC, MAP_SHARED, fd, 0);
    CHECK(maps[i] != MAP_FAILED);
    CHECK(maps[i][0] == 'L');
    CHECK(!close(fd));
  }
  for (unsigned round = 0; round < 8; round++)
    for (unsigned i = 0; i < N; i++)
      CHECK(maps[i][round * 64] == 'L');
  trace_write("tracing_on", "0");
  FILE *f = fopen("/sys/kernel/tracing/trace", "r");
  CHECK(f);
  char line[2048];
  unsigned removals = 0, invalidations = 0;
  while (fgets(line, sizeof(line), f)) {
    if (strstr(line, "remove_entry"))
      removals++;
    if (strstr(line, "ks_queue_invalidation"))
      invalidations++;
  }
  CHECK(!ferror(f));
  CHECK(!fclose(f));
  printf("negative-cache: mapped_lower_files=%u removal_calls=%u "
         "removal_invalidations=%u\n",
         N, removals, invalidations);
  fflush(stdout);
  CHECK(removals >= N);
  CHECK(invalidations == 0);
  trace_write("current_tracer", "nop");
  trace_write("set_graph_function", "");
  /* A quiet absent upper probe is not an absence cache. First copy-up on a
   * second connection must still invalidate the inherited shared mapping. */
  int other = open("/other/lower-cache-pressure-0", O_WRONLY);
  CHECK(other >= 0);
  CHECK(pwrite(other, "U", 1, 0) == 1);
  CHECK(!fsync(other));
  CHECK(!close(other));
  for (unsigned round = 0; round < 500 && maps[0][0] != 'U'; round++)
    usleep(10000);
  CHECK(maps[0][0] == 'U');
  puts("KSTQR_ABSENT_UPPER_COPYUP_INVALIDATION_PASS");
  for (unsigned i = 0; i < N; i++)
    CHECK(!munmap((void *)maps[i], 4096));
  trace_write("current_tracer", "nop");
  trace_write("set_graph_function", "");
  CHECK(!umount("/sys/kernel/tracing"));
  puts("KSTQR_NEGATIVE_INVALIDATION_PASS real_lower_mmap "
       "failed_subscriptions_quiet");
  return 0;
}

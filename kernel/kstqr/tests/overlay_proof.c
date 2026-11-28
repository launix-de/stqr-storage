/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "overlay proof line %d: %s (%s)\n", __LINE__, #x,        \
              strerror(errno));                                                \
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
static void observe(void) {
  struct stat st;
  CHECK(!stat("/mnt/live-directory", &st));
  CHECK(S_ISDIR(st.st_mode));
}
int main(int argc, char **argv) {
  CHECK(argc == 2);
  CHECK(!mount("tracefs", "/sys/kernel/tracing", "tracefs", 0, NULL) ||
        errno == EBUSY);
  int warm = !strcmp(argv[1], "warm"), recover = !strcmp(argv[1], "recover");
  CHECK(warm || recover || !strcmp(argv[1], "quiet"));
  if (recover) {
    /* Reconnect may make the first foreground operation retry. Once an
     * actual directory observation succeeds, subsequent warm stats must
     * use certified cache proofs, not repeatedly resolve the namespace. */
    struct stat st;
    int ok = 0;
    for (unsigned i = 0; i < 50; i++) {
      if (!stat("/mnt/live-directory", &st)) {
        ok = 1;
        break;
      }
      usleep(100000);
    }
    CHECK(ok);
    observe();
  }
  trace_write("tracing_on", "0");
  trace_write("current_tracer", "nop");
  trace_write("trace", "");
  trace_write("set_ftrace_filter", "ks_overlay_lookup");
  trace_write("current_tracer", "function");
  trace_write("tracing_on", "1");
  for (unsigned i = 0; i < (warm ? 1U : 32U); i++)
    observe();
  trace_write("tracing_on", "0");
  FILE *f = fopen("/sys/kernel/tracing/trace", "r");
  CHECK(f);
  char line[4096];
  unsigned lookups = 0;
  while (fgets(line, sizeof(line), f))
    if (strstr(line, "ks_overlay_lookup"))
      lookups++;
  CHECK(!ferror(f));
  CHECK(!fclose(f));
  trace_write("current_tracer", "nop");
  trace_write("set_ftrace_filter", "");
  printf("overlay-proof: %s observations=%u namespace_lookups=%u\n", argv[1],
         warm ? 1U : 32U, lookups);
  fflush(stdout);
  /* The initial cold lookup is a positive control for the tracer. A new
   * root observation may rebuild once before tracing; never fetch again for
   * an unchanged, continuously subscribed directory in this measured loop. */
  CHECK(warm ? lookups > 0 : lookups == 0);
  return 0;
}

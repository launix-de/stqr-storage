/* SPDX-License-Identifier: GPL-3.0-only */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "cache retention line %d: %s (%s)\n", __LINE__, #x,      \
              strerror(errno));                                                \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
int main(void) {
  int fd = open("/mnt/retention-hot", O_RDONLY);
  CHECK(fd >= 0);
  struct stat initial;
  CHECK(!fstat(fd, &initial));
  CHECK(initial.st_size == 65536);
  puts("retention: opened 64-KiB lower file");
  fflush(stdout);
  volatile char *map = mmap(NULL, 65536, PROT_READ, MAP_SHARED, fd, 0);
  CHECK(map != MAP_FAILED);
  CHECK(map[0] == 'H');
  usleep(100000);
  CHECK(map[0] == 'H');
  puts("retention: initial mapping populated");
  fflush(stdout);
  unsigned evicted = 0;
  for (unsigned i = 0; i < 2112; i++) {
    char path[128];
    struct stat info;
    if (!(i % 512)) {
      printf("retention: scan %u\n", i);
      fflush(stdout);
    }
    CHECK(!fstat(fd, &info));
    snprintf(path, sizeof(path), "/mnt/retention-%02u/file-%02u", i / 32,
             i % 32);
    CHECK(!stat(path, &info));
    usleep(1000);
    CHECK(!fstat(fd, &info));
    unsigned char residency[16];
    CHECK(!mincore((void *)map, 65536, residency));
    if (!(residency[0] & 1))
      evicted++;
    CHECK(map[0] == 'H');
  }
  /* Creating an unrelated upper file changes the overlay namespace root. */
  for (unsigned i = 0; i < 32; i++) {
    char path[96];
    snprintf(path, sizeof(path), "/mnt/retention-created-%u", i);
    int created = open(path, O_CREAT | O_WRONLY | O_EXCL, 0600);
    CHECK(created >= 0);
    CHECK(!close(created));
    usleep(10000);
    unsigned char residency[16];
    CHECK(!mincore((void *)map, 65536, residency));
    if (!(residency[0] & 1))
      evicted++;
    CHECK(map[0] == 'H');
  }
  printf("retention: hot mapped page evicted %u times during unrelated "
         "metadata pressure\n",
         evicted);
  fflush(stdout);
  CHECK(evicted == 0);
  int other = open("/other/retention-hot", O_WRONLY);
  CHECK(other >= 0);
  CHECK(pwrite(other, "N", 1, 0) == 1);
  CHECK(!fsync(other));
  CHECK(!close(other));
  for (unsigned i = 0; i < 100 && map[0] != 'N'; i++)
    usleep(10000);
  CHECK(map[0] == 'N');
  CHECK(!munmap((void *)map, 65536));
  CHECK(!close(fd));
  puts("KSTQR_CACHE_RETENTION_PASS");
  return 0;
}

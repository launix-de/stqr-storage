/* SPDX-License-Identifier: GPL-3.0-only */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
typedef struct {
  unsigned char bytes[32];
} stqr_object_id;
struct ks_entry {
  char name[256];
  stqr_object_id id;
};
struct ov_dir {
  stqr_object_id id;
  struct ks_entry *entries;
  size_t count;
  bool removed;
};
struct ks_overlay_state {
  struct ov_dir **dirs;
  size_t count;
  bool immutable, upper_indexed;
  stqr_object_id *upper_ids;
  size_t upper_count;
};
static unsigned long long comparisons;
static bool allocation_fails;
static size_t allocation_bytes;
static int counted_memcmp(const void *a, const void *b, size_t n) {
  comparisons++;
  return memcmp(a, b, n);
}
static int counted_strcmp(const char *a, const char *b) {
  comparisons++;
  return strcmp(a, b);
}
static bool eqid(stqr_object_id a, stqr_object_id b) {
  return !counted_memcmp(a.bytes, b.bytes, 32);
}
static bool nullid(stqr_object_id a) {
  stqr_object_id zero = {{0}};
  return eqid(a, zero);
}
static void *sq_alloc(size_t n) {
  allocation_bytes += n;
  return allocation_fails ? NULL : calloc(1, n);
}
static long find_entry(struct ks_entry *a, size_t n, const char *name) {
  for (size_t i = 0; i < n; i++)
    if (!counted_strcmp(a[i].name, name))
      return (long)i;
  return -1;
}
#define memcmp counted_memcmp
#define strcmp counted_strcmp
#define sort(a, n, size, cmp, unused) qsort(a, n, size, cmp)
#include "../overlay_lookup.h"
#undef memcmp
#undef strcmp
static stqr_object_id id(unsigned n) {
  stqr_object_id r = {{0}};
  r.bytes[0] = (unsigned char)(n >> 24);
  r.bytes[1] = (unsigned char)(n >> 16);
  r.bytes[2] = (unsigned char)(n >> 8);
  r.bytes[3] = (unsigned char)n;
  return r;
}
static struct ks_overlay_state fixture(size_t nd, size_t ne) {
  struct ks_overlay_state s = {0};
  s.count = nd;
  s.dirs = calloc(nd, sizeof(*s.dirs));
  assert(s.dirs);
  unsigned next = 1;
  for (size_t i = 0; i < nd; i++) {
    struct ov_dir *d = s.dirs[i] = calloc(1, sizeof(*d));
    assert(d);
    d->id = id((unsigned)i + 1);
    d->count = ne / nd + (i < ne % nd);
    d->entries = calloc(d->count + 1, sizeof(*d->entries));
    assert(d->entries);
    for (size_t j = 0; j < d->count; j++) {
      snprintf(d->entries[j].name, sizeof(d->entries[j].name), "entry-%06zu",
               j);
      d->entries[j].id = id(next++);
    }
  }
  return s;
}
static void release(struct ks_overlay_state *s) {
  for (size_t i = 0; i < s->count; i++) {
    free(s->dirs[i]->entries);
    free(s->dirs[i]);
  }
  free(s->dirs);
  free(s->upper_ids);
}
static uint64_t ns(void) {
  struct timespec t;
  assert(!clock_gettime(CLOCK_MONOTONIC, &t));
  return (uint64_t)t.tv_sec * 1000000000 + t.tv_nsec;
}
static void verify(void) {
  struct ks_overlay_state s = fixture(119, 1071);
  /* Whiteouts, repeated-ID aliases, and entries in removed directories retain
   * the exact old membership semantics. No visibility decision is cached. */
  s.dirs[0]->entries[0].id = id(0);
  s.dirs[1]->entries[0].id = s.dirs[1]->entries[1].id;
  s.dirs[118]->removed = true;
  bool old[1075];
  for (unsigned i = 0; i < 1075; i++)
    old[i] = ov_upper_ref(&s, id(i));
  ov_index_snapshot(&s);
  assert(s.immutable && s.upper_indexed && s.upper_count == 1069);
  for (unsigned i = 0; i < 1075; i++)
    assert(ov_upper_ref(&s, id(i)) == old[i]);
  for (size_t i = 0; i < s.count; i++) {
    assert(ov_find(&s, id((unsigned)i + 1)) == s.dirs[i]);
    for (size_t j = 0; j < s.dirs[i]->count; j++)
      assert(ov_entry_at(&s, s.dirs[i], s.dirs[i]->entries[j].name) == (long)j);
    assert(ov_entry_at(&s, s.dirs[i], "a-before") == -1);
    assert(ov_entry_at(&s, s.dirs[i], "z-after") == -1);
  }
  assert(!ov_find(&s, id(0)) && !ov_find(&s, id(120)));
  assert(!ov_upper_ref(&s, id(0)));
  release(&s);
  /* Mutable clones may be unsorted and contain first-match aliases during
   * edits. They must never consume an immutable snapshot's reverse index. */
  s = fixture(3, 9);
  struct ov_dir *d = s.dirs[0];
  s.dirs[0] = s.dirs[2];
  s.dirs[2] = d;
  s.dirs[1]->id = s.dirs[0]->id;
  assert(ov_find(&s, s.dirs[0]->id) == s.dirs[0]);
  struct ks_entry e = d->entries[0];
  d->entries[0] = d->entries[2];
  d->entries[2] = e;
  strcpy(d->entries[1].name, d->entries[0].name);
  assert(ov_entry_at(&s, d, d->entries[0].name) == 0);
  assert(ov_upper_ref(&s, d->entries[2].id));
  d->entries[2].id = id(9000);
  assert(ov_upper_ref(&s, id(9000)));
  release(&s);
  s = fixture(1, 0);
  ov_index_snapshot(&s);
  assert(s.upper_indexed && !s.upper_count && !ov_upper_ref(&s, id(1)));
  release(&s);
  s = fixture(3, 9);
  allocation_fails = true;
  ov_index_snapshot(&s);
  allocation_fails = false;
  assert(s.immutable && !s.upper_indexed && !s.upper_ids);
  assert(ov_upper_ref(&s, id(9)));
  release(&s);
  size_t limit = OV_REF_INDEX_MAX_BYTES / sizeof(stqr_object_id);
  s = fixture(1, limit + 1);
  allocation_bytes = 0;
  ov_index_snapshot(&s);
  assert(s.immutable && !s.upper_indexed && !s.upper_ids && !allocation_bytes);
  assert(ov_upper_ref(&s, id((unsigned)limit + 1)));
  release(&s);
  puts("OVERLAY_LOOKUP_PASS");
}
static void benchmark(void) {
  struct ks_overlay_state s = fixture(119, 1071);
  volatile unsigned found = 0;
  /* Match the measured snapshot's aggregate counts: 119 directories,
   * 1071 names, 726 whiteouts and345 referenced objects. This is a controlled
   * lookup microbenchmark, not an end-to-end VM boot measurement. */
  unsigned seen = 0;
  for (size_t i = 0; i < s.count; i++)
    for (size_t j = 0; j < s.dirs[i]->count; j++)
      if (++seen > 345)
        s.dirs[i]->entries[j].id = id(0);
  const unsigned queries = 20000;
  comparisons = 0;
  uint64_t start = ns();
  for (unsigned i = 0; i < queries; i++) {
    found += ov_find(&s, id(i % 120 + 1)) != NULL;
    found += ov_upper_ref(&s, id(i % 1072 + 1));
    found += ov_entry_at(&s, s.dirs[i % 119], "entry-000008") >= 0;
  }
  uint64_t linear_ns = ns() - start;
  unsigned long long linear_cmp = comparisons;
  start = ns();
  ov_index_snapshot(&s);
  uint64_t build_ns = ns() - start;
  comparisons = 0;
  start = ns();
  for (unsigned i = 0; i < queries; i++) {
    found += ov_find(&s, id(i % 120 + 1)) != NULL;
    found += ov_upper_ref(&s, id(i % 1072 + 1));
    found += ov_entry_at(&s, s.dirs[i % 119], "entry-000008") >= 0;
  }
  uint64_t indexed_ns = ns() - start;
  unsigned long long indexed_cmp = comparisons;
  assert(found && s.upper_indexed && linear_cmp > 8 * indexed_cmp);
  printf("overlay-lookup bench dirs=119 entries=1071 queries=%u linear_ns=%llu "
         "indexed_ns=%llu build_ns=%llu linear_comparisons=%llu "
         "indexed_comparisons=%llu index_bytes=%zu\n",
         queries, (unsigned long long)linear_ns, (unsigned long long)indexed_ns,
         (unsigned long long)build_ns, linear_cmp, indexed_cmp,
         s.upper_count * sizeof(*s.upper_ids));
  release(&s);
  puts("OVERLAY_LOOKUP_BENCH_PASS");
}
int main(int argc, char **argv) {
  if (argc == 2 && !strcmp(argv[1], "--benchmark"))
    benchmark();
  else
    verify();
  return 0;
}

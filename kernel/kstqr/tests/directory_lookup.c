/* SPDX-License-Identifier: GPL-3.0-only */
#define _POSIX_C_SOURCE 200809L
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

struct ks_entry {
  char name[256];
  unsigned char id[32];
};

static uint64_t comparisons;
static int counted_strcmp(const char *a, const char *b) {
  comparisons++;
  return strcmp(a, b);
}
#define strcmp counted_strcmp
#include "../directory_lookup.h"
#undef strcmp

static long reference(const struct ks_entry *entries, size_t count,
                      const char *name) {
  for (size_t i = 0; i < count; i++)
    if (!strcmp(entries[i].name, name))
      return (long)i;
  return -1;
}

static struct ks_entry *fixture(size_t count) {
  struct ks_entry *entries = calloc(count ? count : 1, sizeof(*entries));
  assert(entries);
  for (size_t i = 0; i < count; i++)
    snprintf(entries[i].name, sizeof(entries[i].name), "entry-%08zu", i * 2);
  return entries;
}

static void check(const struct ks_entry *entries, size_t count, const char *name) {
  uint64_t bound = 0;
  for (size_t n = count; n; n >>= 1)
    bound++;
  comparisons = 0;
  long got = find_sorted_entry(entries, count, name);
  assert(comparisons <= bound);
  assert(got == reference(entries, count, name));
}

static void differential(void) {
  const size_t sizes[] = {0, 1, 2, 3, 7, 16, 255, 1024, 8192};
  check(NULL, 0, "missing");
  for (size_t s = 0; s < sizeof(sizes) / sizeof(sizes[0]); s++) {
    size_t count = sizes[s];
    struct ks_entry *entries = fixture(count);
    check(entries, count, "");
    check(entries, count, "a-before");
    check(entries, count, "z-after");
    for (size_t i = 0; i < count; i++) {
      char missing[260];
      check(entries, count, entries[i].name);
      snprintf(missing, sizeof(missing), "entry-%08zu", i * 2 + 1);
      check(entries, count, missing);
      snprintf(missing, sizeof(missing), "%sx", entries[i].name);
      check(entries, count, missing);
    }
    free(entries);
  }
  // Long common prefixes exercise the full permitted filename width.
  struct ks_entry long_names[3] = {0};
  for (size_t i = 0; i < 3; i++) {
    memset(long_names[i].name, 'p', 255);
    long_names[i].name[254] = (char)('b' + i * 2);
    check(long_names, i + 1, long_names[i].name);
  }
  char absent[256];
  memcpy(absent, long_names[1].name, sizeof(absent));
  absent[254]++;
  check(long_names, 3, absent);

  // Overlay editing clones append out of order. Their shared production
  // helper must still scan; the duplicate-name control additionally preserves
  // its original first-match semantics. Binary search cannot replace this path.
  struct ks_entry mutable[] = {{.name = "z"}, {.name = "a"},
                               {.name = "m"}, {.name = "a"}};
  assert(find_entry(NULL, 0, "a") == -1);
  assert(find_entry(mutable, 4, "a") == 1);
  assert(find_entry(mutable, 4, "z") == 0);
  assert(find_entry(mutable, 4, "m") == 2);
  assert(find_entry(mutable, 4, "missing") == -1);
  // Positive control demonstrates this fixture detects accidental binary use.
  assert(find_sorted_entry(mutable, 4, "z") != find_entry(mutable, 4, "z"));
  puts("directory lookup differential and comparison bounds passed");
}

static uint64_t ns(void) {
  struct timespec value;
  assert(!clock_gettime(CLOCK_MONOTONIC, &value));
  return (uint64_t)value.tv_sec * 1000000000 + value.tv_nsec;
}

static void benchmark(void) {
  const size_t count = 65536, rounds = 1024;
  struct ks_entry *entries = fixture(count);
  for (unsigned negative = 0; negative < 2; negative++) {
    uint64_t linear_comparisons = 0, binary_comparisons = 0;
    uint64_t elapsed[2];
    volatile long sums[2] = {0};
    for (unsigned binary = 0; binary < 2; binary++) {
      comparisons = 0;
      uint64_t start = ns();
      for (size_t i = 0; i < rounds; i++) {
        char name[256];
        // Deterministic dispersed hits and between-entry negative queries.
        size_t index = (i * 40503) % count;
        snprintf(name, sizeof(name), "entry-%08zu", index * 2 + negative);
        long found = binary ? find_sorted_entry(entries, count, name)
                            : find_entry(entries, count, name);
        assert(found == (negative ? -1 : (long)index));
        sums[binary] += found;
      }
      elapsed[binary] = ns() - start;
      if (binary)
        binary_comparisons = comparisons;
      else
        linear_comparisons = comparisons;
    }
    assert(sums[0] == sums[1]);
    assert(binary_comparisons <= rounds * 17);
    assert(binary_comparisons < linear_comparisons / 100);
    printf("directory lookup benchmark entries=%zu queries=%zu negative=%u "
           "linear_comparisons=%llu binary_comparisons=%llu "
           "linear_ns=%llu binary_ns=%llu\n",
           count, rounds, negative, (unsigned long long)linear_comparisons,
           (unsigned long long)binary_comparisons,
           (unsigned long long)elapsed[0], (unsigned long long)elapsed[1]);
  }
  free(entries);
}

int main(void) {
  differential();
  benchmark();
  return 0;
}

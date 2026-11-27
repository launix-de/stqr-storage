/* SPDX-License-Identifier: GPL-3.0-only */
/* The Go driver includes the actual remove_entry body, not a predicate copy. */
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
typedef struct {
  unsigned char bytes[16];
} stqr_id;
typedef struct {
  unsigned char bytes[32];
} stqr_object_id;
struct completion {
  bool done;
};
struct mutex {
  unsigned held;
};
struct ks_fs {
  unsigned invalidations;
};
struct cached_inode {
  stqr_id subscription;
  stqr_object_id id;
  size_t size;
  bool ready, early;
  int hash, subscription_hash, lru;
};
struct ks_cache {
  struct mutex lock, wire;
  struct ks_fs *fs;
  unsigned count, unsubscribes;
  size_t bytes;
  bool shared;
  void *client;
};
struct ks_subscribe_result {
  struct completion done;
  void *data;
};
#define CHECK(x)                                                               \
  do {                                                                         \
    if (!(x)) {                                                                \
      fprintf(stderr, "cache-removal-invalidation: line%d %s\n", __LINE__,     \
              #x);                                                             \
      exit(1);                                                                 \
    }                                                                          \
  } while (0)
static struct mutex *cache_wire(struct ks_cache *c) { return &c->wire; }
static void mutex_lock(struct mutex *m) {
  CHECK(!m->held);
  m->held++;
}
static void mutex_unlock(struct mutex *m) {
  CHECK(m->held == 1);
  m->held--;
}
static void init_completion(struct completion *c) { c->done = false; }
static void wait_for_completion(struct completion *c) { CHECK(c->done); }
static void subscribe_completed(void *arg) {
  ((struct ks_subscribe_result *)arg)->done.done = true;
}
static int stqr_unsubscribe_async(void *client, stqr_id sid,
                                  void (*done)(void *), void *arg) {
  ((struct ks_cache *)client)->unsubscribes++;
  done(arg);
  return 0;
}
static void stqr_unsubscribe(void *client, stqr_id sid) {
  ((struct ks_cache *)client)->unsubscribes++;
}
static void discard_data(struct ks_cache *c, struct cached_inode *e) {
  CHECK(c->bytes >= e->size);
  c->bytes -= e->size;
  e->size = 0;
}
static void ks_queue_invalidation(struct ks_fs *f, const stqr_object_id *id) {
  f->invalidations++;
}
#define hash_del(p) ((void)(p))
#define list_del(p) ((void)(p))
#define kfree(p) free(p)
#define sq_free(p) free(p)
#ifndef REMOVE_ENTRY_SOURCE
#error actual remove_entry source must be supplied by the test driver
#endif
#include REMOVE_ENTRY_SOURCE
int main(void) {
  for (unsigned absent = 0; absent < 2; absent++)
  for (unsigned shared = 0; shared < 2; shared++)
    for (unsigned ready = 0; ready < 2; ready++)
      for (unsigned early = 0; early < 2; early++) {
        struct ks_fs f = {0};
        struct ks_cache c = {.fs = &f,
                             .shared = shared,
                             .count = 1,
                             .bytes = sizeof(struct cached_inode) + 17,
                             .wire = {.held = 1}};
        c.client = &c;
        struct cached_inode *e = calloc(1, sizeof(*e));
        CHECK(e);
        e->ready = ready;
        e->early = early;
        e->size = 17;
        remove_entry(&c, e, absent);
        CHECK(f.invalidations == (!absent || ready || early));
        CHECK(c.count == 0 && c.bytes == 0);
        CHECK(c.unsubscribes == 1);
        CHECK(c.lock.held == 0 && c.wire.held == 1);
      }
  puts("CACHE_REMOVAL_INVALIDATION_PASS absent_never_ready_quiet existing_rejected_invalidates ready_invalidates "
       "early_invalidates unsubscribe_preserved");
  return 0;
}

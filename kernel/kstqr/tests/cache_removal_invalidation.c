/* SPDX-License-Identifier: GPL-3.0-only */
/* The Go driver includes the actual remove_entry body, not a predicate copy. */
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
typedef struct {
  unsigned char bytes[16];
} stqr_id;
typedef struct {
  unsigned char bytes[32];
} stqr_object_id;
typedef uint32_t u32;
struct completion {
  bool done;
};
struct mutex {
  unsigned held;
};
struct ks_fs {
  unsigned invalidations;
  size_t cache_budget;
};
struct cached_inode {
  stqr_id subscription;
  stqr_object_id id;
  size_t size;
  bool ready, early, valid;
  uint64_t cursor;
  void *raw;
  struct { uint64_t length; } info;
  int hash, subscription_hash, lru;
};
struct ks_cache {
  struct mutex lock, wire;
  struct ks_fs *fs;
  unsigned count, unsubscribes;
  size_t bytes;
  bool shared;
  void *client;
  bool available;
  struct cached_inode *subscriptions, *lru;
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
static void mutex_lock(struct mutex *m) { CHECK(!m->held); m->held++; }
static void mutex_unlock(struct mutex *m) { CHECK(m->held==1); m->held--; }
static int stqr_unsubscribe_discard(void *client, stqr_id sid) {
  struct ks_cache *c=client;
  CHECK(c->count==0 && c->bytes==0);
  c->unsubscribes++;
  return 0;
}
static void discard_data(struct ks_cache *c, struct cached_inode *e) {
  CHECK(c->bytes >= e->size);
  c->bytes -= e->size;
  e->size = 0;
  free(e->raw);e->raw=NULL;e->valid=false;
}
static void ks_queue_invalidation(struct ks_fs *f, const stqr_object_id *id) {
  f->invalidations++;
}
typedef struct {
 stqr_id subscription;
 uint64_t cursor,offset,length;
 unsigned parts,part,change;
 const void *data;
 size_t size;
} stqr_update;
#define STQR_UPDATE_REPLACE 0
#define GFP_NOFS 0
static void *kmemdup(const void *p,size_t n,int flags) { void *copy=malloc(n);if(copy)memcpy(copy,p,n);return copy; }
#define hash_for_each_possible(table,e,member,key) for((void)(key),(e)=(table);(e);(e)=NULL)
#define list_for_each_entry(e,head,member) for((e)=*(head);(e);(e)=NULL)
#ifndef UPDATE_SOURCE
#error actual update source must be supplied by the test driver
#endif
#include UPDATE_SOURCE
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
  {
   struct ks_fs f={.cache_budget=16384};
   struct cached_inode replacement={.ready=true,.valid=true,.cursor=7,.size=3};
   replacement.subscription.bytes[2]=2;replacement.raw=malloc(3);CHECK(replacement.raw);memcpy(replacement.raw,"new",3);
   struct ks_cache c={.fs=&f,.count=1,.bytes=sizeof(replacement)+3,.available=true,.subscriptions=&replacement,.lru=&replacement};
   stqr_update late={.cursor=99,.parts=1,.change=STQR_UPDATE_REPLACE,.data="bad",.size=3,.length=3};late.subscription.bytes[2]=1;
   CHECK(subscription_hash(late.subscription)==subscription_hash(replacement.subscription));
   update(&c,&late);
   CHECK(replacement.cursor==7 && replacement.valid && !memcmp(replacement.raw,"new",3) && f.invalidations==0);
   late.subscription=replacement.subscription;late.cursor=8;late.data="yes";update(&c,&late);
   CHECK(replacement.cursor==8 && replacement.valid && !memcmp(replacement.raw,"yes",3) && f.invalidations==1);
   free(replacement.raw);
  }
  puts("CACHE_REMOVAL_INVALIDATION_PASS absent_never_ready_quiet existing_rejected_invalidates ready_invalidates "
       "early_invalidates no_reply_retirement late_retired_sid_ignored hash_collision_safe");
  return 0;
}

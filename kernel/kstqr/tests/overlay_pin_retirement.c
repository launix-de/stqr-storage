// SPDX-License-Identifier: GPL-3.0-only
/* Execute actual overlay callers and actual mount-owned PIN retirement. */
#define _GNU_SOURCE
#include <errno.h>
#include <pthread.h>
#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
typedef uint8_t u8;
typedef struct {u8 bytes[32];} stqr_object_id;
typedef struct {u8 bytes[16];} stqr_id;
#define REQUIRE(x) do {if(!(x)){fprintf(stderr,"overlay-pin-retirement line%d %s\n",__LINE__,#x);exit(1);}}while(0)
struct workqueue_struct {unsigned unused;};
struct work_struct {void (*fn)(struct work_struct *);pthread_t thread,caller;};
struct completion {pthread_mutex_t lock;pthread_cond_t condition;bool done;};
struct buf {u8 *p;size_t n;int err;};
struct ks_node {unsigned kind,links,depth;bool removed,lower;u8 *write_path;size_t write_path_len;stqr_object_id lower_owner;u8 lower_digest[32];};
struct ks_snapshot {struct ks_node node;};
struct ks_fs {bool references;struct ks_fs *lower,*parent;struct workqueue_struct *reference_queue;};
struct ov_dir {struct ks_node node;stqr_object_id id,lower;bool metadata;struct buf image;};
struct ks_overlay_state {struct ov_dir *added;};
static struct workqueue_struct queue;
static _Thread_local bool fatal_current;
static unsigned pins,unpins,queued,flushed,scenario;
#define current NULL
static bool fatal_signal_pending(void *task){(void)task;return fatal_current;}
#define container_of(ptr,type,member) ((type *)((char *)(ptr)-offsetof(type,member)))
#define INIT_WORK(work,callback) (*(work)=(struct work_struct){.fn=(callback)})
static unsigned memalloc_nofs_save(void){return 0;}
static void memalloc_nofs_restore(unsigned saved){REQUIRE(saved==0);}
static void init_completion(struct completion *c){REQUIRE(!pthread_mutex_init(&c->lock,NULL));REQUIRE(!pthread_cond_init(&c->condition,NULL));c->done=false;}
static void complete(struct completion *c){REQUIRE(!pthread_mutex_lock(&c->lock));c->done=true;REQUIRE(!pthread_cond_signal(&c->condition));REQUIRE(!pthread_mutex_unlock(&c->lock));}
static void wait_for_completion(struct completion *c){REQUIRE(!pthread_mutex_lock(&c->lock));while(!c->done)REQUIRE(!pthread_cond_wait(&c->condition,&c->lock));REQUIRE(!pthread_mutex_unlock(&c->lock));REQUIRE(!pthread_cond_destroy(&c->condition));REQUIRE(!pthread_mutex_destroy(&c->lock));}
static void *run_work(void *p){struct work_struct *w=p;REQUIRE(!pthread_equal(w->caller,pthread_self()));REQUIRE(!fatal_current);w->fn(w);return NULL;}
static bool queue_work(struct workqueue_struct *q,struct work_struct *w){REQUIRE(q==&queue);queued++;w->caller=pthread_self();REQUIRE(!pthread_create(&w->thread,NULL,run_work,w));return true;}
static bool flush_work(struct work_struct *w){REQUIRE(!pthread_join(w->thread,NULL));flushed++;return true;}
static int ks_object_reference(struct ks_fs *f,stqr_object_id id,stqr_id token,unsigned action){(void)f;(void)id;REQUIRE(token.bytes[0]);if(fatal_current)return -EINTR;if(action==0)pins++;else if(action==1){REQUIRE(pins==1);pins--;unpins++;}else REQUIRE(action==4);return 0;}
#ifndef REFERENCE_CLEANUP_SOURCE
#define REFERENCE_CLEANUP_SOURCE "../reference_cleanup.c"
#endif
#include REFERENCE_CLEANUP_SOURCE
static int stqr_request_id(stqr_id *id){id->bytes[0]=42;return 0;}
static void *sq_alloc(size_t n){return calloc(1,n);}
static void sq_free(void *p){free(p);}
static void drop(struct buf *b){free(b->p);memset(b,0,sizeof(*b));}
static struct ov_dir *ov_find(struct ks_overlay_state *s,stqr_object_id id){(void)s;(void)id;return NULL;}
static void ks_cache_forget(struct ks_fs *f,stqr_object_id id){(void)f;(void)id;}
static int ks_plain_snapshot(struct ks_fs *f,stqr_object_id id,struct ks_snapshot *s){(void)id;if(f->lower)return -ENOENT;if(scenario==1){fatal_current=true;return -EIO;}s->node=(struct ks_node){.kind=scenario==2?1:2,.links=1};return 0;}
static void ks_snapshot_free(struct ks_snapshot *s){(void)s;}
static int anchor_reserve(struct ks_fs *f,stqr_object_id id,stqr_object_id lower,stqr_id *anchor){(void)f;(void)id;(void)lower;anchor->bytes[0]=43;return 0;}
static int ov_lower_reference_attr(struct ks_node *n,stqr_id anchor,struct buf *attrs){(void)n;(void)anchor;(void)attrs;return 0;}
static int ov_node(struct ov_dir *d,struct ks_node *n){d->node=*n;return 0;}
static int ov_add_dir(struct ks_overlay_state *s,struct ov_dir *d){s->added=d;fatal_current=true;return 0;}
static int lower_fingerprint(struct ks_node *n,u8 *digest){(void)n;(void)digest;return 0;}
static void encode_node(struct buf *raw,struct ks_node *n){(void)raw;(void)n;}
static int cas_object(struct ks_fs *f,stqr_object_id id,const void *raw,size_t n,const void *old,size_t size,bool absent){(void)f;(void)id;(void)raw;(void)n;(void)old;(void)size;(void)absent;fatal_current=true;return 0;}
#include "overlay_pin_callers.h"
int main(int argc,char **argv){
 (void)ks_reference_workqueue;
 (void)ks_retire_pin; /* Keep the helper linked in the old-caller control too. */
 alarm(5);
 unsigned start=argc>1?(unsigned)strtoul(argv[1],NULL,10):0;
 unsigned end=argc>1?start+1:3;REQUIRE(start<3);
 for(scenario=start;scenario<end;scenario++){
  struct ks_fs lower={.references=true},upper={.lower=&lower,.reference_queue=&queue};
  lower.parent=&upper;REQUIRE(!lower.reference_queue);
  struct ks_overlay_state s={0};struct ov_dir *out=NULL;stqr_object_id id={{7}};
  pins=unpins=queued=flushed=0;fatal_current=false;
  int err=scenario==2?ov_copy_inode(&upper,id,"/file",false):ov_dir(&upper,&s,id,&out);
  REQUIRE(err==(scenario==1?-EIO:0));REQUIRE(fatal_current);
  REQUIRE(!pins&&unpins==1&&queued==1&&flushed==1);
  if(scenario==0)REQUIRE(out==s.added&&out->metadata);
  free(s.added);
 }
 puts("KSTQR_OVERLAY_PIN_RETIREMENT_PASS callers=ov_dir_success,ov_dir_failure,ov_copy_inode_done");return 0;
}

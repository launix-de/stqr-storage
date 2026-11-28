// SPDX-License-Identifier: GPL-3.0-only
/* Execute actual ks_put_super with a queued read's final-fput retirement. */
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define KS_RPC_SLOTS 2
#define WRITE_ONCE(x,v) ((x)=(v))
#define REQUIRE(x,msg) do{if(!(x)){fprintf(stderr,"teardown-queue-order: %s\n",msg);exit(1);}}while(0)
struct workqueue_struct{unsigned kind;bool alive;};
struct work_struct{unsigned unused;};
struct delayed_work{unsigned unused;};
struct rpc{void *client;bool alive;};
struct ks_fs{bool stopping;struct ks_fs *lower;struct workqueue_struct *read_queue,*reference_queue;struct delayed_work orphan_work;struct work_struct invalidation_work;void *fence,*boot_lifetime;char *url;struct rpc rpc[KS_RPC_SLOTS];};
struct super_block{struct ks_fs *s_fs_info;};
static struct ks_fs root,lower;
static struct workqueue_struct reads,references;
static struct work_struct cleanup;
static bool read_pending,retirement_pending,orphans_stopped,cache_closed,lower_closed,freed;
static unsigned retired,invalidations_stopped;
static void assert_transport(void){for(unsigned i=0;i<KS_RPC_SLOTS;i++)REQUIRE(root.rpc[i].alive,"RPC_CLOSED_BEFORE_REFERENCE_RETIREMENT");}
static bool queue_work(struct workqueue_struct *q,struct work_struct *w){REQUIRE(q&&q->alive&&q->kind==2,"READ_PRODUCER_REQUIRES_REFERENCE_QUEUE");REQUIRE(w==&cleanup,"wrong_cleanup");retirement_pending=true;return true;}
static void destroy_workqueue(struct workqueue_struct *q){
 REQUIRE(q&&q->alive&&root.stopping&&orphans_stopped,"queue_destroy_before_quiescence");
 if(root.lower)REQUIRE(root.lower->stopping,"lower_not_stopping");
 if(q->kind==1&&read_pending){assert_transport();REQUIRE(!cache_closed,"cache_closed_before_read_drain");queue_work(root.reference_queue,&cleanup);read_pending=false;}
 if(q->kind==2){REQUIRE(!read_pending,"REFERENCE_DRAIN_PRECEDED_READ_PRODUCER");assert_transport();if(retirement_pending){retired++;retirement_pending=false;}}
 q->alive=false;
}
static void cancel_delayed_work_sync(struct delayed_work *w){REQUIRE(w==&root.orphan_work&&root.stopping,"orphan_stop_order");orphans_stopped=true;}
static void ks_cache_stop(struct ks_fs *f){REQUIRE(f==&root&&!read_pending&&!retirement_pending&&!reads.alive&&!references.alive,"cache_close_before_queue_drain");assert_transport();cache_closed=true;}
static void stqr_fence_close(void *fence){(void)fence;REQUIRE(cache_closed,"fence_close_before_cache");}
static void cancel_work_sync(struct work_struct *w){REQUIRE(w==&root.invalidation_work&&root.stopping,"invalidation_stop_order");invalidations_stopped++;}
static void ks_close_lower(struct ks_fs *f){if(f){REQUIRE(f==&lower&&f->stopping&&!read_pending&&!retirement_pending&&!references.alive,"lower_close_before_retirement");assert_transport();}lower_closed=true;}
static void ks_rpc_stop(struct rpc *r){REQUIRE(lower_closed&&cache_closed&&!references.alive&&!reads.alive,"RPC_STOP_PRECEDED_QUEUE_DRAIN");r->alive=false;}
static void stqr_close(void *client){(void)client;}
static void ks_overlay_stop(struct ks_fs *f){REQUIRE(f==&root&&lower_closed,"overlay_stop_order");}
static void fput(void *file){REQUIRE(file==root.boot_lifetime&&!retirement_pending,"socket_lifetime_close_order");}
static void kfree(void *p){if(p==&root){REQUIRE(!retirement_pending&&cache_closed&&invalidations_stopped==1,"mount_freed_before_drain");freed=true;}}
#include "teardown_caller.h"
int main(int argc,char **argv){
 unsigned mode=argc>1?(unsigned)strtoul(argv[1],NULL,10):0;REQUIRE(mode<3,"mode");
 reads=(struct workqueue_struct){.kind=1,.alive=true};references=(struct workqueue_struct){.kind=2,.alive=mode!=2};
 root=(struct ks_fs){.read_queue=&reads,.reference_queue=mode==2?NULL:&references,.boot_lifetime=(void*)1};lower=(struct ks_fs){0};root.lower=mode==1?NULL:&lower;
 for(unsigned i=0;i<KS_RPC_SLOTS;i++)root.rpc[i].alive=true;
 read_pending=mode!=2;retirement_pending=false;struct super_block sb={.s_fs_info=&root};
 ks_put_super(&sb);
 REQUIRE(!sb.s_fs_info&&freed&&!read_pending&&!retirement_pending&&retired==(mode==2?0u:1u),"unfinished_teardown");
 ks_put_super(&sb); /* Idempotent missing-private-state path. */
 puts("KSTQR_TEARDOWN_QUEUE_PASS read_final_fput_before_reference_drain");return 0;
}

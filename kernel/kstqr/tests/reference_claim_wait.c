/* SPDX-License-Identifier: GPL-3.0-only */
/* Execute the ACTUAL helper with deterministic reference/REDO I/O doubles. */
#define _GNU_SOURCE
#include <assert.h>
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
typedef uint64_t u64;
typedef struct { u8 bytes[32]; } stqr_object_id;
typedef struct { u8 bytes[16]; } stqr_id;
typedef struct { unsigned unused; } stqr_info;
struct workqueue_struct { unsigned unused; };
struct work_struct {
 void (*fn)(struct work_struct *);
 pthread_t thread, caller;
};
struct completion {
 pthread_mutex_t mutex;
 pthread_cond_t condition;
 bool done;
};
static struct workqueue_struct reference_queue;
struct ks_fs { unsigned worker; struct workqueue_struct *reference_queue; struct ks_fs *parent; };
static struct ks_fs mount_owner={.reference_queue=&reference_queue};
static bool lower_context;
static _Thread_local bool fatal_current;
#define current NULL
static bool fatal_signal_pending(void *task) { (void)task;return fatal_current; }
static unsigned cancel_after_pin;
static bool cancel_on_unpin;

struct buf { u8 *p; size_t n; int err; };
struct member { stqr_object_id id; struct buf before, after, writes; bool writes_legacy; };
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_barrier_t initial_pins;
static unsigned workers, pins, claim_owner, unpins, publications, redos, deletions;
static unsigned pin_calls[33], unpin_calls[33], claim_calls[33], release_calls[33];
static stqr_id claim_tokens[33];
static unsigned sequence, decision;
static bool intent_exists, force_first_busy, persistent_dependency, reject_reread;
static u8 head[64]; static size_t head_len;
static stqr_object_id intent = {{1}}, inode = {{2}};
static const u8 before[] = "before", after[] = "after", edits[4] = {0};
static _Thread_local unsigned pauses;
#define REQUIRE(x) do { if (!(x)) { fprintf(stderr,"reference-claim-wait: line%d %s\n",__LINE__,#x); abort(); } } while(0)
/* Exercise the mount-owned work handoff with a real second host thread.
 * Inline callback stubs would hide stack lifetime and completion ordering bugs. */
#define container_of(ptr,type,member) ((type *)((char *)(ptr)-offsetof(type,member)))
#define INIT_WORK(work,callback) (*(work)=(struct work_struct){.fn=(callback)})
static _Thread_local unsigned nofs_depth;
static unsigned queued, completed, flushed;
static unsigned memalloc_nofs_save(void) { return nofs_depth++; }
static void memalloc_nofs_restore(unsigned saved) { REQUIRE(nofs_depth==saved+1);nofs_depth=saved; }
static void init_completion(struct completion *done) {
 REQUIRE(!pthread_mutex_init(&done->mutex,NULL));
 REQUIRE(!pthread_cond_init(&done->condition,NULL));done->done=false;
}
static void complete(struct completion *done) {
 REQUIRE(!pthread_mutex_lock(&done->mutex));done->done=true;
 __atomic_add_fetch(&completed,1,__ATOMIC_RELAXED);
 REQUIRE(!pthread_cond_signal(&done->condition));
 REQUIRE(!pthread_mutex_unlock(&done->mutex));
}
static void wait_for_completion(struct completion *done) {
 REQUIRE(!pthread_mutex_lock(&done->mutex));
 while(!done->done)REQUIRE(!pthread_cond_wait(&done->condition,&done->mutex));
 REQUIRE(!pthread_mutex_unlock(&done->mutex));
 REQUIRE(!pthread_cond_destroy(&done->condition));
 REQUIRE(!pthread_mutex_destroy(&done->mutex));
}
static void *run_work(void *arg) {
 struct work_struct *work=arg;
 REQUIRE(!pthread_equal(work->caller,pthread_self()));
 REQUIRE(nofs_depth==0);work->fn(work);REQUIRE(nofs_depth==0);return NULL;
}
static bool queue_work(struct workqueue_struct *queue,struct work_struct *work) {
 REQUIRE(queue==&reference_queue);work->caller=pthread_self();
 __atomic_add_fetch(&queued,1,__ATOMIC_RELAXED);
 REQUIRE(!pthread_create(&work->thread,NULL,run_work,work));return true;
}
static bool flush_work(struct work_struct *work) {
 REQUIRE(!pthread_join(work->thread,NULL));
 __atomic_add_fetch(&flushed,1,__ATOMIC_RELAXED);return true;
}
static void *sq_alloc(size_t n) { void *p=calloc(1,n); REQUIRE(p); return p; }
static void sq_free(void *p) { free(p); }
static bool zero(const void *p,size_t n) { const u8 *b=p;for(size_t i=0;i<n;i++)if(b[i])return false;return true; }
static bool eqid(stqr_object_id a,stqr_object_id b) { return !memcmp(a.bytes,b.bytes,32); }
static int stqr_request_id(stqr_id *id) { unsigned n=__atomic_add_fetch(&sequence,1,__ATOMIC_RELAXED);memset(id,0,sizeof(*id));memcpy(id->bytes,&n,sizeof(n));return 0; }
static void drop(struct buf *b) { free(b->p);memset(b,0,sizeof(*b)); }
static void put(struct buf *b,const void *p,size_t n) { b->p=realloc(b->p,b->n+n);REQUIRE(b->p);memcpy(b->p+b->n,p,n);b->n+=n; }
static const u8 *take(struct buf *b,size_t n) { if(n>b->n){b->err=-EUCLEAN;return NULL;}const u8 *p=b->p;b->p+=n;b->n-=n;return p; }
static unsigned readnum(struct buf *b,unsigned n) { const u8 *p=take(b,n);unsigned v=0;if(p)for(unsigned i=0;i<n;i++)v|=(unsigned)p[i]<<(i*8);return v; }
static int sq_pause(unsigned ms) { if(persistent_dependency && ++pauses>=3)return -EINTR;usleep(ms*1000);return 0; }
static void cond_resched(void) { }
static int ks_object_reference(struct ks_fs *f,stqr_object_id id,stqr_id token,unsigned action) {
 REQUIRE(eqid(id,intent));unsigned w=f->worker;REQUIRE(w>0&&w<=workers);
 if(action==1&&cancel_on_unpin){cancel_on_unpin=false;fatal_current=true;}
 if(fatal_signal_pending(current))return -EINTR;
 pthread_mutex_lock(&lock);int err=0;bool barrier=false;
 switch(action) {
 case 0: pin_calls[w]++;pins++;barrier=pin_calls[w]==1&&workers>1;break;
 case 1: REQUIRE(pins>0);pins--;unpins++;unpin_calls[w]++;break;
 case 2:
  claim_calls[w]++;
  if(claim_calls[w]==1)claim_tokens[w]=token;
  else REQUIRE(!memcmp(&claim_tokens[w],&token,sizeof(token)));
  if(force_first_busy){force_first_busy=false;err=-EBUSY;}
  else if(persistent_dependency||pins||claim_owner)err=-EBUSY;
  else claim_owner=w;
  break;
 case 3: REQUIRE(claim_owner==w);REQUIRE(!memcmp(&claim_tokens[w],&token,sizeof(token)));claim_owner=0;release_calls[w]++;break;
 default: REQUIRE(false);
 }
 pthread_mutex_unlock(&lock);
 if(barrier){int b=pthread_barrier_wait(&initial_pins);REQUIRE(b==0||b==PTHREAD_BARRIER_SERIAL_THREAD);}
 return err;
}
static int intent_get(struct ks_fs *f,stqr_object_id id,struct buf *raw,struct buf *journal,stqr_info *info) {
 REQUIRE(eqid(id,intent));if(cancel_after_pin){fatal_current=true;if(cancel_after_pin==1)return -EINTR;}pthread_mutex_lock(&lock);
 if(!intent_exists){pthread_mutex_unlock(&lock);return -ENOENT;}
 if(claim_owner==f->worker&&reject_reread){pthread_mutex_unlock(&lock);return -EIO;}
 u8 bytes[11]="STFT\3\0";bytes[6]=decision;put(raw,bytes,sizeof(bytes));put(journal,bytes,sizeof(bytes));
 pthread_mutex_unlock(&lock);return 0;
}
static int tx_decode(struct buf *raw,stqr_object_id id,struct member **out,unsigned *count,unsigned *state) {
 REQUIRE(eqid(id,intent));REQUIRE(raw->n==11);*state=raw->p[6];*count=1;*out=sq_alloc(sizeof(**out));
 **out=(struct member){.id=inode,.before={.p=(u8*)before,.n=sizeof(before)},.after={.p=(u8*)after,.n=sizeof(after)},.writes={.p=(u8*)edits,.n=sizeof(edits)}};return 0;
}
static int get_object(struct ks_fs *f,stqr_object_id id,void **out,size_t *n,stqr_info *info) {
 REQUIRE(eqid(id,inode));pthread_mutex_lock(&lock);*n=head_len;*out=sq_alloc(*n);memcpy(*out,head,*n);pthread_mutex_unlock(&lock);return 0;
}
static int apply_edits(struct ks_fs *f,stqr_object_id id,struct buf *writes,bool legacy,u64 *index) {
 REQUIRE(eqid(id,intent));pthread_mutex_lock(&lock);REQUIRE(claim_owner==f->worker);REQUIRE(pins==0);redos++;pthread_mutex_unlock(&lock);return 0;
}
static int cas_object(struct ks_fs *f,stqr_object_id id,const void *next,size_t nn,const void *expected,size_t en,bool absent) {
 REQUIRE(eqid(id,inode));pthread_mutex_lock(&lock);REQUIRE(claim_owner==f->worker);REQUIRE(pins==0);
 int err=-EAGAIN;if(head_len==en&&!memcmp(head,expected,en)){REQUIRE(nn<=sizeof(head));memcpy(head,next,nn);head_len=nn;publications++;err=0;}
 pthread_mutex_unlock(&lock);return err;
}
static bool marker(const void *p,size_t n,stqr_object_id *id) { if(n!=38||memcmp(p,"STFL\1\0",6))return false;memcpy(id->bytes,(const u8*)p+6,32);return true; }
static int delete_orphan_object(struct ks_fs *f,stqr_object_id id) {
 REQUIRE(eqid(id,intent));pthread_mutex_lock(&lock);REQUIRE(claim_owner==f->worker);REQUIRE(pins==0);REQUIRE(publications==1);intent_exists=false;deletions++;pthread_mutex_unlock(&lock);return 0;
}
static void intent_retire(struct ks_fs *f,stqr_object_id id,struct buf *b,stqr_info *i,unsigned state) { REQUIRE(false); }
#ifndef REFERENCE_REDO_SOURCE
#define REFERENCE_REDO_SOURCE "../reference_redo.c"
#endif
#include "../reference_cleanup.c"
#include REFERENCE_REDO_SOURCE
struct job { struct ks_fs fs; int result; };
static void *run(void *p) { struct job *j=p;pauses=0;j->result=help_tx_pinned(&j->fs,intent,NULL);return NULL; }
static void test(unsigned n,unsigned state,bool dependency,bool reread_error) {
 queued=completed=flushed=0;workers=n;decision=state;pins=claim_owner=unpins=publications=redos=deletions=0;
 memset(pin_calls,0,sizeof(pin_calls));memset(unpin_calls,0,sizeof(unpin_calls));memset(claim_calls,0,sizeof(claim_calls));memset(release_calls,0,sizeof(release_calls));memset(claim_tokens,0,sizeof(claim_tokens));
 intent_exists=true;force_first_busy=true;persistent_dependency=dependency;reject_reread=reread_error;
 memcpy(head,"STFL\1\0",6);memcpy(head+6,intent.bytes,32);head_len=38;
 REQUIRE(!pthread_barrier_init(&initial_pins,NULL,n));
 pthread_t threads[33];struct job jobs[33];
 for(unsigned i=1;i<=n;i++){jobs[i]=(struct job){.fs={.worker=i,.reference_queue=lower_context?NULL:&reference_queue,.parent=lower_context?&mount_owner:NULL}};REQUIRE(!pthread_create(&threads[i],NULL,run,&jobs[i]));}
 for(unsigned i=1;i<=n;i++){REQUIRE(!pthread_join(threads[i],NULL));REQUIRE(jobs[i].result==(dependency?-EINTR:reread_error?-EIO:state==1?0:-EAGAIN));REQUIRE(pin_calls[i]==1);REQUIRE(unpin_calls[i]==1);REQUIRE(claim_calls[i]>=1);REQUIRE(release_calls[i]==(dependency?0:1));}
 REQUIRE(pins==0&&claim_owner==0);
 REQUIRE(queued==(dependency?0:n)&&completed==queued&&flushed==queued);
 if(dependency||reread_error){REQUIRE(publications==0&&redos==0&&deletions==0&&intent_exists&&head_len==38);}
 else {REQUIRE(publications==1&&deletions==1&&!intent_exists);REQUIRE(redos==(state==1?1u:0u));REQUIRE(head_len==(state==1?sizeof(after):sizeof(before)));REQUIRE(!memcmp(head,state==1?after:before,head_len));}
 REQUIRE(!pthread_barrier_destroy(&initial_pins));
 printf("reference-claim-wait: helpers=%u state=%u dependency=%d reread_error=%d pins=%u unpins=%u publish=%u redo=%u\n",n,state,dependency,reread_error,n,unpins,publications,redos);
}
static void test_cancelled_pin(bool origin,unsigned stage) {
 workers=1;cancel_after_pin=stage==3?0:stage;cancel_on_unpin=stage==3;fatal_current=false;pins=claim_owner=unpins=0;intent_exists=true;decision=1;
 memset(pin_calls,0,sizeof(pin_calls));memset(unpin_calls,0,sizeof(unpin_calls));
 struct ks_fs fs={.worker=1,.reference_queue=&reference_queue};stqr_id token={{42}};
 if(origin)REQUIRE(!ks_object_reference(&fs,intent,token,0));
 int err=help_tx_pinned(&fs,intent,origin?&token:NULL);
 REQUIRE(err==-EINTR);
 if(origin&&!zero(token.bytes,16))REQUIRE(!ks_retire_pin(&fs,intent,token));
 REQUIRE(pin_calls[1]==1&&unpin_calls[1]==1&&pins==0);
 fatal_current=false;cancel_after_pin=false;
 printf("reference-pin-cancel: origin=%d stage=%u acknowledged PIN retired without caller signal\n",origin,stage);
}
int main(void) { alarm(10);test_cancelled_pin(false,1);test_cancelled_pin(true,1);test_cancelled_pin(false,2);test_cancelled_pin(true,2);test_cancelled_pin(false,3);test_cancelled_pin(true,3);test(1,1,false,false);test(32,1,false,false);test(32,2,false,false);test(1,1,true,false);test(1,1,false,true);lower_context=true;test(1,1,false,false);test(1,2,false,false);puts("KSTQR_REFERENCE_CLAIM_WAIT_PASS");return 0; }

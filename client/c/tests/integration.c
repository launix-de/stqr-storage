/* SPDX-License-Identifier: GPL-3.0-only */
#include "stqr.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define CHECK(x) do { if (!(x)) { fprintf(stderr, "check failed at line %d: %s\n", __LINE__, #x); exit(1); } } while (0)
static stqr_options fresh(void) {
    stqr_options o = {0}; CHECK(!stqr_request_id(&o.request_id)); return o;
}
struct observer { unsigned changes, lost; stqr_update last; unsigned char data[64]; };
static void observe(void *arg, const stqr_update *update) {
    struct observer *o = arg;
    if (!update) { o->lost++; return; }
    o->changes++; o->last = *update;
    if (update->size <= sizeof(o->data)) memcpy(o->data, update->data, update->size);
}
static void wait_change(stqr_client *c, struct observer *o, unsigned count) {
    for (unsigned j = 0; j < 30 && o->changes < count; j++) CHECK(stqr_poll(c, 100) >= 0);
    CHECK(o->changes == count);
}
struct fence_waiter { stqr_fence *fence; int error; };
static void *wait_fence(void *arg) {
 struct fence_waiter *w=arg; w->error=stqr_fence_wait(w->fence); return NULL;
}
struct async_result {
    unsigned done;
    int error,status;
    void *data;
    size_t size;
};
static void async_completed(void *arg,int error,int status,void *data,size_t size,const stqr_info *info) {
    struct async_result *result=arg;(void)info;
    result->done++;result->error=error;result->status=status;
    result->data=data;result->size=size;
}
static void async_wait(stqr_client *c,struct async_result *results,unsigned count) {
    for(unsigned tries=0;tries<300;tries++) {
        unsigned done=0;for(unsigned i=0;i<count;i++)done+=results[i].done;
        if(done==count)return;
        CHECK(stqr_poll(c,100)>=0);
    }
    CHECK(!"asynchronous completion timeout");
}
int main(void) {
    stqr_client *c = NULL;
    stqr_id bucket = {{0}};
    stqr_object_id id = {{0}};
    stqr_info info, previous;
    stqr_options o;
    void *data = NULL;
    size_t n;
    stqr_u64 offset;
    const char *url = getenv("STQR_TEST_URL"), *mode = getenv("STQR_TEST_MODE");
    CHECK(stqr_parse_id("not-an-id", &bucket) == -EINVAL);
    CHECK(stqr_parse_object_id("1234", &id) == -EINVAL);
    CHECK(stqr_dial(&c, "http://localhost", "user", "pw", bucket,"fs", 1000) == -EINVAL);
    CHECK(c == NULL);
    { stqr_fence *empty=NULL;CHECK(stqr_fence_capture(NULL,&empty)==-EINVAL && empty==NULL);stqr_fence_close(empty); }
    if (!url) { puts("local API validation passed; network tests run through STQR_TEST_C=1 go test ./server"); return 0; }
    CHECK(!stqr_parse_id(getenv("STQR_TEST_BUCKET"), &bucket));
    CHECK(!stqr_parse_object_id("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", &id));
    if(mode && !strcmp(mode,"named-invalid-response")) {
        const char *selector=getenv("STQR_TEST_SELECTOR");
        CHECK(stqr_dial_bucket(&c,url,"user","password",selector?selector:"photos","fs",3000)==-EPROTO);
        CHECK(c==NULL);puts("invalid canonical bucket response rejected");return 0;
    }
    if(mode && !strcmp(mode,"named")) {
        stqr_id canonical;
        const char *invalid[]={" c-client-named","c-client-named ","name:c-client-named ","c-client-named\r\n", "c-client-named\t", ""};
        for(size_t i=0;i<sizeof(invalid)/sizeof(invalid[0]);i++) {
            CHECK(stqr_dial_bucket(&c,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),invalid[i],"fs",3000)==-EINVAL);
            CHECK(c==NULL);
        }
        CHECK(!stqr_dial_bucket(&c,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),"c-client-named","fs",3000));
        CHECK(!stqr_bucket_id(c,&canonical) && !memcmp(canonical.bytes,bucket.bytes,16));
        CHECK(!stqr_reconnect(c));
        memset(&o,0,sizeof(o));CHECK(!stqr_request_id(&o.request_id));CHECK(!stqr_put(c,id,"named",5,&o,&info));
        CHECK(!stqr_get(c,id,&data,&n,&info) && n==5 && !memcmp(data,"named",5));
        stqr_free(data);stqr_close(c);puts("named connection canonicalization and reconnect passed");return 0;
    }
    CHECK(!stqr_dial(&c, url, getenv("STQR_TEST_USER"), getenv("STQR_TEST_PASSWORD"), bucket,"fs", 3000));
    if (mode && !strcmp(mode,"legacy-reconnect")) {
        stqr_commit_mark mark;
        CHECK(!(stqr_capabilities(c)&STQR_CAP_CONNECTION_SYNC));
        o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(!stqr_pending_mark(c,&mark)&&mark.sequence==7);
        CHECK(!stqr_reconnect(c));
        CHECK(!stqr_pending_mark(c,&mark)&&mark.sequence==7);
        CHECK(!stqr_sync(c));CHECK(!stqr_pending_mark(c,&mark)&&!mark.sequence);
        stqr_close(c);puts("legacy reconnect confirms retained mark through SyncThrough");return 0;
    }
    if (mode && !strcmp(mode,"accepted-cas-reconnect")) {
        struct async_result result={0},fence={0};stqr_commit_mark mark;
        o=fresh();CHECK(!stqr_put(c,id,"before",6,&o,&info));
        o=fresh();o.accepted=1;
        CHECK(!stqr_cas_async(c,id,1,0,"before",6,"after",5,&o,async_completed,&result));
        async_wait(c,&result,1);CHECK(!result.error);CHECK(!stqr_async_pending(c));
        CHECK(!stqr_pending_mark(c,&mark)&&mark.sequence);
        CHECK(!stqr_reconnect(c));
        CHECK(stqr_connection_sync_async(c,async_completed,&fence)==-ESTALE);
        CHECK(stqr_sync(c)==-ESTALE);CHECK(!fence.done);
        /* Filesystem opt-in keeps live accepted visibility after uncertainty,
         * while explicit persistence confirmation must remain ESTALE. */
        stqr_set_accepted_references(c,1);memset(&result,0,sizeof(result));
        o=fresh();o.accepted=1;
        CHECK(!stqr_cas_async(c,id,1,0,"after",5,"visible",7,&o,async_completed,&result));
        async_wait(c,&result,1);CHECK(!result.error);
        CHECK(!stqr_get(c,id,&data,&n,&info));CHECK(n==7&&!memcmp(data,"visible",7));stqr_free(data);
        CHECK(stqr_sync(c)==-ESTALE);
        stqr_close(c);puts("accepted CAS reconnect preserves uncertain prefix");return 0;
    }
    if (mode && !strcmp(mode,"async-close")) {
        struct async_result pending={0};
        o=fresh();CHECK(!stqr_put_async(c,id,"unfenced",8,&o));
        CHECK(!stqr_get_async(c,id,async_completed,&pending));
        stqr_close(c);CHECK(pending.done&&pending.error);stqr_free(pending.data);
        puts("C close cancelled pending work without a connection fence");return 0;
    }
    if (mode && !strcmp(mode,"async-progress")) {
        struct async_result fence={0},read={0};
        o=fresh();CHECK(!stqr_put_async(c,id,"hello",5,&o));
        CHECK(!stqr_connection_sync_async(c,async_completed,&fence));
        CHECK(!stqr_stat_async(c,id,async_completed,&read));
        async_wait(c,&read,1);CHECK(!read.error);CHECK(!fence.done);
        puts("READ_BEFORE_FENCE");fflush(stdout);
        async_wait(c,&fence,1);CHECK(!fence.error);
        stqr_close(c);puts("C same-socket read progressed during checkpoint");return 0;
    }
    if (mode && !strcmp(mode,"async")) {
        enum { N=32 };
        stqr_object_id ids[N];struct async_result result[N],fence={0};
        CHECK(stqr_capabilities(c)&STQR_CAP_CONNECTION_SYNC);
        for(unsigned i=0;i<N;i++) {
            unsigned char payload[128];memset(payload,(int)i,sizeof(payload));
            ids[i]=id;ids[i].bytes[0]=(unsigned char)i;
            o=fresh();CHECK(!stqr_put_async(c,ids[i],payload,sizeof(payload),&o));
        }
        CHECK(stqr_async_pending(c)==N);
        CHECK(!stqr_connection_sync_async(c,async_completed,&fence));
        async_wait(c,&fence,1);CHECK(!fence.error&&fence.status==200);
        CHECK(!stqr_async_pending(c));
        memset(result,0,sizeof(result));
        for(unsigned i=0;i<N;i++)CHECK(!stqr_get_async(c,ids[i],async_completed,&result[i]));
        CHECK(stqr_async_pending(c)==N);
        /* A legacy blocking wrapper must dispatch unrelated completed IDs. */
        CHECK(!stqr_stat(c,ids[0],&info));
        async_wait(c,result,N);
        for(unsigned i=0;i<N;i++) {
            unsigned char payload[128];memset(payload,(int)i,sizeof(payload));
            CHECK(!result[i].error&&result[i].size==sizeof(payload));
            CHECK(!memcmp(result[i].data,payload,sizeof(payload)));stqr_free(result[i].data);
        }
        memset(result,0,sizeof(result));
        for(unsigned i=0;i<N;i++) {
            unsigned char before[128],after[128];memset(before,(int)i,sizeof(before));memset(after,(int)(i+1),sizeof(after));
            o=fresh();o.accepted=1;
            CHECK(!stqr_cas_async(c,ids[i],1,0,before,sizeof(before),after,sizeof(after),&o,async_completed,&result[i]));
        }
        async_wait(c,result,N);
        for(unsigned i=0;i<N;i++)CHECK(!result[i].error);
        CHECK(!stqr_sync(c));
        {
            stqr_id sid[8];stqr_u64 cursor[8];
            memset(result,0,sizeof(result));
            for(unsigned i=0;i<8;i++) {
                CHECK(!stqr_request_id(&sid[i]));cursor[i]=0;
                CHECK(!stqr_subscribe_async(c,ids[i],sid[i],&cursor[i],async_completed,&result[i]));
            }
            async_wait(c,result,8);
            for(unsigned i=0;i<8;i++) {
                CHECK(!result[i].error&&cursor[i]&&result[i].size==128);
                CHECK(((unsigned char *)result[i].data)[0]==i+1);stqr_free(result[i].data);
            }
            memset(result,0,sizeof(result));
            for(unsigned i=0;i<8;i++)CHECK(!stqr_unsubscribe_async(c,sid[i],async_completed,&result[i]));
            async_wait(c,result,8);for(unsigned i=0;i<8;i++)CHECK(!result[i].error);
        }
        /* A rejected no-ACK write must remain visible at its connection fence. */
        o=fresh();o.absent=1;CHECK(!stqr_put_async(c,ids[0],"wrong",5,&o));
        memset(&fence,0,sizeof(fence));CHECK(!stqr_connection_sync_async(c,async_completed,&fence));
        async_wait(c,&fence,1);CHECK(fence.error&&fence.status>=400);
        CHECK(!stqr_async_pending(c));stqr_close(c);
        puts("C asynchronous multiplexing, no-ACK writes and connection fences passed");return 0;
    }
    if (mode && !strcmp(mode,"accepted-references")) {
        stqr_id pin,claim;stqr_commit_mark mark;struct async_result result={0};
        CHECK(stqr_capabilities(c)&STQR_CAP_ACCEPTED_REFERENCES);
        stqr_set_accepted_references(c,1);
        CHECK(!stqr_request_id(&pin)&&!stqr_request_id(&claim));
        CHECK(!stqr_object_reference_async(c,id,pin,0,async_completed,&result));
        async_wait(c,&result,1);CHECK(!result.error);
        CHECK(!stqr_pending_mark(c,&mark)&&mark.sequence);
        CHECK(!stqr_sync(c));CHECK(!stqr_reconnect(c));
        /* Mode survives reconnect; every endpoint checks its own capability. */
        CHECK(!stqr_object_unpin(c,id,pin));
        CHECK(!stqr_pending_mark(c,&mark)&&mark.sequence);
        CHECK(!stqr_object_claim(c,id,claim));
        CHECK(!stqr_object_release_claim(c,id,claim));
        CHECK(!stqr_sync(c));CHECK(!stqr_pending_mark(c,&mark)&&!mark.sequence);
        stqr_close(c);puts("C accepted reference receipts and explicit fences passed");return 0;
    }
    if (mode && !strcmp(mode,"references")) {
        stqr_client *other=NULL;
        stqr_id pin, claim, second;
        CHECK(!stqr_request_id(&pin) && !stqr_request_id(&claim) && !stqr_request_id(&second));
        CHECK(!stqr_dial(&other,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,"fs",3000));
        CHECK(!stqr_object_pin(c,id,pin));
        CHECK(!stqr_object_pin(c,id,pin));
        CHECK(!stqr_reconnect(c));
        CHECK(stqr_object_claim(other,id,claim)==-EBUSY);
        CHECK(!stqr_object_unpin(c,id,pin));
        CHECK(!stqr_object_unpin(c,id,pin));
        CHECK(!stqr_object_claim(other,id,claim));
        CHECK(!stqr_object_claim(other,id,claim));
        CHECK(stqr_object_pin(c,id,second)==-EBUSY);
        CHECK(!stqr_reconnect(other));
        CHECK(stqr_object_pin(c,id,second)==-EBUSY);
        CHECK(!stqr_object_release_claim(other,id,claim));
        CHECK(!stqr_object_release_claim(other,id,claim));
        CHECK(!stqr_object_pin(c,id,second));
        CHECK(!stqr_object_unpin(c,id,second));
        stqr_close(other);stqr_close(c);
        puts("C durable pin/claim coordination passed");return 0;
    }
    if (mode && !strcmp(mode,"reuse")) {
        unsigned char before[8192], after[8192], wrong[8192];
        for (unsigned step=0;step<100;step++) {
            memset(before,(int)step,sizeof(before));
            memset(after,(int)(step^255),sizeof(after));
            memset(wrong,(int)(step^127),sizeof(wrong));
            o=fresh();CHECK(!stqr_put(c,id,before,sizeof(before),&o,&info));
            CHECK(info.length==sizeof(before));
            o=fresh();CHECK(stqr_cas(c,id,1,0,wrong,sizeof(wrong),after,sizeof(after),&o,&info)==-EAGAIN);
            CHECK(stqr_status(c)==412);
            CHECK(!stqr_get(c,id,&data,&n,&info));
            CHECK(n==sizeof(before) && !memcmp(data,before,n));stqr_free(data);
            o=fresh();CHECK(!stqr_cas(c,id,1,0,before,sizeof(before),after,sizeof(after),&o,&info));
            CHECK(!stqr_get(c,id,&data,&n,&info));
            CHECK(n==sizeof(after) && !memcmp(data,after,n));stqr_free(data);
            if (!(step%25)) CHECK(!stqr_reconnect(c));
        }
        /* Reusing a deleted address must release the old payload without
         * retaining another visible object for each generation. */
        for (unsigned step=0;step<20;step++) {
            o=fresh();CHECK(!stqr_delete(c,id,&o));
            CHECK(stqr_get(c,id,&data,&n,&info)==-ENOENT && data==NULL);
            o=fresh();o.absent=1;CHECK(!stqr_put(c,id,after,sizeof(after),&o,&info));
        }
        CHECK(!stqr_sync(c));CHECK(!stqr_reconnect(c));
        CHECK(!stqr_get(c,id,&data,&n,&info));
        CHECK(n==sizeof(after) && !memcmp(data,after,n));stqr_free(data);
        stqr_close(c);puts("C PUT/CAS address-reuse lifecycle passed");return 0;
    }
    if (mode && !strcmp(mode,"fence")) {
        stqr_fence *fence=NULL; stqr_commit_mark mark;
        struct fence_waiter waiter; pthread_t worker;
        o=fresh();o.accepted=1;{ int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
        CHECK(!stqr_fence_capture(c,&fence));
        CHECK(!stqr_fence_add(c,fence) && stqr_fence_count(fence)==1);
        waiter.fence=fence;waiter.error=1;
        CHECK(!pthread_create(&worker,NULL,wait_fence,&waiter));
        // The fixture holds SYNC until this original session reads again.
        CHECK(!stqr_stat(c,id,&info));
        CHECK(!pthread_join(worker,NULL) && !waiter.error);
        stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
        o=fresh();o.accepted=1;{ int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
        CHECK(!stqr_fence_capture(c,&fence));
        o=fresh();o.accepted=1;{ int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
        CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && mark.sequence==3);
        CHECK(!stqr_fence_capture(c,&fence));
        CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
        {
            stqr_client *second=NULL,*other_bucket=NULL;stqr_id foreign=bucket;
            CHECK(!stqr_dial(&second,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,"fs",3000));
            o=fresh();o.accepted=1;CHECK(!stqr_put(second,id,"hello",5,&o,&info));
            o=fresh();o.accepted=1;{ int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
            CHECK(!stqr_fence_reset(&fence));
            CHECK(!stqr_fence_add(second,fence) && !stqr_fence_add(c,fence));
            CHECK(stqr_fence_count(fence)==1); // maximum mark for the same owner
            CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);stqr_fence_apply(second,fence);
            CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
            CHECK(!stqr_pending_mark(second,&mark) && !mark.sequence);
            foreign.bytes[0]^=2;
            CHECK(!stqr_dial(&other_bucket,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),foreign,"fs",3000));
            o=fresh();o.accepted=1;{ int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
            o=fresh();o.accepted=1;CHECK(!stqr_put(other_bucket,id,"hello",5,&o,&info));
            CHECK(!stqr_fence_capture(other_bucket,&fence));
            CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);
            // Node boot IDs are shared by buckets; their sequence counters are not.
            CHECK(!stqr_pending_mark(c,&mark) && mark.sequence==6);
            stqr_fence_apply(other_bucket,fence);
            CHECK(!stqr_pending_mark(other_bucket,&mark) && !mark.sequence);
            CHECK(!stqr_fence_capture(c,&fence) && !stqr_fence_wait(fence));stqr_fence_apply(c,fence);
            CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
            stqr_close(other_bucket);stqr_close(second);
        }
        stqr_fence_close(fence);stqr_close(c);puts("independent storage fence passed");return 0;
    }
    if (mode && (!strcmp(mode,"transient") || !strcmp(mode,"unavailable") || !strcmp(mode,"transient-stream") || !strcmp(mode,"disconnect-stream"))) {
        size_t length=strstr(mode,"-stream")?(2U<<20)+17:5;
        unsigned char *payload=malloc(length);CHECK(payload);memset(payload,'r',length);
        o=fresh();
        int err=stqr_put(c,id,payload,length,&o,&info);
        if (!strncmp(mode,"transient",9)) { CHECK(!err && info.length==length); }
        else if (!strcmp(mode,"disconnect-stream")) {CHECK(err==-EINPROGRESS);CHECK(stqr_stat(c,id,&info)==-ENOTCONN);}
        else { CHECK(err==-EHOSTUNREACH && stqr_status(c)==503); }
        free(payload);stqr_close(c); puts("transient retry test passed"); return 0;
    }
    if (mode && !strcmp(mode,"routing-lease")) {
        CHECK(!stqr_object_lease(c,0));
        CHECK(stqr_object_lease(c,0)==-EBUSY);
        for (unsigned step=0;step<8;step++) {
            memset(&id,0,sizeof(id));id.bytes[0]=(step&1)?0x81:1;
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"leased",6,&o,&info));
            CHECK(!stqr_get(c,id,&data,&n,&info));
            CHECK(n==6 && !memcmp(data,"leased",6));stqr_free(data);
        }
        CHECK(!stqr_sync(c));CHECK(stqr_reconnect(c)==-ESTALE);
        stqr_close(c);puts("cluster lease owner redirects passed");return 0;
    }
    if (mode && !strcmp(mode,"routing")) {
        for (unsigned step=0;step<8;step++) {
            memset(&id,0,sizeof(id)); id.bytes[0]=(step&1)?0x81:1;
            o=fresh();o.accepted=1;
            { int err=stqr_put(c,id,"routed",6,&o,&info);if(err)fprintf(stderr,"routed PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
            CHECK(!stqr_get(c,id,&data,&n,&info));
            CHECK(n==6 && !memcmp(data,"routed",6));stqr_free(data);
        }
        CHECK(!stqr_sync(c));
        {
            stqr_object_id left={{1}},right={{0x81}};
            stqr_commit_mark a,b,durable;
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,left,"routed",6,&o,&info));
            CHECK(!stqr_pending_mark(c,&a) && a.sequence);
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,right,"routed",6,&o,&info));
            CHECK(!stqr_pending_mark(c,&b) && b.sequence);
            CHECK(memcmp(a.boot.bytes,b.boot.bytes,16));
            CHECK(!stqr_sync_through(c,&a,&durable));
            CHECK(!memcmp(durable.boot.bytes,a.boot.bytes,16) && durable.sequence>=a.sequence);
            CHECK(!stqr_sync(c));
        }
        {
            /* Routing a subscription retains an accepted dependency. The format
             * client explicitly fences before publishing on another owner. */
            stqr_object_id left={{1}},right={{0x81}};
            stqr_id sid; stqr_u64 cursor; stqr_commit_mark before,after; stqr_fence *retained=NULL;
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,left,"volatile",8,&o,&info));
            CHECK(!stqr_pending_mark(c,&before) && before.sequence);
            CHECK(!stqr_request_id(&sid));
            CHECK(!stqr_subscribe(c,right,sid,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(!stqr_fence_capture(c,&retained) && stqr_fence_count(retained)==1);
            stqr_fence_close(retained);
            CHECK(!stqr_unsubscribe(c,sid));
            CHECK(!stqr_sync(c));
            o=fresh();CHECK(!stqr_put(c,right,"dependent",9,&o,&info));
            CHECK(!stqr_sync(c));
            CHECK(!stqr_pending_mark(c,&after) && !after.sequence);
        }
        {
            stqr_client *writer = NULL;
            stqr_object_id left = {{1}}, right = {{0x81}};
            stqr_id a, b; stqr_u64 cursor; struct observer events = {0};
            stqr_set_update_handler(c,observe,&events);
            CHECK(!stqr_request_id(&a));CHECK(!stqr_request_id(&b));
            CHECK(!stqr_subscribe(c,left,a,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            unsigned lost = events.lost;
            CHECK(!stqr_subscribe(c,right,b,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(events.lost == lost); /* Routing must preserve the first subscription. */
            CHECK(stqr_subscribe(c,right,a,0,0,0,NULL,&data,&n,&info,&cursor)==-EBUSY);
            CHECK(!stqr_dial(&writer,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,"fs",3000));
            o=fresh();CHECK(!stqr_put(writer,left,"left",4,&o,&info));
            wait_change(c,&events,1);CHECK(!memcmp(events.last.subscription.bytes,a.bytes,16));
            o=fresh();CHECK(!stqr_put(writer,right,"right",5,&o,&info));
            wait_change(c,&events,2);CHECK(!memcmp(events.last.subscription.bytes,b.bytes,16));
            CHECK(!stqr_unsubscribe(c,a));
            o=fresh();CHECK(!stqr_put(writer,left,"again",5,&o,&info));CHECK(stqr_poll(c,20)==0);
            o=fresh();CHECK(!stqr_put(writer,right,"still",5,&o,&info));
            wait_change(c,&events,3);CHECK(!memcmp(events.last.subscription.bytes,b.bytes,16));
            CHECK(!stqr_unsubscribe(c,b));CHECK(events.lost==lost);stqr_close(writer);
            CHECK(!stqr_request_id(&a));CHECK(!stqr_request_id(&b));
            CHECK(!stqr_subscribe(c,left,a,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(!stqr_subscribe(c,right,b,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(!stqr_reconnect(c));CHECK(events.lost==lost+1);
            CHECK(!stqr_subscribe(c,left,a,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(!stqr_subscribe(c,right,b,0,0,0,NULL,&data,&n,&info,&cursor));stqr_free(data);
            CHECK(events.lost==lost+1);
            CHECK(!stqr_unsubscribe(c,a));CHECK(!stqr_unsubscribe(c,b));
            stqr_set_update_handler(c,NULL,NULL);
        }
        {
            /* Streamed writes must use the same owner redirect path as small
             * operations. Start on the wrong owner and alternate both ranges. */
            size_t length=(2U<<20)+17;
            unsigned char *payload=malloc(length);CHECK(payload);memset(payload,0x5a,length);
            for (unsigned side=0;side<2;side++) {
                memset(&id,0,sizeof(id));id.bytes[0]=side?0xa1:0x21;
                o=fresh();CHECK(!stqr_put(c,id,payload,length,&o,&info));CHECK(info.length==length);
                CHECK(!stqr_get(c,id,&data,&n,&info));
                CHECK(n==length && !memcmp(data,payload,n));stqr_free(data);
            }
            memset(payload,0x37,length);
            for (unsigned side=0;side<2;side++) {
                stqr_object_id other={{0}};other.bytes[0]=side?1:0x81;
                memset(&id,0,sizeof(id));id.bytes[0]=side?0xa1:0x21;
                /* Force a redirect for both ranged writes and append, too.
                 * Check the preserved prefix and the returned append offset. */
                CHECK(!stqr_stat(c,other,&info));
                o=fresh();CHECK(!stqr_put_range(c,id,17,payload,length-17,&o,&info));
                CHECK(info.length==length);
                CHECK(!stqr_get(c,id,&data,&n,&info));
                CHECK(n==length);
                for (unsigned j=0;j<17;j++) CHECK(((unsigned char *)data)[j]==0x5a);
                CHECK(!memcmp((unsigned char *)data+17,payload,n-17));stqr_free(data);
                CHECK(!stqr_stat(c,other,&info));
                o=fresh();CHECK(!stqr_append(c,id,payload,length,&o,&info,&offset));
                CHECK(offset==length && info.length==2*length);
                CHECK(!stqr_get(c,id,&data,&n,&info));
                CHECK(n==2*length);
                for (unsigned j=0;j<17;j++) CHECK(((unsigned char *)data)[j]==0x5a);
                CHECK(!memcmp((unsigned char *)data+17,payload,length-17));
                CHECK(!memcmp((unsigned char *)data+length,payload,length));stqr_free(data);
            }
            free(payload);
        }
        stqr_close(c);puts("cluster routing and independent subscriptions passed");return 0;
    }
    if (mode) {
        if (!strcmp(mode,"disconnect")) {
            o = fresh(); CHECK(stqr_append(c,id,"x",1,&o,&info,&offset) == -EINPROGRESS);
            CHECK(stqr_stat(c,id,&info) == -ENOTCONN);
        } else if (!strcmp(mode,"subscription-digest")) {
            struct observer events = {0}; stqr_set_update_handler(c,observe,&events);
            CHECK(stqr_stat(c,id,&info) == -EBADMSG);
            CHECK(events.lost == 1 && !events.changes);
            CHECK(stqr_stat(c,id,&info) == -ENOTCONN);
            stqr_set_update_handler(c,NULL,NULL);
        } else if (!strcmp(mode,"digest")) {
            CHECK(stqr_get(c,id,&data,&n,&info) == -EBADMSG); CHECK(data == NULL);
            CHECK(stqr_stat(c,id,&info) == -ENOTCONN);
        } else if (!strcmp(mode,"malformed")) {
            CHECK(stqr_stat(c,id,&info) == -EPROTO);
            CHECK(stqr_stat(c,id,&info) == -ENOTCONN);
        } else CHECK(0);
        stqr_close(c); puts("fault test passed"); return 0;
    }
    CHECK(stqr_stat(c,id,&info) == -ENOENT); CHECK(stqr_status(c) == 404);
    {
        stqr_space space;
        CHECK(!stqr_bucket_space(c,&space));
        CHECK(!space.quota_limited && space.total > 0 && space.available > 0);
        CHECK(space.total-space.free == space.used && space.available <= space.free);
    }
    o = fresh(); o.absent = 1;
    { int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); } CHECK(info.length == 5);
    CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == 5 && !memcmp(data,"hello",5)); stqr_free(data);
    {
        stqr_commit_mark mark, wrong, durable;
        o = fresh(); o.accepted = 1;
        { int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
        CHECK(!stqr_pending_mark(c,&mark) && mark.sequence);
        wrong = mark; wrong.boot.bytes[0] ^= 1;
        CHECK(stqr_sync_through(c,&wrong,&durable) == -ESTALE);
        CHECK(!stqr_pending_mark(c,&wrong) && wrong.sequence == mark.sequence);
        CHECK(!stqr_sync(c)); CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
    }
    {
        stqr_client *watch = NULL; stqr_id sid; stqr_u64 cursor;
        struct observer events = {0};
        CHECK(!stqr_dial(&watch,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,"fs",3000));
        CHECK(stqr_capabilities(watch) & STQR_CAP_SUBSCRIPTIONS);
        stqr_set_update_handler(watch,observe,&events);
        CHECK(!stqr_request_id(&sid));
        CHECK(!stqr_subscribe(watch,id,sid,1,1,3,NULL,&data,&n,&info,&cursor));
        CHECK(n == 3 && !memcmp(data,"ell",3)); stqr_free(data);
        o = fresh(); CHECK(!stqr_put(c,id,"HELLO",5,&o,&info));
        wait_change(watch,&events,1);
        CHECK(events.last.cursor == cursor+1 && events.last.length == info.length);
        CHECK(events.last.change == STQR_UPDATE_REPLACE && events.last.size == 3 && !memcmp(events.data,"ELL",3));
        CHECK(!stqr_unsubscribe(watch,sid));
        o = fresh(); { int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); }
        CHECK(stqr_poll(watch,20) == 0);
        CHECK(!stqr_request_id(&sid));
        CHECK(!stqr_subscribe(watch,id,sid,0,0,0,NULL,&data,&n,&info,&cursor)); stqr_free(data);
        o = fresh(); CHECK(!stqr_delete(c,id,&o)); wait_change(watch,&events,2);
        CHECK(events.last.change == STQR_UPDATE_DELETE);
        o = fresh(); { int err=stqr_put(c,id,"hello",5,&o,&info);if(err)fprintf(stderr,"PUT: error %d HTTP %d\n",err,stqr_status(c));CHECK(!err); } wait_change(watch,&events,3);
        CHECK(events.last.change == STQR_UPDATE_REPLACE && events.last.size == 5);
        cursor = events.last.cursor;
        CHECK(!stqr_reconnect(watch)); CHECK(events.lost == 1);
        CHECK(!stqr_subscribe(watch,id,sid,0,0,0,&cursor,&data,&n,&info,&offset));
        CHECK(offset > cursor && n == 5 && !memcmp(data,"hello",5)); stqr_free(data);
        CHECK(stqr_subscribe(watch,id,sid,0,0,0,NULL,&data,&n,&info,&offset) == -EBUSY);
        CHECK(!stqr_unsubscribe(watch,sid));
        stqr_close(watch); CHECK(events.lost == 2);
    }
    o = fresh();
    CHECK(!stqr_append(c,id," world",6,&o,&info,&offset)); CHECK(offset == 5 && info.length == 11);
    previous = info;
    CHECK(!stqr_append(c,id," world",6,&o,&info,&offset)); CHECK(offset == 5 && info.length == previous.length && info.length == 11);
    CHECK(!stqr_get_range(c,id,6,5,&data,&n,&info)); CHECK(n == 5 && !memcmp(data,"world",5)); stqr_free(data);
    o = fresh();
    CHECK(!stqr_cas(c,id,0,6,"world",5,"STQR!",5,&o,&info));
    o = fresh(); o.has_compare = 1; o.compare = "hello world"; o.compare_size = 11;
    CHECK(stqr_put(c,id,"bad",3,&o,&info) == -EAGAIN); CHECK(stqr_status(c) == 412);
    CHECK(!stqr_stat(c,id,&info));
    o = fresh();
    CHECK(!stqr_cas(c,id,1,0,"hello STQR!",11,"new",3,&o,&info)); CHECK(info.length == 3);
    o = fresh(); CHECK(!stqr_put_range(c,id,1,"XX",2,&o,&info));
    CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == 3 && !memcmp(data,"nXX",3)); stqr_free(data);
    o = fresh(); CHECK(!stqr_trunc(c,id,1,&o,&info)); CHECK(info.length == 1);
    CHECK(!stqr_reconnect(c)); CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == 1 && !memcmp(data,"n",1)); stqr_free(data);
    {
        size_t length = (5U << 19) + 17, j;
        unsigned char *big = malloc(length); CHECK(big);
        for (j = 0; j < length; j++) big[j] = (unsigned char)(j*13);
        o = fresh(); CHECK(!stqr_put(c,id,big,length,&o,&info)); CHECK(info.length == length);
        CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == length && !memcmp(data,big,n)); stqr_free(data);
        {
            stqr_id sid; stqr_u64 cursor;
            CHECK(!stqr_request_id(&sid));
            CHECK(!stqr_subscribe(c,id,sid,0,0,0,NULL,&data,&n,&info,&cursor));
            CHECK(n == length && !memcmp(data,big,n)); stqr_free(data);
            CHECK(!stqr_unsubscribe(c,sid));
        }
        o = fresh(); CHECK(!stqr_append(c,id,big,length,&o,&info,&offset)); CHECK(offset == length && info.length == 2*length);
        CHECK(!stqr_get_range(c,id,length,length,&data,&n,&info)); CHECK(n == length && !memcmp(data,big,n)); stqr_free(data);
        free(big);
    }
    o = fresh(); CHECK(!stqr_put(c,id,NULL,0,&o,&info));
    CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == 0); stqr_free(data);
    o = fresh(); CHECK(!stqr_delete(c,id,&o)); CHECK(stqr_get(c,id,&data,&n,&info) == -ENOENT);
    stqr_close(c); stqr_close(NULL);
    CHECK(stqr_dial(&c,url,getenv("STQR_TEST_USER"),"wrong-password",bucket,"fs",3000) == -EACCES);
    CHECK(!c);
    puts("C client integration passed");
    return 0;
}

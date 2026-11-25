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
    CHECK(stqr_dial(&c, "http://localhost", "user", "pw", bucket, 1000) == -EINVAL);
    CHECK(c == NULL);
    if (!url) { puts("local API validation passed; network tests run through STQR_TEST_C=1 go test ./server"); return 0; }
    CHECK(!stqr_parse_id(getenv("STQR_TEST_BUCKET"), &bucket));
    CHECK(!stqr_parse_object_id("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", &id));
    CHECK(!stqr_dial(&c, url, getenv("STQR_TEST_USER"), getenv("STQR_TEST_PASSWORD"), bucket, 3000));
    if (mode && !strcmp(mode,"fence")) {
        stqr_fence *fence=NULL; stqr_commit_mark mark;
        struct fence_waiter waiter; pthread_t worker;
        o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(!stqr_fence_capture(c,&fence));
        CHECK(!stqr_fence_add(c,fence) && stqr_fence_count(fence)==1);
        waiter.fence=fence;waiter.error=1;
        CHECK(!pthread_create(&worker,NULL,wait_fence,&waiter));
        // The fixture holds SYNC until this original session reads again.
        CHECK(!stqr_stat(c,id,&info));
        CHECK(!pthread_join(worker,NULL) && !waiter.error);
        stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
        o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(!stqr_fence_capture(c,&fence));
        o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && mark.sequence==3);
        CHECK(!stqr_fence_capture(c,&fence));
        CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);
        CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
        {
            stqr_client *second=NULL,*other_bucket=NULL;stqr_id foreign=bucket;
            CHECK(!stqr_dial(&second,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,3000));
            o=fresh();o.accepted=1;CHECK(!stqr_put(second,id,"hello",5,&o,&info));
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
            CHECK(!stqr_fence_reset(&fence));
            CHECK(!stqr_fence_add(second,fence) && !stqr_fence_add(c,fence));
            CHECK(stqr_fence_count(fence)==1); // maximum mark for the same owner
            CHECK(!stqr_fence_wait(fence));stqr_fence_apply(c,fence);stqr_fence_apply(second,fence);
            CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
            CHECK(!stqr_pending_mark(second,&mark) && !mark.sequence);
            foreign.bytes[0]^=2;
            CHECK(!stqr_dial(&other_bucket,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),foreign,3000));
            o=fresh();o.accepted=1;CHECK(!stqr_put(c,id,"hello",5,&o,&info));
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
    if (mode && !strcmp(mode,"routing")) {
        for (unsigned step=0;step<8;step++) {
            memset(&id,0,sizeof(id)); id.bytes[0]=(step&1)?0x81:1;
            o=fresh();o.accepted=1;
            CHECK(!stqr_put(c,id,"routed",6,&o,&info));
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
            CHECK(!stqr_dial(&writer,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,3000));
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
    CHECK(!stqr_put(c,id,"hello",5,&o,&info)); CHECK(info.length == 5);
    CHECK(!stqr_get(c,id,&data,&n,&info)); CHECK(n == 5 && !memcmp(data,"hello",5)); stqr_free(data);
    {
        stqr_commit_mark mark, wrong, durable;
        o = fresh(); o.accepted = 1;
        CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(!stqr_pending_mark(c,&mark) && mark.sequence);
        wrong = mark; wrong.boot.bytes[0] ^= 1;
        CHECK(stqr_sync_through(c,&wrong,&durable) == -ESTALE);
        CHECK(!stqr_pending_mark(c,&wrong) && wrong.sequence == mark.sequence);
        CHECK(!stqr_sync(c)); CHECK(!stqr_pending_mark(c,&mark) && !mark.sequence);
    }
    {
        stqr_client *watch = NULL; stqr_id sid; stqr_u64 cursor;
        struct observer events = {0};
        CHECK(!stqr_dial(&watch,url,getenv("STQR_TEST_USER"),getenv("STQR_TEST_PASSWORD"),bucket,3000));
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
        o = fresh(); CHECK(!stqr_put(c,id,"hello",5,&o,&info));
        CHECK(stqr_poll(watch,20) == 0);
        CHECK(!stqr_request_id(&sid));
        CHECK(!stqr_subscribe(watch,id,sid,0,0,0,NULL,&data,&n,&info,&cursor)); stqr_free(data);
        o = fresh(); CHECK(!stqr_delete(c,id,&o)); wait_change(watch,&events,2);
        CHECK(events.last.change == STQR_UPDATE_DELETE);
        o = fresh(); CHECK(!stqr_put(c,id,"hello",5,&o,&info)); wait_change(watch,&events,3);
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
    CHECK(stqr_dial(&c,url,getenv("STQR_TEST_USER"),"wrong-password",bucket,3000) == -EACCES);
    CHECK(!c);
    puts("C client integration passed");
    return 0;
}

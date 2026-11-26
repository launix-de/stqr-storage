/* SPDX-License-Identifier: GPL-3.0-only */
/* Deterministic protocol-state regressions: no live socket or fault timing. */
#include "../src/core.c"
#include <assert.h>
#include <stdio.h>
struct result {int called,error,status;};
static void completed(void *arg,int error,int status,void *data,size_t n,const stqr_info *info) {
    struct result *r=arg;(void)n;(void)info;sq_free(data);
    r->called++;r->error=error;r->status=status;
}
static struct sq_async *ticket(stqr_client *c,struct sq_channel *channel,unsigned sequence,int fire) {
    struct sq_async *p=sq_alloc(sizeof(*p));unsigned h;assert(p);
    p->channel=channel;p->id.bytes[0]=(stqr_u8)sequence;p->sequence=sequence;p->fire=fire;
    p->op=p->wire_op=fire?0x118:0x122;p->cut=sequence-1;
    h=async_hash(p->id.bytes);p->next=c->async[h];c->async[h]=p;c->async_count++;return p;
}
static void failed_fence(int malformed) {
    stqr_client c={0};struct sq_channel channel={0};struct result r={0};struct buf response={0},headers={0};
    struct sq_async *good=ticket(&c,&channel,1,1),*bad=ticket(&c,&channel,2,1),*fence=ticket(&c,&channel,3,0);
    stqr_id good_id=good->id;int error=malformed?-EPROTO:status_errno(503);
    bad->error=-EEXIST;bad->status=409;fence->complete=completed;fence->arg=&r;
    c.active=&channel;channel.submitted=3;
    varint(&response,malformed?200:503);varint(&headers,0);blob(&response,headers.p,headers.n);
    /* A 200 without boot/cut is a malformed fence, not confirmation. */
    assert(async_dispatch(&c,2,0x122,fence->id.bytes,&response)==1);
    assert(r.called==1&&r.error==error&&r.status==(malformed?200:503));
    assert(c.async_count==1&&async_find(&c,good_id.bytes)==good);
    assert(!c.durability_lost);async_fail(&c,&channel,-ECONNRESET);
    assert(c.durability_lost&&c.async_count==0);
    drop(&response);drop(&headers);
}
static void successful_fence(void) {
    stqr_client c={0};struct sq_channel channel={0};struct result r={0};struct buf response={0},headers={0};stqr_u8 boot[16]={1};
    struct sq_async *bad=ticket(&c,&channel,1,1),*fence=ticket(&c,&channel,2,0);
    bad->error=-EEXIST;bad->status=409;fence->complete=completed;fence->arg=&r;c.active=&channel;
    varint(&response,200);varint(&headers,0);blob(&response,headers.p,headers.n);put(&response,boot,16);number(&response,1,8);
    assert(async_dispatch(&c,2,0x122,fence->id.bytes,&response)==1);
    assert(r.called==1&&r.error==-EEXIST&&r.status==409&&!c.async_count);
    drop(&response);drop(&headers);
}
static void confirmed_and_legacy_marks(void) {
    stqr_client c={0};struct sq_channel channel={0};struct buf mark={0};stqr_u8 boot[16]={1};
    c.active=&channel;c.capabilities=STQR_CAP_CONNECTION_SYNC|STQR_CAP_DURABILITY;
    put(&mark,boot,16);number(&mark,7,8);
    assert(!observe_mark(&c,&mark)&&c.pending_mark.sequence==7);
    mark.pos=0;assert(!confirm_mark(&c,&mark)&&!c.pending_mark.sequence&&c.synced_mark.sequence==7);
    async_fail(&c,&channel,-ECONNRESET);assert(!c.durability_lost&&!channel.lost);
    drop(&mark);put(&mark,boot,16);number(&mark,8,8);
    assert(!observe_mark(&c,&mark));async_fail(&c,&channel,-ECONNRESET);
    assert(c.durability_lost&&channel.lost);
    mark.pos=0;assert(!confirm_mark(&c,&mark)&&c.durability_lost&&c.pending_mark.sequence==8);
    memset(&c,0,sizeof(c));memset(&channel,0,sizeof(channel));
    c.active=&channel;c.capabilities=STQR_CAP_DURABILITY;
    mark.pos=0;assert(!observe_mark(&c,&mark));
    async_fail(&c,&channel,-ECONNRESET);
    assert(!c.durability_lost&&!channel.lost&&c.pending_mark.sequence==8);
    /* Legacy same-boot confirmation is still accepted after socket loss. */
    mark.pos=0;assert(!confirm_mark(&c,&mark)&&!c.pending_mark.sequence&&c.synced_mark.sequence==8);
    channel.pending=c.synced_mark;channel.capabilities=STQR_CAP_DURABILITY;
    c.active=NULL;async_fail(&c,&channel,-ECONNRESET);assert(!channel.lost);
    drop(&mark);
}
int main(void) {
    failed_fence(0);failed_fence(1);successful_fence();confirmed_and_legacy_marks();
    puts("C failed fences retain unconfirmed tickets and original errors");return 0;
}

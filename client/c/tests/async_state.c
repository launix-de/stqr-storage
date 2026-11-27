/* SPDX-License-Identifier: GPL-3.0-only */
/* Deterministic protocol-state regressions: no live socket or fault timing. */
/* A deterministic WebSocket peer counts actual emitted protocol fences.
 * Crypto/allocation/time remain the normal userspace port implementation. */
#define sq_send policy_send
#define sq_recv policy_recv
#define sq_wait policy_wait
#define sq_connect policy_connect
#define sq_disconnect policy_disconnect
#include "../src/core.c"
#undef sq_send
#undef sq_recv
#undef sq_wait
#undef sq_connect
#undef sq_disconnect
#include <assert.h>
#include <stdio.h>
struct stqr_socket { struct buf replies,held; unsigned calls[512]; stqr_u64 sequence,reply_length; unsigned boot;int closed,hold,redirect; };
int policy_connect(struct stqr_socket **out,const char *host,unsigned port,unsigned ms) {
    (void)out;(void)host;(void)port;(void)ms;return -EOPNOTSUPP;
}
int policy_send(struct stqr_socket *socket,const void *data,size_t size) {
    const stqr_u8 *wire=data;size_t at=2,n;struct buf input={0},message={0},headers={0},mark={0};
    assert(size>=6 && (wire[0]&15)==2 && (wire[1]&128));
    n=wire[1]&127;
    if(n==126){assert(size>=8);n=((size_t)wire[2]<<8)|wire[3];at=4;}
    if(n==127){assert(size>=14);n=0;for(unsigned i=0;i<8;i++)n=(n<<8)|wire[2+i];at=10;}
    assert(n<=SQ_WIRE && at+4+n==size);
    for(size_t i=0;i<n;i++){stqr_u8 b=wire[at+4+i]^wire[at+i%4];put(&input,&b,1);}
    assert(readnum(&input,1)==1);unsigned kind=(unsigned)readnum(&input,1);
    unsigned op=(unsigned)readnum(&input,2);const stqr_u8 *id=take(&input,16);assert(op<512 && id);
    socket->calls[op]++;
    if(kind==0){assert(op==5 || op==6);drop(&input);return 0;}
    assert(kind==1);
    int redirected=socket->redirect && op==0x118;
    number(&message,1,1);number(&message,2,1);number(&message,op,2);put(&message,id,16);varint(&message,redirected?301:200);
    if(redirected) {
        struct buf patch={0};stqr_u8 zeroes[32]={0},node[16]={2},volume[16]={3},runtime[16]={9};
        number(&patch,1,8);number(&patch,1,8);number(&patch,1,8);put(&patch,input.p+input.pos,16);
        put(&patch,zeroes,32);number(&patch,1,1);put(&patch,zeroes,32);varint(&patch,1);
        put(&patch,zeroes,32);number(&patch,0,2);number(&patch,1,8);number(&patch,1,8);number(&patch,0,1);put(&patch,runtime,16);
        varint(&patch,1);put(&patch,node,16);put(&patch,volume,16);number(&patch,0,1);number(&patch,0,1);number(&patch,0,2);number(&patch,0,4);
        varint(&patch,1);put(&patch,node,16);varint(&patch,1);blob(&patch,"http://owner-b:80",17);varint(&patch,0);
        varint(&headers,1);varint(&headers,2);blob(&headers,patch.p,patch.n);drop(&patch);socket->redirect=0;
    }else if(op==0x121 || op==0x118) {
        if(op==0x121)assert(input.p[input.n-1]==1); /* accepted reference trailer */
        stqr_u8 boot[16]={1};boot[0]=(stqr_u8)(socket->boot?socket->boot:1);put(&mark,boot,16);number(&mark,++socket->sequence,8);
        varint(&headers,1);varint(&headers,48);blob(&headers,mark.p,mark.n);
    }else { assert(op==0x122 || op==0x119);varint(&headers,0); }
    blob(&message,headers.p,headers.n);
    if(redirected) { /* A rejected original owner publishes no object result. */ }
    else if(op==0x121)number(&message,1,1);
    else if(op==0x118) {
        stqr_u8 runtime[16]={0};number(&message,socket->reply_length?socket->reply_length:4,8);number(&message,0,1);number(&message,0,4);
        put(&message,runtime,16);number(&message,0,1);number(&message,0,2);
    }else {stqr_u8 boot[16]={1};boot[0]=(stqr_u8)(socket->boot?socket->boot:1);put(&message,boot,16);number(&message,socket->sequence,8);}
    stqr_u8 frame[4]={0x82,0};size_t h=2;
    if(message.n<126)frame[1]=(stqr_u8)message.n;
    else{frame[1]=126;frame[2]=(stqr_u8)(message.n>>8);frame[3]=(stqr_u8)message.n;h=4;}
    struct buf *output=socket->hold && op!=0x122 && op!=0x119?&socket->held:&socket->replies;
    put(output,frame,h);put(output,message.p,message.n);
    drop(&input);drop(&message);drop(&headers);drop(&mark);return 0;
}
int policy_recv(struct stqr_socket *socket,void *out,size_t n) {
    if(n>socket->replies.n-socket->replies.pos)return -EIO;
    memcpy(out,socket->replies.p+socket->replies.pos,n);socket->replies.pos+=n;return 0;
}
int policy_wait(struct stqr_socket *socket,unsigned ms) {
    (void)ms;return socket->replies.pos<socket->replies.n;
}
void policy_disconnect(struct stqr_socket *socket) {if(socket){socket->closed=1;drop(&socket->replies);drop(&socket->held);}}
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
static void observation_and_capacity_never_sync(void) {
    stqr_client c={0};struct sq_channel a={0},b={0};stqr_commit_mark out;
    c.channels=c.active=&a;c.channel_count=SQ_CHANNEL_LIMIT;a.next=&b;
    c.pending_mark.sequence=3;c.pending_mark.boot.bytes[0]=1;
    b.pending.sequence=8;b.pending.boot.bytes[0]=2;
    assert(stqr_pending_mark(&c,&out)==-EOPNOTSUPP && !out.sequence);
    b.pending.boot.bytes[0]=1;
    assert(!stqr_pending_mark(&c,&out) && out.sequence==8);
    strcpy(a.authority,"original:80");strcpy(b.authority,"dirty:80");
    assert(select_endpoint(&c,"http://next:80")==-ENOSPC);
    assert(c.active==&a && a.pending.sequence==3 && b.pending.sequence==8);
}
static void accepted_mode_never_downgrades(void) {
    struct stqr_socket socket={0};stqr_client c={.socket=&socket};stqr_object_id object={{1}};stqr_id token={{1}};struct result r={0};
    struct sq_async request={.op=0x108,.mutation=2};
    c.capabilities=STQR_CAP_OBJECT_REFERENCES;stqr_set_accepted_references(&c,1);
    assert(stqr_object_pin(&c,object,token)==-EOPNOTSUPP);
    assert(stqr_object_reference_async(&c,object,token,0,completed,&r)==-EOPNOTSUPP);
    assert(async_send(&c,&request)==-EOPNOTSUPP && !c.channels && !r.called);
    {
        struct buf body={0},reply={0};stqr_id id={{1}};stqr_u8 hash[32]={0};
        c.capabilities=STQR_CAP_DURABILITY;
        assert(request_payload(&c,0x102,&body,id,2,&reply,NULL,"x",1,hash)==-EOPNOTSUPP);
        assert(!socket.calls[0x118]);
    }
}
static void burst_has_only_explicit_fence(void) {
    enum {N=16};struct stqr_socket socket={0};stqr_client *c=sq_alloc(sizeof(*c));
    struct result references[N]={{0}},writes[N]={{0}},fence={0};stqr_commit_mark mark;
    assert(c);c->socket=&socket;c->bucket.bytes[0]=1;
    c->capabilities=STQR_CAP_DURABILITY|STQR_CAP_OBJECT_REFERENCES|STQR_CAP_CONNECTION_SYNC|STQR_CAP_ACCEPTED_REFERENCES|STQR_CAP_ACCEPTED_STREAMS;
    stqr_set_accepted_references(c,1);
    for(unsigned i=0;i<N;i++) {
        stqr_object_id object={{0}};stqr_id token={{0}};stqr_options options={.accepted=1};
        object.bytes[0]=(stqr_u8)(i+1);token.bytes[0]=(stqr_u8)(i+1);options.request_id.bytes[0]=(stqr_u8)(i+1);
        assert(!stqr_object_reference_async(c,object,token,0,completed,&references[i]));
        assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&writes[i]));
    }
    while(stqr_async_pending(c))assert(stqr_poll(c,0)>0);
    for(unsigned i=0;i<N;i++)assert(references[i].called==1&&!references[i].error&&writes[i].called==1&&!writes[i].error);
    assert(socket.calls[0x121]==N && socket.calls[0x118]==N && !socket.calls[0x122]);
    {
        size_t size=SQ_CHUNK+17;void *data=sq_alloc(size);stqr_options options={.accepted=1};stqr_info info;
        stqr_object_id object={{127}};assert(data);options.request_id.bytes[0]=127;socket.reply_length=size;
        assert(!stqr_put(c,object,data,size,&options,&info) && info.length==size);sq_free(data);
        assert(socket.calls[0x118]==N+1 && socket.calls[5]==2 && socket.calls[6]==1 && !socket.calls[0x122]);
    }
    assert(!stqr_pending_mark(c,&mark)&&mark.sequence==2*N+1 && !socket.calls[0x122]);
    assert(!stqr_connection_sync_async(c,completed,&fence));
    while(!fence.called)assert(stqr_poll(c,0)>0);
    assert(!fence.error && socket.calls[0x122]==1);
    {
        struct result dirty={0};stqr_object_id object={{99}};stqr_id token={{99}};
        assert(!stqr_object_reference_async(c,object,token,0,completed,&dirty));
        while(!dirty.called)assert(stqr_poll(c,0)>0);
        assert(!dirty.error && !stqr_pending_mark(c,&mark) && mark.sequence);
    }
    stqr_close(c);assert(socket.closed && socket.calls[0x122]==1);
}
static void dirty_owner_pool_preserves_explicit_cut(void) {
    struct stqr_socket sockets[SQ_CHANNEL_LIMIT]={0};
    struct sq_channel *owners[SQ_CHANNEL_LIMIT];struct result results[SQ_CHANNEL_LIMIT]={{0}},again={0},fence={0};
    stqr_client *c=sq_alloc(sizeof(*c));assert(c);c->bucket.bytes[0]=1;
    const stqr_u64 caps=STQR_CAP_DURABILITY|STQR_CAP_CONNECTION_SYNC;
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++) {
        struct sq_channel *owner=sq_alloc(sizeof(*owner));assert(owner);owners[i]=owner;
        owner->socket=&sockets[i];owner->capabilities=caps;sockets[i].boot=i+1;
        snprintf(owner->authority,sizeof(owner->authority),"owner-%u:80",i);
        owner->next=c->channels;c->channels=owner;c->channel_count++;
    }
    load_channel(c,owners[0]);
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++) {
        stqr_options options={.accepted=1};stqr_object_id object={{0}};object.bytes[0]=(stqr_u8)(i+1);
        options.request_id.bytes[0]=(stqr_u8)(i+1);save_channel(c);load_channel(c,owners[i]);
        assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&results[i]));
        while(!results[i].called)assert(stqr_poll(c,0)>0);
        assert(!results[i].error && !sockets[i].calls[0x122]);
    }
    assert(select_endpoint(c,"http://one-more-owner:80")==-ENOSPC);
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++)assert(owners[i]->pending.sequence && !sockets[i].closed && !sockets[i].calls[0x122]);
    {
        stqr_options options={.accepted=1};stqr_object_id object={{127}};options.request_id.bytes[0]=127;
        save_channel(c);load_channel(c,owners[1]);
        assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&again));
        while(!again.called)assert(stqr_poll(c,0)>0);
        assert(!again.error);
    }
    assert(!stqr_connection_sync_async(c,completed,&fence));
    while(!fence.called)assert(stqr_poll(c,0)>0);
    assert(!fence.error);
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++)assert(sockets[i].calls[0x122]==1);
    stqr_close(c);
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++)assert(sockets[i].closed && sockets[i].calls[0x122]==1);
}
static void release_held(struct stqr_socket *socket) {
    put(&socket->replies,socket->held.p,socket->held.n);drop(&socket->held);
}
static void legacy_sync_joins_result(void) {
    struct stqr_socket socket={0};stqr_client *c=sq_alloc(sizeof(*c));
    struct result result={0};stqr_options options={.accepted=1};stqr_object_id object={{7}};
    assert(c);c->socket=&socket;c->bucket.bytes[0]=1;c->capabilities=STQR_CAP_DURABILITY;options.request_id.bytes[0]=7;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&result));
    assert(!result.called && stqr_async_pending(c)==1);
    assert(!stqr_sync(c));
    assert(result.called==1 && !result.error && !stqr_async_pending(c));
    assert(socket.calls[0x119]>=1 && !socket.calls[0x122]);
    stqr_close(c);
}
static void redirected_result_precedes_fence(void) {
    struct stqr_socket a={.redirect=1},b={.boot=2,.hold=1};
    struct sq_channel *left=sq_alloc(sizeof(*left)),*right=sq_alloc(sizeof(*right));
    stqr_client *c=sq_alloc(sizeof(*c));struct result first={0},later={0},fence={0},later_fence={0};
    stqr_options options={.accepted=1};stqr_object_id object={{7}};
    assert(c&&left&&right);c->bucket.bytes[0]=1;c->channels=left;left->next=right;c->channel_count=2;
    left->socket=&a;right->socket=&b;left->capabilities=right->capabilities=STQR_CAP_DURABILITY|STQR_CAP_CONNECTION_SYNC;
    strcpy(left->authority,"owner-a:80");strcpy(right->authority,"owner-b:80");load_channel(c,left);
    options.request_id.bytes[0]=7;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&first));
    assert(!stqr_connection_sync_async(c,completed,&fence));
    assert(!fence.called);
    for(unsigned i=0;i<8;i++){int n=stqr_poll(c,0);assert(n>=0);if(!n)break;}
    assert(b.calls[0x118]==1 && !first.called && !fence.called);
    release_held(&b);
    save_channel(c);load_channel(c,right);options.request_id.bytes[0]=8;object.bytes[0]=8;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&later));
    assert(!stqr_connection_sync_async(c,completed,&later_fence));
    for(unsigned i=0;i<32&&!fence.called;i++)assert(stqr_poll(c,0)>=0);
    assert(first.called==1&&!first.error && fence.called==1&&!fence.error);
    assert(!later.called && !later_fence.called && a.calls[0x122]>=1 && b.calls[0x122]>=1);
    unsigned left_fences=a.calls[0x122],right_fences=b.calls[0x122];
    stqr_close(c);
    assert(later.called==1&&later.error==-EINPROGRESS && fence.called==1);
    assert(later_fence.called==1&&later_fence.error==-ECONNRESET);
    assert(a.calls[0x122]==left_fences&&b.calls[0x122]==right_fences);
}
static void close_cancels_deferred_fence(int reconnect) {
    struct stqr_socket socket={.hold=1};stqr_client *c=sq_alloc(sizeof(*c));
    struct result result={0},fence={0};stqr_options options={.accepted=1};stqr_object_id object={{7}};
    assert(c);c->socket=&socket;c->bucket.bytes[0]=1;c->capabilities=STQR_CAP_DURABILITY|STQR_CAP_CONNECTION_SYNC;options.request_id.bytes[0]=7;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&result));
    assert(!stqr_connection_sync_async(c,completed,&fence));
    assert(!fence.called);
    unsigned fences=socket.calls[0x122];
    if(reconnect)assert(stqr_reconnect(c)==-EOPNOTSUPP);
    else stqr_close(c);
    assert(result.called==1&&result.error==-EINPROGRESS);
    assert(fence.called==1&&fence.error==-ECONNRESET);
    assert(socket.closed&&socket.calls[0x122]==fences);
    if(reconnect){stqr_close(c);assert(fence.called==1&&result.called==1);}
}
static void clean_eviction_preserves_deferred_fence(void) {
    struct stqr_socket sockets[SQ_CHANNEL_LIMIT]={0};struct sq_channel *owner[SQ_CHANNEL_LIMIT];
    stqr_client *c=sq_alloc(sizeof(*c));struct result result={0},fence={0};
    stqr_options options={.accepted=1};stqr_object_id object={{7}};
    assert(c);c->bucket.bytes[0]=1;
    for(unsigned i=0;i<SQ_CHANNEL_LIMIT;i++) {
        owner[i]=sq_alloc(sizeof(*owner[i]));assert(owner[i]);
        owner[i]->socket=&sockets[i];owner[i]->capabilities=STQR_CAP_DURABILITY|STQR_CAP_CONNECTION_SYNC;
        snprintf(owner[i]->authority,sizeof(owner[i]->authority),"clean-%u:80",i);
        owner[i]->next=c->channels;c->channels=owner[i];c->channel_count++;
    }
    load_channel(c,owner[0]);sockets[0].hold=1;options.request_id.bytes[0]=7;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&result));
    assert(!stqr_connection_sync_async(c,completed,&fence));
    assert(select_endpoint(c,"http://one-more-owner:80")==-EOPNOTSUPP);
    assert(!fence.called&&!result.called);
    release_held(&sockets[0]);
    for(unsigned i=0;i<32&&!fence.called;i++)assert(stqr_poll(c,0)>=0);
    assert(result.called==1&&!result.error && fence.called==1&&!fence.error);
    stqr_close(c);
}
static void overlapping_legacy_fences(void) {
    struct stqr_socket socket={0};stqr_client *c=sq_alloc(sizeof(*c));
    struct result first={0},later={0},one={0},two={0};
    stqr_options options={.accepted=1};stqr_object_id object={{7}};
    assert(c);c->socket=&socket;c->bucket.bytes[0]=1;c->capabilities=STQR_CAP_DURABILITY;options.request_id.bytes[0]=7;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&first));
    assert(!stqr_connection_sync_async(c,completed,&one));
    options.request_id.bytes[0]=8;object.bytes[0]=8;
    assert(!stqr_cas_async(c,object,1,0,"old!",4,"new!",4,&options,completed,&later));
    assert(!stqr_connection_sync_async(c,completed,&two));
    for(unsigned i=0;i<32&&(!one.called||!two.called);i++){int e=stqr_poll(c,0);if(e<0)break;}
    assert(first.called==1&&!first.error&&later.called==1&&!later.error);
    assert(one.called==1&&!one.error&&two.called==1&&!two.error);
    stqr_close(c);
}
int main(void) {
    failed_fence(0);failed_fence(1);successful_fence();confirmed_and_legacy_marks();
    observation_and_capacity_never_sync();accepted_mode_never_downgrades();burst_has_only_explicit_fence();dirty_owner_pool_preserves_explicit_cut();
    overlapping_legacy_fences();legacy_sync_joins_result();redirected_result_precedes_fence();close_cancels_deferred_fence(0);close_cancels_deferred_fence(1);clean_eviction_preserves_deferred_fence();
    puts("C fences are explicit; ordinary bursts and close never checkpoint");return 0;
}

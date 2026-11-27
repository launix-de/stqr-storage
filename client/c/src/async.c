/* SPDX-License-Identifier: GPL-3.0-only */
/* Included by core.c. One caller/event loop owns framing, while callbacks and
 * request identities let arbitrarily ordered completions progress independently.
 * No worker thread, second connection per request, or response sorting exists. */
struct sq_async {
    struct sq_async *next;
    struct sq_channel *channel;
    stqr_id id, subscription;
    stqr_u64 *cursor;
    stqr_object_id object;
    unsigned op, wire_op, redirects;
    stqr_u64 offset, length, sequence, cut;
    int mutation, fire, error, status;
    stqr_commit_mark mark;
    struct buf body;
    struct download stream;
    stqr_completion complete;
    void *arg;
};
static unsigned async_hash(const stqr_u8 *id) {
    return (unsigned)(id[0]^id[5]^id[10]^id[15])&63;
}
static struct sq_async *async_find(stqr_client *c,const stqr_u8 *id) {
    struct sq_async *p;
    if (!id) return NULL;
    for (p=c->async[async_hash(id)];p;p=p->next)
        if (!memcmp(p->id.bytes,id,16)) return p;
    return NULL;
}
static void async_remove(stqr_client *c,struct sq_async *p) {
    struct sq_async **at=&c->async[async_hash(p->id.bytes)];
    while (*at && *at!=p) at=&(*at)->next;
    if (*at) { *at=p->next; c->async_count--; }
}
static void async_free(struct sq_async *p) {
    drop(&p->body);drop(&p->stream.data);sq_free(p);
}
static void async_sync_progress(stqr_client *c);
static void async_sync_cancel(stqr_client *c,struct sq_channel *channel,int error);
static void async_finish(stqr_client *c,struct sq_async *p,int error,int status,
                         void *data,size_t n,const stqr_info *info) {
    stqr_completion cb=p->complete;void *arg=p->arg;
    async_remove(c,p);async_free(p);
    if (cb) cb(arg,error,status,data,n,info);
    else sq_free(data);
    async_sync_progress(c);
}
static void async_fail(stqr_client *c,struct sq_channel *channel,int error) {
    unsigned i;
    /* A replacement ConnectionSync only covers its new socket. An accepted
     * reply's boot-qualified mark still identifies the old, known prefix:
     * retain it for SyncThrough on the caller's next EXPLICIT sync. Reconnect,
     * read/write and close never initiate persistence. Unknown mutation
     * outcomes are different: no later empty connection fence can certify
     * whether those operations were admitted, so their uncertainty stays
     * latched even if normal filesystem visibility can subsequently recover. */
    if (channel==c->active) {
        if ((c->capabilities&STQR_CAP_CONNECTION_SYNC) && c->pending_mark.sequence)
            c->detached_mark=c->pending_mark;
    } else if (channel && (channel->capabilities&STQR_CAP_CONNECTION_SYNC) && channel->pending.sequence)
        channel->detached=channel->pending;
    for (i=0;i<64;i++)
        for (struct sq_async *p=c->async[i];p;p=p->next)
            if (p->channel==channel && (p->mutation || p->fire)) {
                if (channel) channel->lost=1;
                if (channel==c->active) c->durability_lost=1;
            }
    if (channel==c->active && channel && c->durability_lost) channel->lost=1;
    async_sync_cancel(c,channel,error);
    for (i=0;i<64;i++) {
        struct sq_async *p=c->async[i],*next;
        while(p) {
            next=p->next;
            if (p->channel==channel) {
                async_finish(c,p,p->mutation?-EINPROGRESS:error,0,NULL,0,NULL);
            }
            p=next;
        }
    }
}
static int async_channel_pending(stqr_client *c,struct sq_channel *channel) {
    unsigned i;struct sq_async *p;
    for(i=0;i<64;i++)for(p=c->async[i];p;p=p->next)if(p->channel==channel)return 1;
    return 0;
}
unsigned stqr_async_pending(const stqr_client *c) { return c?c->async_count:0; }
void stqr_set_readable_handler(stqr_client *c,void (*ready)(void *),void *arg) {
    struct sq_channel *s;
    if(!c)return;
    c->readable=ready;c->readable_arg=arg;
    if(!c->channels) {if(c->socket)sq_set_readable(c->socket,ready,arg);return;}
    save_channel(c);
    for(s=c->channels;s;s=s->next)if(s->socket)sq_set_readable(s->socket,ready,arg);
}
static int async_send(stqr_client *c,struct sq_async *p) {
    struct buf wrapped={0};struct buf *body=&p->body;
    int err;
    if(p->mutation && any_lost(c) && !accepted_visibility_after_loss(c,p->op,p->mutation))return -ESTALE;
    if(p->mutation>1 && !(c->capabilities&STQR_CAP_DURABILITY))return -EOPNOTSUPP;
    if(p->op==0x121 && c->accepted_references && !references_accepted(c))return -EOPNOTSUPP;
    err=ensure_channel(c);if(err)return err;
    /* A redirect may reach an older peer after the original negotiation. Such
     * a peer cannot certify a no-ACK prefix; surface this instead of returning
     * a false successful legacy fence. */
    if(p->fire && !(c->capabilities&STQR_CAP_CONNECTION_SYNC))return -EOPNOTSUPP;
    p->wire_op=p->op;
    if(p->op==0x121 && references_accepted(c)) {
        put(&wrapped,body->p,body->n);number(&wrapped,1,1);body=&wrapped;
    }
    if(p->mutation>1 && (c->capabilities&STQR_CAP_DURABILITY)) {
        if(body->n<80)return -EINVAL;
        put(&wrapped,body->p,80);number(&wrapped,p->op,2);put(&wrapped,body->p+80,body->n-80);
        body=&wrapped;p->wire_op=0x118;
    }
    p->channel=c->active;
    p->channel->submitted=p->sequence;
    err=message_send(c,p->fire?0:1,p->wire_op,p->id,body);
    drop(&wrapped);save_channel(c);
    if(err) {if(p->mutation)c->durability_lost=1;return p->mutation?-EINPROGRESS:err;}
    return 0;
}
static int async_submit(stqr_client *c,unsigned op,struct buf *body,stqr_id id,
                        stqr_object_id object,int mutation,int fire,
                        stqr_u64 off,stqr_u64 len,stqr_completion cb,void *arg) {
    struct sq_async *p;unsigned h;int err;
    if(!c || !c->socket || body->err)return body->err?body->err:-ENOTCONN;
    if(!cb && !fire)return -EINVAL;
    if(zero(id.bytes,16)) {err=stqr_request_id(&id);if(err)return err;}
    if(async_find(c,id.bytes))return -EBUSY;
    p=sq_alloc(sizeof(*p));if(!p)return -ENOMEM;
    p->id=id;p->object=object;p->op=op;p->mutation=mutation;p->fire=fire;
    p->offset=off;p->length=len;p->complete=cb;p->arg=arg;
    p->sequence=++c->async_sequence;p->cut=p->sequence-1;
    if(op==0x122)p->mark=c->pending_mark;
    if(op==0x119)p->mark=c->detached_mark;
    put(&p->body,body->p,body->n);
    if(p->body.err){err=p->body.err;async_free(p);return err;}
    h=async_hash(id.bytes);p->next=c->async[h];c->async[h]=p;c->async_count++;
    err=async_send(c,p);
    if(err){async_remove(c,p);async_free(p);}
    return err;
}
static int async_headers(stqr_client *c,struct sq_async *p,struct buf *m,int *status) {
    struct buf headers={0};const stqr_u8 *raw;size_t n;stqr_u64 count,tags[64];unsigned i,j;
    *status=(int)readvar(m);raw=readblob(m,&n);headers.p=(stqr_u8 *)raw;headers.n=n;
    c->query=p->object;c->query_valid=!zero(p->object.bytes,32);c->redirect[0]=0;
    count=readvar(&headers);if(count>64)headers.err=-EPROTO;
    for(i=0;i<count&&!headers.err;i++) {
        tags[i]=readvar(&headers);for(j=0;j<i;j++)if(tags[j]==tags[i])headers.err=-EPROTO;
        raw=readblob(&headers,&n);
        if(tags[i]==2 && (*status==301||*status==302) && !headers.err)headers.err=redirect_patch(c,raw,n);
        if((tags[i]==48||tags[i]==49) && !headers.err){struct buf mark={.p=(stqr_u8 *)raw,.n=n};headers.err=tags[i]==48?observe_mark(c,&mark):confirm_mark(c,&mark);}
    }
    if(m->err||done(&headers)||*status<100||*status>599)return -EPROTO;
    return 0;
}
static int async_decode_get(stqr_client *c,struct sq_async *p,struct buf *r,void **data,size_t *actual,stqr_info *info) {
    const stqr_u8 *digest,*in;size_t n;stqr_u64 total,range=0;unsigned mode;int err;
    info_decode(r,info);
    if(p->op==0x101) {
        if(readnum(r,8)!=p->offset)r->err=-EPROTO;
        range=readnum(r,8);
        if(readnum(r,8)!=info->length||range>p->length||p->offset>info->length||range>info->length-p->offset)r->err=-EPROTO;
    }
    mode=(unsigned)readnum(r,1);total=readnum(r,8);digest=take(r,32);in=readblob(r,&n);
    if(done(r)||mode>1||total!=(p->op==0x101?range:info->length)||total>STQR_BUFFER_LIMIT)return -EPROTO;
    if(!mode) {
        stqr_u8 hash[32];if(p->stream.started||n!=total)return -EPROTO;
        err=sq_hash(256,in,n,hash);if(err)return err;
        if(memcmp(hash,digest,32))return -EBADMSG;
        put(&p->stream.data,in,n);if(p->stream.data.err)return p->stream.data.err;
    } else if(n||!p->stream.ended||total!=p->stream.length||memcmp(digest,p->stream.digest,32))return -EPROTO;
    err=accept_info(c,info);if(err)return err;
    *data=p->stream.data.p;*actual=p->stream.data.n;p->stream.data.p=NULL;return 0;
}
/* A failed checkpoint/protocol response does not certify successful writes.
 * Keep their tickets until a later successful cut or an uncertain disconnect;
 * definite per-write failures may retire without replacing the fence error. */
static void async_retire_fire(stqr_client *c,struct sq_async *fence,int confirmed,int *error,int *status) {
    unsigned i;int original_error=*error;stqr_u64 first=~(stqr_u64)0;
    for(i=0;i<64;i++) {
        struct sq_async *p=c->async[i],*next;
        while(p){next=p->next;
            if(p->fire&&p->channel==fence->channel&&p->sequence<=fence->cut&&(confirmed||p->error)){
                if(!original_error&&p->error&&p->sequence<first){first=p->sequence;*error=p->error;*status=p->status;}
                async_remove(c,p);async_free(p);
            }
            p=next;
        }
    }
}
static int async_dispatch(stqr_client *c,unsigned kind,unsigned op,const stqr_u8 *id,struct buf *m) {
    struct sq_async *p;int err,status=0;stqr_info info={0};void *data=NULL;size_t n=0;
    if(kind==3&&(op==5||op==6||op==13)) {
        const stqr_u8 *parent=take(m,16);p=async_find(c,parent);
        if(!p)return 0;
        err=stream_push(&p->stream,op,m);return err?err:1;
    }
    if(kind!=2 || !(p=async_find(c,id)))return 0;
    if(op!=p->wire_op)return -EPROTO;
    err=async_headers(c,p,m,&status);if(err)return err;
    c->status=status;
    if((status==301||status==302)&&c->redirect[0]&&++p->redirects<=16) {
        char endpoint[512];stqr_u64 revision=c->redirect_revision,epoch=c->redirect_epoch;stqr_id runtime=c->redirect_runtime;
        memcpy(endpoint,c->redirect,sizeof(endpoint));
        err=select_endpoint(c,endpoint);
        if(!err && p->body.n>=80) {
            unsigned k;for(k=0;k<8;k++){p->body.p[48+k]=(stqr_u8)(revision>>(8*k));p->body.p[56+k]=(stqr_u8)(epoch>>(8*k));}
            memcpy(p->body.p+64,runtime.bytes,16);drop(&p->stream.data);memset(&p->stream,0,sizeof(p->stream));
            err=async_send(c,p);
        }
        if(!err)return 1;
    } else err=status>=300?status_errno(status):0;
    if(p->fire){p->error=err;p->status=status;return 1;}
    if(!err&&p->op==0x110) {
        const stqr_u8 *sid=take(m,16);stqr_u64 cursor=readnum(m,8);
        if(!sid||memcmp(sid,p->subscription.bytes,16)||!cursor)err=-EPROTO;
        if(!err)err=async_decode_get(c,p,m,&data,&n,&info);
        if(!err) {
            struct sq_binding *binding=sq_alloc(sizeof(*binding));
            if(!binding)err=-ENOMEM;
            else {binding->id=p->subscription;binding->channel=p->channel;binding->next=c->bindings[binding_bucket(binding->id)];c->bindings[binding_bucket(binding->id)]=binding;*p->cursor=cursor;}
        }
    }
    else if(!err&&(p->op==0x100||p->op==0x101))err=async_decode_get(c,p,m,&data,&n,&info);
    else if(!err&&(p->op==0x107||p->op==0x108||p->op==0x102||p->op==0x103)){
        info_decode(m,&info);
        if(p->op==0x103 && (readnum(m,8)!=p->offset || readnum(m,8)!=p->length || readnum(m,8)!=info.length))m->err=-EPROTO;
        err=done(m);if(!err)err=accept_info(c,&info);
    }
    else if(!err&&p->op==0x121){if(readnum(m,1)!=1)m->err=-EPROTO;err=done(m);}
    else if(!err&&p->op==0x106)err=done(m);
    else if(!err&&p->op==0x111){
        struct sq_binding **at=binding_slot(c,p->subscription);err=done(m);
        if(!err&&*at){struct sq_binding *binding=*at;*at=binding->next;sq_free(binding);}
    }
    else if(!err&&p->op==0x119){
        stqr_commit_mark durable;
        err=decode_mark(m,&durable);
        if(!err && (memcmp(durable.boot.bytes,p->mark.boot.bytes,16) || durable.sequence<p->mark.sequence))err=-EPROTO;
        if(!err)note_active(c,&durable);
    }
    else if(p->op==0x122){
        if(!err){
            const stqr_u8 *boot=take(m,16);readnum(m,8);
            if(done(m)||!boot||zero(boot,16))err=-EPROTO;
            else if(p->mark.sequence&&memcmp(p->mark.boot.bytes,boot,16)) {
                /* A new server boot cannot certify an older accepted prefix,
                 * even if no fresh receipt has exposed that mismatch yet. */
                c->durability_lost=1;err=-ESTALE;
            }
        }
        /* Notices already queued before this ACK are covered too. If another
         * request was submitted meanwhile retain its potentially newer mark. */
        if(!err && !c->detached_mark.sequence && p->channel->submitted==p->sequence) {
            stqr_commit_mark covered=c->pending_mark;
            if(covered.sequence)note_active(c,&covered);
        }
        else if(!err && !c->detached_mark.sequence && p->mark.sequence)note_active(c,&p->mark);
        async_retire_fire(c,p,!err,&err,&status);
    }
    async_finish(c,p,err,status,data,n,(p->op==0x100||p->op==0x101||p->op==0x107||p->op==0x108||p->op==0x110||p->op==0x102||p->op==0x103)?&info:NULL);
    return 1;
}
int stqr_object_reference_async(stqr_client *c,stqr_object_id target,stqr_id token,unsigned action,stqr_completion cb,void *arg) {
    static const char domain[]="STQR object references v1";
    stqr_object_id root;stqr_id rid={{0}};struct buf b={0};int err;
    if(!c||!cb||action>5||zero(token.bytes,16))return -EINVAL;
    if(!(c->capabilities&STQR_CAP_OBJECT_REFERENCES))return -EOPNOTSUPP;
    if(c->accepted_references && !references_accepted(c))return -EOPNOTSUPP;
    err=sq_hash(256,domain,sizeof(domain)-1,root.bytes);if(err)return err;
    reference(c,&b,root);put(&b,target.bytes,32);number(&b,action,1);put(&b,token.bytes,16);
    err=async_submit(c,0x121,&b,rid,root,1,0,0,0,cb,arg);drop(&b);return err;
}
int stqr_delete_async(stqr_client *c,stqr_object_id id,const stqr_options *o,stqr_completion cb,void *arg) {
    struct buf b={0};int err;if(!c||!o||!cb)return -EINVAL;
    reference(c,&b,id);err=options(&b,o);
    if(!err)err=async_submit(c,0x106,&b,o->request_id,id,1+!!o->accepted,0,0,0,cb,arg);
    drop(&b);return err;
}
int stqr_unsubscribe_async(stqr_client *c,stqr_id sid,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_id rid;struct sq_async *p;int err;
    if(!c||!cb||zero(sid.bytes,16))return -EINVAL;
    {struct sq_binding *binding=*binding_slot(c,sid);
        if(binding){save_channel(c);load_channel(c,binding->channel);}}
    err=stqr_request_id(&rid);if(err)return err;
    put(&b,sid.bytes,16);err=async_submit(c,0x111,&b,rid,(stqr_object_id){{0}},0,0,0,0,cb,arg);drop(&b);
    if(!err){p=async_find(c,rid.bytes);p->subscription=sid;}
    return err;
}
int stqr_subscribe_async(stqr_client *c,stqr_object_id id,stqr_id sid,stqr_u64 *cursor,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_id rid;struct sq_async *p;int err;
    if(!c||!cursor||!cb||zero(sid.bytes,16))return -EINVAL;
    if(!(c->capabilities&STQR_CAP_SUBSCRIPTIONS))return -EOPNOTSUPP;
    if(*binding_slot(c,sid))return -EBUSY;
    err=stqr_request_id(&rid);if(err)return err;
    reference(c,&b,id);put(&b,sid.bytes,16);number(&b,0,1);
    number(&b,0,8);number(&b,0,8);number(&b,0,1);
    err=async_submit(c,0x110,&b,rid,id,0,0,0,0,cb,arg);drop(&b);
    if(!err){p=async_find(c,rid.bytes);p->subscription=sid;p->cursor=cursor;}
    return err;
}
int stqr_get_async(stqr_client *c,stqr_object_id id,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_id rid={{0}};int err;if(!c)return -EINVAL;
    reference(c,&b,id);number(&b,0,1);err=async_submit(c,0x100,&b,rid,id,0,0,0,0,cb,arg);drop(&b);return err;
}
int stqr_get_range_async(stqr_client *c,stqr_object_id id,stqr_u64 off,stqr_u64 len,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_id rid={{0}};int err;if(!c||len>~(stqr_u64)0-off)return -EINVAL;
    reference(c,&b,id);number(&b,off,8);number(&b,len,8);number(&b,0,1);err=async_submit(c,0x101,&b,rid,id,0,0,off,len,cb,arg);drop(&b);return err;
}
int stqr_stat_async(stqr_client *c,stqr_object_id id,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_id rid={{0}};int err;if(!c)return -EINVAL;reference(c,&b,id);
    err=async_submit(c,0x107,&b,rid,id,0,0,0,0,cb,arg);drop(&b);return err;
}
int stqr_cas_async(stqr_client *c,stqr_object_id id,unsigned scope,stqr_u64 off,const void *expected,size_t en,const void *replacement,size_t rn,const stqr_options *o,stqr_completion cb,void *arg) {
    struct buf b={0};int err;
    if(!c||!o||!cb||scope>1||en>SQ_CHUNK||rn>SQ_CHUNK||(en&&!expected)||(rn&&!replacement)||(scope==0&&en!=rn)||(scope==1&&off)||rn>~(stqr_u64)0-off)return -EINVAL;
    reference(c,&b,id);number(&b,scope,1);number(&b,off,8);err=options(&b,o);
    if(!err){blob(&b,expected,en);blob(&b,replacement,rn);err=async_submit(c,0x108,&b,o->request_id,id,1+!!o->accepted,0,off,rn,cb,arg);}
    drop(&b);return err;
}
static int async_put(stqr_client *c,stqr_object_id id,stqr_u64 off,const void *data,size_t n,const stqr_options *o,int ranged,stqr_completion cb,void *arg) {
    struct buf b={0};stqr_u8 hash[32];int err;
    if(!c||!o||n>SQ_CHUNK||(n&&!data)||n>~(stqr_u64)0-off)return -EINVAL;
    if(!cb&&!(c->capabilities&STQR_CAP_CONNECTION_SYNC)){stqr_options opt=*o;stqr_info info;opt.accepted=1;return ranged?stqr_put_range(c,id,off,data,n,&opt,&info):stqr_put(c,id,data,n,&opt,&info);}
    reference(c,&b,id);if(ranged)number(&b,off,8);err=options(&b,o);
    if(!err)err=sq_hash(256,data,n,hash);
    if(!err){number(&b,0,1);number(&b,n,8);put(&b,hash,32);blob(&b,data,n);err=async_submit(c,ranged?0x103:0x102,&b,o->request_id,id,cb?(1+!!o->accepted):2,!cb,off,n,cb,arg);}
    drop(&b);return err;
}
int stqr_put_async(stqr_client *c,stqr_object_id id,const void *p,size_t n,const stqr_options *o){return async_put(c,id,0,p,n,o,0,NULL,NULL);}
int stqr_put_range_async(stqr_client *c,stqr_object_id id,stqr_u64 off,const void *p,size_t n,const stqr_options *o){return async_put(c,id,off,p,n,o,1,NULL,NULL);}
int stqr_put_result_async(stqr_client *c,stqr_object_id id,const void *p,size_t n,const stqr_options *o,stqr_completion cb,void *arg){if(!cb)return -EINVAL;return async_put(c,id,0,p,n,o,0,cb,arg);}
int stqr_put_range_result_async(stqr_client *c,stqr_object_id id,stqr_u64 off,const void *p,size_t n,const stqr_options *o,stqr_completion cb,void *arg){if(!cb)return -EINVAL;return async_put(c,id,off,p,n,o,1,cb,arg);}
struct async_sync_group {
    struct async_sync_group *next;
    stqr_client *client;stqr_completion complete;void *arg;
    stqr_u64 cut;
    unsigned pending;int error,status;
};
static int async_connection_sync_cut(stqr_client *,stqr_completion,void *,stqr_u64);
static void async_sync_done(void *arg,int error,int status,void *data,size_t n,const stqr_info *info) {
    struct async_sync_group *g=arg;(void)n;(void)info;sq_free(data);
    if(error&&!g->error){g->error=error;g->status=status;}
    if(--g->pending==0 && (!g->error || g->status==301 || g->status==302)) {
        unsigned i;int retry=0;struct sq_async *p;
        for(i=0;i<64;i++)for(p=g->client->async[i];p;p=p->next)
            if(p->fire&&p->sequence<=g->cut&&!p->error&&p->redirects){retry=1;break;}
        if(retry) {
            int result;g->pending=1;g->error=0;g->status=0;
            result=async_connection_sync_cut(g->client,async_sync_done,g,g->cut);
            if(!result)return;
            g->pending=0;g->error=result;
        }
    }
    if(g->pending==0){stqr_completion cb=g->complete;void *user=g->arg;int err=g->error,s=g->error?g->status:200;sq_free(g);cb(user,err,s,NULL,0,NULL);}
}
/* A result-bearing request can redirect after barrier submission. Join that
 * finite logical prefix before fencing its eventual owners. Later result jobs
 * are not prerequisites. The logical join is event driven; legacy checkpoint
 * requests retain their existing synchronous fallback. */
static int async_prior_results(stqr_client *c,stqr_u64 cut) {
    for(unsigned i=0;i<64;i++)for(struct sq_async *p=c->async[i];p;p=p->next)
        if(!p->fire && p->op!=0x122 && p->op!=0x119 && p->sequence<=cut)return 1;
    return 0;
}
static void async_sync_start(struct async_sync_group *g) {
    stqr_client *c=g->client;struct sq_channel *s,*original;int err=0;
    if(any_lost(c)){async_sync_done(g,-ESTALE,0,NULL,0,NULL);return;}
    save_channel(c);original=c->active;
    for(s=c->channels;s;s=s->next) {
        struct buf b={0};stqr_id rid={{0}};
        if(!s->socket) {
            if(s->pending.sequence&&!g->error)g->error=-ENOTCONN;
            continue;
        }
        load_channel(c,s);
        if ((c->capabilities&STQR_CAP_CONNECTION_SYNC) && c->detached_mark.sequence) {
            /* Join the old acknowledged prefix and this socket's new work.
             * Both requests are asynchronous and owner-local; only an explicit
             * client sync reaches this path. A failed old-prefix confirmation
             * leaves its mark intact even if the new ConnectionSync succeeds. */
            put(&b,c->bucket.bytes,16);put(&b,c->detached_mark.boot.bytes,16);number(&b,c->detached_mark.sequence,8);
            g->pending++;
            err=async_submit(c,0x119,&b,rid,(stqr_object_id){{0}},0,0,0,0,async_sync_done,g);
            drop(&b);
            if(err){g->pending--;if(!g->error){g->error=err;g->status=0;}}
        }
        g->pending++;
        if(c->capabilities&STQR_CAP_CONNECTION_SYNC)err=async_submit(c,0x122,&b,rid,(stqr_object_id){{0}},0,0,0,0,async_sync_done,g);
        else {err=sync_active(c);async_sync_done(g,err,c->status,NULL,0,NULL);err=0;}
        save_channel(c);if(err){g->pending--;if(!g->error){g->error=err;g->status=0;}}
    }
    load_channel(c,original);async_sync_done(g,0,200,NULL,0,NULL);
}
static void async_sync_progress(stqr_client *c) {
    struct async_sync_group **at=&c->sync_waiting;
    /* A legacy synchronous fence can dispatch normal async completions. Those
     * completions may release another group, but must not nest a reply loop. */
    if(c->sync_progressing)return;
    c->sync_progressing=1;
    while(*at) {
        struct async_sync_group *g=*at;
        if(async_prior_results(c,g->cut)){at=&g->next;continue;}
        *at=g->next;g->next=NULL;
        async_sync_start(g);
        /* Legacy barriers may dispatch other completed requests while waiting. */
        at=&c->sync_waiting;
    }
    c->sync_progressing=0;
}
static void async_sync_cancel(stqr_client *c,struct sq_channel *channel,int error) {
    struct async_sync_group **at=&c->sync_waiting;
    while(*at) {
        struct async_sync_group *g=*at;int affected=any_lost(c);
        for(unsigned i=0;i<64&&!affected;i++)for(struct sq_async *p=c->async[i];p;p=p->next)
            if(p->channel==channel && p->op!=0x122 && p->sequence<=g->cut){affected=1;break;}
        /* Evicting a clean unrelated channel does not cancel this prefix. */
        if(!affected){at=&g->next;continue;}
        *at=g->next;
        stqr_completion cb=g->complete;void *arg=g->arg;
        sq_free(g);cb(arg,error,0,NULL,0,NULL);
    }
}
static int async_connection_sync_cut(stqr_client *c,stqr_completion cb,void *arg,stqr_u64 cut) {
    struct async_sync_group *g;int err;
    if(!c||!cb)return -EINVAL;
    if(any_lost(c))return -ESTALE;
    err=ensure_channel(c);if(err)return err;
    g=sq_alloc(sizeof(*g));if(!g)return -ENOMEM;
    g->client=c;g->complete=cb;g->arg=arg;g->pending=1;g->cut=cut;
    g->next=c->sync_waiting;c->sync_waiting=g;
    async_sync_progress(c);return 0;
}
int stqr_connection_sync_async(stqr_client *c,stqr_completion cb,void *arg) {
    return async_connection_sync_cut(c,cb,arg,c?c->async_sequence:0);
}

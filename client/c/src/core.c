/* SPDX-License-Identifier: GPL-3.0-only */
#include "port.h"
#define SQ_CHUNK (1U << 20)
#define SQ_WIRE (16U << 20)
#define SQ_CHANNEL_LIMIT 32
#define SQ_BINDING_BUCKETS 256
struct sq_channel {
    struct sq_channel *next;
    struct stqr_socket *socket;
    char host[256], authority[272];
    unsigned port;
    stqr_id runtime;
    stqr_u64 capabilities, used, submitted;
    stqr_commit_mark pending, synced;
    int lost;
};
struct sq_binding { struct sq_binding *next; stqr_id id; struct sq_channel *channel; };
struct sq_route {
    struct sq_route *child[2];
    char endpoint[512];
    stqr_u64 revision, epoch;
    stqr_id runtime;
};
struct sq_async;
struct stqr_client {
    struct sq_async *async[64];
    struct sq_route *routes;
    unsigned async_count;
    stqr_u64 async_sequence;
    void (*readable)(void *);
    void *readable_arg;
    struct stqr_socket *socket;
    char host[256], authority[272];
    char *authorization;
    char format[256];
    size_t authorization_len;
    unsigned port, timeout;
    stqr_id bucket, runtime, lease;
    int status, accepted_references;
    stqr_u64 capabilities;
    stqr_update_handler update;
    void *update_arg;
    stqr_commit_mark pending_mark, synced_mark;
    int durability_lost;
    char redirect[512];
    stqr_object_id query;
    int query_valid;
    stqr_u64 redirect_revision, redirect_epoch;
    stqr_id redirect_runtime;
    struct sq_channel *channels, *active;
    struct sq_binding *bindings[SQ_BINDING_BUCKETS];
    unsigned channel_count;
    struct sq_channel *lease_owner;
    int lease_taken;
    stqr_u64 channel_clock;
};
struct buf { stqr_u8 *p; size_t n, cap, pos; int err; };
void stqr_set_accepted_references(stqr_client *c,int enabled) {
    if(c)c->accepted_references=!!enabled;
}
static int references_accepted(stqr_client *c) {
    stqr_u64 required=STQR_CAP_ACCEPTED_REFERENCES|STQR_CAP_OBJECT_REFERENCES|STQR_CAP_CONNECTION_SYNC|STQR_CAP_DURABILITY;
    return c->accepted_references && (c->capabilities&required)==required;
}
/* A filesystem's accepted session may regain live visibility after a restart
 * even when an older prefix has unknown persistence. Keep that uncertainty
 * latched for explicit Sync; never extend this exception to durable operations
 * or a generic C connection which did not opt into the filesystem mode. */
static int accepted_visibility_after_loss(stqr_client *c,unsigned op,int mutation) {
    return references_accepted(c) && (op==0x121 || mutation>1);
}
static void async_fail(stqr_client *, struct sq_channel *, int);
static int async_channel_pending(stqr_client *, struct sq_channel *);
static void route_free(struct sq_route *r) {
    if (!r) return;
    route_free(r->child[0]); route_free(r->child[1]); sq_free(r);
}
static int zero(const void *v, size_t n) {
    const stqr_u8 *p = v;
    while (n--) if (*p++) return 0;
    return 1;
}
static void drop(struct buf *b) { sq_free(b->p); memset(b, 0, sizeof(*b)); }
static void put(struct buf *b, const void *p, size_t n) {
    size_t cap;
    void *next;
    if (b->err) return;
    if (n > SQ_WIRE || b->n > SQ_WIRE - n || (n && !p)) { b->err = -E2BIG; return; }
    if (b->n + n > b->cap) {
        cap = b->cap ? b->cap : 128;
        while (cap < b->n + n) cap *= 2;
        next = sq_alloc(cap);
        if (!next) { b->err = -ENOMEM; return; }
        if (b->n) memcpy(next, b->p, b->n);
        sq_free(b->p); b->p = next; b->cap = cap;
    }
    if (n) memcpy(b->p + b->n, p, n);
    b->n += n;
}
static void number(struct buf *b, stqr_u64 n, unsigned bytes) {
    stqr_u8 p[8]; unsigned i;
    for (i = 0; i < bytes; i++) { p[i] = (stqr_u8)n; n >>= 8; }
    put(b, p, bytes);
}
static void varint(struct buf *b, stqr_u64 n) {
    stqr_u8 p;
    do { p = n & 127; n >>= 7; if (n) p |= 128; put(b, &p, 1); } while (n);
}
static void blob(struct buf *b, const void *p, size_t n) { varint(b, n); put(b, p, n); }
static const stqr_u8 *take(struct buf *b, size_t n) {
    const stqr_u8 *p;
    if (b->err) return NULL;
    if (b->pos > b->n || n > b->n - b->pos) { b->err = -EPROTO; return NULL; }
    p = b->p + b->pos; b->pos += n; return p;
}
static stqr_u64 readnum(struct buf *b, unsigned n) {
    const stqr_u8 *p = take(b, n);
    stqr_u64 v = 0;
    if (p) while (n) v = (v << 8) | p[--n];
    return v;
}
static int boolean(struct buf *b) {
    stqr_u64 v = readnum(b, 1);
    if (v > 1) b->err = -EPROTO;
    return (int)v;
}
static stqr_u64 readvar(struct buf *b) {
    stqr_u64 v = 0; unsigned i;
    for (i = 0; i < 10; i++) {
        unsigned x = (unsigned)readnum(b, 1);
        if (b->err || (i == 9 && x > 1)) break;
        v |= (stqr_u64)(x & 127) << (7 * i);
        if (!(x & 128)) {
            if (i && !x) break;
            return v;
        }
    }
    b->err = -EPROTO; return 0;
}
static const stqr_u8 *readblob(struct buf *b, size_t *n) {
    stqr_u64 len = readvar(b);
    if (len > SQ_WIRE) { b->err = -EPROTO; len = 0; }
    *n = (size_t)len;
    return take(b, *n);
}
static int done(struct buf *b) { return b->err ? b->err : b->pos == b->n ? 0 : -EPROTO; }
static int unhex(const char *s, void *out, size_t n) {
    stqr_u8 *p = out; size_t i;
    if (!s || !out || strlen(s) != n * 2) return -EINVAL;
    for (i = 0; i < n; i++) {
        unsigned a = (unsigned char)s[i*2], b = (unsigned char)s[i*2+1];
        a = a >= '0' && a <= '9' ? a-'0' : (a|32) >= 'a' && (a|32) <= 'f' ? (a|32)-'a'+10 : 256;
        b = b >= '0' && b <= '9' ? b-'0' : (b|32) >= 'a' && (b|32) <= 'f' ? (b|32)-'a'+10 : 256;
        if (a > 15 || b > 15) { memset(out, 0, n); return -EINVAL; }
        p[i] = (stqr_u8)(a*16+b);
    }
    return 0;
}
int stqr_parse_id(const char *s, stqr_id *id) { return unhex(s, id, 16); }
int stqr_parse_object_id(const char *s, stqr_object_id *id) { return unhex(s, id, 32); }
int stqr_request_id(stqr_id *id) { return id ? sq_random(id, sizeof(*id)) : -EINVAL; }
void stqr_free(void *p) { sq_free(p); }
int stqr_status(const stqr_client *c) { return c ? c->status : 0; }
static void hex(const stqr_u8 *p, size_t n, char *out) {
    const char *alphabet = "0123456789abcdef";
    while (n--) { *out++ = alphabet[*p >> 4]; *out++ = alphabet[*p++ & 15]; }
    *out = 0;
}
static void base64(const stqr_u8 *in, size_t n, char *out) {
    static const char abc[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i;
    for (i = 0; i < n; i += 3) {
        unsigned v = (unsigned)in[i] << 16;
        if (i+1 < n) v |= (unsigned)in[i+1] << 8;
        if (i+2 < n) v |= in[i+2];
        *out++ = abc[v >> 18]; *out++ = abc[(v >> 12) & 63];
        *out++ = i+1 < n ? abc[(v >> 6) & 63] : '=';
        *out++ = i+2 < n ? abc[v & 63] : '=';
    }
    *out = 0;
}
static int samecase(const char *a, const char *b) {
    while (*a && *b) { unsigned x = (unsigned char)*a++, y = (unsigned char)*b++;
        if (x >= 'A' && x <= 'Z') x += 32;
        if (y >= 'A' && y <= 'Z') y += 32;
        if (x != y) return 0;
    }
    return !*a && !*b;
}
static int contains_token(char *s, const char *token) {
    char *end;
    while (*s) {
        while (*s == ' ' || *s == '\t' || *s == ',') s++;
        end = s; while (*end && *end != ',') end++;
        if (*end) *end++ = 0;
        { char *tail = s + strlen(s); while (tail > s && (tail[-1] == ' ' || tail[-1] == '\t')) *--tail = 0; }
        if (samecase(s, token)) return 1;
        s = end;
    }
    return 0;
}
static int status_errno(int s) {
    switch (s) {
    case 400: return -EINVAL;
    case 401: case 403: return -EACCES;
    case 404: return -ENOENT;
    case 409: case 423: return -EBUSY;
    case 410: return -ESTALE;
    case 412: return -EAGAIN;
    case 413: return -E2BIG;
    case 501: return -EOPNOTSUPP;
    case 507: return -ENOSPC;
    case 503: return -EHOSTUNREACH;
    default: return -EREMOTEIO;
    }
}
static int parse_url(stqr_client *c, const char *url) {
    const char *start, *end, *colon = NULL, *p;
    size_t n, hostlen;
    if (!url) return -EINVAL;
    if (!strncmp(url, "http://", 7)) start = url+7;
    else if (!strncmp(url, "ws://", 5)) start = url+5;
    else return -EINVAL;
    end = strchr(start, '/'); if (!end) end = start+strlen(start);
    if (*end && strcmp(end, "/") && strcmp(end, "/api/ws")) return -EINVAL;
    n = (size_t)(end-start);
    if (!n || n >= sizeof(c->authority)) return -EINVAL;
    for (p = start; p < end; p++) if ((unsigned char)*p <= 32 || *p == '@' || *p == '?' || *p == '#') return -EINVAL;
    memcpy(c->authority, start, n); c->authority[n] = 0;
    if (*start == '[') {
        const char *close = memchr(start, ']', n);
        if (!close || close == start+1 || (close+1 < end && close[1] != ':')) return -EINVAL;
        hostlen = (size_t)(close-start-1); start++;
        if (close+1 < end) colon = close+1;
    } else {
        colon = memchr(start, ':', n);
        hostlen = colon ? (size_t)(colon-start) : n;
    }
    if (!hostlen || hostlen >= sizeof(c->host)) return -EINVAL;
    memcpy(c->host, start, hostlen); c->host[hostlen] = 0;
    c->port = 80;
    if (colon) {
        unsigned port = 0;
        for (p = colon+1; p < end; p++) { if (*p < '0' || *p > '9' || port > 6553) return -EINVAL; port = port*10 + *p-'0'; }
        if (p == colon+1 || !port || port > 65535) return -EINVAL;
        c->port = port;
    }
    return 0;
}
/* Logical bucket clients retain authenticated sessions per endpoint. Switching
 * owners is not a disconnect: subscriptions on the previous owner stay live.
 * Only real loss/explicit reconnect/eviction invalidates their cache coverage.
 * All helpers run under the public client's existing serialization contract. */
static int reconnect_active(stqr_client *);
static int sync_active(stqr_client *);
static void broken(stqr_client *);
static void save_channel(stqr_client *c) {
    struct sq_channel *s=c->active;
    if (!s) return;
    s->socket=c->socket; memcpy(s->host,c->host,sizeof(s->host));
    memcpy(s->authority,c->authority,sizeof(s->authority)); s->port=c->port;
    s->runtime=c->runtime; s->capabilities=c->capabilities;
    s->pending=c->pending_mark; s->synced=c->synced_mark; s->lost=c->durability_lost;
}
static void load_channel(stqr_client *c,struct sq_channel *s) {
    c->active=s; c->socket=s->socket; memcpy(c->host,s->host,sizeof(c->host));
    memcpy(c->authority,s->authority,sizeof(c->authority)); c->port=s->port;
    c->runtime=s->runtime; c->capabilities=s->capabilities;
    c->pending_mark=s->pending; c->synced_mark=s->synced; c->durability_lost=s->lost;
    s->used=++c->channel_clock;
}
static int ensure_channel(stqr_client *c) {
    struct sq_channel *s;
    if (c->active) return 0;
    s=sq_alloc(sizeof(*s)); if (!s) return -ENOMEM;
    c->channels=c->active=s; c->channel_count=1; save_channel(c);
    s->used=++c->channel_clock; return 0;
}
/* Owner channels and subscription descriptors are process-local RAM only.
 * Hash by subscription ID; list traversal is reserved for channel teardown. */
static unsigned binding_bucket(stqr_id id) {
    stqr_u32 h=2166136261U;
    for (unsigned i=0;i<16;i++) h=(h^id.bytes[i])*16777619U;
    return h & (SQ_BINDING_BUCKETS-1);
}
static struct sq_binding **binding_slot(stqr_client *c,stqr_id id) {
    struct sq_binding **p=&c->bindings[binding_bucket(id)];
    while (*p && memcmp((*p)->id.bytes,id.bytes,16)) p=&(*p)->next;
    return p;
}
static void forget_bindings(stqr_client *c,struct sq_channel *channel) {
    for (unsigned i=0;i<SQ_BINDING_BUCKETS;i++) {
        struct sq_binding **p=&c->bindings[i];
        while (*p) {
            struct sq_binding *b=*p;
            if (!channel || b->channel==channel) { *p=b->next; sq_free(b); }
            else p=&b->next;
        }
    }
}
static int any_lost(stqr_client *c) {
    struct sq_channel *s;
    if (c->durability_lost) return 1;
    for (s=c->channels;s;s=s->next) if (s!=c->active && s->lost) return 1;
    return 0;
}
static int select_endpoint(stqr_client *c,const char *endpoint) {
    stqr_client *parsed=sq_alloc(sizeof(*parsed));
    struct sq_channel *s,*original;
    int err;
    if (!parsed) return -ENOMEM;
    err=parse_url(parsed,endpoint); if (err) goto out;
    err=ensure_channel(c); if (err) goto out;
    save_channel(c); original=c->active;
    for (s=c->channels;s;s=s->next) if (!strcmp(s->authority,parsed->authority)) break;
    if (!s) {
        if (c->channel_count>=SQ_CHANNEL_LIMIT) {
            struct sq_channel *candidate;
            for (candidate=c->channels;candidate;candidate=candidate->next)
                if (candidate!=original && candidate!=c->lease_owner && !async_channel_pending(c,candidate) && (!s || candidate->used<s->used)) s=candidate;
            if (!s) { err=-ENOSPC; goto out; }
            load_channel(c,s); err=sync_active(c);
            if (err) { save_channel(c); load_channel(c,original); goto out; }
            broken(c); save_channel(c); load_channel(c,original);
            struct sq_channel *next=s->next; memset(s,0,sizeof(*s)); s->next=next;
        } else {
            s=sq_alloc(sizeof(*s)); if (!s) { err=-ENOMEM; goto out; }
            s->next=c->channels; c->channels=s; c->channel_count++;
        }
        memcpy(s->host,parsed->host,sizeof(s->host));
        memcpy(s->authority,parsed->authority,sizeof(s->authority)); s->port=parsed->port;
    }
    load_channel(c,s);
    err=c->socket?0:reconnect_active(c);
out:
    sq_free(parsed); return err;
}
static void disconnect_channels(stqr_client *c) {
    struct sq_channel *s;
    int notify=0;
    if (!c->channels) { broken(c); return; }
    save_channel(c);
    for (s=c->channels;s;s=s->next) {
        notify|=s->socket!=NULL; async_fail(c,s,-ECONNRESET); sq_disconnect(s->socket); s->socket=NULL;
    }
    c->socket=NULL; forget_bindings(c,NULL);
    if (notify && c->update) c->update(c->update_arg,NULL);
}
static int handshake(stqr_client *c) {
    char key[25], expected[29], combined[61], bucket[33], lease[80] = {0}, token[33];
    char *request = NULL, *headers = NULL, *line, *next;
    stqr_u8 random[16], digest[20];
    size_t n = 0, capacity;
    int err, accept = 0, protocol = 0, upgrade = 0, connection = 0;
    if ((err = sq_random(random, 16))) return err;
    base64(random, 16, key);
    snprintf(combined, sizeof(combined), "%s258EAFA5-E914-47DA-95CA-C5AB0DC85B11", key);
    if ((err = sq_hash(160, combined, strlen(combined), digest))) return err;
    base64(digest, 20, expected); hex(c->bucket.bytes, 16, bucket);
    if (!zero(c->lease.bytes, 16)) {
        hex(c->lease.bytes, 16, token);
        snprintf(lease, sizeof(lease), "X-STQR-Maintenance: %s\r\n", token);
    }
    capacity = c->authorization_len + 1024;
    request = sq_alloc(capacity); headers = sq_alloc(16385);
    if (!request || !headers) { err = -ENOMEM; goto out; }
    n = (size_t)snprintf(request, capacity,
        "GET /api/ws HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\nSec-WebSocket-Protocol: stqr.v2\r\nAuthorization: Basic %s\r\nX-STQR-Bucket: %s\r\nX-STQR-Format: %s\r\n%s\r\n",
        c->authority, key, c->authorization, bucket, c->format, lease);
    if (n >= capacity) { err = -E2BIG; goto out; }
    if ((err = sq_send(c->socket, request, n))) goto out;
    for (n = 0; n < 16384; n++) {
        if ((err = sq_recv(c->socket, headers+n, 1))) goto out;
        if (n >= 3 && !memcmp(headers+n-3, "\r\n\r\n", 4)) { n++; break; }
    }
    if (n == 16384) { err = -E2BIG; goto out; }
    headers[n] = 0;
    if (strncmp(headers, "HTTP/1.1 ", 9) || strlen(headers) < 12 || headers[9] < '1' || headers[9] > '5' || headers[10] < '0' || headers[10] > '9' || headers[11] < '0' || headers[11] > '9') { err = -EPROTO; goto out; }
    c->status = (headers[9]-'0')*100+(headers[10]-'0')*10+headers[11]-'0';
    if (c->status != 101) { err = status_errno(c->status); goto out; }
    line = strstr(headers, "\r\n")+2;
    while ((next = strstr(line, "\r\n")) && next != line) {
        char *value, *tail;
        *next = 0; next += 2;
        value = strchr(line, ':');
        if (!value) { err = -EPROTO; goto out; }
        *value++ = 0; while (*value == ' ' || *value == '\t') value++;
        tail = value+strlen(value); while (tail > value && (tail[-1] == ' ' || tail[-1] == '\t')) *--tail = 0;
        if (samecase(line, "Sec-WebSocket-Accept")) { if (accept || strcmp(value, expected)) { err = -EPROTO; goto out; } accept = 1; }
        if (samecase(line, "Sec-WebSocket-Protocol")) { if (protocol || strcmp(value, "stqr.v2")) { err = -EPROTO; goto out; } protocol = 1; }
        if (samecase(line, "Upgrade")) upgrade = samecase(value, "websocket");
        if (samecase(line, "Connection")) connection = contains_token(value, "upgrade");
        if (samecase(line, "Sec-WebSocket-Extensions")) { err = -EPROTO; goto out; }
        line = next;
    }
    err = accept && protocol && upgrade && connection ? 0 : -EPROTO;
out:
    sq_secret_free(request, capacity); sq_free(headers); return err;
}
static int frame_send(stqr_client *c, unsigned opcode, const void *data, size_t n) {
    stqr_u8 header[14], mask[4], *masked;
    const stqr_u8 *p = data;
    size_t h = 2, i; int err;
    if (n > SQ_WIRE) return -E2BIG;
    if (!c->socket) return -ENOTCONN;
    if ((err = sq_random(mask, 4))) return err;
    header[0] = 128 | opcode;
    if (n < 126) header[1] = 128 | n;
    else if (n <= 65535) { header[1] = 128|126; header[2] = (n>>8)&255; header[3] = n&255; h = 4; }
    else { header[1] = 128|127; for (i = 0; i < 8; i++) header[2+i] = (stqr_u64)n >> (56-i*8); h = 10; }
    memcpy(header+h, mask, 4); h += 4;
    /* Submit the whole frame in one send. Sending a tiny header first can
     * leave its small payload behind Nagle/delayed ACK on TCP connections. */
    masked = sq_alloc(h+n); if (!masked) return -ENOMEM;
    memcpy(masked, header, h);
    for (i = 0; i < n; i++) masked[h+i] = p[i] ^ mask[i%4];
    err = sq_send(c->socket, masked, h+n);
    sq_free(masked); return err;
}
static int message_send(stqr_client *c, unsigned kind, unsigned op, stqr_id id, struct buf *body) {
    struct buf m = {0}; int err;
    if (body->err) return body->err;
    number(&m, 1, 1); number(&m, kind, 1); number(&m, op, 2); put(&m, id.bytes, 16); put(&m, body->p, body->n);
    err = m.err ? m.err : frame_send(c, 2, m.p, m.n); drop(&m); return err;
}
static int frame_recv(stqr_client *c, struct buf *m, int polling) {
    int fragmented = 0, err; unsigned controls = 0;
    for (;;) {
        stqr_u8 h[10], small[125];
        stqr_u64 n; unsigned op, fin, i;
        if ((err = sq_recv(c->socket, h, 2))) return err;
        fin = h[0] >> 7; op = h[0] & 15; n = h[1] & 127;
        if ((h[0] & 112) || (h[1] & 128)) return -EPROTO;
        if (n == 126 || n == 127) {
            unsigned ext = n == 126 ? 2 : 8;
            if ((err = sq_recv(c->socket, h+2, ext))) return err;
            n = 0; for (i = 0; i < ext; i++) n = (n<<8) | h[2+i];
            if ((ext == 2 && n < 126) || (ext == 8 && n <= 65535)) return -EPROTO;
        }
        if (op >= 8) {
            if (!fin || n > 125 || ++controls > 64) return -EPROTO;
            if ((err = sq_recv(c->socket, small, (size_t)n))) return err;
            if (op == 8) return -ECONNRESET;
            if (op == 9) { if ((err = frame_send(c, 10, small, (size_t)n))) return err; }
            else if (op != 10) return -EPROTO;
            if (polling && !fragmented) return 1;
            continue;
        }
        if ((!fragmented && op != 2) || (fragmented && op != 0) || n > SQ_WIRE || m->n > SQ_WIRE - n) return -EPROTO;
        /* Reserve without trusting a peer-supplied allocation beyond MaxMessage. */
        if (n) {
            stqr_u8 *tmp = sq_alloc((size_t)n);
            if (!tmp) return -ENOMEM;
            err = sq_recv(c->socket, tmp, (size_t)n);
            if (!err) put(m, tmp, (size_t)n);
            sq_free(tmp);
            if (err || m->err) return err ? err : m->err;
        }
        if (fin) return 0;
        fragmented = 1;
        if (++controls > 64) return -EPROTO;
    }
}
static int info_decode(struct buf *d, stqr_info *info) {
    const stqr_u8 *p;
    memset(info, 0, sizeof(*info));
    info->length = readnum(d, 8);
    if (boolean(d)) take(d, 32);
    info->flags = (stqr_u32)readnum(d, 4);
    p = take(d, 16); if (p) memcpy(info->runtime.bytes, p, 16);
    info->compression = (stqr_u8)readnum(d, 1); info->layout = (stqr_u16)readnum(d, 2);
    if (info->compression > 3) d->err = -EPROTO;
    return d->err;
}
static int accept_info(stqr_client *c, const stqr_info *info) {
    /* Cluster runtimes belong to individual ranges, never the whole bucket. */
    if (c->capabilities & 8) return 0;
    if (!zero(c->runtime.bytes, 16) && memcmp(c->runtime.bytes, info->runtime.bytes, 16)) return -ESTALE;
    c->runtime = info->runtime; return 0;
}
struct download { struct buf data; stqr_u64 length, part; stqr_u8 digest[32]; int started, ended; };
static int stream_push(struct download *s, unsigned op, struct buf *d) {
    stqr_u64 len, part, off; const stqr_u8 *p; size_t n;
    if (op == 13) {
        stqr_info ignored;
        if (s->started) return -EPROTO;
        s->started = 1; s->length = readnum(d, 8);
        p = take(d, 32); if (p) memcpy(s->digest, p, 32);
        if (boolean(d)) info_decode(d, &ignored);
        if (boolean(d)) { readnum(d, 8); readnum(d, 8); readnum(d, 8); }
        if (s->length > STQR_BUFFER_LIMIT) return -E2BIG;
        return done(d);
    }
    if (!s->started || s->ended) return -EPROTO;
    if (op == 5) {
        part = readnum(d, 8); off = readnum(d, 8); p = readblob(d, &n);
        if (done(d) || part != s->part || off != s->data.n || n > SQ_CHUNK || s->data.n > s->length || n > s->length-s->data.n) return -EPROTO;
        put(&s->data, p, n); s->part++; return s->data.err;
    }
    if (op == 6) {
        stqr_u8 digest[32]; int err;
        len = readnum(d, 8); p = take(d, 32);
        if (done(d) || len != s->length || s->data.n != len || memcmp(p, s->digest, 32)) return -EPROTO;
        err = sq_hash(256, s->data.p, s->data.n, digest);
        if (err) return err;
        if (memcmp(digest, s->digest, 32)) return -EBADMSG;
        s->ended = 1; return 0;
    }
    return -EPROTO;
}
static int decode_mark(struct buf *b, stqr_commit_mark *m) {
    const stqr_u8 *p = take(b, 16);
    memset(m, 0, sizeof(*m)); if (p) memcpy(m->boot.bytes, p, 16);
    m->sequence = readnum(b, 8);
    if (done(b) || zero(m->boot.bytes, 16) || !m->sequence) return -EPROTO;
    return 0;
}
static void note_active(stqr_client *, const stqr_commit_mark *);
static int observe_mark(stqr_client *c, struct buf *b) {
    stqr_commit_mark m; int err = decode_mark(b, &m);
    if (err) return err;
    if (c->pending_mark.sequence && memcmp(c->pending_mark.boot.bytes, m.boot.bytes, 16)) {
        c->durability_lost = 1; return 0;
    }
    if (!memcmp(c->synced_mark.boot.bytes, m.boot.bytes, 16) && m.sequence <= c->synced_mark.sequence) return 0;
    if (!c->pending_mark.sequence || m.sequence > c->pending_mark.sequence) c->pending_mark = m;
    return 0;
}
/* Header 49 confirms an exact server-boot/bucket prefix already on storage.
 * A generic accepted mark (48) alone never proves that checkpoint completed. */
static int confirm_mark(stqr_client *c, struct buf *b) {
    stqr_commit_mark m;int err=decode_mark(b,&m);if(err)return err;
    if(c->pending_mark.sequence&&memcmp(c->pending_mark.boot.bytes,m.boot.bytes,16))c->durability_lost=1;
    note_active(c,&m);return 0;
}
static int dispatch_update(stqr_client *c, unsigned opcode, struct buf *m) {
    if (opcode == 0x11a) return observe_mark(c, m);
    stqr_update u = {0};
    const stqr_u8 *p, *digest; stqr_u8 hash[32];
    unsigned mode; stqr_u64 total; int err;
    p = take(m, 16); if (p) memcpy(u.subscription.bytes, p, 16);
    if (opcode == 0x113) {
        /* Routing is deliberately not guessed by this non-routing C client. */
        if (m->err || zero(u.subscription.bytes, 16)) return -EPROTO;
        u.change = STQR_UPDATE_RESYNC;
        if (c->update) c->update(c->update_arg, &u);
        return 0;
    }
    u.cursor = readnum(m, 8);
    u.part = (stqr_u32)readnum(m, 4);
    u.parts = (stqr_u32)readnum(m, 4); u.change = (unsigned)readnum(m, 1);
    u.offset = readnum(m, 8); u.length = readnum(m, 8);
    mode = (unsigned)readnum(m, 1); total = readnum(m, 8);
    digest = take(m, 32); u.data = readblob(m, &u.size);
    if (done(m) || mode || total != u.size || !u.cursor || !u.parts ||
        u.part >= u.parts || u.change > STQR_UPDATE_RESYNC ||
        zero(u.subscription.bytes, 16) || u.size > ~(stqr_u64)0-u.offset) return -EPROTO;
    err = sq_hash(256, u.data, u.size, hash);
    if (err) return err;
    if (memcmp(hash, digest, 32)) return -EBADMSG;
    if (c->update) c->update(c->update_arg, &u);
    {
        struct buf ack = {0}; stqr_id rid = {{0}};
        put(&ack, u.subscription.bytes, 16); number(&ack, u.cursor, 8);
        err = message_send(c, 0, 0x114, rid, &ack); drop(&ack);
    }
    return err;
}
/* The node supplies one authoritative range in a redirect. Validate the
 * bucket/range before selecting its primary address; never guess another node. */
static int redirect_patch(stqr_client *c, const stqr_u8 *raw, size_t size) {
    struct buf d = {.p = (stqr_u8 *)raw, .n = size};
    const stqr_u8 *p, *start; stqr_id owner = {{0}};
    stqr_u64 n, j; unsigned bits; char endpoint[512] = {0};
    if (!c->query_valid) return -EPROTO;
    if (!readnum(&d,8) || !readnum(&d,8)) return -EPROTO;
    c->redirect_revision = readnum(&d,8);
    p = take(&d,16);
    if (!p || memcmp(p,c->bucket.bytes,16)) return -EPROTO;
    take(&d,32); boolean(&d); take(&d,32);
    if (readvar(&d) != 1) return -EPROTO;
    start = take(&d,32); bits = (unsigned)readnum(&d,2);
    readnum(&d,8); c->redirect_epoch = readnum(&d,8); readnum(&d,1);
    p = take(&d,16); if (p) memcpy(c->redirect_runtime.bytes,p,16);
    if (!start || bits>256 || !c->redirect_epoch || !c->redirect_revision) return -EPROTO;
    for (unsigned bit=0;bit<bits;bit++)
        if ((start[bit/8] ^ c->query.bytes[bit/8]) & (128>>(bit%8))) return -EPROTO;
    n = readvar(&d); if (!n || n>256) return -EPROTO;
    for (j=0;j<n && !d.err;j++) {
        const stqr_u8 *node = take(&d,16); take(&d,16);
        unsigned role = (unsigned)readnum(&d,1);
        readnum(&d,1); readnum(&d,2); readnum(&d,4);
        if (role==0 && node) {
            if (!zero(owner.bytes,16)) return -EPROTO;
            memcpy(owner.bytes,node,16);
        }
    }
    if (zero(owner.bytes,16)) return -EPROTO;
    n=readvar(&d); if (n>10000) return -EPROTO;
    for (j=0;j<n && !d.err;j++) {
        const stqr_u8 *node=take(&d,16); stqr_u64 addresses=readvar(&d);
        int selected=node && !memcmp(node,owner.bytes,16);
        if (addresses>64) return -EPROTO;
        for (stqr_u64 a=0;a<addresses && !d.err;a++) {
            size_t length; p=readblob(&d,&length);
            if (!p || !length || length>4096) return -EPROTO;
            if (selected && !endpoint[0]) {
                if (length>=sizeof(endpoint) || memchr(p,0,length)) return -E2BIG;
                memcpy(endpoint,p,length); endpoint[length]=0;
            }
        }
    }
    /* Direct routing patches do not contain policy descriptors. */
    if (readvar(&d)!=0 || done(&d) || !endpoint[0]) return -EPROTO;
    /* parse_url validates schemes/authority when the redirect is followed. */
    memcpy(c->redirect,endpoint,sizeof(endpoint));
    {
        struct sq_route **at=&c->routes;
        unsigned bit;
        for(bit=0;bit<=bits;bit++) {
            if(!*at) {*at=sq_alloc(sizeof(**at));if(!*at)return -ENOMEM;}
            if(bit==bits)break;
            at=&(*at)->child[(start[bit/8]>>(7-bit%8))&1];
        }
        memcpy((*at)->endpoint,endpoint,sizeof(endpoint));
        (*at)->revision=c->redirect_revision;(*at)->epoch=c->redirect_epoch;
        (*at)->runtime=c->redirect_runtime;
    }
    return 0;
}
static int async_dispatch(stqr_client *, unsigned, unsigned, const stqr_u8 *, struct buf *);
static void async_fail(stqr_client *, struct sq_channel *, int);
static int reply(stqr_client *c, unsigned op, stqr_id id, struct buf *result, struct download *stream) {
    unsigned skipped = 0;
    for (;;) {
        struct buf m = {0}, headers = {0};
        const stqr_u8 *p; size_t n; unsigned kind, opcode;
        int err = frame_recv(c, &m, 0);
        if (err) { drop(&m); return err; }
        if (readnum(&m, 1) != 1) m.err = -EPROTO;
        kind = (unsigned)readnum(&m, 1); opcode = (unsigned)readnum(&m, 2); p = take(&m, 16);
        if (m.err) { drop(&m); return -EPROTO; }
        if (kind == 3) {
            if (opcode == 0x112 || opcode == 0x113 || opcode == 0x11a) {
                err = dispatch_update(c, opcode, &m); drop(&m);
                if (err) return err;
                continue;
            }
            if (opcode == 5 || opcode == 6 || opcode == 13) {
                p = take(&m, 16);
                if (!p) { drop(&m); return -EPROTO; }
                if (memcmp(p, id.bytes, 16) || !stream) {
                    m.pos -= 16;
                    err = async_dispatch(c, kind, opcode, NULL, &m);
                    drop(&m); if (err < 0) return err;
                    if (!err) return -EPROTO;
                    continue;
                }
                err = stream_push(stream, opcode, &m);
                drop(&m); if (err) return err;
                continue;
            }
            drop(&m); if (++skipped > 32) return -EPROTO;
            continue;
        }
        if (kind != 2) { drop(&m); return -EPROTO; }
        if (opcode != op || memcmp(p, id.bytes, 16)) {
            err = async_dispatch(c, kind, opcode, p, &m);
            drop(&m); if (err < 0) return err;
            if (!err) return -EPROTO;
            continue;
        }
        c->status = (int)readvar(&m);
        p = readblob(&m, &n); headers.p = (stqr_u8 *)p; headers.n = n;
        { stqr_u64 count = readvar(&headers), tags[64]; unsigned i, j;
          if (count > 64) headers.err = -EPROTO;
          for (i = 0; i < count && !headers.err; i++) {
              tags[i] = readvar(&headers);
              for (j = 0; j < i; j++) if (tags[j] == tags[i]) headers.err = -EPROTO;
              { const stqr_u8 *value = readblob(&headers, &n);
                if (tags[i] == 2 && (c->status == 301 || c->status == 302) && !headers.err) {
                    int decoded = redirect_patch(c,value,n);
                    if (decoded) headers.err = decoded;
                }
                if ((tags[i] == 48 || tags[i] == 49) && !headers.err) {
                    struct buf mark = {.p = (stqr_u8 *)value, .n = n};
                    int observed = tags[i]==48?observe_mark(c, &mark):confirm_mark(c, &mark);
                    if (observed) headers.err = observed;
                }
              }
          }
        }
        if (m.err || done(&headers) || c->status < 100 || c->status > 599) { drop(&m); return -EPROTO; }
        if (c->status >= 300) { err = status_errno(c->status); drop(&m); return err; }
        put(result, m.p+m.pos, m.n-m.pos); drop(&m); return result->err;
    }
}
static void broken(stqr_client *c) {
    int notify=c->socket!=NULL;
    async_fail(c,c->active,-ECONNRESET);
    sq_disconnect(c->socket); c->socket = NULL;
    forget_bindings(c,c->active);
    if (notify && c->update) c->update(c->update_arg, NULL);
}
static int send_payload(stqr_client *c, stqr_id id, const void *data, size_t n, const stqr_u8 hash[32]) {
    size_t pos = 0; stqr_u64 part = 0; int err = 0;
    while (!err && pos < n) {
        struct buf chunk = {0}; stqr_id rid; size_t count = n-pos < SQ_CHUNK ? n-pos : SQ_CHUNK;
        err = stqr_request_id(&rid);
        if (err) return err;
        put(&chunk, id.bytes, 16); number(&chunk, part++, 8); number(&chunk, pos, 8);
        blob(&chunk, (const stqr_u8 *)data+pos, count);
        err = message_send(c, 0, 5, rid, &chunk);
        drop(&chunk); pos += count;
    }
    if (!err) {
        struct buf end = {0}; stqr_id rid;
        err = stqr_request_id(&rid);
        if (err) return err;
        put(&end, id.bytes, 16); number(&end, n, 8); put(&end, hash, 32);
        err = message_send(c, 0, 6, rid, &end);
        drop(&end);
    }
    return err;
}
static int request_payload(stqr_client *c, unsigned op, struct buf *body, stqr_id id, int mutation, struct buf *result, struct download *s, const void *payload, size_t size, const stqr_u8 *hash) {
    int err;
    stqr_u64 started = sq_now_ms();
    if (!c || !c->socket) return -ENOTCONN;
    if (mutation && any_lost(c) && !accepted_visibility_after_loss(c,op,mutation)) return -ESTALE;
    /* Reads/subscriptions may change channels without a durability fence.
     * Fence their retained causal dependencies only when publishing a write
     * on a different owner, or when the caller explicitly requests SYNC. */
    if (mutation && !(c->capabilities&STQR_CAP_CONNECTION_SYNC)) {
        for (struct sq_channel *channel=c->channels;channel;channel=channel->next)
            if (channel!=c->active && channel->pending.sequence) {
                err=stqr_sync(c); if (err) return err;
                break;
            }
    }
    if (body->err) return body->err;
    if (zero(id.bytes,16)) { if (mutation) return -EINVAL; if ((err=stqr_request_id(&id))) return err; }
    for (unsigned redirects=0;redirects<16;) {
        struct buf wrapped={0}; unsigned wire_op=op; struct buf *wire_body=body;
        /* Redirect/reconnect can change the peer's accepted capabilities. */
        if(mutation && any_lost(c) && !accepted_visibility_after_loss(c,op,mutation))return -ESTALE;
        c->status=0; c->redirect[0]=0;
        c->query_valid=((op>=0x100 && op<=0x110) || op==0x121) && body->n>=80;
        if (c->query_valid) memcpy(c->query.bytes,body->p+16,32);
        if(op==0x121 && references_accepted(c)) {
            put(&wrapped,body->p,body->n);number(&wrapped,1,1);wire_body=&wrapped;
        }
        if (mutation>1 && (c->capabilities & STQR_CAP_DURABILITY)) {
            if (body->n<80) return -EINVAL;
            put(&wrapped,body->p,80); number(&wrapped,op,2); put(&wrapped,body->p+80,body->n-80);
            wire_op=0x118; wire_body=&wrapped;
        }
        if(c->active)c->active->submitted=++c->async_sequence;
        err=message_send(c,1,wire_op,id,wire_body);
        if (!err && payload) err=send_payload(c,id,payload,size,hash);
        if (!err) err=reply(c,wire_op,id,result,s);
        drop(&wrapped);
        /* 503 denotes temporary cluster backpressure, including quota-credit
         * transfer and maintenance. It is not a permanent filesystem failure.
         * Retry the identical operation/ID on this connection; never reissue an
         * uncertain transport failure here. Bound retries by the caller's timeout.
         */
        if (err && c->status==503) {
            stqr_u64 elapsed = sq_now_ms()-started;
            if (elapsed >= c->timeout) return err;
            err = sq_pause((unsigned)((c->timeout-elapsed)<50 ? c->timeout-elapsed : 50));
            if (err) return err;
            drop(result);
            if (s) { drop(&s->data); memset(s,0,sizeof(*s)); }
            continue;
        }
        if (err && (c->status==301 || c->status==302) && c->redirect[0]) {
            redirects++;
            char endpoint[512]; stqr_u64 revision=c->redirect_revision, epoch=c->redirect_epoch;
            stqr_id runtime=c->redirect_runtime;
            memcpy(endpoint,c->redirect,sizeof(endpoint));
            /* Modern sessions retain owner-local touches. Legacy publications
             * fence before moving; reads never force a checkpoint. */
            if (mutation && !(c->capabilities & STQR_CAP_CONNECTION_SYNC)) {
                err=stqr_sync(c);if(err)return err;
            }
            err=select_endpoint(c,endpoint); if (err) return err;
            for (unsigned k=0;k<8;k++) { body->p[48+k]=(stqr_u8)(revision>>(k*8)); body->p[56+k]=(stqr_u8)(epoch>>(k*8)); }
            memcpy(body->p+64,runtime.bytes,16);
            drop(result);
            if (s) { drop(&s->data); memset(s,0,sizeof(*s)); }
            continue;
        }
        if (err && !c->status) { broken(c); return mutation ? -EINPROGRESS : err; }
        if (err==-EPROTO || err==-EBADMSG) broken(c);
        return err;
    }
    return -ELOOP;
}
static int request(stqr_client *c, unsigned op, struct buf *body, stqr_id id, int mutation, struct buf *result, struct download *s) {
    return request_payload(c,op,body,id,mutation,result,s,NULL,0,NULL);
}
static int reconnect_active(stqr_client *c) {
    if (c && c->lease_taken && c->active == c->lease_owner) return -ESTALE;
    struct buf body = {0}, result = {0}; stqr_id id = {{0}}; int err;
    if (!c) return -EINVAL;
    broken(c); c->status = 0;
    err = sq_connect(&c->socket, c->host, c->port, c->timeout);
    if (!err) err = handshake(c);
    if (err) { broken(c); return err; }
    c->status = 0;
    number(&body, 2, 2); number(&body, 2, 2); number(&body, STQR_CAP_DURABILITY|STQR_CAP_OBJECT_REFERENCES|STQR_CAP_CONNECTION_SYNC|STQR_CAP_ACCEPTED_REFERENCES, 8); blob(&body, "stqr-c", 6);
    err = request(c, 1, &body, id, 0, &result, NULL);
    if (!err) {
        if (readnum(&result, 2) != 2) result.err = -EPROTO;
        c->capabilities = readnum(&result, 8); take(&result, 48); err = done(&result);
    }
    drop(&body); drop(&result);
    if (err) broken(c);
    else if (c->readable) sq_set_readable(c->socket,c->readable,c->readable_arg);
    return err;
}
int stqr_reconnect(stqr_client *c) {
    if (c && c->lease_taken) return -ESTALE;
    if (!c) return -EINVAL;
    disconnect_channels(c);
    return reconnect_active(c);
}
int stqr_dial(stqr_client **out, const char *url, const char *user, const char *password, stqr_id bucket, const char *format, unsigned timeout) {
    stqr_client *c; char *plain; size_t u, p, n; int err;
    if (!out) return -EINVAL;
    *out = NULL;
    if (!user || !password || !*user || !*password || strchr(user, ':') || zero(bucket.bytes, 16) || !timeout || timeout > 3600000) return -EINVAL;
    if (!format || !*format || strlen(format) > 255 || format[0] == ' ' || format[strlen(format)-1] == ' ') return -EINVAL;
    for (n = 0; format[n]; n++) if ((unsigned char)format[n] < 32 || (unsigned char)format[n] == 127) return -EINVAL;
    u = strlen(user); p = strlen(password);
    if (u > 4096 || p > 4096) return -E2BIG;
    c = sq_alloc(sizeof(*c)); if (!c) return -ENOMEM;
    c->bucket = bucket; c->timeout = timeout;
    memcpy(c->format, format, strlen(format)+1);
    if ((err = parse_url(c, url))) { sq_free(c); return err; }
    n = u+p+1; plain = sq_alloc(n);
    c->authorization_len = 4*((n+2)/3)+1; c->authorization = sq_alloc(c->authorization_len);
    if (!plain || !c->authorization) { sq_secret_free(plain, n); stqr_close(c); return -ENOMEM; }
    memcpy(plain, user, u); plain[u] = ':'; memcpy(plain+u+1, password, p);
    base64((stqr_u8 *)plain, n, c->authorization); sq_secret_free(plain, n);
    err = stqr_reconnect(c);
    if (err) { stqr_close(c); return err; }
    *out = c; return 0;
}
void stqr_close(stqr_client *c) {
    if (!c) return;
    disconnect_channels(c);
    while (c->channels) { struct sq_channel *next=c->channels->next; sq_free(c->channels); c->channels=next; }
    route_free(c->routes);
    sq_secret_free(c->authorization, c->authorization_len); sq_secret_free(c, sizeof(*c));
}
static void reference(stqr_client *c, struct buf *b, stqr_object_id id) {
    struct sq_route *r=c->routes,*best=NULL;unsigned bit;
    for(bit=0;r&&bit<=256;bit++) {
        if(r->endpoint[0]&&(!best||r->revision>=best->revision))best=r;
        if(bit==256)break;
        r=r->child[(id.bytes[bit/8]>>(7-bit%8))&1];
    }
    if(best) {int err=select_endpoint(c,best->endpoint);if(err){b->err=err;return;}}
    put(b,c->bucket.bytes,16);put(b,id.bytes,32);
    number(b,best?best->revision:0,8);number(b,best?best->epoch:0,8);
    put(b,best?best->runtime.bytes:c->runtime.bytes,16);
}
static int object_reference(stqr_client *c, stqr_object_id target, stqr_id token, unsigned action) {
    static const char domain[]="STQR object references v1";
    stqr_object_id root;
    stqr_id request_id;
    struct buf body={0}, result={0};
    int err;
    if (!c || !c->socket) return -ENOTCONN;
    if (!(c->capabilities & STQR_CAP_OBJECT_REFERENCES)) return -EOPNOTSUPP;
    if (zero(token.bytes,sizeof(token.bytes))) return -EINVAL;
    err=sq_hash(256,domain,sizeof(domain)-1,root.bytes);
    if (err) return err;
    err=stqr_request_id(&request_id);
    if (err) return err;
    reference(c,&body,root);
    put(&body,target.bytes,sizeof(target.bytes));
    number(&body,action,1);put(&body,token.bytes,sizeof(token.bytes));
    err=request(c,0x121,&body,request_id,1,&result,NULL);
    if (!err) {
        if (readnum(&result,1)!=1) result.err=-EPROTO;
        err=done(&result);
    }
    drop(&body);drop(&result);
    return err;
}
int stqr_object_pin(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,0); }
int stqr_object_unpin(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,1); }
int stqr_object_claim(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,2); }
int stqr_object_release_claim(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,3); }
int stqr_object_pin_dependency(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,4); }
int stqr_object_unpin_dependency(stqr_client *c, stqr_object_id target, stqr_id token) { return object_reference(c,target,token,5); }
int stqr_object_lease(stqr_client *c, int readonly) {
    struct buf b = {0}, r = {0}; stqr_id id; int err;
    if (!c || !c->socket) return -ENOTCONN;
    if (c->lease_taken || c->channel_count > 1) return -EBUSY;
    err = ensure_channel(c); if (err) return err;
    err = stqr_request_id(&id); if (err) return err;
    reference(c, &b, (stqr_object_id){{0}}); number(&b, !!readonly, 1);
    err = request(c, 0x11c, &b, id, 0, &r, NULL);
    if (!err && r.n) {
        const stqr_u8 *token;
        if (readnum(&r, 1) != 1) r.err = -EPROTO;
        token = take(&r, 16); if (token) memcpy(c->lease.bytes, token, 16);
        err = done(&r);
        if (!err && zero(c->lease.bytes, 16)) err = -EPROTO;
    }
    if (!err) { c->lease_taken = 1; c->lease_owner = c->active; }
    drop(&b); drop(&r); return err;
}
static int options(struct buf *b, const stqr_options *o) {
    if (!o || zero(o->request_id.bytes, 16)) return -EINVAL;
    number(b, !!o->has_length, 1); if (o->has_length) number(b, o->length, 8);
    number(b, !!o->absent, 1);
 number(b, !!o->has_compare, 1);
 if (o->has_compare) {
 if (o->compare_size > SQ_CHUNK || (o->compare_size && !o->compare)) return -EINVAL;
 number(b, o->compare_offset, 8); blob(b, o->compare, o->compare_size);
 }
 return b->err;
}
int stqr_stat(stqr_client *c, stqr_object_id id, stqr_info *info) {
    struct buf body = {0}, r = {0}; stqr_id rid = {{0}}; int err;
    if (!c || !info) return -EINVAL;
    reference(c, &body, id); err = request(c, 0x107, &body, rid, 0, &r, NULL);
    if (!err) { info_decode(&r, info); err = done(&r); if (!err) err = accept_info(c, info); }
    drop(&body); drop(&r); return err;
}
static int get(stqr_client *c, stqr_object_id id, int ranged, stqr_u64 offset, stqr_u64 length, void **data, size_t *actual, stqr_info *info) {
    struct buf body = {0}, r = {0}; struct download s = {0}; stqr_id rid = {{0}};
    const stqr_u8 *digest, *inline_data; size_t n; stqr_u64 total, range_length = 0; unsigned mode; int err;
    if (!data || !actual || !info) return -EINVAL;
    *data = NULL; *actual = 0;
    if (!c || (ranged && length > ~(stqr_u64)0-offset)) return -EINVAL;
    reference(c, &body, id);
    if (ranged) { number(&body, offset, 8); number(&body, length, 8); }
    number(&body, 0, 1); /* optional read byte predicate */
    err = request(c, ranged ? 0x101 : 0x100, &body, rid, 0, &r, &s);
    if (err) goto out;
    info_decode(&r, info);
    if (ranged) {
        if (readnum(&r, 8) != offset) r.err = -EPROTO;
        range_length = readnum(&r, 8);
        if (readnum(&r, 8) != info->length || range_length > length || offset > info->length || range_length > info->length-offset) r.err = -EPROTO;
    }
    mode = (unsigned)readnum(&r, 1); total = readnum(&r, 8); digest = take(&r, 32); inline_data = readblob(&r, &n);
    err = done(&r);
    if (err || mode > 1 || total != (ranged ? range_length : info->length)) { err = -EPROTO; goto out; }
    if (total > STQR_BUFFER_LIMIT) { err = -E2BIG; goto out; }
    if (mode == 0) {
        stqr_u8 hash[32];
        if (s.started || n != total) { err = -EPROTO; goto out; }
        err = sq_hash(256, inline_data, n, hash);
        if (err) goto out;
        if (memcmp(hash, digest, 32)) { err = -EBADMSG; goto out; }
        put(&s.data, inline_data, n);
        if (s.data.err) { err = s.data.err; goto out; }
    } else if (n || !s.ended || total != s.length || memcmp(digest, s.digest, 32)) { err = -EPROTO; goto out; }
    err = accept_info(c, info);
    if (!err) { *data = s.data.p; *actual = s.data.n; s.data.p = NULL; }
out:
    if (err == -EPROTO || err == -EBADMSG || (err == -E2BIG && s.started && !s.ended)) broken(c);
    drop(&body); drop(&r); drop(&s.data); return err;
}
int stqr_get(stqr_client *c, stqr_object_id id, void **data, size_t *n, stqr_info *info) { return get(c,id,0,0,0,data,n,info); }
int stqr_get_range(stqr_client *c, stqr_object_id id, stqr_u64 off, stqr_u64 len, void **data, size_t *n, stqr_info *info) { return get(c,id,1,off,len,data,n,info); }
static int write_payload(stqr_client *c, unsigned op, stqr_object_id id, stqr_u64 off, const void *data, size_t n, const stqr_options *opt, stqr_info *info, stqr_u64 *start) {
    struct buf b = {0}, r = {0}; stqr_u8 hash[32]; int err;
    if (!c || !info || (n && !data) || n > STQR_BUFFER_LIMIT || (op == 0x103 && n > ~(stqr_u64)0-off)) return -EINVAL;
    reference(c, &b, id); if (op == 0x103) number(&b, off, 8);
    if ((err = options(&b, opt))) goto out;
    if ((err = sq_hash(256, data, n, hash))) goto out;
    number(&b, n > SQ_CHUNK, 1); number(&b, n, 8); put(&b, hash, 32); blob(&b, n > SQ_CHUNK ? NULL : data, n > SQ_CHUNK ? 0 : n);
    if (b.err) { err = b.err; goto out; }
    if (n <= SQ_CHUNK) { err = request(c, op, &b, opt->request_id, 1+!!opt->accepted, &r, NULL); }
    else {
        /* Replay a fully rejected upload with the same logical request ID.
         * Unknown transport outcomes retain the common EINPROGRESS contract. */
        err=request_payload(c,op,&b,opt->request_id,1,&r,NULL,data,n,hash);
    }
    if (!err) {
        info_decode(&r, info);
        if (op == 0x104) { stqr_u64 at = readnum(&r, 8); if (start) *start = at; }
        if (op == 0x103) { if (readnum(&r, 8) != off || readnum(&r, 8) != n || readnum(&r, 8) != info->length) r.err = -EPROTO; }
        err = done(&r); if (!err) err = accept_info(c, info);
    }
out:
    if (err == -EPROTO || err == -EBADMSG) broken(c);
    drop(&b); drop(&r); return err;
}
int stqr_put(stqr_client *c, stqr_object_id id, const void *p, size_t n, const stqr_options *o, stqr_info *i) { return write_payload(c,0x102,id,0,p,n,o,i,NULL); }
int stqr_put_range(stqr_client *c, stqr_object_id id, stqr_u64 off, const void *p, size_t n, const stqr_options *o, stqr_info *i) { return write_payload(c,0x103,id,off,p,n,o,i,NULL); }
int stqr_append(stqr_client *c, stqr_object_id id, const void *p, size_t n, const stqr_options *o, stqr_info *i, stqr_u64 *start) {
    if (!start) return -EINVAL;
    return write_payload(c,0x104,id,0,p,n,o,i,start);
}
static int metadata_write(stqr_client *c, unsigned op, stqr_object_id id, stqr_u64 len, const stqr_options *o, stqr_info *info) {
    struct buf b = {0}, r = {0}; int err;
    if (!c || (op == 0x105 && !info)) return -EINVAL;
    reference(c, &b, id); if (op == 0x105) number(&b, len, 8);
    err = options(&b, o);
    if (!err) err = request(c, op, &b, o->request_id, 1+!!o->accepted, &r, NULL);
    if (!err) {
        if (op == 0x105) info_decode(&r, info);
        err = done(&r); if (!err && info) err = accept_info(c, info);
    }
    drop(&b); drop(&r); return err;
}
int stqr_trunc(stqr_client *c, stqr_object_id id, stqr_u64 len, const stqr_options *o, stqr_info *info) { return metadata_write(c,0x105,id,len,o,info); }
int stqr_delete(stqr_client *c, stqr_object_id id, const stqr_options *o) { return metadata_write(c,0x106,id,0,o,NULL); }
int stqr_cas(stqr_client *c, stqr_object_id id, unsigned scope, stqr_u64 off, const void *expected, size_t en, const void *replacement, size_t rn, const stqr_options *o, stqr_info *info) {
    struct buf b = {0}, r = {0}; int err;
    if (!c || !info || !o || scope > 1 || en > SQ_CHUNK || rn > SQ_CHUNK || (en && !expected) || (rn && !replacement) || (scope == 0 && en != rn) || (scope == 1 && off) || rn > ~(stqr_u64)0-off) return -EINVAL;
    reference(c, &b, id); number(&b, scope, 1); number(&b, off, 8); err = options(&b, o);
    if (!err) {
        blob(&b, expected, en); blob(&b, replacement, rn);
        err = request(c, 0x108, &b, o->request_id, 1+!!o->accepted, &r, NULL);
    }
    if (!err) { info_decode(&r, info); err = done(&r); if (!err) err = accept_info(c, info); }
    drop(&b); drop(&r); return err;
}

void stqr_set_update_handler(stqr_client *c, stqr_update_handler handler, void *arg) {
    if (c) { c->update = handler; c->update_arg = arg; }
}
stqr_u64 stqr_capabilities(const stqr_client *c) { return c ? c->capabilities : 0; }
static int poll_active(stqr_client *c, unsigned ms) {
    struct buf m = {0}; unsigned opcode, kind; int err;
    if (!c || !c->socket) return -ENOTCONN;
    err = sq_wait(c->socket, ms);
    if (err <= 0) return err;
    err = frame_recv(c, &m, 1);
    if (err == 1) { drop(&m); return 0; }
    if (!err) {
        if (readnum(&m, 1) != 1) m.err = -EPROTO;
        kind = (unsigned)readnum(&m, 1); opcode = (unsigned)readnum(&m, 2);
        const stqr_u8 *id = take(&m, 16);
        if (m.err) err = -EPROTO;
        else if (kind == 3 && (opcode == 0x112 || opcode == 0x113 || opcode == 0x11a)) err = dispatch_update(c, opcode, &m);
        else { err = async_dispatch(c,kind,opcode,id,&m); if (!err) err=-EPROTO; else if (err>0) err=0; }
    }
    drop(&m);
    if (err) { broken(c); return err; }
    return 1;
}
int stqr_poll(stqr_client *c,unsigned ms) {
    stqr_u64 start=sq_now_ms();
    if (!c) return -EINVAL;
    if (!c->channels) return poll_active(c,ms);
    for (;;) {
        struct sq_channel *s,*original=c->active;
        int result=0,err=0,open=0;
        save_channel(c);
        for (s=c->channels;s;s=s->next) {
            int n;
            if (!s->socket) continue; /* A failed new owner has no live subscriptions. */
            open=1;
            load_channel(c,s); n=poll_active(c,0); save_channel(c);
            if (n<0) { err=n; break; }
            result|=n>0;
        }
        load_channel(c,original);
        if (!open) return -ENOTCONN;
        if (err || result || sq_now_ms()-start>=ms) return err?err:result;
        err=sq_pause(1); if (err) return err;
    }
}
int stqr_subscribe(stqr_client *c, stqr_object_id id, stqr_id sid,
                   int ranged, stqr_u64 offset, stqr_u64 length,
                   const stqr_u64 *resume, void **data, size_t *actual,
                   stqr_info *info, stqr_u64 *cursor) {
    struct buf b = {0}, r = {0}; struct download stream = {0}; stqr_id rid = {{0}};
    const stqr_u8 *p, *digest, *payload; stqr_u8 hash[32]; size_t n;
    stqr_u64 total, expected; unsigned mode; int err;
    if (!data || !actual || !info || !cursor) return -EINVAL;
    *data = NULL; *actual = 0; *cursor = 0;
    if (!c || zero(sid.bytes, 16) || length > ~(stqr_u64)0-offset) return -EINVAL;
    if (*binding_slot(c,sid)) return -EBUSY;
    if (!(c->capabilities & STQR_CAP_SUBSCRIPTIONS)) return -EOPNOTSUPP;
    reference(c, &b, id); put(&b, sid.bytes, 16); number(&b, !!ranged, 1);
    number(&b, offset, 8); number(&b, length, 8); number(&b, !!resume, 1);
    if (resume) number(&b, *resume, 8);
    err = request(c, 0x110, &b, rid, 0, &r, &stream);
    if (err) goto out;
    p = take(&r, 16);
    if (p && memcmp(p, sid.bytes, 16)) r.err = -EPROTO;
    *cursor = readnum(&r, 8); info_decode(&r, info);
    mode = (unsigned)readnum(&r, 1); total = readnum(&r, 8);
    digest = take(&r, 32); payload = readblob(&r, &n);
    expected = info->length;
    if (ranged) expected = offset < expected ? (length < expected-offset ? length : expected-offset) : 0;
    if (done(&r) || !*cursor || total != expected || mode > 1) { err = -EPROTO; goto out; }
    if (total > STQR_BUFFER_LIMIT) { err = -E2BIG; goto out; }
    if (!mode) {
        if (stream.started || n != total) { err = -EPROTO; goto out; }
        err = sq_hash(256, payload, n, hash);
        if (err) goto out;
        if (memcmp(hash, digest, 32)) { err = -EBADMSG; goto out; }
        put(&stream.data, payload, n); err = stream.data.err;
    } else if (n || !stream.ended || total != stream.length || memcmp(digest, stream.digest, 32)) err = -EPROTO;
    if (!err) err = accept_info(c, info);
    if (!err) err=ensure_channel(c);
    if (!err) {
        struct sq_binding *binding=sq_alloc(sizeof(*binding));
        if (!binding) err=-ENOMEM;
        else { unsigned slot=binding_bucket(sid);
            binding->id=sid; binding->channel=c->active;
            binding->next=c->bindings[slot]; c->bindings[slot]=binding; }
    }
    if (!err) { *data = stream.data.p; *actual = stream.data.n; stream.data.p = NULL; }
out:
    /* An incomplete snapshot must not leave an invisible descriptor behind. */
    if (err && c->socket && (c->status < 300 || err == -EPROTO || err == -EBADMSG)) broken(c);
    drop(&b); drop(&r); drop(&stream.data); return err;
}
int stqr_unsubscribe(stqr_client *c, stqr_id sid) {
    struct buf b = {0}, r = {0}; stqr_id rid = {{0}}; int err;
    if (!c || zero(sid.bytes, 16)) return -EINVAL;
    struct sq_binding *binding=*binding_slot(c,sid);
    if (binding) { save_channel(c); load_channel(c,binding->channel); }
    put(&b, sid.bytes, 16); err = request(c, 0x111, &b, rid, 0, &r, NULL);
    if (!err) err = done(&r);
    if (!err) {
        struct sq_binding **at=binding_slot(c,sid);
        if (*at) { struct sq_binding *item=*at; *at=item->next; sq_free(item); }
    }
    drop(&b); drop(&r); return err;
}

static int pending_active(stqr_client *c, stqr_commit_mark *out) {
    if (!c || !out) return -EINVAL;
    if (c->durability_lost) return -ESTALE;
    *out = c->pending_mark; return 0;
}
int stqr_pending_mark(stqr_client *c,stqr_commit_mark *out) {
    struct sq_channel *s,*original;
    int err=0;
    if (!c || !out) return -EINVAL;
    if (!c->channels) return pending_active(c,out);
    save_channel(c); original=c->active;
    /* A public mark represents the whole logical connection: confirm other
     * server incarnations before returning the remaining active-node mark. */
    for (s=c->channels;s;s=s->next) if (s!=original) {
        load_channel(c,s); err=sync_active(c); save_channel(c); if (err) break;
    }
    load_channel(c,original);
    return err?err:pending_active(c,out);
}
static int sync_through_active(stqr_client *c, const stqr_commit_mark *mark, stqr_commit_mark *durable) {
    struct buf b = {0}, r = {0}; stqr_id rid = {{0}}; int err;
    if (!c || !mark || !durable || !mark->sequence || zero(mark->boot.bytes,16)) return -EINVAL;
    if (c->durability_lost) return -ESTALE;
    put(&b,c->bucket.bytes,16); put(&b,mark->boot.bytes,16); number(&b,mark->sequence,8);
    err = request(c,0x119,&b,rid,0,&r,NULL);
    if (!err) err = decode_mark(&r,durable);
    if (!err && (memcmp(durable->boot.bytes,mark->boot.bytes,16) || durable->sequence < mark->sequence)) err = -EPROTO;
    if (!err) {
        c->synced_mark = *durable;
        if (!memcmp(c->pending_mark.boot.bytes,durable->boot.bytes,16) && c->pending_mark.sequence <= durable->sequence)
            memset(&c->pending_mark,0,sizeof(c->pending_mark));
    }
    drop(&b); drop(&r); return err;
}
int stqr_sync_through(stqr_client *c,const stqr_commit_mark *mark,stqr_commit_mark *durable) {
    struct sq_channel *s,*original;
    int err;
    if (!c || !mark || !durable) return -EINVAL;
    if (!c->channels) return sync_through_active(c,mark,durable);
    save_channel(c); original=c->active;
    for (s=c->channels;s;s=s->next)
        if (!memcmp(s->pending.boot.bytes,mark->boot.bytes,16) || !memcmp(s->synced.boot.bytes,mark->boot.bytes,16)) { load_channel(c,s); break; }
    err=sync_through_active(c,mark,durable); save_channel(c); load_channel(c,original);
    return err;
}
static int sync_active(stqr_client *c) {
    stqr_commit_mark mark, durable; int err = pending_active(c,&mark);
    if (err || !mark.sequence) return err;
    return sync_through_active(c,&mark,&durable);
}

struct async_sync_wait { int done,error; };
static void sync_async_complete(void *arg,int error,int status,void *data,size_t n,const stqr_info *info) {
    struct async_sync_wait *w=arg;(void)status;(void)n;(void)info;sq_free(data);w->error=error;w->done=1;
}
int stqr_sync(stqr_client *c) {
    struct sq_channel *s,*original;
    if(c) {
        int modern=!!(c->capabilities&STQR_CAP_CONNECTION_SYNC);
        for(s=c->channels;s;s=s->next)modern|=!!(s->capabilities&STQR_CAP_CONNECTION_SYNC);
        if(modern) {
            struct async_sync_wait wait={0};int result=stqr_connection_sync_async(c,sync_async_complete,&wait);
            if(result)return result;
            while(!wait.done) {
                result=stqr_poll(c,c->timeout);
                /* Broken channels complete their pending callbacks. Continue
                 * draining other owners before this stack context expires. */
                if(result<0 && !wait.done) broken(c);
            }
            return wait.error;
        }
    }
    int err=0;
    if (!c) return -EINVAL;
    if (!c->channels) return sync_active(c);
    save_channel(c); original=c->active;
    for (s=c->channels;s;s=s->next) {
        load_channel(c,s); err=sync_active(c); save_channel(c); if (err) break;
    }
    load_channel(c,original); return err;
}
int stqr_bucket_space(stqr_client *c, stqr_space *space) {
    struct buf b = {0}, r = {0}; stqr_id rid; stqr_space out = {0}; int err;
    if (!c || !space) return -EINVAL;
    err = stqr_request_id(&rid); if (err) return err;
    put(&b, c->bucket.bytes, 16);
    err = request(c, 0x11b, &b, rid, 0, &r, NULL);
    if (!err) {
        out.used = readnum(&r, 8); out.total = readnum(&r, 8);
        out.free = readnum(&r, 8); out.available = readnum(&r, 8);
        out.quota = readnum(&r, 8); out.quota_limited = boolean(&r);
        err = done(&r);
        if (!err && (out.free > out.total || out.used != out.total-out.free || out.available > out.free)) err = -EPROTO;
    }
    drop(&b); drop(&r);
    if (!err) *space = out;
    return err;
}
static void note_active(stqr_client *c, const stqr_commit_mark *mark) {
    if (!c || !mark || c->durability_lost) return;
    if (!memcmp(c->pending_mark.boot.bytes,mark->boot.bytes,16) && c->pending_mark.sequence <= mark->sequence)
        memset(&c->pending_mark,0,sizeof(c->pending_mark));
    if (!memcmp(c->synced_mark.boot.bytes,mark->boot.bytes,16) && c->synced_mark.sequence > mark->sequence) return;
    c->synced_mark = *mark;
}

void stqr_note_durable(stqr_client *c,const stqr_commit_mark *mark) {
    struct sq_channel *s,*original;
    if (!c || !mark) return;
    if (!c->channels) { note_active(c,mark); return; }
    save_channel(c); original=c->active;
    for (s=c->channels;s;s=s->next)
        if (!memcmp(s->pending.boot.bytes,mark->boot.bytes,16) || !memcmp(s->synced.boot.bytes,mark->boot.bytes,16)) {
            load_channel(c,s); note_active(c,mark); save_channel(c);
        }
    load_channel(c,original);
}

/* A captured fence owns separate, bounded control sessions. Waiting never uses
 * the original client's socket or mutable owner state, so object I/O and update
 * delivery may continue. Capture/apply still follow that client's normal lock. */
struct sq_fence_entry {
    stqr_client *client;
    stqr_commit_mark target, durable;
    int active, confirmed;
};
struct stqr_fence {
 struct sq_fence_entry entries[SQ_CHANNEL_LIMIT];
 unsigned count, targets, active[SQ_CHANNEL_LIMIT];
};
static int fence_capture_owner(stqr_fence *f, const stqr_client *source,
                               const char *host, const char *authority, unsigned port,
                               stqr_commit_mark target, int lost) {
    struct sq_fence_entry *e = NULL;
    unsigned i;
    if (lost) return -ESTALE;
    if (!target.sequence) return 0;
    for (i=0;i<f->count;i++)
        if (!strcmp(f->entries[i].client->authority,authority) &&
            !memcmp(f->entries[i].client->bucket.bytes,source->bucket.bytes,16) &&
            f->entries[i].client->authorization_len==source->authorization_len &&
            !memcmp(f->entries[i].client->authorization,source->authorization,source->authorization_len)) {
            e=&f->entries[i]; break;
        }
    if (!e) {
        if (f->count<SQ_CHANNEL_LIMIT) e=&f->entries[f->count++];
        else {
            for (i=0;i<f->count;i++) if (!f->entries[i].active) { e=&f->entries[i]; break; }
            if (!e) return -ENOSPC;
            stqr_close(e->client); memset(e,0,sizeof(*e));
        }
        e->client=sq_alloc(sizeof(*e->client));
        if (!e->client) { /* Keep every retained cache entry well formed. */
            unsigned index=(unsigned)(e-f->entries);
            memmove(e,e+1,(f->count-index-1)*sizeof(*e)); f->count--; return -ENOMEM;
        }
        memcpy(e->client->host,host,sizeof(e->client->host));
        memcpy(e->client->authority,authority,sizeof(e->client->authority));
        e->client->port=port; e->client->timeout=source->timeout;
        e->client->bucket=source->bucket;
        memcpy(e->client->format,source->format,sizeof(e->client->format));
        e->client->authorization=sq_alloc(source->authorization_len+1);
        if (!e->client->authorization) {
            unsigned index=(unsigned)(e-f->entries); stqr_close(e->client);
            memmove(e,e+1,(f->count-index-1)*sizeof(*e)); f->count--; return -ENOMEM;
        }
        memcpy(e->client->authorization,source->authorization,source->authorization_len);
        e->client->authorization_len=source->authorization_len;
    }
    if (e->active) {
        if (memcmp(e->target.boot.bytes,target.boot.bytes,16)) return -ESTALE;
        if (e->target.sequence>=target.sequence) return 0;
    } else f->active[f->targets++]=(unsigned)(e-f->entries);
    e->target=target; e->active=1; e->confirmed=0;
    memset(&e->durable,0,sizeof(e->durable));
    return 0;
}
int stqr_fence_reset(stqr_fence **out) {
    unsigned i; stqr_fence *f;
    if (!out) return -EINVAL;
    if (!*out) { *out=sq_alloc(sizeof(**out)); if (!*out) return -ENOMEM; }
    f=*out; f->targets=0;
    for (i=0;i<f->count;i++) { f->entries[i].active=0; f->entries[i].confirmed=0; }
    return 0;
}
int stqr_fence_add(stqr_client *c, stqr_fence *f) {
    struct sq_channel *s; int err=0;
    if (!c || !f) return -EINVAL;
    if (!c->channels)
        return fence_capture_owner(f,c,c->host,c->authority,c->port,c->pending_mark,c->durability_lost);
    save_channel(c);
    for (s=c->channels;s && !err;s=s->next)
        err=fence_capture_owner(f,c,s->host,s->authority,s->port,s->pending,s->lost);
    return err;
}
int stqr_fence_capture(stqr_client *c, stqr_fence **out) {
    int err;
    if (!c || !out) return -EINVAL;
    err=stqr_fence_reset(out);
    return err?err:stqr_fence_add(c,*out);
}
unsigned stqr_fence_count(const stqr_fence *f) { return f?f->targets:0; }
int stqr_fence_wait_one(stqr_fence *f, unsigned target) {
    struct sq_fence_entry *e; int err;
    if (!f || target>=f->targets) return -EINVAL;
    e=&f->entries[f->active[target]];
    if (e->confirmed) return 0;
    err=e->client->socket?0:stqr_reconnect(e->client);
    if (!err) err=stqr_sync_through(e->client,&e->target,&e->durable);
    /* A barrier is idempotent, even after an unknown transport outcome. */
    if (err==-ENOTCONN || err==-ECONNRESET || err==-EPIPE || err==-ETIMEDOUT) {
        err=stqr_reconnect(e->client);
        if (!err) err=stqr_sync_through(e->client,&e->target,&e->durable);
    }
    e->confirmed=!err;
    return err;
}
int stqr_fence_wait(stqr_fence *f) {
    unsigned i; int first=0;
    if (!f) return 0;
    for (i=0;i<f->targets;i++) {
        int err=stqr_fence_wait_one(f,i);
        if (!first) first=err;
    }
    return first;
}
void stqr_fence_apply(stqr_client *c, const stqr_fence *f) {
    unsigned i;
    if (!c || !f) return;
    for (i=0;i<f->count;i++)
        if (f->entries[i].active && f->entries[i].confirmed &&
            !memcmp(f->entries[i].client->bucket.bytes,c->bucket.bytes,16))
            stqr_note_durable(c,&f->entries[i].durable);
}
void stqr_fence_close(stqr_fence *f) {
    unsigned i;
    if (!f) return;
    for (i=0;i<f->count;i++) stqr_close(f->entries[i].client);
    sq_secret_free(f,sizeof(*f));
}

#include "async.c"

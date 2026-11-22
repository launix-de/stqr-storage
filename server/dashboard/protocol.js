// SPDX-License-Identifier: GPL-3.0-only
// Same canonical LE/LEB128 codec as client/protocol.go and spec §13. No JSON
// control channel: administrative JSON field patches are explicitly Text bodies
// of NODE_CONFIG_GET/SET; all messages use the original STQR binary envelope.
import {writeBucketPolicy,writeBucketQuota,parseBucketPage} from './bucket-codec.js';
export const OP={FORCE_DISCONNECT:0x2013,CLUSTER_SHUTDOWN:0x2012,PG_LIST:0x2030,PG_SET:0x2031,PG_DELETE:0x2032,BUCKET_SET_PG:0x2033,NODE_DRAIN:0x2010,NODE_SHUTDOWN:0x2011,USER_S3_LIST:0x2025,USER_S3_SET:0x2026,USER_S3_DELETE:0x2027,NODE_ADMIN:0x2024,HELLO:1,PING:2,EVENT:4,STATS_GET:8,STATS_SUBSCRIBE:9,STATS_UNSUBSCRIBE:10,STATS_SNAPSHOT:11,VOLUME_ADD:0x2001,VOLUME_REMOVE:0x2002,OPERATION_STATUS:0x2019,CONFIG_GET:0x201a,CONFIG_SET:0x201b,VOLUME_CANDIDATES:0x201c,VOLUME_RESCAN:0x201d,VOLUME_CAPACITY:0x201e,VOLUME_GROW_INFO:0x201f,VOLUME_GROW:0x2020,USER_LIST:0x2021,USER_CREATE:0x200b,USER_UPDATE:0x200c,USER_SET_PASSWORD:0x200d,USER_DELETE:0x200e,BUCKET_LIST:0x2022,BUCKET_QUOTA_CAPACITY:0x2023,BUCKET_CREATE:0x2003,BUCKET_SET_POLICY:0x2004,BUCKET_SET_QUOTA:0x2005,BUCKET_RENAME:0x2006,BUCKET_LOCK:0x2007,BUCKET_DELETE:0x2008,BUCKET_GRANT:0x2009,BUCKET_REVOKE:0x200a};
const utf8=new TextEncoder(),text=new TextDecoder('utf-8',{fatal:true});
export const newID=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');
export class Writer{
 constructor(){this.a=[]}u8(v){this.a.push(Number(v));return this}bool(v){return this.u8(v?1:0)}
 u16(v){return this.u8(v&255).u8((v>>>8)&255)}u32(v){for(let i=0;i<4;i++)this.u8((v>>>(8*i))&255);return this}
 u64(v){v=BigInt(v);if(v<0n||v>0xffffffffffffffffn)throw Error('u64 overflow');for(let i=0;i<8;i++){this.u8(Number(v&255n));v>>=8n}return this}
 v(v){v=BigInt(v);if(v<0n||v>0xffffffffffffffffn)throw Error('varint overflow');do{let b=Number(v&127n);v>>=7n;this.u8(b|(v?128:0))}while(v);return this}
 id(s){if(!/^[0-9a-f]{32}$/i.test(s))throw Error('Ungültige ID');for(let i=0;i<32;i+=2)this.u8(parseInt(s.slice(i,i+2),16));return this}
 raw(b){for(const v of b)this.a.push(v);return this}blob(b){return this.v(b.length).raw(b)}text(s){return this.blob(utf8.encode(s))}bytes(){return new Uint8Array(this.a)}
}
export class Reader{
 constructor(b){this.b=b instanceof Uint8Array?b:new Uint8Array(b);this.at=0}take(n){if(!Number.isSafeInteger(n)||n<0||n>this.b.length-this.at)throw Error('Unvollständige STQR-Nachricht');const b=this.b.subarray(this.at,this.at+n);this.at+=n;return b}
 u8(){return this.take(1)[0]}bool(){const b=this.u8();if(b>1)throw Error('Ungültiges Bool');return b===1}
 u16(){const b=this.take(2);return b[0]|b[1]<<8}u32(){const b=this.take(4);return new DataView(b.buffer,b.byteOffset,4).getUint32(0,true)}
 u64(){const b=this.take(8);return new DataView(b.buffer,b.byteOffset,8).getBigUint64(0,true)}
 v(){let n=0n;for(let i=0;i<10;i++){const b=this.u8();if(i===9&&b>1)throw Error('Varint overflow');n|=BigInt(b&127)<<BigInt(7*i);if(!(b&128)){if(i>0&&b===0)throw Error('Nichtkanonisches Varint');return n}}throw Error('Varint overflow')}
 count(max=1<<24){const n=this.v();if(n>BigInt(max)||n>BigInt(this.b.length-this.at))throw Error('Zu große STQR-Länge');return Number(n)}
 blob(){return this.take(this.count())}text(){return text.decode(this.blob())}id(){return Array.from(this.take(16),b=>b.toString(16).padStart(2,'0')).join('')}
 done(){if(this.at!==this.b.length)throw Error('Unerwartete Daten am Nachrichtenende')}
}
const filter=w=>w.u8(1).v(0).v(0).v(0).bool(false);
export function envelope(op,body,id=newID(),kind=1){return {id,bytes:new Writer().u8(1).u8(kind).u16(op).id(id).raw(body).bytes()}}
function io(r){return {ops:Array.from({length:6},()=>r.u64()),logicalRead:r.u64(),logicalWrite:r.u64(),read:r.u64(),write:r.u64(),errors:r.u64(),inFlight:r.u64(),queued:r.u64(),flushes:r.u64(),flushNS:r.u64(),latency:Array.from({length:18},()=>r.u64()),samples:Array.from({length:6},()=>r.u64())}}
function readMany(r,n){return Array.from({length:n},()=>r.u64())}
export function decodeStats(data){const r=new Reader(data),s={schema:r.u16(),node:r.id(),boot:r.id(),sequence:r.u64(),captured:r.u64(),window:r.u64(),membership:r.u64(),generation:r.u64(),revision:r.u64(),state:r.u8(),needRebuild:r.bool(),reason:r.u64(),coordinator:r.id()};if(s.schema!==1)throw Error('Unbekanntes Statistik-Schema');readMany(r,2);r.id();r.u64();[s.cpu,s.resident,s.netRead,s.netWrite]=readMany(r,4);s.io=io(r);s.volumes=[];
 for(let i=0,n=r.count(65536);i<n;i++){const v={id:r.id(),path:r.text(),class:r.u8(),state:r.u8()};[v.capacity,v.allocated,v.reclaimable,v.payload,v.metadata,v.arena,v.guaranteed,v.inFlightReserved,v.operating,v.admission]=readMany(r,10);v.health=r.u8();v.io=io(r);s.volumes.push(v)}
 for(let i=0,n=r.count(65536);i<n;i++){r.id();r.u8();r.u8();readMany(r,5);r.bool();readMany(r,2);io(r)}
 for(let i=0,n=r.count(65536);i<n;i++){r.id();r.take(32);r.u16();r.id();r.take(3);r.id();readMany(r,13);io(r)}r.done();return s}
function ack(r){return {operation_id:r.id(),state:['requested','running','fulfilled','failed'][r.u8()],volume_id:r.bool()?r.id():null}}
function decodeResponse(op,body){const r=new Reader(body),status=Number(r.v()),headers=new Reader(r.blob());let detail='';for(let i=0,n=headers.count(100);i<n;i++){const tag=Number(headers.v()),value=new Reader(headers.blob());if(tag===3){detail=value.text();value.done()}}headers.done();if(status>=300){const e=Error(detail||`STQR-Fehler ${status}`);e.status=status;throw e}
 let result;
 if(op===OP.USER_S3_LIST){result=[];for(let i=0,n=r.count(32);i<n;i++)result.push(r.text())}else if(op===OP.BUCKET_QUOTA_CAPACITY){result={used:r.u64(),available:r.u64(),reservedOther:r.u64(),maximum:r.u64(),nodes:r.u32(),overhead:r.u32(),profiles:[]};for(let i=0,n=r.count(32);i<n;i++)result.profiles.push({key:r.u8(),label:r.text(),size:r.u64(),overhead:r.u32()})}else if(op===OP.BUCKET_LIST)result=parseBucketPage(r.text());else if(op===OP.USER_LIST){result=[];for(let i=0,n=r.count(100000);i<n;i++)result.push({id:r.id(),name:r.text(),role:r.u8(),disabled:r.bool()})}else if(op===OP.VOLUME_GROW_INFO)result={kind:r.u8(),current:r.u64(),available:r.u64(),maximum:r.u64()};else if(op===OP.VOLUME_CAPACITY)result={fileAvailable:r.u64(),ramAvailable:r.u64(),ramLimit:r.u64()};else if([OP.CONFIG_GET,OP.CONFIG_SET,OP.VOLUME_CANDIDATES,OP.PG_LIST].includes(op))result=JSON.parse(r.text());else if([OP.FORCE_DISCONNECT,OP.CLUSTER_SHUTDOWN,OP.PG_SET,OP.PG_DELETE,OP.BUCKET_SET_PG,OP.NODE_DRAIN,OP.NODE_SHUTDOWN,OP.USER_S3_SET,OP.USER_S3_DELETE,OP.VOLUME_ADD,OP.VOLUME_REMOVE,OP.VOLUME_RESCAN,OP.VOLUME_GROW,OP.USER_CREATE,OP.USER_UPDATE,OP.USER_SET_PASSWORD,OP.USER_DELETE,OP.BUCKET_CREATE,OP.BUCKET_SET_POLICY,OP.BUCKET_SET_QUOTA,OP.BUCKET_RENAME,OP.BUCKET_LOCK,OP.BUCKET_DELETE,OP.BUCKET_GRANT,OP.BUCKET_REVOKE].includes(op))result=ack(r);else if(op===OP.OPERATION_STATUS){result=ack(r);result.status=Number(r.v());result.detail=r.text()}else if(op===OP.STATS_GET){result=decodeStats(r.take(r.b.length-r.at))}else if(op===OP.STATS_SUBSCRIBE){result=r.id()}else if(op===OP.PING){result=r.u64()}else result=r.take(r.b.length-r.at);r.done();return result}

// SHA-256 works on HTTP intranets where WebCrypto's secure-context-only digest
// is unavailable. Verify complete StatsPart data before publishing a snapshot.
function sha256(bytes){const k=[0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];const h=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);const len=Math.ceil((bytes.length+9)/64)*64,b=new Uint8Array(len);b.set(bytes);b[bytes.length]=128;const view=new DataView(b.buffer);view.setBigUint64(len-8,BigInt(bytes.length)*8n);const w=new Uint32Array(64),rr=(a,n)=>(a>>>n)|(a<<(32-n));for(let off=0;off<len;off+=64){for(let i=0;i<16;i++)w[i]=view.getUint32(off+i*4);for(let i=16;i<64;i++){const a=w[i-15],c=w[i-2];w[i]=(rr(a,7)^rr(a,18)^(a>>>3))+w[i-16]+(rr(c,17)^rr(c,19)^(c>>>10))+w[i-7]}let [a,c,d,e,f,g,j,l]=h;for(let i=0;i<64;i++){const t1=l+(rr(f,6)^rr(f,11)^rr(f,25))+((f&g)^(~f&j))+k[i]+w[i],t2=(rr(a,2)^rr(a,13)^rr(a,22))+((a&c)^(a&d)^(c&d));l=j;j=g;g=f;f=(e+t1)>>>0;e=d;d=c;c=a;a=(t1+t2)>>>0}for(const [i,v]of [a,c,d,e,f,g,j,l].entries())h[i]+=v}const out=new Uint8Array(32),ov=new DataView(out.buffer);h.forEach((v,i)=>ov.setUint32(i*4,v));return out}

export class NodeConnection{
 constructor(csrf,onState,onStats){this.csrf=csrf;this.onState=onState;this.onStats=onStats;this.pending=new Map;this.outbox=[];this.parts=new Map;this.stopped=false;this.retry=500;this.sub=newID();this.connect()}
 connect(){if(this.stopped)return;const ws=this.ws=new WebSocket(`ws://${location.host}/api/ws`,['stqr.v1',`csrf.${this.csrf}`]);ws.binaryType='arraybuffer';ws.onopen=()=>{this.retry=500;this.onState(true);this.onOpen?.();this.flush()};ws.onmessage=e=>{try{this.receive(e.data)}catch(err){console.error(err);this.onProtocolError?.(err);ws.close(1002,'invalid STQR message')}};ws.onerror=()=>{};ws.onclose=()=>{this.onState(false);this.outbox=[];this.parts.clear();for(const [id,p]of this.pending){const e=Error(`Verbindung unterbrochen. Ausgang unklar; Operations-ID: ${id}`);e.operationID=id;p.reject(e)}this.pending.clear();if(!this.stopped){Promise.resolve().then(()=>this.onDisconnect?.()).catch(()=>{});this.reconnect=setTimeout(()=>this.connect(),this.retry);this.retry=Math.min(10000,this.retry*2)}}}
 request(op,body=new Uint8Array,id=newID(),resultOpcode=op){if(this.stopped||this.ws?.readyState!==WebSocket.OPEN)return Promise.reject(Error('Keine Verbindung zur Node'));if(this.outbox.length>=64||this.pending.size>=12)return Promise.reject(Error('Sendequeue voll (WOULD_BLOCK)'));const m=envelope(op,body,id);return new Promise((resolve,reject)=>{this.pending.set(id,{resolve,reject,op,resultOpcode});this.outbox.push(m.bytes);this.flush()})}
 fireAndForget(op,body=new Uint8Array){if(this.ws?.readyState!==WebSocket.OPEN||this.outbox.length>=64)throw Error('Sendequeue nicht verfügbar (WOULD_BLOCK)');this.outbox.push(envelope(op,body,newID(),0).bytes);this.flush()}
 flush(){clearTimeout(this.flushTimer);while(this.outbox.length&&this.ws.readyState===WebSocket.OPEN&&this.ws.bufferedAmount<1<<20)this.ws.send(this.outbox.shift());if(this.outbox.length)this.flushTimer=setTimeout(()=>this.flush(),25)}
 receive(buffer){const r=new Reader(buffer);if(r.u8()!==1)throw Error('Unbekannte STQR-Version');const kind=r.u8(),op=r.u16(),id=r.id(),body=r.take(r.b.length-r.at);if(kind===2){const p=this.pending.get(id);if(!p)return;this.pending.delete(id);try{if(op!==p.op)throw Error('Antwort-Opcode stimmt nicht');p.resolve(decodeResponse(p.resultOpcode??op,body))}catch(e){e.operationID=id;p.reject(e)}return}if(kind!==3)throw Error('Unerwarteter Nachrichtentyp');if(op===OP.STATS_SNAPSHOT)this.statsPart(body);else if(op===OP.EVENT){const e=new Reader(body);const cause=e.id(),status=Number(e.v()),message=e.text();e.done();this.onEvent?.({cause,status,message})}}
 statsPart(body){
  const r=new Reader(body),subscription=r.id(),source=r.id(),boot=r.id(),seq=r.u64(),index=r.u32(),count=r.u32(),digest=r.take(32),data=r.blob();r.done();
  if(subscription!==this.sub)return;
  const zero='0'.repeat(32);
  if(source===zero||boot===zero||seq===0n||count<1||count>64||index>=count||data.length===0||data.length>1<<20)throw Error('Ungültiges Statistikfragment');
  let entry=this.parts.get(source);
  if(entry&&entry.boot===boot&&seq<entry.seq)return;
  if(!entry||entry.boot!==boot||seq>entry.seq){
   if(!entry&&this.parts.size>=10000)throw Error('Zu viele Statistikquellen');
   entry={boot,seq,count,digest,parts:new Array(count),received:0,bytes:0,complete:false};this.parts.set(source,entry);
  }
  if(entry.count!==count||entry.digest.some((v,i)=>v!==digest[i]))throw Error('Widersprüchliche Statistikfragmente');
  if(entry.complete)return;
  const prior=entry.parts[index];
  if(prior){if(prior.length!==data.length||prior.some((v,i)=>v!==data[i]))throw Error('Widersprüchliches Statistikfragment');return}
  let pendingBytes=data.length;for(const value of this.parts.values())pendingBytes+=value.bytes;
  if(pendingBytes>64<<20)throw Error('Statistikpuffer voll');
  entry.parts[index]=data.slice();entry.received++;entry.bytes+=data.length;
  if(entry.received!==count)return;
  const joined=new Uint8Array(entry.bytes);let at=0;for(const p of entry.parts){joined.set(p,at);at+=p.length}
  if(sha256(joined).some((b,i)=>b!==digest[i]))throw Error('Statistik-Digest stimmt nicht');
  const snapshot=decodeStats(joined);
  if(snapshot.node!==source||snapshot.boot!==boot||snapshot.sequence!==seq)throw Error('Statistikquelle stimmt nicht');
  entry.parts=[];entry.bytes=0;entry.complete=true;this.onStats(snapshot);
 }

 stop(){this.stopped=true;clearTimeout(this.reconnect);clearTimeout(this.flushTimer);if(this.ws?.readyState===WebSocket.OPEN){try{this.fireAndForget(OP.STATS_UNSUBSCRIBE,new Writer().id(this.sub).bytes())}catch{}}this.ws?.close()}
 forNode(target){
  const scoped={request:(op,body=new Uint8Array,id=newID())=>this.request(OP.NODE_ADMIN,new Writer().id(target).u16(op).blob(body).bytes(),id,op)};
  for(const name of ['drainNode','shortShutdown','getConfig','setConfig','volumeGrowInfo','growVolume','volumeCapacity','listVolumeCandidates','addVolume','removeVolume','rescanVolume','operationStatus'])scoped[name]=NodeConnection.prototype[name];
  return scoped;
 }
 drainNode(target,id){return this.request(OP.NODE_DRAIN,new Writer().id(target).bytes(),id)}
 shortShutdown(target,id){return this.request(OP.NODE_SHUTDOWN,new Writer().id(target).u8(1).bytes(),id)}
 forceDisconnect(nodes,epoch,acceptLoss,fenced,id){
  const ids=[...new Set(nodes)].sort(),w=new Writer().v(ids.length);
  for(const node of ids)w.id(node);
  return this.request(OP.FORCE_DISCONNECT,w.bool(acceptLoss).bool(fenced).u64(epoch).bytes(),id);
 }
 clusterShutdown(id){return this.request(OP.CLUSTER_SHUTDOWN,new Writer().u8(2).bytes(),id)}
 getConfig(){return this.request(OP.CONFIG_GET)}setConfig(fields){return this.request(OP.CONFIG_SET,new Writer().text(JSON.stringify(fields)).bytes())}
 bucketQuotaCapacity(bucket,strategy,profile=255,overhead=0){return this.request(OP.BUCKET_QUOTA_CAPACITY,new Writer().id(bucket).u8({hdd:0,ssd:1,ram:2}[strategy]).u8(profile).u32(overhead).bytes())}
 listPlacementGroups(after='',limit=100){return this.request(OP.PG_LIST,new Writer().u16(1).text(after).u32(limit).bytes())}
 setPlacementGroup(group,expected,name,nodes,id){return this.request(OP.PG_SET,new Writer().u16(1).id(group).u64(expected).text(name).text(JSON.stringify(nodes)).bytes(),id)}
 deletePlacementGroup(group,expected,id){return this.request(OP.PG_DELETE,new Writer().u16(1).id(group).u64(expected).bytes(),id)}
 setBucketPlacementGroup(bucket,expected,group,id){const w=new Writer().u16(1).id(bucket).u64(expected).bool(!!group);if(group)w.id(group);return this.request(OP.BUCKET_SET_PG,w.bytes(),id)}
 listBuckets(after='',limit=100){return this.request(OP.BUCKET_LIST,new Writer().text(after).u32(limit).bytes())}
 createBucket(bucket,id){return this.request(OP.BUCKET_CREATE,writeBucketPolicy(new Writer(),bucket).bytes(),id)}
 setBucketPolicy(bucket,expected,id){return this.request(OP.BUCKET_SET_POLICY,writeBucketPolicy(new Writer(),bucket).u64(expected).bytes(),id)}
 setBucketQuota(bucket,expected,id){return this.request(OP.BUCKET_SET_QUOTA,writeBucketQuota(new Writer(),bucket,expected).bytes(),id)}
 renameBucket(bucket,name,id){return this.request(OP.BUCKET_RENAME,new Writer().id(bucket).text(name).bytes(),id)}
 lockBucket(bucket,locked,id){return this.request(OP.BUCKET_LOCK,new Writer().id(bucket).bool(locked).bytes(),id)}
 deleteBucket(bucket,id){return this.request(OP.BUCKET_DELETE,new Writer().id(bucket).bytes(),id)}
 grantBucket(bucket,user,permissions,id){return this.request(OP.BUCKET_GRANT,new Writer().id(bucket).id(user).u32(permissions).bytes(),id)}
 revokeBucket(bucket,user,id){return this.request(OP.BUCKET_REVOKE,new Writer().id(bucket).id(user).bytes(),id)}
 listS3Keys(user){return this.request(OP.USER_S3_LIST,new Writer().id(user).bytes())}
 setS3Key(user,access,secret,id){return this.request(OP.USER_S3_SET,new Writer().id(user).text(access).text(secret).bytes(),id)}
 deleteS3Key(user,access,id){return this.request(OP.USER_S3_DELETE,new Writer().id(user).text(access).bytes(),id)}
 listUsers(){return this.request(OP.USER_LIST)}
 createUser(user,password,id){return this.request(OP.USER_CREATE,new Writer().id(user.id).text(user.name).u8(user.role??0).bool(false).text(password).bytes(),id)}
 setUserRole(user,role,id){return this.request(OP.USER_UPDATE,new Writer().id(user.id).text(user.name).u8(role).bool(user.disabled).bytes(),id)}
 setUserPassword(user,password,id){return this.request(OP.USER_SET_PASSWORD,new Writer().id(user).text(password).bytes(),id)}
 deleteUser(user,id){return this.request(OP.USER_DELETE,new Writer().id(user).bytes(),id)}
 volumeGrowInfo(volume){return this.request(OP.VOLUME_GROW_INFO,new Writer().id(volume).bytes())}
 growVolume(volume,size,id){return this.request(OP.VOLUME_GROW,new Writer().id(volume).u64(size).bytes(),id)}
 volumeCapacity(){return this.request(OP.VOLUME_CAPACITY)}
 listVolumeCandidates(){return this.request(OP.VOLUME_CANDIDATES)}
 addVolume(v,id){return this.request(OP.VOLUME_ADD,new Writer().text(v.name).u8(v.source).text(v.path||'').u8({hdd:0,ssd:1,ram:2}[v.class]).u64(v.size).bytes(),id)}
 removeVolume(volume,id){return this.request(OP.VOLUME_REMOVE,new Writer().id(volume).bytes(),id)}
 rescanVolume(volume,id=newID()){return this.request(OP.VOLUME_RESCAN,new Writer().id(volume).bytes(),id)}
 operationStatus(id){return this.request(OP.OPERATION_STATUS,new Writer().id(id).bytes())}
 getStats(){return this.request(OP.STATS_GET,filter(new Writer()).bytes())}
 subscribeStats(){return this.request(OP.STATS_SUBSCRIBE,filter(new Writer().id(this.sub)).u32(3000).bytes())}
}

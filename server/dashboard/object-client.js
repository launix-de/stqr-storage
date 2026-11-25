// SPDX-License-Identifier: GPL-3.0-only
// Administrative browser transport for ordinary native objects. No filesystem
// or disk formats belong here; all operations retain normal server checks.
import {Writer,Reader,newID,sha256} from './protocol.js';
export const hex=b=>Array.from(b,v=>v.toString(16).padStart(2,'0')).join('');
export const unhex=s=>{if(!/^[0-9a-f]{64}$/.test(s))throw Error('Ungültige Objekt-ID');return Uint8Array.from(s.match(/../g),v=>parseInt(v,16))};
export function readInfo(r){const length=r.u64();if(r.bool())r.take(32);r.u32();r.id();r.u8();r.u16();return {length}}
export class ObjectClient{
 constructor(connection,bucket,node){this.connection=connection;this.bucket=bucket;this.node=node}
 ref(oid){return new Writer().id(this.bucket).raw(unhex(oid)).u64(0).u64(0).id('0'.repeat(32))}
 async request(op,body,id=newID()){
  let node=this.node;
  for(let attempt=0;attempt<8;attempt++){
   try{return await this.connection.forNode(node).request(op,body,id)}catch(e){
    if(e.status!==301||!e.patch)throw e;
    const r=new Reader(e.patch);r.u64();r.u64();r.u64();if(r.id()!==this.bucket)throw Error('Falscher Routing-Bucket');
    r.take(32);r.bool();r.take(32);
    // Native per-object redirects contain exactly the affected routing range.
    if(r.count(1)!==1)throw Error('Ungültiger Routing-Bereich');
    r.take(32);r.u16();r.u64();r.u64();r.u8();r.id();let primary;
    for(let i=0,n=r.count(256);i<n;i++){const owner=r.id();r.id();const role=r.u8();r.u8();r.u16();r.u32();if(role===0)primary=owner}
    if(!primary)throw Error('Kein zuständiger Objekt-Server');node=primary;
   }
  }
  throw Error('Objekt-Routing ändert sich; bitte erneut versuchen.');
 }
 async stat(oid){const r=new Reader(await this.request(0x107,this.ref(oid).bytes())),info=readInfo(r);r.done();return info}
 async get(oid){
  const r=new Reader(await this.request(0x100,this.ref(oid).bool(false).bytes())),info=readInfo(r),mode=r.u8(),length=r.u64(),digest=r.take(32),data=r.blob();r.done();
  if(mode!==0||length!==BigInt(data.length)||info.length!==length||hex(sha256(data))!==hex(digest))throw Error('Ungültige Objektübertragung');return data;
 }
 async scan(after,limit){
  const w=this.ref('0'.repeat(64)).bool(!!after);if(after)w.raw(unhex(after));w.u32(limit);
  const r=new Reader(await this.request(0x117,w.bytes())),count=r.u32(),objects=[];
  if(count>limit)throw Error('Zu große Objektseite');let previous=after;
  for(let i=0;i<count;i++){const id=hex(r.take(32)),info=readInfo(r);if(previous&&id<=previous)throw Error('Unsortierte Objektseite');previous=id;objects.push({id,...info})}
  const next=r.bool()?hex(r.take(32)):'';r.done();if(next&&previous&&next<previous)throw Error('Ungültiger Objektcursor');return {objects,next};
 }
 async casRange(oid,offset,before,replacement,condition,id){
  const expected=before.slice(offset,offset+replacement.length);
  const w=this.ref(oid).u8(0).u64(offset).bool(true).u64(before.length).bool(false).bool(true).u64(0).blob(condition).blob(expected).blob(replacement);
  const r=new Reader(await this.request(0x108,w.bytes(),id)),info=readInfo(r);r.done();return info;
 }
}

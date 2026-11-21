// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {NodeConnection,Writer,Reader,OP,envelope} from './dashboard/protocol.js';

const id=n=>n.toString(16).padStart(32,'0');
function snapshot(node,boot,sequence){
 const w=new Writer().u16(1).id(node).id(boot);
 for(const n of [sequence,100,3000000000,1,1,1])w.u64(n);
 w.u8(0).bool(false).u64(0).id(id(0)).u64(0).u64(0).id(id(0)).u64(0);
 for(let i=0;i<4+39;i++)w.u64(i);
 return w.v(0).v(0).v(0).bytes();
}
function parts(node,boot,seq,sub,patch={}){
 const raw=snapshot(node,boot,seq),digest=createHash('sha256').update(raw).digest(),count=3;
 return Array.from({length:count},(_,i)=>{
  const data=raw.slice(Math.floor(raw.length*i/count),Math.floor(raw.length*(i+1)/count));
  return new Writer().id(sub).id(patch.source??node).id(patch.boot??boot).u64(patch.sequence??seq).u32(i).u32(count).raw(digest).blob(data).bytes();
 });
}
function receiver(){
 const c=Object.create(NodeConnection.prototype),received=[];c.sub=id(9);c.parts=new Map;c.onStats=s=>received.push(s);return {c,received};
}
test('interleaved nodes finish independently without dropping fragments',()=>{
 const {c,received}=receiver(),a=parts(id(1),id(11),7,c.sub),b=parts(id(2),id(22),8,c.sub);
 for(let i=0;i<3;i++){c.statsPart(a[i]);c.statsPart(b[2-i]);c.statsPart(a[i])}
 assert.deepEqual(received.map(s=>[s.node,s.sequence]),[[id(1),7n],[id(2),8n]]);
 assert([...c.parts.values()].every(e=>e.bytes===0&&e.complete));
});
test('later sequence supersedes only its own source; stale fragments are ignored',()=>{
 const {c,received}=receiver(),old=parts(id(1),id(11),7,c.sub),next=parts(id(1),id(11),8,c.sub),b=parts(id(2),id(22),8,c.sub);
 c.statsPart(old[0]);c.statsPart(b[0]);c.statsPart(next[0]);c.statsPart(old[1]);c.statsPart(b[1]);c.statsPart(next[1]);c.statsPart(b[2]);c.statsPart(next[2]);
 assert.deepEqual(received.map(s=>[s.node,s.sequence]),[[id(2),8n],[id(1),8n]]);
});
test('misattributed payloads and conflicting duplicates are rejected',()=>{
 for(const patch of [{source:id(2)},{boot:id(22)},{sequence:99}]){
  const {c}=receiver(),messages=parts(id(1),id(11),7,c.sub,patch);
  c.statsPart(messages[0]);c.statsPart(messages[1]);assert.throws(()=>c.statsPart(messages[2]),/Statistikquelle/);
 }
 const {c}=receiver(),messages=parts(id(1),id(11),7,c.sub);c.statsPart(messages[0]);
 const bad=messages[0].slice();bad[bad.length-1]^=1;assert.throws(()=>c.statsPart(bad),/Widersprüchliches/);
});
test('targeted volume wrappers keep target, operation ID and response decoder',async()=>{
 const c=Object.create(NodeConnection.prototype),calls=[];c.request=(...args)=>{calls.push(args);return Promise.resolve({})};
 await c.forNode(id(2)).growVolume(id(3),2097152n,id(4));
 const [opcode,body,operation,resultOpcode]=calls[0];assert.equal(opcode,OP.NODE_ADMIN);assert.equal(operation,id(4));assert.equal(resultOpcode,OP.VOLUME_GROW);
 const r=new Reader(body);assert.equal(r.id(),id(2));assert.equal(r.u16(),OP.VOLUME_GROW);const inner=new Reader(r.blob());assert.equal(inner.id(),id(3));assert.equal(inner.u64(),2097152n);inner.done();r.done();
});
test('targeted responses use inner decoding and preserve errors',async()=>{
 const c=Object.create(NodeConnection.prototype);c.pending=new Map;
 const operation=id(4),ack=new Writer().v(201).blob(new Writer().v(0).bytes()).id(operation).u8(2).bool(true).id(id(3)).bytes();
 const result=new Promise((resolve,reject)=>c.pending.set(operation,{op:OP.NODE_ADMIN,resultOpcode:OP.VOLUME_ADD,resolve,reject}));
 c.receive(envelope(OP.NODE_ADMIN,ack,operation,2).bytes);
 assert.equal((await result).volume_id,id(3));
 const failed=new Promise((resolve,reject)=>c.pending.set(operation,{op:OP.NODE_ADMIN,resultOpcode:OP.VOLUME_ADD,resolve,reject}));
 c.receive(envelope(OP.NODE_ADMIN,new Writer().v(507).blob(new Writer().v(0).bytes()).bytes(),operation,2).bytes);
 await assert.rejects(failed,e=>e.status===507&&e.operationID===operation);
});

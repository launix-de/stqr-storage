// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {ObjectClient} from './dashboard/object-client.js';
import {Reader, envelope} from './dashboard/protocol.js';

let input='';
for await (const chunk of process.stdin) input+=chunk;
const cfg=JSON.parse(input);
// Authenticate through the real dashboard session path once, rather than
// measuring bcrypt on every native object request (especially under -race).
const login=await fetch(cfg.url+'/api/session',{method:'POST',headers:{Authorization:'Basic '+Buffer.from(cfg.username+':'+cfg.password).toString('base64')}});
assert.equal(login.status,200);
const session=await login.json();
assert.equal(typeof session.csrf_token,'string');assert.ok(session.csrf_token.length);
const cookie=login.headers.get('set-cookie')?.split(';',1)[0];
assert.ok(cookie?.startsWith('stqr_session='));
const connection={forNode:()=>({request:async(op,body,id)=>{
 const message=envelope(op,body,id);
 const response=await fetch(cfg.url+'/api/message',{method:'POST',headers:{Cookie:cookie,'X-CSRF-Token':session.csrf_token},body:message.bytes});
 const r=new Reader(await response.arrayBuffer());
 assert.equal(r.u8(),1);assert.equal(r.u8(),2);assert.equal(r.u16(),op);assert.equal(r.id(),message.id);
 const status=Number(r.v());r.blob();
 if(status>=300){const e=Error('native request rejected');e.status=status;throw e}
 assert.equal(response.status,status);
 return r.take(r.b.length-r.at);
}})};
const objects=new ObjectClient(connection,cfg.bucket,cfg.node);
const started=performance.now();
let data=await objects.get(cfg.object);
assert.equal(data.length,8192);
for(let i=0;i<100;i++){
 const wrong=data.slice();wrong[0]^=1;
 const next=new Uint8Array(data.length).fill(i);
 await assert.rejects(objects.casRange(cfg.object,0,wrong,next,new Uint8Array()),e=>e.status===412);
 assert.deepEqual(await objects.get(cfg.object),data);
 await objects.casRange(cfg.object,0,data,next,new Uint8Array());
 data=await objects.get(cfg.object);
 assert.deepEqual(data,next);
 if((i+1)%25===0)console.log(`CAS rounds completed: ${i+1}/100 (${((performance.now()-started)/1000).toFixed(2)}s)`);
}
assert.equal((await objects.stat(cfg.object)).length,8192n);
const page=await objects.scan('',16);
assert.deepEqual(page.objects.map(v=>v.id),[cfg.object]);
console.log('JavaScript/native reclamation interoperability passed');

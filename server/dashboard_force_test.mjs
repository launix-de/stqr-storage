// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const moduleURL=source=>'data:text/javascript;base64,'+Buffer.from(source).toString('base64');
const codec=moduleURL(await readFile(new URL('./dashboard/bucket-codec.js',import.meta.url)));
const protocol=(await readFile(new URL('./dashboard/protocol.js',import.meta.url),'utf8')).replace("'./bucket-codec.js'",JSON.stringify(codec));
const {NodeConnection,Reader,OP}=await import(moduleURL(protocol));
const a='1'.repeat(32),b='2'.repeat(32),operation='3'.repeat(32),epoch=9007199254740993n;
let wire;
await NodeConnection.prototype.forceDisconnect.call({request(op,body,id){wire={op,body,id}}},[b,a,b],epoch,true,true,operation);
assert.equal(wire.op,0x2013);assert.equal(OP.FORCE_DISCONNECT,0x2013);assert.equal(wire.id,operation);
const r=new Reader(wire.body);assert.equal(r.v(),2n);assert.equal(r.id(),a);assert.equal(r.id(),b);
assert.equal(r.bool(),true);assert.equal(r.bool(),true);assert.equal(r.u64(),epoch);r.done();
const {setupForceDisconnect}=await import(moduleURL(await readFile(new URL('./dashboard/force-disconnect.js',import.meta.url))));
class Element{
 listeners=new Map;checked=false;disabled=false;open=false;textContent='';
 addEventListener(name,fn){this.listeners.set(name,fn)}
 async emit(name){return this.listeners.get(name)?.({preventDefault(){}})}
 showModal(){this.open=true}close(){this.open=false;this.emit('close')}
}
const elements=new Map;
globalThis.document={getElementById(id){if(!elements.has(id))elements.set(id,new Element);return elements.get(id)}};
const $=id=>document.getElementById(id);
let calls=[],fail=true,refreshed=0,view={member_nodes:[a,b],membership_epoch:String(epoch)};
const form=setupForceDisconnect({
 connection:()=>({async forceDisconnect(...args){calls.push(args);if(fail)throw Error('network lost');return {state:'fulfilled'}}}),
 config:()=>view,states:new Map([[b,{config:{config:{node:{name:'Storage B'}}}}]]),toast(){},refresh:async()=>refreshed++,newID:()=>operation,
});
form.open(b);assert.equal($('force-dialog').open,true);assert.equal($('force-submit').disabled,true);
await $('force-form').emit('submit');assert.equal(calls.length,0);
$('force-fenced').checked=true;await $('force-fenced').emit('change');assert.equal($('force-submit').disabled,true);
$('force-loss').checked=true;await $('force-loss').emit('change');assert.equal($('force-submit').disabled,false);
await $('force-form').emit('submit');assert.equal($('force-error').textContent,'network lost');
// A newer heartbeat must not silently rebind a retry to a different intent.
view={...view,membership_epoch:String(epoch+1n)};fail=false;
await $('force-form').emit('submit');assert.deepEqual(calls,[[[b],epoch,true,true,operation],[[b],epoch,true,true,operation]]);
assert.equal(refreshed,1);assert.equal($('force-dialog').open,false);
view={...view,member_nodes:[a]};form.open(a);assert.equal($('force-dialog').open,false);
console.log('FORCE canonical wire, confirmations, retry identity and exact epoch verified.');

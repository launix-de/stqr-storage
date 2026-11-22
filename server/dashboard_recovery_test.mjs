// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const moduleURL=source=>'data:text/javascript;base64,'+Buffer.from(source).toString('base64');
const codec=moduleURL(await readFile(new URL('./dashboard/bucket-codec.js',import.meta.url)));
const source=(await readFile(new URL('./dashboard/protocol.js',import.meta.url),'utf8')).replace("'./bucket-codec.js'",JSON.stringify(codec));
const {NodeConnection,Reader,Writer,OP,envelope}=await import(moduleURL(source));
const a='1'.repeat(32),b='2'.repeat(32),id='3'.repeat(32),epoch=9007199254740993n;
let wire;
const sink={request(op,body,id,resultOpcode){wire={op,body,id,resultOpcode}}};
await NodeConnection.prototype.beginNodeRecovery.call(sink,[b,a,b],epoch,id);
assert.equal(wire.op,OP.NODE_RECOVERY_BEGIN);assert.equal(wire.id,id);
let r=new Reader(wire.body);assert.equal(r.v(),2n);assert.equal(r.id(),a);assert.equal(r.id(),b);assert.equal(r.bool(),false);assert.equal(r.bool(),true);assert.equal(r.u64(),epoch);r.done();
await NodeConnection.prototype.commitRecoveredVolumes.call(sink,[b,a,b],epoch,id);
assert.equal(wire.op,OP.VOLUME_RECOVERY_COMMIT);r=new Reader(wire.body);assert.equal(r.u64(),epoch);assert.equal(r.v(),2n);assert.equal(r.id(),a);assert.equal(r.id(),b);r.done();
const scoped=NodeConnection.prototype.forNode.call(sink,b);
await scoped.stageRecoveryVolume('saved','/dev/mapper/saved',id);
r=new Reader(wire.body);assert.equal(wire.op,OP.NODE_ADMIN);assert.equal(r.id(),b);assert.equal(r.u16(),OP.VOLUME_RECOVERY_STAGE);const body=new Reader(r.blob());assert.equal(body.text(),'saved');assert.equal(body.text(),'/dev/mapper/saved');body.done();r.done();assert.equal(wire.resultOpcode,OP.VOLUME_RECOVERY_STAGE);
// Delegated responses retain the outer opcode but decode their inner result.
let result;
const connection={pending:new Map([[id,{op:OP.NODE_ADMIN,resultOpcode:OP.VOLUME_RECOVERY_STAGE,resolve:value=>result=value,reject:error=>{throw error}}]])};
const response=new Writer().v(201).blob(new Writer().v(0).bytes()).text(JSON.stringify({volume_id:a,state:'recovery'})).bytes();
NodeConnection.prototype.receive.call(connection,envelope(OP.NODE_ADMIN,response,id,2).bytes);
assert.equal(result.volume_id,a);assert.equal(result.state,'recovery');
console.log('Recovery fencing mode, exact epochs, canonical cohorts, delegated commands and response decoding verified.');

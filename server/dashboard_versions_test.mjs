// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {NodeConnection,Writer,OP,envelope} from './dashboard/protocol.js';
import {versionWarning,versionLabel} from './dashboard/node-versions.js';
const zero='0'.repeat(32);
test('new version telemetry uses an explicit native subscription and push',async()=>{
 const c=Object.create(NodeConnection.prototype);c.pending=new Map;
 const statuses={node:{profile:{version:'2.0'},outdated:true,incompatible:false,restricted:false}};
 let received;c.onVersions=v=>received=v;
 c.receive(envelope(OP.NODE_VERSIONS,new Writer().text(JSON.stringify(statuses)).bytes(),zero,3).bytes);
 assert.deepEqual(received,statuses);
 const result=new Promise((resolve,reject)=>c.pending.set(zero,{op:OP.NODE_VERSIONS,resolve,reject}));
 c.receive(envelope(OP.NODE_VERSIONS,new Writer().v(200).blob(new Writer().v(0).bytes()).text(JSON.stringify(statuses)).bytes(),zero,2).bytes);
 assert.deepEqual(await result,statuses);
 let request;c.request=(op,body)=>{request={op,body};return Promise.resolve(statuses)};
 assert.deepEqual(await c.subscribeVersions(),statuses);
 assert.equal(request.op,OP.NODE_VERSIONS);assert.deepEqual([...request.body],[1]);
 assert.throws(()=>c.receive(envelope(OP.NODE_VERSIONS,new Writer().text('{').bytes(),zero,3).bytes));
});
test('older, restricted and wire-incompatible nodes are all red; healthy unknown versions are neutral',()=>{
 for(const v of [{outdated:true},{restricted:true},{incompatible:true}])assert.equal(versionWarning(v),true);
 assert.equal(versionWarning(undefined),false);assert.equal(versionWarning({}),false);
 assert.equal(versionLabel({outdated:true}),'Veraltete Version');
 assert.equal(versionLabel({restricted:true,outdated:true}),'Teilweise inkompatibel');
 assert.equal(versionLabel({incompatible:true,restricted:true}),'Version inkompatibel');
});

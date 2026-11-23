// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {NodeConnection,Reader,OP} from './dashboard/protocol.js';
import {defaultBucketPolicy,parseBucketPage} from './dashboard/bucket-codec.js';
const id='01000000000000000000000000000000',group='02000000000000000000000000000000';
function connection(){const c=Object.create(NodeConnection.prototype);c.calls=[];c.request=(...args)=>{c.calls.push(args);return Promise.resolve({})};return c}
test('central PG API preserves uint64 storage budgets and revision',async()=>{
 const c=connection(),p=defaultBucketPolicy();p.read_acceleration.cache_bytes_budget='18446744073709551615';
 await c.setPlacementGroup(group,'9007199254740993','DB',{node_selection:'all',nodes:[],storage:p},'retry');
 const [op,body,retry]=c.calls[0],r=new Reader(body);assert.equal(op,OP.PG_SET);assert.equal(retry,'retry');assert.equal(r.u16(),2);assert.equal(r.id(),group);assert.equal(r.u64(),9007199254740993n);assert.equal(r.text(),'DB');
 const settings=parseBucketPage(r.text());assert.equal(BigInt(settings.storage.read_acceleration.cache_bytes_budget),18446744073709551615n);r.done();
});
test('bucket creation references a PG and encodes individual quota without storage overrides',async()=>{
 const c=connection(),b={id,name:'db',policy:defaultBucketPolicy()};b.policy.placement_group=group;b.policy.limits.absolute_quota_bytes='18446744073709551615';b.policy.limits.absolute_unlimited=false;
 await c.createBucket(b,'retry');const [op,body]=c.calls[0],r=new Reader(body);assert.equal(op,OP.BUCKET_CREATE_PG);assert.equal(r.u16(),1);assert.equal(r.id(),id);assert.equal(r.text(),'db');assert.equal(r.id(),group);assert.equal(r.u8(),0);assert.equal(r.u64(),0n);assert.equal(r.bool(),false);assert.equal(r.u64(),18446744073709551615n);assert.equal(r.u32(),540);r.done();
});
test('new volume list selections use a versioned node-local operation',async()=>{
 const c=connection();await c.addVolume({name:'data',source:0,path:'',class:'hdd',size:2097152n,placementGroups:[group]},'same');
 const [op,body]=c.calls[0],r=new Reader(body);assert.equal(op,OP.VOLUME_ADD_GROUPS);assert.equal(r.text(),'data');assert.equal(r.u8(),0);assert.equal(r.text(),'');assert.equal(r.u8(),0);assert.equal(r.u64(),2097152n);assert.equal(r.u16(),1);assert.equal(r.v(),1n);assert.equal(r.id(),group);r.done();
});

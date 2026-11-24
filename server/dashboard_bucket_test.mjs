// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {Writer,Reader,NodeConnection,OP} from './dashboard/protocol.js';
import {writeBucketPolicy,writeBucketQuota,parseBucketPage,parseQuota,defaultBucketPolicy} from './dashboard/bucket-codec.js';
const draft=()=>({id:'01000000000000000000000000000000',name:'example',state:'active',policy:defaultBucketPolicy()});
test('dashboard Policy bytes match native BucketInfo.MarshalBinary without permission suffix',()=>{
 assert.equal(Buffer.from(writeBucketPolicy(new Writer(),draft()).bytes()).toString('hex'),'010000000000000000000000000000000100000000000000076578616d706c65000000000001000000000000000000000000000000000000000000000000000100000000000000001c020000');
});
test('JSON bucket pages preserve uint64 values and leave quoted names intact',()=>{
 const raw='{"buckets":[{"name":"9007199254740993 and \\"quoted\\"","revision":9007199254740993,"policy":{"version":18446744073709551615,"limits":{"absolute_quota_bytes":18446744073709551614}}}],"next":"9007199254740993"}';
 const page=parseBucketPage(raw),b=page.buckets[0];assert.equal(b.name,'9007199254740993 and "quoted"');assert.equal(BigInt(b.revision),9007199254740993n);assert.equal(BigInt(b.policy.version),18446744073709551615n);assert.equal(BigInt(b.policy.limits.absolute_quota_bytes),18446744073709551614n);assert.equal(page.next,'9007199254740993');
});
test('quota parsing accepts zero, small objects and full unsigned 64-bit range',()=>{
 for(const [value,expected]of [['0',0n],['10B',10n],['1k',1024n],['50M',52428800n],['1.5G',1610612736n],['18446744073709551615',18446744073709551615n]])assert.equal(parseQuota(value),expected);
 for(const value of ['-1','5Z','18446744073709551616',''])assert.throws(()=>parseQuota(value));
});
test('quota changes encode exact bytes and expected policy version',()=>{
 const b=draft();b.policy.limits.absolute_unlimited=false;b.policy.limits.absolute_quota_bytes='9007199254740993';
 const r=new Reader(writeBucketQuota(new Writer(),b,'9007199254740995').bytes());assert.equal(r.id(),b.id);assert.equal(r.u8(),0);assert.equal(r.u64(),0n);assert.equal(r.bool(),false);assert.equal(r.u64(),9007199254740993n);assert.equal(r.u32(),540);assert.equal(r.u64(),9007199254740995n);r.done();
});
test('node wrappers use existing bucket commands, pagination and grants',async()=>{
 const calls=[],connection=Object.create(NodeConnection.prototype);connection.request=(...args)=>{calls.push(args);return Promise.resolve({})};
 await connection.listBuckets('previous',25);let r=new Reader(calls.pop()[1]);assert.equal(r.text(),'previous');assert.equal(r.u32(),25);r.done();
 const b=draft(),user='02000000000000000000000000000000';await connection.grantBucket(b.id,user,5,'request');const grant=calls.pop();assert.equal(grant[0],OP.BUCKET_GRANT);r=new Reader(grant[1]);assert.equal(r.id(),b.id);assert.equal(r.id(),user);assert.equal(r.u32(),5);r.done();
 await connection.setBucketPolicy(b,3,'same-request');const policy=calls.pop();assert.equal(policy[0],OP.BUCKET_SET_POLICY);assert.equal(policy[2],'same-request');assert.deepEqual(policy[1].slice(0,-8),writeBucketPolicy(new Writer(),b).bytes());assert.equal(new Reader(policy[1].slice(-8)).u64(),3n);
});
test('physical PG capacity requests explicitly encode their accounting unit',async()=>{
 const connection=Object.create(NodeConnection.prototype);let call;connection.request=(...args)=>{call=args;return Promise.resolve({})};
 const bucket='01000000000000000000000000000000',group='02000000000000000000000000000000';
 await connection.bucketQuotaCapacity(bucket,'hdd',5,540,group,'physical');assert.equal(call[0],OP.BUCKET_GROUP_CAPACITY);const r=new Reader(call[1]);assert.equal(r.u16(),2);assert.equal(r.id(),group);assert.equal(r.id(),bucket);assert.equal(r.u8(),0);assert.equal(r.u8(),5);assert.equal(r.u32(),540);assert.equal(r.u8(),1);r.done();
});
test('bucket copy sends native source and destination IDs with a stable operation ID',async()=>{
 const connection=Object.create(NodeConnection.prototype);let call;connection.request=(...args)=>{call=args;return Promise.resolve({})};
 const source='01000000000000000000000000000000',destination='02000000000000000000000000000000';
 await connection.cloneBucket(source,destination,'copy-request');
 assert.equal(call[0],OP.BUCKET_CLONE);assert.equal(call[2],'copy-request');
 const r=new Reader(call[1]);assert.equal(r.id(),source);assert.equal(r.id(),destination);r.done();
});

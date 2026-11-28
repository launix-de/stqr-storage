// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import test from 'node:test';
import {entityRate,nodeRate,bucketThroughput,bucketTotals,bucketRates,recordRateSnapshot,recordAccumulatorRates,sumRatePairs,userTrafficSummary,RateRefreshQueue} from './dashboard/throughput.js';
import {formatRate,formatRatePair} from './dashboard/numbers.js';
import {NodeConnection,OP,Reader,Writer,envelope} from './dashboard/protocol.js';

const pair=(last,average)=>({last,average});
function source(node='n',boot='a'){
 const entry={};
 recordRateSnapshot(entry,{node,boot,captured:4000000000n,io:{read:123456789n,write:0n,logicalRead:12000n,logicalWrite:18000n},volumes:[{id:'v'}],buckets:[{id:'b'},{id:'c'}]},1000);
 recordAccumulatorRates(entry,{schema:1,node,boot,captured_ns:'4000000000',half_life_ns:'604800000000000',io:{read:pair(2000,1000),write:pair(4000,2500)},user_io:{logicalRead:pair(3000,2000),logicalWrite:pair(6000,3000)},volumes:{v:{read:pair(2000,123.5),write:pair(0,0)}},buckets:{b:{logicalRead:pair(3000,1000),logicalWrite:pair(4000,2000)},c:{logicalRead:pair(1000,500),logicalWrite:pair(2000,750)}},users:{u:{logicalRead:pair(3000,1000),logicalWrite:pair(4000,2000)}}},1000);
 return entry;
}

test('rates use server completed frames, preserve fractions, and ignore cumulative delta guesses',()=>{
 const entry=source();
 assert.deepEqual(entityRate(entry,'volumes','v','read',1000),pair(2000,123.5));
 entry.snapshot.io.read=999999999999999n;
 assert.deepEqual(nodeRate(entry,'read',1000),pair(2000,1000));
 assert.equal(entityRate(entry,'volumes','missing','read',1000),null);
 assert.deepEqual(entityRate(entry,'volumes','v','write',1000),pair(0,0));
 assert.equal(nodeRate({received:1000,snapshot:entry.snapshot,previous:{boot:'a',captured:1000000000n,io:{read:0n}}},'read',1000),null);
 assert.equal(formatRate(123.5),'123,5 B/s');
 assert.equal(formatRatePair(pair(2048,1024)),'2 KiB/s · ø 1 KiB/s');
 assert.equal(formatRatePair(null),'— · ø —');
});

test('missing, stale and wrong-source frames remain unknown, including weekly mean',()=>{
 const entry=source();
 assert.equal(entityRate(entry,'volumes','v','read',11000),null);
 assert.equal(nodeRate({...entry,ratesReceived:-9000},'read',1000),null);
 assert.equal(nodeRate({...entry,rates:{...entry.rates,boot:'old'}},'read',1000),null);
 assert.equal(nodeRate({...entry,rates:{...entry.rates,node:'other'}},'read',1000),null);
 assert.equal(nodeRate({...entry,rates:{...entry.rates,io:{read:pair(Infinity,5)}}},'read',1000),null);
 assert.equal(nodeRate({...entry,rates:{...entry.rates,io:{read:pair(5,null)}}},'read',1000),null);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,boot:'old',captured_ns:'9000000000'},2000),false);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,node:'other',captured_ns:'9000000000'},2000),false);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,captured_ns:'nonsense'},2000),false);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,captured_ns:'3000000000'},2000),false);
 const old=entry.rates;
 recordRateSnapshot(entry,{...entry.snapshot,boot:'next',captured:5000000000n},2000);
 assert.equal(nodeRate(entry,'read',2000),null);
 assert.equal(recordAccumulatorRates(entry,old,2000),false);
});

test('repeated captures merge paged users but cannot indefinitely refresh stale measurements',()=>{
 const entry=source();
 recordRateSnapshot(entry,{...entry.snapshot,captured:9000000000n},9000);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,users:{second:{logicalRead:pair(7,3.5)}}},9000),true);
 assert.deepEqual(entityRate(entry,'users','second','logicalRead',9000),pair(7,3.5));
 assert.deepEqual(entityRate(entry,'users','u','logicalRead',9000),pair(3000,1000));
 assert.equal(entry.ratesReceived,1000);
 assert.equal(nodeRate(entry,'read',11000),null);
 assert.equal(recordAccumulatorRates(entry,{...entry.rates,captured_ns:'12000000000'},12000),true);
 assert.equal(entry.ratesReceived,12000);
});

test('disjoint user batches survive source ticks and expire independently',()=>{
 const entry=source();
 recordRateSnapshot(entry,{...entry.snapshot,captured:8000000000n},8000);
 recordAccumulatorRates(entry,{...entry.rates,captured_ns:'8000000000',users:{second:{logicalRead:pair(10,2.5)}}},8000);
 assert.deepEqual(entityRate(entry,'users','u','logicalRead',8000),pair(3000,1000));
 assert.deepEqual(entityRate(entry,'users','second','logicalRead',8000),pair(10,2.5));
 recordAccumulatorRates(entry,{...entry.rates,users:{u:{logicalRead:pair(5,4)}}},9000);
 assert.deepEqual(entityRate(entry,'users','u','logicalRead',9000),pair(5,4));
 recordAccumulatorRates(entry,{...entry.rates,users:{u:{logicalRead:pair(5,4)}}},10000);
 assert.equal(entry.userRateFrames.u.received,9000);
 recordRateSnapshot(entry,{...entry.snapshot,captured:19000000000n},19000);
 assert.equal(entityRate(entry,'users','u','logicalRead',19000),null);
 recordRateSnapshot(entry,{...entry.snapshot,boot:'next'},19000);
 assert.equal(entry.userRateFrames,undefined);
});

test('bucket rows and summaries aggregate both rate values and propagate missing nodes',()=>{
 const a=source('a'),b=source('b');
 assert.deepEqual(bucketRates([a,b],'b',1000),[pair(6000,2000),pair(8000,4000)]);
 assert.deepEqual(bucketTotals([a,b],1000),[pair(8000,3000),pair(12000,5500)]);
 const html=bucketThroughput([a,b],'b',1000);
 assert.match(html,/5,9 KiB\/s · ø 2 KiB\/s/);
 assert.match(html,/7,8 KiB\/s · ø 3,9 KiB\/s/);
 assert.match(bucketThroughput([a],'b',11000),/— · ø —/);
 assert.deepEqual(bucketTotals([a,{}],1000),[null,null]);
 assert.deepEqual(bucketTotals([],1000),[null,null]);
 assert.equal(sumRatePairs([pair(1,2),null]),null);
 assert.equal(sumRatePairs([]),null);
 assert.equal(sumRatePairs([pair(Number.MAX_VALUE,1),pair(Number.MAX_VALUE,1)]),null);
});

test('user summaries retain exact cumulative totals while summing server user rate pairs once per node',()=>{
 const a={node:'a',entry:source('a')},b={node:'b',entry:source('b')};
 assert.deepEqual(userTrafficSummary([a,b,a],1000),{read:24000n,write:36000n,readRate:pair(6000,4000),writeRate:pair(12000,6000)});
 assert.deepEqual(userTrafficSummary([a,{node:'missing'}],1000),{read:null,write:null,readRate:null,writeRate:null});
 assert.deepEqual(userTrafficSummary([a],11000),{read:null,write:null,readRate:null,writeRate:null});
 a.entry.snapshot.io.logicalRead=18446744073709551615n;
 assert.equal(userTrafficSummary([a],1000).read,18446744073709551615n);
 assert.deepEqual(entityRate(a.entry,'users','u','logicalWrite',1000),pair(4000,2000));
});

test('rate request bursts coalesce and source requests execute one at a time',async()=>{
 const calls=[],applied=[],deferred=[];
 const queue=new RateRefreshQueue(node=>{calls.push(node);return new Promise(resolve=>deferred.push(resolve))},(node,result)=>applied.push([node,result]));
 const now=Date.now();
 queue.request('a',now);queue.request('a',now);queue.request('b',now);queue.request('b',now);
 assert.deepEqual(calls,['a']);
 deferred.shift()('first');await new Promise(setImmediate);
 assert.deepEqual(calls,['a','b']);
 deferred.shift()('second');await new Promise(setImmediate);
 assert.deepEqual(applied,[['a','first'],['b','second']]);
 queue.request('a',now+10);assert.deepEqual(calls,['a','b']);
 queue.request('c',now);queue.stop();deferred.shift()('late');await new Promise(setImmediate);
 assert.deepEqual(applied,[['a','first'],['b','second']]);
});

test('rates use a bounded native admin request and JSON response',async()=>{
 const user='1'.repeat(32),node='2'.repeat(32),id='3'.repeat(32);let sent;
 const connection=Object.create(NodeConnection.prototype);
 connection.request=(op,body,operation,resultOpcode)=>{sent={op,body,operation,resultOpcode};return Promise.resolve()};
 await connection.forNode(node).getRates([user]);
 assert.equal(sent.op,OP.NODE_ADMIN);assert.equal(sent.resultOpcode,OP.STATS_RATES);
 const outer=new Reader(sent.body);assert.equal(outer.id(),node);assert.equal(outer.u16(),0x2040);
 const inner=new Reader(outer.blob());outer.done();assert.equal(inner.u16(),1);assert.equal(inner.v(),1n);assert.equal(inner.id(),user);inner.done();
 await assert.rejects(connection.getRates(Array(1001).fill(user)),/Zu viele Benutzer/);
 const decoded=new Promise((resolve,reject)=>connection.pending=new Map([[id,{op:OP.STATS_RATES,resolve,reject}]]));
 const payload=source(node).rates;
 connection.receive(envelope(OP.STATS_RATES,new Writer().v(200).blob(new Writer().v(0).bytes()).text(JSON.stringify(payload)).bytes(),id,2).bytes);
 assert.deepEqual(await decoded,payload);
});

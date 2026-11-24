// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {entityRate,bucketThroughput,bucketTotals,bucketRates} from './dashboard/throughput.js';
const entry={received:1000,snapshot:{boot:'a',captured:4000000000n,volumes:[{id:'v',io:{read:7000n}}],buckets:[{id:'b',io:{logicalRead:9000n,logicalWrite:15000n}}]},previous:{boot:'a',captured:1000000000n,volumes:[{id:'v',io:{read:1000n}}],buckets:[{id:'b',io:{logicalRead:0n,logicalWrite:3000n}}]}};
assert.equal(entityRate(entry,'volumes','v','read',1000),2000n);
assert.equal(entityRate(entry,'volumes','v','read',11000),null);
assert.equal(entityRate({...entry,previous:{...entry.previous,boot:'old'}},'volumes','v','read',1000),null);
assert.equal(entityRate(entry,'volumes','missing','read',1000),null);
assert.equal(entityRate({...entry,snapshot:{...entry.snapshot,volumes:[{id:'v',io:{read:0n}}]}},'volumes','v','read',1000),null);
const html=bucketThroughput([entry,entry],'b',1000);
assert.match(html,/5,9 KiB\/s/);assert.match(html,/7,8 KiB\/s/);
assert.match(bucketThroughput([entry],'b',11000),/—/);
console.log('Throughput deltas, cluster sums, restarts, stale and absent counters passed.');

const second={...entry,snapshot:{...entry.snapshot,buckets:[...entry.snapshot.buckets,{id:'c',io:{logicalRead:3000n,logicalWrite:6000n}}]},previous:{...entry.previous,buckets:[...entry.previous.buckets,{id:'c',io:{logicalRead:0n,logicalWrite:0n}}]}};
const totals=bucketTotals([second],1000),b=bucketRates([second],'b',1000),c=bucketRates([second],'c',1000);
assert.deepEqual(totals,[b[0]+c[0],b[1]+c[1]]);
const {recordRateSnapshot}=await import('./dashboard/throughput.js');
const burst={};
for(let i=0;i<=300;i++)recordRateSnapshot(burst,{boot:'burst',captured:BigInt(i)*10000000n,io:{write:BigInt(Math.floor(i/30))*3000n},volumes:[],buckets:[]},i*10);
assert.equal((await import('./dashboard/throughput.js')).nodeRate(burst,'write',3000),10000n);
assert(burst.rateSamples.length<=32);
recordRateSnapshot(burst,{boot:'next',captured:4000000000n,io:{write:0n},volumes:[],buckets:[]},4000);
assert.equal(burst.previous,null);

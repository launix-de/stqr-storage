// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {quotaUsageBase} from './dashboard/buckets.js';
import {userTrafficSummary} from './dashboard/throughput.js';
import {currentSnapshots,seedMembers} from './dashboard/membership.js';
import {parseBucketPage} from './dashboard/bucket-codec.js';
import {defaultBucketPolicy} from './dashboard/bucket-codec.js';
const escape=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');
test('unlimited buckets show absolute physical occupancy without a fake percentage',()=>{
 const b={name:'ubuntu',policy:defaultBucketPolicy(),used_bytes:'8192',occupied_physical_bytes:'12288'};
 const html=quotaUsageBase(b,escape);
 assert.match(html,/12 KiB/);assert.match(html,/Unbegrenzt/);assert.match(html,/Physisch belegt/);assert.match(html,/8192|8 KiB/);
 assert.doesNotMatch(html,/progressbar|data-percent|aria-valuenow/);
 const cow=quotaUsageBase({...b,used_bytes:null,usage_error:'COW <unknown>'},escape);
 assert.match(cow,/12 KiB/);assert.match(cow,/logische Belegung nicht verfügbar/);assert.match(cow,/&lt;unknown>/);
 const empty=quotaUsageBase({...b,used_bytes:'0',occupied_physical_bytes:'0'},escape);
 assert.match(empty,/0B/);assert.doesNotMatch(empty,/Belegung nicht verfügbar/);
 const missing=quotaUsageBase({...b,used_bytes:null,occupied_physical_bytes:null},escape);
 assert.match(missing,/Belegung nicht verfügbar/);
});
test('user summary sums full node totals once independently of paged user rows',()=>{
 const entry=(node,read,write)=>({node,entry:{received:1000,snapshot:{boot:node,captured:4000000000n,users:[{id:'only-visible-user',io:{logicalRead:1n,logicalWrite:1n}}],io:{logicalRead:read,logicalWrite:write}},previous:{boot:node,captured:1000000000n,io:{logicalRead:0n,logicalWrite:0n}}}});
 const a=entry('a',9000n,12000n),b=entry('b',3000n,6000n);
 assert.deepEqual(userTrafficSummary([a,b,a],1000),{read:12000n,write:18000n,readRate:4000n,writeRate:6000n});
 assert.deepEqual(userTrafficSummary([a,{node:'missing'}],1000),{read:null,write:null,readRate:null,writeRate:null});
 assert.deepEqual(userTrafficSummary([a],11000),{read:null,write:null,readRate:null,writeRate:null});
 const restarted=entry('a',100n,200n);restarted.entry.previous.boot='old';
 assert.deepEqual(userTrafficSummary([restarted],1000),{read:100n,write:200n,readRate:null,writeRate:null});
 assert.deepEqual(userTrafficSummary([],1000),{read:null,write:null,readRate:null,writeRate:null});
});

test('user totals retain an unsampled current member instead of shrinking the sum',()=>{
 const view={member_nodes:['a','b'],membership_epoch:'1',config:{generation:1}};
 const sample={received:1000,snapshot:{node:'a',boot:'a',captured:2000000000n,io:{logicalRead:100n,logicalWrite:200n}},previous:{boot:'a',captured:1000000000n,io:{logicalRead:0n,logicalWrite:0n}}};
 const states=new Map([['a',sample],['departed',{snapshot:{node:'departed'}}]]);
 seedMembers(view,states);
 const sources=currentSnapshots(view,states,true).map(entry=>({node:entry.snapshot.node,entry:entry.placeholder?null:entry}));
 assert.deepEqual(userTrafficSummary(sources,1000),{read:null,write:null,readRate:null,writeRate:null});
 const fresh={received:1000,snapshot:{node:'b',boot:'b',captured:2000000000n,io:{logicalRead:300n,logicalWrite:400n}},previous:{boot:'b',captured:1000000000n,io:{logicalRead:0n,logicalWrite:0n}}};
 states.set('b',fresh);
 assert.deepEqual(userTrafficSummary(currentSnapshots(view,states,true).map(entry=>({node:entry.snapshot.node,entry})),1000),{read:400n,write:600n,readRate:400n,writeRate:600n});
});
test('new counters keep uint64 precision and old/reset samples stay unknown',()=>{
 const n='18446744073709551615';
 const page=parseBucketPage('{"occupied_physical_bytes":'+n+',"read_bytes_total":"'+n+'","write_bytes_total":"'+n+'"}');
 assert.equal(page.occupied_physical_bytes,n);
 const entry={received:1000,snapshot:{boot:'boot',captured:2000000000n,io:{logicalRead:BigInt(page.read_bytes_total),logicalWrite:BigInt(page.write_bytes_total)}},previous:{boot:'boot',captured:1000000000n,io:{logicalRead:BigInt(n),logicalWrite:BigInt(n)}}};
 assert.equal(userTrafficSummary([{node:'a',entry}],1000).read,18446744073709551615n);
 const wrapped={...entry,snapshot:{...entry.snapshot,io:{logicalRead:0n,logicalWrite:0n}}};
 assert.deepEqual(userTrafficSummary([{node:'a',entry:wrapped}],1000),{read:0n,write:0n,readRate:null,writeRate:null});
 assert.deepEqual(userTrafficSummary([{node:'old',entry:{received:1000,snapshot:{boot:'old',captured:2000000000n,users:[]}}}],1000),{read:null,write:null,readRate:null,writeRate:null});
});

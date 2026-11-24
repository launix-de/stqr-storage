// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {entityRate,bucketThroughput} from './dashboard/throughput.js';
const entry={received:1000,snapshot:{boot:'a',captured:4000000000n,volumes:[{id:'v',io:{read:7000n}}],buckets:[{id:'b',io:{logicalRead:9000n,logicalWrite:15000n}}]},previous:{boot:'a',captured:1000000000n,volumes:[{id:'v',io:{read:1000n}}],buckets:[{id:'b',io:{logicalRead:0n,logicalWrite:3000n}}]}};
assert.equal(entityRate(entry,'volumes','v','read',1000),2000n);
assert.equal(entityRate(entry,'volumes','v','read',11000),null);
assert.equal(entityRate({...entry,previous:{...entry.previous,boot:'old'}},'volumes','v','read',1000),null);
assert.equal(entityRate(entry,'volumes','missing','read',1000),null);
assert.equal(entityRate({...entry,snapshot:{...entry.snapshot,volumes:[{id:'v',io:{read:0n}}]}},'volumes','v','read',1000),null);
const html=bucketThroughput([entry,entry],'b',1000);
assert.match(html,/5\.86K\/s/);assert.match(html,/7\.813K\/s/);
assert.match(bucketThroughput([entry],'b',11000),/—/);
console.log('Throughput deltas, cluster sums, restarts, stale and absent counters passed.');

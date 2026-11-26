// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityKnown,capacityModel} from './dashboard/capacity.js';
import {currentSnapshots} from './dashboard/membership.js';

test('capacity meter partitions occupied and protected free bytes with exact byte ticks',()=>{
 const m=capacityModel({capacity:1024n,allocated:256n,reserved:128n,admission:640n});
 assert.equal(m.used,256n);assert.equal(m.free,768n);assert.equal(m.usable,640n);
 assert.equal(m.usedPercent,25);assert.equal(m.reservedPercent,12.5);
 assert.deepEqual(m.ticks,[0n,256n,512n,768n,1024n]);
});
test('empty/unknown and inconsistent capacity never invent negative/free or overfilled segments',()=>{
 assert.deepEqual(capacityModel({}).ticks,[0n,0n,0n,0n,0n]);
 assert.equal(capacityModel({capacity:0n}).usedPercent,0);
 const m=capacityModel({capacity:100n,allocated:125n,reserved:20n});
 assert.equal(m.used,125n);assert.equal(m.free,0n);assert.equal(m.usedPercent,100);assert.equal(m.reservedPercent,0);
 const reserved=capacityModel({capacity:100n,allocated:20n,reserved:120n});assert.equal(reserved.reservedPercent,80);
});
test('scale remains exact above Number integer precision',()=>{
 const capacity=2n**62n,m=capacityModel({capacity,allocated:capacity/4n,reserved:0n,admission:capacity*3n/4n});
 assert.equal(m.usedPercent,25);assert.equal(m.ticks[4],capacity);assert.equal(m.ticks[2],capacity/2n);
});
test('global totals use all reported member volumes, excluding departed and joining capacity',()=>{
 const volume=(capacity,allocated)=>({capacity:BigInt(capacity),allocated:BigInt(allocated),admission:0n,reserved:0n});
 const states=new Map([
 ['a',{snapshot:{volumes:[volume(100,10),volume(200,20)]}}],
 ['b',{snapshot:{volumes:[volume(400,40)]}}],
 ['old',{snapshot:{volumes:[volume(900,900)]}}],
 ['joining',{snapshot:{state:4,volumes:[volume(800,800)]}}],
 ['no-report',{placeholder:true,snapshot:{volumes:[]}}]
 ]);
 const entries=currentSnapshots({member_nodes:['a','b','no-report'],excluded_nodes:['old']},states);
 const totals=entries.flatMap(e=>e.snapshot.volumes).reduce((a,v)=>({capacity:a.capacity+v.capacity,allocated:a.allocated+v.allocated}),{capacity:0n,allocated:0n});
 assert.equal(capacityModel(totals).capacity,700n);assert.equal(capacityModel(totals).used,70n);
 // A table search is not an input: global totals must not change with visible
 // rows, collapsed nodes, pagination or the selected dashboard page.
 assert.equal(entries.length,2);
});
test('a missing current-member report leaves global capacity unknown, not a partial total',()=>{
 const states=new Map([
 ['a',{snapshot:{volumes:[{capacity:100n}]}}],
 ['b',{placeholder:true,snapshot:{volumes:[]}}],
 ['retired',{snapshot:{volumes:[{capacity:999n}]}}]
 ]),view={member_nodes:['a','b']};
 assert.equal(capacityKnown(currentSnapshots(view,states),2),false);
 states.set('b',{snapshot:{volumes:[]}});
 assert.equal(capacityKnown(currentSnapshots(view,states),2),true,'a reported empty member is known');
 assert.equal(capacityKnown([],2),false);
 assert.equal(capacityKnown(currentSnapshots({},states),states.size),true,'legacy report-only snapshots remain supported');
});

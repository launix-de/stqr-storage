// SPDX-License-Identifier: GPL-3.0-only
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('./dashboard/membership.js',import.meta.url));
const {memberIDs,nodeMembership,currentSnapshots,seedMembers}=await import('data:text/javascript;base64,'+source.toString('base64'));
const view={member_nodes:['a','b'],excluded_nodes:['x'],membership_epoch:'9007199254740993',config:{generation:1}};
const states=new Map([
 ['a',{snapshot:{state:0,volumes:[{capacity:10n}]}}],
 ['x',{snapshot:{state:0,volumes:[{capacity:1000n}]}}],
 ['old',{snapshot:{state:1,volumes:[{capacity:2000n}]}}],
 ['joining',{snapshot:{state:4,volumes:[{capacity:3000n}]}}],
]);
seedMembers(view,states);
assert.equal(memberIDs(view).size,2);
assert.equal(states.get('b').placeholder,true);
assert.equal(states.get('b').snapshot.membership,9007199254740993n);
assert.equal(nodeMembership(view,'b',states.get('b').snapshot),'member');
assert.equal(nodeMembership(view,'x',states.get('x').snapshot),'excluded');
assert.equal(nodeMembership(view,'old',states.get('old').snapshot),'departed');
assert.equal(nodeMembership(view,'joining',states.get('joining').snapshot),'joining');
assert.equal(currentSnapshots(view,states).flatMap(e=>e.snapshot.volumes).reduce((n,v)=>n+v.capacity,0n),10n);
assert.equal(states.get('x').snapshot.volumes[0].capacity,1000n,'historical snapshots must remain inspectable');
seedMembers(view,states);
assert.equal(states.get('a').placeholder,undefined,'a real report must not be replaced by a placeholder');
assert.equal(currentSnapshots({},states).length,4,'legacy transport dashboards retain their report-only behavior');

const includingUnknown=currentSnapshots(view,states,true);
assert.equal(includingUnknown.length,2,'current members without a first sample must remain in user totals');
assert.equal(includingUnknown.filter(e=>e.placeholder).length,1);

console.log('Committed members, offline placeholders, exact epochs and retired capacity verified.');

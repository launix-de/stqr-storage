// SPDX-License-Identifier: GPL-3.0-only
// Membership is authority; a missing heartbeat never removes a node. Keep old
// snapshots for inspection, but exclude departed capacity from cluster totals.
export function memberIDs(view){return Array.isArray(view?.member_nodes)?new Set(view.member_nodes):null}
export function nodeMembership(view,id,snapshot){
 const members=memberIDs(view);
 if(!members||members.has(id))return 'member';
 if(view.excluded_nodes?.includes(id))return 'excluded';
 return snapshot?.state===4?'joining':'departed';
}
export function currentSnapshots(view,states){
 return [...states.entries()].filter(([id,e])=>e.snapshot&&!e.placeholder&&nodeMembership(view,id,e.snapshot)==='member').map(([,e])=>e);
}
export function seedMembers(view,states){
 const members=memberIDs(view);if(!members)return;
 for(const id of members){
  if(states.get(id)?.snapshot)continue;
  const entry=states.get(id)??{};
  entry.placeholder=true;entry.received=0;
  entry.snapshot={node:id,boot:'0'.repeat(32),sequence:0n,captured:0n,membership:BigInt(view.membership_epoch),generation:BigInt(view.config?.generation??0),state:0,needRebuild:false,reason:0n,volumes:[],io:{read:0n,write:0n}};
  states.set(id,entry);
 }
}

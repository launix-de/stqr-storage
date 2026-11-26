// SPDX-License-Identifier: GPL-3.0-only
import {formatRate} from './numbers.js';
// Missing/stale snapshots, restarts and reset counters are unknown, not zero.
export function entityRate(entry, collection, id, key, now=Date.now()) {
 const s=entry?.snapshot,p=entry?.previous;
 if(!s||!p||s.boot!==p.boot||now-entry.received>=10000)return null;
 const elapsed=Number(s.captured-p.captured)/1e9;
 const current=s[collection]?.find(v=>v.id===id)?.io?.[key];
 const prior=p[collection]?.find(v=>v.id===id)?.io?.[key];
 if(elapsed<=0||current==null||prior==null||current<prior)return null;
 return BigInt(Math.round(Number(current-prior)/elapsed));
}
export function nodeRate(entry,key,now=Date.now()){
 const s=entry?.snapshot,p=entry?.previous;
 if(!s||!p||s.boot!==p.boot||now-entry.received>=10000||s.captured<=p.captured||s.io[key]==null||p.io[key]==null||s.io[key]<p.io[key])return null;
 return BigInt(Math.round(Number(s.io[key]-p.io[key])/(Number(s.captured-p.captured)/1e9)));
}
export function throughputHTML(read,write,logical=false){
 const text=formatRate;
 return `<div class="io-rates" title="${logical?'Logische Nutzdaten · alle meldenden Nodes':'Speicherzugriffe · einschließlich RAM/mmap, Metadaten und Migration · kein Datenträger-I/O'}"><span>Lesen <strong>${text(read)}</strong></span><span>Schreiben <strong>${text(write)}</strong></span></div>`;
}
export function bucketRates(entries,id,now=Date.now()){
 return ['logicalRead','logicalWrite'].map(key=>{
  let total=0n,known=false;
  for(const entry of entries){const value=entityRate(entry,'buckets',id,key,now);if(value==null)return null;total+=value;known=true}
  return known?total:null;
 });
 }
export function bucketThroughput(entries,id,now=Date.now()){
 return throughputHTML(...bucketRates(entries,id,now),true);
}

// Summary cards and bucket rows must use the same logical counter family.
export function bucketTotals(entries,now=Date.now()){
 const ids=new Set(entries.flatMap(e=>(e.snapshot.buckets??[]).map(b=>b.id)));
 if(!ids.size)return [null,null];
 const total=[0n,0n];
 for(const id of ids){const rates=bucketRates(entries,id,now);for(let i=0;i<2;i++){if(total[i]!=null)total[i]=rates[i]==null?null:total[i]+rates[i]}}
 return total;
}

// Bursty event snapshots are not independent throughput windows. Keep a bounded
// three-second baseline rather than dividing every tiny no-payload update.
export function recordRateSnapshot(entry,snapshot,now=Date.now()){
 if(entry.snapshot?.boot!==snapshot.boot)entry.rateSamples=[];
 const samples=entry.rateSamples??=[];
 const last=samples.at(-1);
 if(!last||snapshot.captured-last.captured>=250000000n)samples.push(snapshot);
 const cutoff=snapshot.captured-3000000000n;
 while(samples.length>1&&samples[1].captured<=cutoff)samples.shift();
 while(samples.length>32)samples.shift();
 const baseline=samples[0];
 entry.previous=baseline&&snapshot.captured-baseline.captured>=1000000000n?baseline:null;
 entry.snapshot=snapshot;entry.received=now;entry.placeholder=false;
 return entry;
}

// Each page carries node-wide counters for all current user accounts, independent
// of page size/search. A node contributes once, even if callers supply duplicates.
// Missing or stale nodes remain unknown instead of silently shrinking the sum.
export function userTrafficSummary(entries,now=Date.now()){
 let read=0n,write=0n,readRate=0n,writeRate=0n;const seen=new Set();
 for(const {node,entry}of entries){
  if(seen.has(node))continue;seen.add(node);
  const s=entry?.snapshot;
  if(!s?.io||now-entry.received>=10000||s.io.logicalRead==null||s.io.logicalWrite==null){read=null;write=null}else{
   if(read!=null)read+=s.io.logicalRead;if(write!=null)write+=s.io.logicalWrite;
  }
  const r=s?.io?.logicalRead==null?null:nodeRate(entry,'logicalRead',now),w=s?.io?.logicalWrite==null?null:nodeRate(entry,'logicalWrite',now);
  readRate=readRate==null||r==null?null:readRate+r;writeRate=writeRate==null||w==null?null:writeRate+w;
 }
 if(!seen.size)return {read:null,write:null,readRate:null,writeRate:null};
 return {read,write,readRate,writeRate};
}

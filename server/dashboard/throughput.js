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
 if(!s||!p||s.boot!==p.boot||now-entry.received>=10000||s.captured<=p.captured||s.io[key]<p.io[key])return null;
 return BigInt(Math.round(Number(s.io[key]-p.io[key])/(Number(s.captured-p.captured)/1e9)));
}
export function throughputHTML(read,write,logical=false){
 const text=formatRate;
 return `<div class="io-rates" title="${logical?'Logische Nutzdaten · alle meldenden Nodes':'Physische Bytes · einschließlich Metadaten und Migration'}"><span>Lesen <strong>${text(read)}</strong></span><span>Schreiben <strong>${text(write)}</strong></span></div>`;
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

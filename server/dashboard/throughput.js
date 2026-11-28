// SPDX-License-Identifier: GPL-3.0-only
import {formatRatePair} from './numbers.js';

const staleAfter=10000;
function validPair(value){return value!=null&&['last','average'].every(key=>typeof value[key]==='number'&&Number.isFinite(value[key])&&value[key]>=0)}
function freshRates(entry,now){
 const s=entry?.snapshot,r=entry?.rates;
 return s&&r&&s.boot===r.boot&&(!s.node||s.node===r.node)&&now-entry.received<staleAfter&&now-entry.ratesReceived<staleAfter;
}
// Rates come exclusively from completed server accumulator frames. A browser
// reconnect never reconstructs a weekly mean from its own short observation.
export function entityRate(entry,collection,id,key,now=Date.now()){
 if(collection==='users'){
  const frame=entry?.userRateFrames?.[id],s=entry?.snapshot;
  if(!frame||!s||frame.boot!==s.boot||frame.node!==s.node||now-entry.received>=staleAfter||now-frame.received>=staleAfter)return null;
  const value=frame.io?.[key];return validPair(value)?value:null;
 }
 if(!freshRates(entry,now))return null;
 const value=entry.rates[collection]?.[id]?.[key];
 return validPair(value)?value:null;
}
export function nodeRate(entry,key,now=Date.now(),collection='io'){
 if(!freshRates(entry,now))return null;
 const value=entry.rates[collection]?.[key];
 return validPair(value)?value:null;
}
export function sumRatePairs(values){
 if(!values.length||values.some(value=>!validPair(value)))return null;
 const result=values.reduce((sum,value)=>({last:sum.last+value.last,average:sum.average+value.average}),{last:0,average:0});
 return validPair(result)?result:null;
}
export function throughputHTML(read,write,logical=false){
 return `<div class="io-rates" title="${logical?'Logische Nutzdaten · alle meldenden Nodes':'Speicherzugriffe · einschließlich RAM/mmap, Metadaten und Migration · kein Datenträger-I/O'} · letztes abgeschlossenes Messintervall, danach ø mit einer Woche Halbwertszeit"><span>Lesen <strong>${formatRatePair(read)}</strong></span><span>Schreiben <strong>${formatRatePair(write)}</strong></span></div>`;
}
export function bucketRates(entries,id,now=Date.now()){
 return ['logicalRead','logicalWrite'].map(key=>sumRatePairs(entries.map(entry=>entityRate(entry,'buckets',id,key,now))));
}
export function bucketThroughput(entries,id,now=Date.now()){
 return throughputHTML(...bucketRates(entries,id,now),true);
}
// Summary cards and bucket rows use the same logical accumulator family.
export function bucketTotals(entries,now=Date.now()){
 const ids=new Set(entries.flatMap(e=>(e.snapshot?.buckets??[]).map(b=>b.id)));
 return [0,1].map(index=>sumRatePairs([...ids].map(id=>bucketRates(entries,id,now)[index])));
}

// Cumulative snapshots retain their role for totals, capacity and membership;
// they are not measurement intervals and cannot produce a rate on their own.
export function recordRateSnapshot(entry,snapshot,now=Date.now()){
 if(entry.snapshot?.boot!==snapshot.boot){delete entry.rates;delete entry.ratesReceived;delete entry.userRateFrames}
 entry.snapshot=snapshot;entry.received=now;entry.placeholder=false;
 return entry;
}
export function recordAccumulatorRates(entry,rates,now=Date.now()){
 if(rates?.schema!==1||rates.boot!==entry.snapshot?.boot||entry.snapshot?.node&&entry.snapshot.node!==rates.node)return false;
 let captured,previous;
 try{captured=BigInt(rates.captured_ns);previous=entry.rates?BigInt(entry.rates.captured_ns):0n}catch{return false}
 if(captured<=0n||captured<previous)return false;
 const users=entry.userRateFrames??={};
 for(const [id,io]of Object.entries(rates.users??{})){
  const prior=users[id];
  if(!prior||captured>prior.captured)users[id]={io,captured,node:rates.node,boot:rates.boot,received:now};
 }
 if(captured===previous){
  // Different admin pages can request disjoint users from the same completed
  // frame. Merge that frame's rows without pretending a new frame arrived.
  entry.rates={...entry.rates,users:{...entry.rates.users,...rates.users}};return true;
 }
 entry.rates=rates;entry.ratesReceived=now;
 return true;
}

// Serialize rate requests across nodes and coalesce event bursts. The server
// ticks independently; repeatedly fetching an unchanged frame cannot refresh it.
export class RateRefreshQueue{
 constructor(fetch,apply,onError=()=>{}){this.fetch=fetch;this.apply=apply;this.onError=onError;this.pending=new Set;this.attempted=new Map;this.running=false;this.stopped=false}
 request(node,now=Date.now()){
  if(this.stopped||this.pending.has(node)||now-(this.attempted.get(node)??-Infinity)<2500)return;
  this.pending.add(node);this.drain();
 }
 async drain(){
  if(this.running)return;this.running=true;
  try{while(this.pending.size&&!this.stopped){
   const node=this.pending.values().next().value;this.pending.delete(node);this.attempted.set(node,Date.now());
   try{const rates=await this.fetch(node);if(!this.stopped)this.apply(node,rates)}catch(error){if(!this.stopped)this.onError(node,error)}
  }}finally{this.running=false}
 }
 stop(){this.stopped=true;this.pending.clear();this.attempted.clear()}
}

// Pages carry node-wide cumulative totals independent of search/page size.
// A source contributes once and missing sources keep the cluster sum unknown.
export function userTrafficSummary(entries,now=Date.now()){
 let read=0n,write=0n;const seen=new Set,reads=[],writes=[];
 for(const {node,entry}of entries){
  if(seen.has(node))continue;seen.add(node);
  const s=entry?.snapshot;
  if(!s?.io||now-entry.received>=staleAfter||s.io.logicalRead==null||s.io.logicalWrite==null){read=null;write=null}else{
   if(read!=null)read+=s.io.logicalRead;if(write!=null)write+=s.io.logicalWrite;
  }
  reads.push(nodeRate(entry,'logicalRead',now,'user_io'));writes.push(nodeRate(entry,'logicalWrite',now,'user_io'));
 }
 if(!seen.size)return {read:null,write:null,readRate:null,writeRate:null};
 return {read,write,readRate:sumRatePairs(reads),writeRate:sumRatePairs(writes)};
}

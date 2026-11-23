// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {EndlessList} from './dashboard/endless.js';
globalThis.document={createElement:()=>({setAttribute(){},append(){},textContent:'',getBoundingClientRect:()=>({top:10000,bottom:10100})})};
globalThis.IntersectionObserver=class{constructor(callback){this.callback=callback}observe(){}};
globalThis.requestAnimationFrame=fn=>queueMicrotask(fn);globalThis.innerHeight=800;
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});return {promise,resolve,reject}}
function pager(fetchPage,extra={}){return new EndlessList({container:{append(){}},fetchPage,entries:p=>p.items,onUpdate(){},...extra})}
test('endless list fetches only the requested cursor and deduplicates observer events',async()=>{
 const calls=[],second=deferred();const list=pager((after,limit)=>{calls.push([after,limit]);return calls.length===1?Promise.resolve({items:[{id:'1',name:'a'}],next:'a',total:2}):second.promise});
 await list.reset();assert.equal(calls.length,1);list.observer.callback([{isIntersecting:true}]);list.observer.callback([{isIntersecting:true}]);assert.equal(calls.length,2);assert.deepEqual(calls[1],['a',50]);second.resolve({items:[{id:'2',name:'b'}],next:'',total:2});await new Promise(resolve=>setImmediate(resolve));assert.deepEqual(list.items.map(e=>e.id),['1','2']);
});
test('changing search ignores a late response from the previous query',async()=>{
 const old=deferred(),newer=deferred();const list=pager((after,limit,query)=>query==='old'?old.promise:newer.promise);
 const first=list.reset('old'),next=list.reset('new');newer.resolve({items:[{id:'new',name:'new'}],next:'',total:1});await next;old.resolve({items:[{id:'old',name:'old'}],next:'later',total:99});await first;assert.deepEqual(list.items.map(e=>e.id),['new']);assert.equal(list.more,false);
});
test('hidden views do not drain pages and a failed page can retry its cursor',async()=>{
 let visible=false,calls=0;const list=pager(async()=>{calls++;if(calls===2)throw Error('offline');return {items:[{id:String(calls),name:'entry'}],next:calls===1?'cursor':'',total:2}},{visible:()=>visible});
 await list.reset();await list.loadMore();assert.equal(calls,1);visible=true;await list.loadMore();assert.equal(list.error,true);assert.equal(list.cursor,'cursor');await list.loadMore();assert.equal(list.error,false);assert.equal(list.more,false);assert.equal(list.items.length,2);
});

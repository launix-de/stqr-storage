// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {MIN_VOLUME_SIZE,parseSize,formatSize,sizeSteps,sizeLimit} from './dashboard/size.js';

test('bare byte input and case-insensitive units describe the same size',()=>{
 for(const input of ['50M','50m','50MiB','52428800','52428800B'])assert.equal(parseSize(input),52428800n);
 assert.equal(formatSize(parseSize('52428800')),'50M');
});
test('slider uses discrete decade steps and changes units at the boundary',()=>{
 const steps=sizeSteps(parseSize('4P'));
 for(const [left,right]of [['9M','10M'],['10M','20M'],['90M','100M'],['100M','200M'],['900M','1G'],['900G','1T'],['900T','1P']])assert.equal(steps[steps.indexOf(parseSize(left))+1],parseSize(right));
});
test('free-space endpoint is fixed and exact, including a custom typed size',()=>{
 const limit=sizeLimit(48316579841n),custom=parseSize('125M');
 const steps=sizeSteps(limit,custom);
 assert.equal(steps[0],MIN_VOLUME_SIZE);assert.equal(steps.at(-1),limit);assert(steps.includes(custom));
 assert(steps.every((n,i)=>i===0||steps[i-1]<n));assert(steps.every(n=>n<=limit));
 assert.equal(parseSize(formatSize(limit)),limit);assert.equal(sizeSteps(limit,limit+4096n).at(-1),limit);
});
test('50M has a logarithmic position rather than sticking to the left',()=>{
 const steps=sizeSteps(parseSize('45G'));assert(steps.indexOf(parseSize('50M'))>steps.length/3);
});
test('small capacity and invalid size input are rejected',()=>{
 assert.deepEqual(sizeSteps(MIN_VOLUME_SIZE-1n),[]);
 for(const input of ['50','0K','-1M','NaN','10Z',''])assert.throws(()=>parseSize(input));
});
test('decimal sizes and all supported suffixes round-trip exactly',()=>{
 for(const input of ['256K','1M','1.2G','2T','2P']){const n=parseSize(input);assert.equal(parseSize(formatSize(n)),n)}
});
test('growth slider starts at current size and ends at current plus free space',()=>{
 const old=parseSize('125M'),free=parseSize('50M'),steps=sizeSteps(old+free,undefined,old);
 assert.equal(steps[0],old);assert.equal(steps.at(-1),old+free);
 assert(steps.every(n=>n>=old&&n<=old+free));
 assert.deepEqual(sizeSteps(old,undefined,old),[old]);
});

// SPDX-License-Identifier: GPL-3.0-only
import test from 'node:test';
import assert from 'node:assert/strict';
import {Writer,Reader,parseBlockDevicePage} from './dashboard/protocol.js';
import {readInfo} from './dashboard/object-client.js';
import {parseSize,sizeSteps} from './dashboard/size.js';

test('disk metadata uses exact u64 sizes above JavaScript number precision',()=>{
 const size=9007199254741504n,id='12'.repeat(16),owner='34'.repeat(16);
 const metadata=new Writer().raw(new TextEncoder().encode('STNBD001')).u16(1).u32(1048576).u64(size).id(id).text('disk <archive>').text('STQR').text('Sparse').text('serial').bytes();
 const page=new Writer().u16(1).u32(1).blob(metadata).text('00'.repeat(32)).bytes();
 const result=parseBlockDevicePage(new Reader(new Writer().bool(true).bool(false).id(owner).blob(page).bytes()));
 assert.equal(result.devices[0].size,size);assert.equal(result.devices[0].name,'disk <archive>');assert.equal(result.owner,owner);assert(result.live);assert(!result.readonly);
});
test('block sizes permit 512-byte disks and sector-aligned slider steps',()=>{
 assert.equal(parseSize('512',512n),512n);assert.throws(()=>parseSize('511',512n));
 const steps=sizeSteps(1048576n,8192n,1024n,512n);assert(steps.includes(8192n));assert(steps.every(n=>n%512n===0n));
});

test('native object info preserves optional digest framing without content versions',()=>{
 const length=9007199254741504n,w=new Writer().u64(length).bool(true).raw(new Uint8Array(32)).u32(0).id('0'.repeat(32)).u8(0).u16(5),r=new Reader(w.bytes());
 assert.equal(readInfo(r).length,length);r.done();
});

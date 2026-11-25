// SPDX-License-Identifier: GPL-3.0-only
// NBD descriptor interpretation is a browser client concern, never server code.
import {Writer,Reader,sha256} from './protocol.js';
import {ObjectClient,hex} from './object-client.js';
const utf8=new TextEncoder(),text=new TextDecoder('utf-8',{fatal:true});
export function metadataID(name){const hash=sha256(new Uint8Array([...utf8.encode('STQR NBD metadata v1\0'),...utf8.encode(name)]));return '0'.repeat(16)+hex(hash.slice(0,24))}
export function decodeMetadata(raw){
 const r=new Reader(raw);if(text.decode(r.take(8))!=='STNBD001'||r.u16()!==1||r.u32()!==1048576)throw Error('Unbekanntes Disk-Format');
 const size=r.u64(),id=r.id(),name=r.text(),manufacturer=r.text(),model=r.text(),serial=r.text();r.done();
 if(size===0n||size%512n||size>0x7fffffffffffffffn||id==='0'.repeat(32)||!name)throw Error('Ungültige Disk-Metadaten');
 for(const value of [name,manufacturer,model,serial])if(utf8.encode(value).length>255||/[\0\r\n]/.test(value))throw Error('Ungültige Disk-Metadaten');
 return {size,id,name,manufacturer,model,serial};
}
async function client(connection,bucket,state){
 const store=new ObjectClient(connection,bucket,state.owner),root=hex(sha256(utf8.encode('STQR filesystem root v1')));
 try{await store.stat(root)}catch(e){if(e.status===404)return store;throw e}
 throw Error('Dieser Bucket enthält ein Dateisystem statt benannter Disks.');
}
export async function listOfflineDevices(connection,bucket,state,after,limit,query){
 const store=await client(connection,bucket,state),page=await store.scan(after,limit),devices=[];let next=page.next;
 for(const entry of page.objects){
  if(!entry.id.startsWith('0'.repeat(16))){next='';break}
  if(entry.length>2048n)throw Error('Ungültige Disk-Metadaten');
  const m=decodeMetadata(await store.get(entry.id));if(metadataID(m.name)!==entry.id)throw Error('Ungültige Disk-Adresse');
  if(m.name.toLowerCase().includes(query.toLowerCase()))devices.push(m);
 }
 return {...state,devices,next};
}
export async function growOfflineDevice(connection,bucket,state,device,size,id){
 if(size<device.size||size===0n||size%512n||size>0x7ffffffffffffe00n)throw Error('Ungültige Diskgröße');
 const store=await client(connection,bucket,state),oid=metadataID(device.name),raw=await store.get(oid),current=decodeMetadata(raw);
 if(current.id!==device.id||current.name!==device.name||current.size!==device.size&&current.size!==size)throw Error('Die Disk wurde geändert; bitte aktualisieren.');
 if(current.size!==size)await store.casRange(oid,14,raw,new Writer().u64(size).bytes(),raw,id);
 return {...state,devices:[{...current,size}],next:''};
}

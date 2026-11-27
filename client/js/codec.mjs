// SPDX-License-Identifier: GPL-3.0-only
// Canonical STQR wire primitives. Lengths and offsets use BigInt.
const utf8=new TextEncoder(),text=new TextDecoder('utf-8',{fatal:true});
export const hex=b=>Array.from(b,v=>v.toString(16).padStart(2,'0')).join('');
export function unhex(s,bytes=32){if(typeof s!=='string'||s.length!==bytes*2||!/^[0-9a-f]+$/i.test(s))throw RangeError('Invalid hexadecimal ID');return Uint8Array.from(s.match(/../g),v=>parseInt(v,16))}
export const newID=()=>hex(crypto.getRandomValues(new Uint8Array(16)));
export const bytes=value=>value instanceof Uint8Array?value:new Uint8Array(value);
export function equal(a,b){return a.length===b.length&&a.every((v,i)=>v===b[i])}
export class Writer{
 constructor(){this.a=[]}u8(v){if(!Number.isInteger(Number(v))||Number(v)<0||Number(v)>255)throw RangeError('u8 overflow');this.a.push(Number(v));return this}bool(v){return this.u8(v?1:0)}
 u16(v){return this.u8(v&255).u8((v>>>8)&255)}u32(v){for(let i=0;i<4;i++)this.u8((v>>>(8*i))&255);return this}
 u64(v){if(typeof v==='number'&&!Number.isSafeInteger(v))throw RangeError('unsafe integer');v=BigInt(v);if(v<0n||v>0xffffffffffffffffn)throw Error('u64 overflow');for(let i=0;i<8;i++){this.u8(Number(v&255n));v>>=8n}return this}
 v(v){v=BigInt(v);if(v<0n||v>0xffffffffffffffffn)throw Error('varint overflow');do{let b=Number(v&127n);v>>=7n;this.u8(b|(v?128:0))}while(v);return this}
 id(s){if(!/^[0-9a-f]{32}$/i.test(s))throw Error('Ungültige ID');for(let i=0;i<32;i+=2)this.u8(parseInt(s.slice(i,i+2),16));return this}
 raw(b){for(const v of b)this.a.push(v);return this}blob(b){return this.v(b.length).raw(b)}text(s){return this.blob(utf8.encode(s))}bytes(){return new Uint8Array(this.a)}
}
export class Reader{
 constructor(b){this.b=b instanceof Uint8Array?b:new Uint8Array(b);this.at=0}take(n){if(!Number.isSafeInteger(n)||n<0||n>this.b.length-this.at)throw Error('Unvollständige STQR-Nachricht');const b=this.b.subarray(this.at,this.at+n);this.at+=n;return b}
 u8(){return this.take(1)[0]}bool(){const b=this.u8();if(b>1)throw Error('Ungültiges Bool');return b===1}
 u16(){const b=this.take(2);return b[0]|b[1]<<8}u32(){const b=this.take(4);return new DataView(b.buffer,b.byteOffset,4).getUint32(0,true)}
 u64(){const b=this.take(8);return new DataView(b.buffer,b.byteOffset,8).getBigUint64(0,true)}
 v(){let n=0n;for(let i=0;i<10;i++){const b=this.u8();if(i===9&&b>1)throw Error('Varint overflow');n|=BigInt(b&127)<<BigInt(7*i);if(!(b&128)){if(i>0&&b===0)throw Error('Nichtkanonisches Varint');return n}}throw Error('Varint overflow')}
 count(max=1<<24){const n=this.v();if(n>BigInt(max)||n>BigInt(this.b.length-this.at))throw Error('Zu große STQR-Länge');return Number(n)}
 blob(){return this.take(this.count())}text(){return text.decode(this.blob())}id(){return Array.from(this.take(16),b=>b.toString(16).padStart(2,'0')).join('')}
 done(){if(this.at!==this.b.length)throw Error('Unerwartete Daten am Nachrichtenende')}
}
const SHA256_K=[0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2];
const rotate=(v,n)=>(v>>>n)|(v<<(32-n));
// Incremental hashing keeps streamed reads bounded by one transfer chunk.
export class SHA256 {
 constructor(){this.state=new Uint32Array([0x6a09e667,0xbb67ae85,0x3c6ef372,0xa54ff53a,0x510e527f,0x9b05688c,0x1f83d9ab,0x5be0cd19]);this.buffer=new Uint8Array(64);this.used=0;this.length=0n;this.finished=false;this.words=new Uint32Array(64)}
 compress(block){const view=new DataView(block.buffer,block.byteOffset,64),w=this.words;for(let i=0;i<16;i++)w[i]=view.getUint32(i*4);for(let i=16;i<64;i++){const x=w[i-15],y=w[i-2];w[i]=(rotate(x,7)^rotate(x,18)^(x>>>3))+w[i-16]+(rotate(y,17)^rotate(y,19)^(y>>>10))+w[i-7]}let [a,b,c,d,e,f,g,h]=this.state;for(let i=0;i<64;i++){const t1=h+(rotate(e,6)^rotate(e,11)^rotate(e,25))+((e&f)^(~e&g))+SHA256_K[i]+w[i],t2=(rotate(a,2)^rotate(a,13)^rotate(a,22))+((a&b)^(a&c)^(b&c));h=g;g=f;f=e;e=(d+t1)>>>0;d=c;c=b;b=a;a=(t1+t2)>>>0}for(const [i,v] of [a,b,c,d,e,f,g,h].entries())this.state[i]+=v}
 update(input){if(this.finished)throw Error('Hash already finalized');const data=bytes(input);this.length+=BigInt(data.length);let offset=0;if(this.used){const n=Math.min(64-this.used,data.length);this.buffer.set(data.subarray(0,n),this.used);this.used+=n;offset=n;if(this.used===64){this.compress(this.buffer);this.used=0}}while(offset+64<=data.length){this.compress(data.subarray(offset,offset+64));offset+=64}if(offset<data.length){this.buffer.set(data.subarray(offset));this.used=data.length-offset}return this}
 digest(){if(this.finished)throw Error('Hash already finalized');this.finished=true;const tail=new Uint8Array(this.used<56?64:128);tail.set(this.buffer.subarray(0,this.used));tail[this.used]=128;new DataView(tail.buffer).setBigUint64(tail.length-8,this.length*8n);for(let i=0;i<tail.length;i+=64)this.compress(tail.subarray(i,i+64));const out=new Uint8Array(32),v=new DataView(out.buffer);this.state.forEach((n,i)=>v.setUint32(i*4,n));return out}
}
export function sha256(value){return new SHA256().update(value).digest()}

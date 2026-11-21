// SPDX-License-Identifier: GPL-3.0-only
// Canonical Policy encoding; all u64 fields stay exact through the JSON page.
function enumValue(value,values){const n=values.indexOf(value);if(n<0)throw Error('Unbekannter Policy-Wert: '+value);return n}
export function writeBucketPolicy(w,b){
 const p=b.policy,r=p.redundancy,c=p.read_acceleration,l=p.limits;
 return w.id(b.id).u64(p.version).text(b.name).u8(enumValue(b.state,['active','locked','deleting']))
  .u8(enumValue(p.storage_strategy,['hdd','ssd','ram'])).u8(enumValue(p.compression,['raw','gzip','lz4','xz']))
  .u8(enumValue(p.placement_mode,['count','everywhere'])).u8(enumValue(r.mode,['single','replication']))
  .u16(r.replicas).u16(r.ec_k||0).u16(r.ec_m||0).bool(c.enabled).u16(c.max_copies_per_hot_range).u64(c.cache_bytes_budget)
  .u8(enumValue(l.quota_accounting,['logical','physical'])).u64(l.guaranteed_quota_bytes).bool(l.absolute_unlimited).u64(l.absolute_quota_bytes).u32(l.metadata_reserve_ppm);
}
export function writeBucketQuota(w,b,expected){const l=b.policy.limits;return w.id(b.id).u8(enumValue(l.quota_accounting,['logical','physical'])).u64(l.guaranteed_quota_bytes).bool(l.absolute_unlimited).u64(l.absolute_quota_bytes).u32(l.metadata_reserve_ppm).u64(expected)}
export function parseBucketPage(raw){
 // Match complete quoted strings before number tokens so names/escapes cannot
 // be mistaken for numbers. Quote unsafe integer tokens before JSON.parse.
 return JSON.parse(raw.replace(/"(?:\\.|[^"\\])*"|-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?/g,token=>{
  if(!/^-?\d+$/.test(token))return token;
  const n=BigInt(token);return n>BigInt(Number.MAX_SAFE_INTEGER)||n<BigInt(Number.MIN_SAFE_INTEGER)?JSON.stringify(token):token;
 }));
}
export function parseQuota(raw){
 const m=/^\s*(\d+(?:[.,]\d+)?)\s*([kmgtp]?)(?:i?b)?\s*$/i.exec(raw);if(!m)throw Error('Bitte eine Größe wie 50M, 2G oder 0 eingeben.');
 const [whole,fraction='']=m[1].replace(',','.').split('.'),den=10n**BigInt(fraction.length),scale=m[2]?1024n**BigInt('kmgtp'.indexOf(m[2].toLowerCase())+1):1n;
 const n=(BigInt(whole)*den+BigInt(fraction||0))*scale/den;if(n>0xffffffffffffffffn)throw Error('Kontingent ist zu groß.');return n;
}
export function defaultBucketPolicy(){return {version:1,storage_strategy:'hdd',compression:'raw',placement_mode:'count',redundancy:{mode:'single',replicas:1,ack:'all'},read_acceleration:{enabled:false,max_copies_per_hot_range:0,cache_bytes_budget:0},limits:{quota_accounting:'logical',metadata_reserve_ppm:540,guaranteed_quota_bytes:0,absolute_quota_bytes:0,absolute_unlimited:true}}}

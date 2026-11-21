// SPDX-License-Identifier: GPL-3.0-only
// Slider positions are indices into stable, discrete size steps, never bytes.
export const MIN_VOLUME_SIZE=256n*1024n;
const UNITS=['K','M','G','T','P'];
export function parseSize(raw){
 const match=/^\s*(\d+(?:[.,]\d+)?)\s*([kmgtp]?)(?:i?b)?\s*$/i.exec(raw);
 if(!match)throw Error('Bitte eine Größe wie 50M, 2G oder 52428800 eingeben.');
 const [whole,fraction='']=match[1].replace(',','.').split('.');
 const scale=(match[2]?1024n**BigInt('kmgtp'.indexOf(match[2].toLowerCase())+1):1n),denominator=10n**BigInt(fraction.length);
 const size=(BigInt(whole)*denominator+BigInt(fraction||0))*scale/denominator;
 if(size<MIN_VOLUME_SIZE||size>0x7fffffffffffffffn)throw Error('Die Größe muss mindestens 256K betragen und in int64 passen.');
 return size;
}
export function sizeLimit(bytes){return bytes/4096n*4096n}
export function sizeSteps(limit,custom,minimum=MIN_VOLUME_SIZE){
 limit=sizeLimit(limit);if(limit<minimum)return [];
 const values=new Set([minimum,limit]);
 for(let unit=1024n,prefix=0;prefix<UNITS.length;prefix++,unit*=1024n){
  for(let magnitude=1n;magnitude<=100n;magnitude*=10n){
   for(let digit=1n;digit<=9n;digit++){const n=digit*magnitude*unit;if(n>=minimum&&n<=limit)values.add(n)}
  }
 }
 if(custom!==undefined&&custom>=minimum&&custom<=limit)values.add(custom);
 return [...values].sort((a,b)=>a<b?-1:a>b?1:0);
}
// Return the shortest decimal in the largest applicable unit that parses back
// to the very same byte count. This also represents df's exact non-round limit
// without rounding it upward or secretly changing the selected size.
export function formatSize(n){
 n=BigInt(n);if(n===0n)return '0K';let unit=1024n,index=0;
 while(index<UNITS.length-1&&n>=unit*1024n){unit*=1024n;index++}
 for(let precision=0,denominator=1n;precision<=16;precision++,denominator*=10n){
  const scaled=(n*denominator+unit-1n)/unit;
  if(scaled*unit/denominator!==n)continue;
  const whole=scaled/denominator,fraction=precision?String(scaled%denominator).padStart(precision,'0').replace(/0+$/,''):'';
  return `${whole}${fraction?'.'+fraction:''}${UNITS[index]}`;
 }
 throw Error('Größe kann nicht exakt dargestellt werden.');
}
export function sizeLabel(n){
 n=BigInt(n);let unit=1024n,index=0;while(index<UNITS.length-1&&n>=unit*1024n){unit*=1024n;index++}
 return `${(Number(n*100n/unit)/100).toLocaleString('de-DE',{maximumFractionDigits:2})}${UNITS[index]}`;
}

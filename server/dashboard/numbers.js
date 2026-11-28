// SPDX-License-Identifier: GPL-3.0-only
// Display-only formatting. Editable size inputs retain their exact byte value.
const quantity=new Intl.NumberFormat('de-DE',{maximumFractionDigits:1});
export function formatNumber(value){
 if(value==null||typeof value==='number'&&!Number.isFinite(value))return '—';
 return quantity.format(value);
}
export function formatBytes(value=0n){
 if(value==null)return '—';
 const n=BigInt(value),absolute=n<0n?-n:n,units=['B','KiB','MiB','GiB','TiB','PiB','EiB'];
 let index=0,scale=1n;
 while(index<units.length-1&&absolute>=scale*1024n){scale*=1024n;index++}
 // Rounding must not render 1024.0 KiB instead of 1 MiB at unit boundaries.
 if(index<units.length-1&&absolute*10n>=scale*10240n-scale/2n){scale*=1024n;index++}
 return `${formatNumber(Number(n)/Number(scale))} ${units[index]}`;
}
export function formatRate(value){
 if(value==null||typeof value==='number'&&!Number.isFinite(value))return '—';
 if(typeof value!=='number')return formatBytes(value)+'/s';
 const units=['B','KiB','MiB','GiB','TiB','PiB','EiB'];let index=0,amount=value;
 while(index<units.length-1&&Math.abs(amount)>=1023.95){amount/=1024;index++}
 return `${formatNumber(amount)} ${units[index]}/s`;
}
export function formatRatePair(value){return `${formatRate(value?.last)} · ø ${formatRate(value?.average)}`}

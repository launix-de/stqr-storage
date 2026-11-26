// SPDX-License-Identifier: GPL-3.0-only
import {formatBytes,formatNumber} from './numbers.js';

// A missing member report is unknown, not a smaller cluster capacity.
export function capacityKnown(entries,memberCount){return entries.length>0&&entries.length===memberCount}
// Physical superblock totals can be known before reservation recovery completes.
export function capacityAdmissionKnown(entries){return entries.every(e=>(BigInt(e.snapshot.reason??0)&1024n)===0n)}

// The same member-filtered RAM aggregate drives both global physical cards.
// Free includes protected reserves; usable excludes them. No object scan/fetch.
export function capacityModel(t){
 const capacity=BigInt(t.capacity??0n),used=BigInt(t.allocated??0n),reserved=BigInt(t.reserved??0n);
 const occupied=used<0n?0n:used>capacity?capacity:used;
 const protectedBytes=reserved<0n?0n:reserved>capacity-occupied?capacity-occupied:reserved;
 const percent=n=>capacity>0n?Number(n*10000n/capacity)/100:0;
 return {capacity,used,free:capacity>used?capacity-used:0n,usable:BigInt(t.admission??0n),usedPercent:percent(occupied),reservedPercent:percent(protectedBytes),ticks:[0n,capacity/4n,capacity/2n,capacity*3n/4n,capacity]};
}
export function renderCapacitySummary(root,totals,known=true,admissionKnown=true){
 if(!root)return;
 const m=capacityModel(totals),q=name=>root.querySelector(`[data-capacity-${name}]`);
 q('used').textContent=known?formatBytes(m.used):'—';
 q('total').textContent=known?formatBytes(m.capacity):'—';
 q('detail').textContent=known?`${formatBytes(m.free)} frei · ${admissionKnown?formatBytes(m.usable)+' nutzbar':'Nutzbarkeit wird ermittelt'}`:'Warte auf Statistikdaten';
 q('percent').textContent=known&&m.capacity>0n?`${formatNumber(m.usedPercent)} %`:'—';
 const meter=q('meter');meter.dataset.capacity=String(m.capacity);meter.dataset.used=String(m.used);meter.dataset.free=String(m.free);
 meter.setAttribute('aria-label',known?`${formatBytes(m.used)} von ${formatBytes(m.capacity)} physisch belegt; ${formatBytes(m.free)} frei; ${admissionKnown?formatBytes(m.usable)+' nutzbar':'Nutzbarkeit noch unbekannt'}`:'Physische Belegung noch unbekannt');
 meter.classList.toggle('capacity-unknown',!known||m.capacity===0n);
 q('fill').style.width=m.usedPercent+'%';q('reserve').style.width=(admissionKnown?m.reservedPercent:0)+'%';
 for(const [index,tick] of [...root.querySelectorAll('[data-capacity-tick]')].entries())tick.textContent=known?formatBytes(m.ticks[index]):'—';
}

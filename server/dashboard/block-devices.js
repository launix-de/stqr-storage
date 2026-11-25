// SPDX-License-Identifier: GPL-3.0-only
import {EndlessList} from './endless.js';
import {newID} from './protocol.js';
import {formatSize,parseSize,sizeLabel,sizeSteps} from './size.js';

const MAX_SIZE=0x7ffffffffffffe00n;
export function setupBlockDevices({connection,escape,toast}){
 const list=document.createElement('dialog');list.id='block-devices-dialog';list.className='block-devices-dialog';
 list.innerHTML=`<div class="dialog-heading"><div><span class="eyebrow">BLOCKDEVICES</span><h2>Virtuelle Disks</h2></div><button type="button" class="icon-button" data-block-close aria-label="Schließen">×</button></div>
 <p class="dialog-subtitle" data-bucket-name></p><p class="field-note" data-exporter role="status">Verbindung wird geprüft …</p>
 <section class="storage-panel"><div class="panel-heading"><h3>Disks <span class="count" data-count>0</span></h3><div class="block-device-tools"><label class="search"><input data-search aria-label="Disks durchsuchen" placeholder="Disk suchen …"></label><button type="button" class="secondary" data-refresh>Aktualisieren</button></div></div>
 <div class="block-device-table"><table><thead><tr><th>Disk</th><th>Virtuelle Größe</th><th>Gerät</th><th>Aktion</th></tr></thead><tbody data-disks></tbody></table></div></section>
 <p class="error" data-error role="alert"></p><p class="field-note">Virtueller Speicher wird erst beim Schreiben belegt. Vorhandene Daten bleiben beim Vergrößern erhalten.</p><div class="dialog-actions"><button type="button" class="secondary" data-block-close>Schließen</button></div>`;
 const grow=document.createElement('dialog');grow.id='block-device-grow-dialog';
 grow.innerHTML=`<form novalidate><div class="dialog-heading"><div><span class="eyebrow">VIRTUELLE DISK</span><h2>Disk vergrößern</h2></div><button type="button" class="icon-button" data-block-close aria-label="Schließen">×</button></div>
 <p class="dialog-subtitle" data-name></p><label>Neue Gesamtgröße<input data-size inputmode="decimal" placeholder="z. B. 50G" autocomplete="off" required aria-describedby="block-device-size-note"></label>
 <p class="error" data-validation role="alert"></p><p class="field-note" id="block-device-size-note">K/M/G/T/P sind 1024er-Schritte. Die Größe muss ein Vielfaches von 512 Bytes sein.</p>
 <div class="block-device-presets" aria-label="Größe erhöhen"><button type="button" class="row-action" data-add="1073741824">+1G</button><button type="button" class="row-action" data-add="10737418240">+10G</button><button type="button" class="row-action" data-add="107374182400">+100G</button><button type="button" class="row-action" data-double>Verdoppeln</button></div>
 <input data-slider type="range" aria-label="Neue Diskgröße"><div class="block-device-slider-labels"><span data-min></span><strong data-slider-value></strong><span>Max. 8 EiB</span></div>
 <div class="block-device-summary" aria-live="polite"><div><span>Bisher</span><strong data-current></strong></div><div><span>Zusätzlich</span><strong data-delta>—</strong></div><div><span>Neue Größe</span><strong data-target>—</strong></div></div>
 <p class="notice">Der neue Bereich ist zunächst leer. Bestehende NBD-Clients müssen neu verbinden, um die größere Disk zu erkennen. Partitionen und Dateisysteme im Gast werden separat vergrößert.</p>
 <p class="error" data-error role="alert"></p><div class="dialog-actions"><button type="button" class="secondary" data-block-close>Abbrechen</button><button type="submit" class="primary" data-submit disabled>Vergrößern</button></div></form>`;
 document.body.append(list,grow);
 const el=s=>list.querySelector(s),ge=s=>grow.querySelector(s);
 let bucket,devices=[],meta={},epoch=0,searchTimer,selected,steps=[],pending=false,retry;
 function render(){
  el('[data-count]').textContent=String(devices.length);
  el('[data-exporter]').textContent=meta.live?`Exporter verbunden · ${meta.readonly?'Nur lesen':'Schreibzugriff'} · Node ${meta.owner.slice(0,8)}`:'Kein Exporter verbunden · Verwaltung über native Objektoperationen';
  el('[data-disks]').innerHTML=devices.map(d=>`<tr data-device="${d.id}"><td><div class="volume-name">${escape(d.name)}</div><div class="volume-path">${escape(d.id)}</div></td><td><strong title="${d.size} Bytes">${escape(sizeLabel(d.size))}</strong><div class="bucket-sub">Sparse · ${d.size.toLocaleString('de-DE')} Bytes</div></td><td>${escape(d.manufacturer)} · ${escape(d.model)}<div class="volume-path">${escape(d.serial)}</div></td><td><button type="button" class="row-action" data-grow="${d.id}" ${meta.readonly||d.size===MAX_SIZE?'disabled':''}>Vergrößern</button></td></tr>`).join('')||`<tr><td colspan="4" class="empty">${el('[data-search]').value?'Keine passenden Disks.':'In diesem Bucket sind keine virtuellen Disks angelegt.'}</td></tr>`;
  for(const button of el('[data-disks]').querySelectorAll('[data-grow]'))button.onclick=()=>openGrow(devices.find(d=>d.id===button.dataset.grow));
 }
 const pager=new EndlessList({container:el('.storage-panel'),visible:()=>list.open,fetchPage:(after,limit,query)=>connection().listBlockDevices(bucket.id,after,Math.min(limit,100),query),entries:p=>p.devices,
  onUpdate:(items,state)=>{devices=items;if(state.loading){el('[data-count]').textContent='0';el('[data-disks]').innerHTML='<tr><td colspan="4" class="empty">Disks werden geladen …</td></tr>';return}meta=state.meta;render()},onError:e=>{el('[data-error]').textContent=e.message},onLoading:busy=>{el('[data-refresh]').disabled=busy;el('[data-refresh]').textContent=busy?'Wird geladen …':'Aktualisieren'}});
 async function refresh(){el('[data-error]').textContent='';await pager.reset(el('[data-search]').value.trim())}
 el('[data-refresh]').onclick=refresh;
 el('[data-search]').oninput=()=>{clearTimeout(searchTimer);searchTimer=setTimeout(refresh,250)};
 for(const button of list.querySelectorAll('[data-block-close]'))button.onclick=()=>list.close();
 list.addEventListener('close',()=>{epoch++;clearTimeout(searchTimer);pager.clear();devices=[];bucket=null;if(grow.open&&!pending)grow.close()});
 function setSteps(value){
  steps=sizeSteps(MAX_SIZE,value,selected.size+512n,512n);
  const slider=ge('[data-slider]');slider.min='0';slider.max=String(steps.length-1);slider.value=String(Math.max(0,steps.indexOf(value)));slider.step='1';slider.setAttribute('aria-valuetext',formatSize(value));
  ge('[data-slider-value]').textContent=formatSize(value);ge('[data-min]').textContent=formatSize(selected.size+512n);
 }
 function validate(){
  const input=ge('[data-size]');
  try{
   const value=parseSize(input.value,512n);
   if(value<=selected.size)throw Error(`Die neue Größe muss größer als ${formatSize(selected.size)} sein.`);
   if(value%512n)throw Error('Die Größe muss ein Vielfaches von 512 Bytes sein.');
   input.setCustomValidity('');input.setAttribute('aria-invalid','false');ge('[data-validation]').textContent='';ge('[data-submit]').disabled=pending;setSteps(value);
   ge('[data-delta]').textContent='+'+sizeLabel(value-selected.size);ge('[data-target]').textContent=sizeLabel(value);return value;
  }catch(e){input.setCustomValidity(e.message);input.setAttribute('aria-invalid','true');ge('[data-validation]').textContent=e.message;ge('[data-submit]').disabled=true;ge('[data-delta]').textContent='—';ge('[data-target]').textContent='—';return null}
 }
 function openGrow(device){
  if(!device||pending)return;selected=device;retry=null;pending=false;ge('[data-name]').textContent=`${device.name} · ${bucket.name}`;ge('[data-current]').textContent=sizeLabel(device.size);ge('[data-error]').textContent='';ge('[data-size]').value=formatSize(device.size);ge('[data-size]').setCustomValidity('');ge('[data-submit]').disabled=true;
  const suggested=device.size<=MAX_SIZE/2n?device.size*2n:MAX_SIZE;setSteps(suggested);ge('[data-delta]').textContent='—';ge('[data-target]').textContent='—';
  for(const button of grow.querySelectorAll('[data-add]'))button.disabled=device.size+BigInt(button.dataset.add)>MAX_SIZE;
  ge('[data-double]').disabled=device.size>MAX_SIZE/2n;
  ge('[data-size]').value=formatSize(suggested);validate();grow.showModal();ge('[data-size]').focus();ge('[data-size]').select();
 }
 ge('[data-size]').oninput=()=>validate();
 ge('[data-size]').onchange=()=>{const value=validate();if(value!==null)ge('[data-size]').value=formatSize(value)};
 ge('[data-slider]').oninput=()=>{ge('[data-size]').value=formatSize(steps[Number(ge('[data-slider]').value)]);validate()};
 for(const button of grow.querySelectorAll('[data-add]'))button.onclick=()=>{ge('[data-size]').value=formatSize(selected.size+BigInt(button.dataset.add));validate()};
 ge('[data-double]').onclick=()=>{ge('[data-size]').value=formatSize(selected.size*2n);validate()};
 for(const button of grow.querySelectorAll('[data-block-close]'))button.onclick=()=>{if(!pending)grow.close()};
 grow.addEventListener('cancel',e=>{if(pending)e.preventDefault()});
 grow.querySelector('form').onsubmit=async event=>{
  event.preventDefault();if(pending)return;const size=validate();if(size===null){ge('[data-size]').reportValidity();return}
  const seq=epoch,target=bucket,device=selected;if(!retry||retry.size!==size)retry={id:newID(),size};
  pending=true;ge('[data-submit]').disabled=true;ge('[data-submit]').textContent='Wird vergrößert …';ge('[data-error]').textContent='';
  for(const input of grow.querySelectorAll('input,button'))input.disabled=true;
  try{
   const result=await connection().growBlockDevice(target.id,device,size,retry.id);
   if(seq!==epoch)return;
   const actual=result.devices[0];if(!actual||actual.id!==device.id||actual.size!==size)throw Error('Unerwartete Größenbestätigung. Bitte Disks aktualisieren.');
   retry=null;grow.close();toast(`${device.name} auf ${sizeLabel(size)} vergrößert`);await refresh();
  }catch(e){if(seq===epoch)ge('[data-error]').textContent=e.message}
  finally{pending=false;for(const input of grow.querySelectorAll('input,button'))input.disabled=false;ge('[data-submit]').textContent='Vergrößern';if(grow.open)validate()}
 };
 return {async open(target){bucket=target;devices=[];meta={};epoch++;el('[data-bucket-name]').textContent=target.name;el('[data-search]').value='';el('[data-exporter]').textContent='Verbindung wird geprüft …';el('[data-disks]').replaceChildren();list.showModal();await refresh()},clear(){epoch++;if(grow.open)grow.close();if(list.open)list.close();pager.clear();devices=[];bucket=null}};
}

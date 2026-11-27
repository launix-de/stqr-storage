// SPDX-License-Identifier: GPL-3.0-only
import {requestDialogClose} from './dialogs.js';
import {newID} from './protocol.js';
import {parseSize,formatSize,sizeLabel,sizeSteps} from './size.js';

const $=id=>document.getElementById(id);
export function setupVolumeGrowth({connection,describe,toast,refresh}){
 const dialog=$('grow-dialog');let selection,info,steps=[],retry,pending=false,epoch=0;
 function summary(value){
  for(const [id,n]of [['grow-current',info.current],['grow-added',value==null?null:value-info.current],['grow-target',value]]){
   $(id).textContent=n==null?'—':(id==='grow-added'?'+':'')+sizeLabel(n);$(id).title=n==null?'':`${n.toLocaleString('de-DE')} Bytes`;
  }
 }
 function slider(value){
  steps=sizeSteps(info.maximum,value,info.current);
  $('grow-slider').min='0';$('grow-slider').max=String(Math.max(0,steps.length-1));$('grow-slider').value=String(Math.max(0,steps.indexOf(value)));$('grow-slider').step='1';$('grow-slider').disabled=pending||steps.length<2;
  $('grow-slider').setAttribute('aria-valuetext',sizeLabel(value));$('grow-min').textContent=sizeLabel(info.current);$('grow-max').textContent=sizeLabel(info.maximum);$('grow-value').textContent=sizeLabel(value);
 }
 function validate(){
  if(!info)return null;
  try{
   const size=info.kind===1?info.maximum:parseSize($('grow-size').value);
   if(size<info.current)throw Error('Ein Storage-Volume kann hier nur vergrößert werden.');
   if(size>info.maximum)throw Error(`Aktuell sind höchstens ${sizeLabel(info.maximum)} möglich. Bitte verfügbaren Platz erneut prüfen.`);
   if(info.kind!==1&&size%4096n)throw Error('Die Größe muss ein Vielfaches von 4096 Bytes sein (4 KiB).');
   $('grow-size').setCustomValidity('');$('grow-size').setAttribute('aria-invalid','false');$('grow-validation').textContent='';$('grow-submit').disabled=pending||size<=info.current;slider(size);summary(size);return size;
  }catch(e){$('grow-size').setCustomValidity(e.message);$('grow-size').setAttribute('aria-invalid','true');$('grow-validation').textContent=e.message;$('grow-submit').disabled=true;summary(null);return null}
 }
 function controls(){
  $('grow-size').disabled=pending||!info;$('grow-refresh').disabled=pending;
  for(const b of dialog.querySelectorAll('[data-grow-close]'))b.disabled=pending;
  for(const b of dialog.querySelectorAll('[data-grow-add]'))b.disabled=pending||!info||info.current+BigInt(b.dataset.growAdd)>info.maximum;
  $('grow-double').disabled=pending||!info||info.current*2n>info.maximum;
  $('grow-maximum').disabled=pending||!info||info.maximum<=info.current;
  if(info)validate();else{$('grow-submit').disabled=true;$('grow-slider').disabled=true}
 }
 async function load(){
  const current=selection,seq=++epoch;info=null;retry=null;$('grow-error').textContent='';$('grow-validation').textContent='';$('grow-note').textContent='Verfügbarer Speicher wird ermittelt …';$('grow-slider-wrap').hidden=true;$('grow-refresh').disabled=true;$('grow-summary').hidden=true;controls();$('grow-refresh').disabled=true;
  try{
   const result=await connection(current.target).volumeGrowInfo(current.id);
   if(seq!==epoch||selection!==current||!dialog.open)return;
   if(![0,1,2].includes(result.kind)||result.maximum<result.current)throw Error('Ungültige Wachstumsgrenzen. Bitte erneut prüfen.');
   info=result;const raw=info.kind===1;$('grow-source').textContent=`${current.node} · ${raw?'Physisches Device':info.kind===2?'RAM-Volume':'Dateivolume'}${current.path?' · '+current.path:''}`;
   $('grow-size').readOnly=raw;$('grow-size').value=formatSize(raw?info.maximum:info.current);$('grow-size').setCustomValidity('');$('grow-slider-wrap').hidden=raw;$('grow-summary').hidden=false;
   $('grow-note').textContent=raw?(info.maximum>info.current?'Zusätzlicher Platz des bereits vergrößerten Devices wird vollständig ins Volume übernommen. Die LV- oder Partitionsgröße selbst wird nicht verändert.':'Das physische Device ist noch nicht größer als das Volume. Zuerst z. B. das LVM-LV erweitern und anschließend hier erneut prüfen.'):`Bisher ${sizeLabel(info.current)} · zusätzlich verfügbar ${sizeLabel(info.maximum-info.current)} · maximal ${sizeLabel(info.maximum)}. K/M/G/T/P = 1024er-Schritte; 4-KiB-Ausrichtung. Bestehende Objekte und die Volume-ID bleiben erhalten.`;
   $('grow-submit').textContent=raw?'Zusätzlichen Device-Platz übernehmen':'Volume vergrößern';controls();
  }catch(e){if(seq===epoch&&selection===current){$('grow-error').textContent=e.message;$('grow-note').textContent='Grenzen konnten nicht geladen werden. Bitte erneut prüfen.';$('grow-refresh').disabled=false}}
 }
 $('grow-refresh').onclick=()=>{if(!pending&&selection)load()};
 $('grow-size').oninput=validate;
 $('grow-size').onchange=()=>{const value=validate();if(value!=null)$('grow-size').value=formatSize(value)};
 $('grow-slider').oninput=()=>{const value=steps[Number($('grow-slider').value)];if(value!==undefined){$('grow-size').value=formatSize(value);validate()}};
 for(const b of dialog.querySelectorAll('[data-grow-add]'))b.onclick=()=>{if(info){$('grow-size').value=formatSize(info.current+BigInt(b.dataset.growAdd));validate()}};
 $('grow-double').onclick=()=>{if(info){$('grow-size').value=formatSize(info.current*2n);validate()}};
 $('grow-maximum').onclick=()=>{if(info){$('grow-size').value=formatSize(info.maximum);validate()}};
 for(const b of dialog.querySelectorAll('[data-grow-close]'))b.onclick=()=>{if(!pending)requestDialogClose(dialog,'button')};
 dialog.addEventListener('cancel',e=>{if(pending)e.preventDefault()});
 dialog.addEventListener('close',()=>{epoch++;selection=null;info=null;retry=null});
 $('grow-form').onsubmit=async event=>{
  event.preventDefault();if(pending||!selection||!info)return;const value=validate();if(value==null){$('grow-size').reportValidity();return}if(value<=info.current)return;
  const current=selection,seq=epoch,size=info.kind===1?0n:value,signature=`${current.target}:${current.id}:${size}`,remote=connection(current.target);
  if(retry?.signature!==signature)retry={signature,id:newID()};const request=retry.id;pending=true;controls();$('grow-error').textContent='';$('grow-submit').textContent='Wird vergrößert …';
  try{
   let result=await remote.growVolume(current.id,size,request);
   const deadline=Date.now()+30000;
   while(result.state==='requested'||result.state==='running'){
    if(Date.now()>=deadline)throw Error('Der Auftrag läuft noch. Bitte denselben Auftrag erneut prüfen; die Operations-ID bleibt erhalten.');
    await new Promise(resolve=>setTimeout(resolve,250));result=await remote.operationStatus(request);
   }
   if(result.state!=='fulfilled'){const error=Error(result.detail??'Volume konnte nicht vergrößert werden.');error.status=result.status;throw error}
   if(seq!==epoch||selection!==current)return;
   retry=null;toast(`${current.name} auf ${sizeLabel(value)} vergrößert`);dialog.close();
   try{await refresh(current.target)}catch(e){toast('Volume vergrößert; Ansicht bitte aktualisieren: '+e.message,true)}
  }catch(e){if(seq===epoch&&selection===current){$('grow-error').textContent=e.message;if([400,403,404,409,507].includes(e.status))retry=null}}
  finally{pending=false;if(selection===current){$('grow-submit').textContent=info?.kind===1?'Zusätzlichen Device-Platz übernehmen':'Volume vergrößern';controls()}}
 };
 return {open(id,target){if(pending)return;const detail=describe(id,target);selection={id,target,node:detail.node,name:detail.volume?.name??id,path:detail.volume?.path};$('grow-description').textContent=selection.name;$('grow-source').textContent=selection.node;$('grow-size').value='';$('grow-size').setCustomValidity('');dialog.showModal();return load()},clear(){epoch++;selection=null;info=null;retry=null;if(dialog.open)dialog.close()}};
}

// SPDX-License-Identifier: GPL-3.0-only
import {newID} from './protocol.js';
export function setupS3Keys({connection}) {
 const dialog=document.createElement('dialog');
 dialog.innerHTML=`<form method="dialog"><div class="dialog-heading"><div><span class="eyebrow">USER MANAGEMENT</span><h2>S3-Zugangsschlüssel</h2></div><button class="icon-button" aria-label="Schließen">×</button></div><p class="dialog-subtitle" data-user></p><p class="field-note">Die Schlüssel verwenden die vorhandenen Bucket-Rechte dieses Users. Endpoint: /s3</p><div data-keys></div><section data-created hidden><p class="notice">Secret jetzt kopieren. Nach dem Schließen wird es nicht erneut angezeigt.</p><label>Access Key<input data-access readonly autocomplete="off"></label><label>Secret Key<input data-secret readonly autocomplete="off"></label></section><p class="error" role="alert" data-error></p><div class="dialog-actions"><button type="button" class="primary" data-create>Schlüssel anlegen</button><button class="secondary">Schließen</button></div></form>`;
 document.body.append(dialog);
 const el=selector=>dialog.querySelector(selector);
 let user,retry,epoch=0;
 async function load(seq){
  const keys=await connection().listS3Keys(user.id);if(seq!==epoch)return;
  const rows=keys.map(key=>{const row=document.createElement('p'),code=document.createElement('code'),remove=document.createElement('button');code.textContent=key;remove.type='button';remove.className='row-action danger-action';remove.textContent='Widerrufen';const operation=newID();remove.onclick=async()=>{remove.disabled=true;el('[data-error]').textContent='';try{await connection().deleteS3Key(user.id,key,operation);await load(seq)}catch(e){if(seq===epoch)el('[data-error]').textContent=e.message}finally{remove.disabled=false}};row.append(code,document.createTextNode(' '),remove);return row});
  if(!rows.length){const empty=document.createElement('p');empty.textContent='Keine S3-Schlüssel vorhanden.';rows.push(empty)}el('[data-keys]').replaceChildren(...rows);
 }
 el('[data-create]').onclick=async()=>{
  const seq=epoch,target=user;if(!retry)retry={id:newID(),access:'STQR'+newID().toUpperCase(),secret:newID()+newID()};const request=retry;
  el('[data-create]').disabled=true;el('[data-error]').textContent='';
  try{await connection().setS3Key(target.id,request.access,request.secret,request.id);if(seq!==epoch)return;el('[data-created]').hidden=false;el('[data-access]').value=request.access;el('[data-secret]').value=request.secret;retry=null;await load(seq)}catch(e){if(seq===epoch)el('[data-error]').textContent=e.message}finally{if(seq===epoch)el('[data-create]').disabled=false}
 };
 const clearSecret=()=>{el('[data-access]').value='';el('[data-secret]').value=''};
 dialog.addEventListener('submit',clearSecret);dialog.addEventListener('cancel',clearSecret);
 dialog.addEventListener('close',()=>{epoch++;retry=null;user=null;el('[data-access]').value='';el('[data-secret]').value='';el('[data-created]').hidden=true;el('[data-keys]').replaceChildren()});
 return {async open(selected){user=selected;retry=null;const seq=++epoch;el('[data-user]').textContent=selected.name;el('[data-error]').textContent='';el('[data-create]').disabled=false;dialog.showModal();try{await load(seq)}catch(e){if(seq===epoch)el('[data-error]').textContent=e.message}},clear(){if(dialog.open)dialog.close()}};
}

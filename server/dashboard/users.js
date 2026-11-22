// SPDX-License-Identifier: GPL-3.0-only
import {newID} from './protocol.js';
import {setupS3Keys} from './s3-keys.js';
const $=id=>document.getElementById(id);
export function setupUserViews({connection,currentUsername,escape,toast,showView,requireLogin,openBucketCreate}){
 let users=[],createRetry,passwordRetry,selected,deleteID,loading=0;
 const keys=setupS3Keys({connection});
 const roleRetries=new Map();
 const utf8=new TextEncoder();
 function passwordValid(input){input.setCustomValidity(utf8.encode(input.value).length>72?'Das Passwort darf maximal 72 UTF-8-Bytes lang sein.':'');return input.reportValidity()}
 function render(){
  $('user-count').textContent=String(users.length);$('nav-users-count').textContent=String(users.length);
  $('user-rows').innerHTML=users.map(u=>{
   const self=u.name===currentUsername();
   return `<tr data-user="${u.id}"><td><div class="user-cell"><span class="user-avatar">${escape(u.name.slice(0,1).toUpperCase())}</span><div><div class="volume-name">${escape(u.name)}${self?'<span class="system-label">DU</span>':''}</div><div class="volume-path">${escape(u.id)}</div></div></div></td><td><select class="user-role-select" data-user-role="${u.id}" ${self?'disabled title="Die eigene Rolle kann nicht geändert werden"':''} aria-label="Rolle für ${escape(u.name)}"><option value="0" ${u.role===0?'selected':''}>User</option><option value="1" ${u.role===1?'selected':''}>Administrator</option></select></td><td><span class="state-badge ${u.disabled?'detached':''}"><i></i>${u.disabled?'Deaktiviert':'Aktiv'}</span></td><td><div class="row-actions"><button class="row-action" data-user-s3="${u.id}">S3-Schlüssel</button><button class="row-action" data-user-password="${u.id}">Passwort ändern</button><button class="row-action danger-action" data-user-delete="${u.id}" ${self?'disabled title="Das eigene Konto kann nicht gelöscht werden"':''}>Löschen</button></div></td></tr>`;
  }).join('')||'<tr><td colspan="4">Keine Benutzer vorhanden.</td></tr>';
  for(const select of document.querySelectorAll('[data-user-role]'))select.onchange=()=>changeRole(select);
  for(const b of document.querySelectorAll('[data-user-s3]'))b.onclick=()=>keys.open(users.find(u=>u.id===b.dataset.userS3));
  for(const b of document.querySelectorAll('[data-user-password]'))b.onclick=()=>openPassword(b.dataset.userPassword);
  for(const b of document.querySelectorAll('[data-user-delete]'))b.onclick=()=>openDelete(b.dataset.userDelete);
 }
 async function load(){
  const seq=++loading;$('users-error').textContent='';$('refresh-users').disabled=true;
  try{const next=await connection().listUsers();if(seq!==loading)return;users=next;render()}
  catch(e){if(seq===loading)$('users-error').textContent=e.message}
  finally{if(seq===loading)$('refresh-users').disabled=false}
 }
 $('refresh-users').onclick=load;
 $('add-user').onclick=()=>{createRetry=null;$('user-add-form').reset();$('user-password').setCustomValidity('');$('user-add-error').textContent='';$('user-add-dialog').showModal();$('user-name').focus()};
 $('user-create-bucket').onchange=()=>{$('user-add-submit').textContent=$('user-create-bucket').checked?'User anlegen & weiter':'User anlegen'};
 $('user-password').addEventListener('input',()=>passwordValid($('user-password')));
 $('user-new-password').addEventListener('input',()=>passwordValid($('user-new-password')));
 $('user-add-dialog').addEventListener('close',()=>{$('user-password').value='';createRetry=null;$('user-add-submit').textContent='User anlegen'});
 $('user-password-dialog').addEventListener('close',()=>{$('user-new-password').value='';passwordRetry=null});
 $('user-add-form').addEventListener('submit',async e=>{
  e.preventDefault();if(!passwordValid($('user-password')))return;
  const name=$('user-name').value,password=$('user-password').value,bucket=$('user-create-bucket').checked,role=$('user-admin').checked?1:0,signature=JSON.stringify([name,password,role]);
  if(createRetry?.signature!==signature)createRetry={signature,id:newID(),userID:newID()};
  const retry=createRetry;e.submitter.disabled=true;$('user-add-error').textContent='';
  try{await connection().createUser({id:retry.userID,name,role},password,retry.id);$('user-add-dialog').close();toast(`User „${name}“ wurde angelegt.`);await load();if(bucket)await openBucketCreate({name,userID:retry.userID})}
  catch(e){$('user-add-error').textContent=e.message}finally{e.submitter.disabled=false}
 });
 async function changeRole(select){
  const user=users.find(u=>u.id===select.dataset.userRole),role=Number(select.value);if(!user||role===user.role)return;if(user.name===currentUsername()){select.value=String(user.role);return;}
  const signature=JSON.stringify([user.id,user.name,user.disabled,role]);let retry=roleRetries.get(user.id);if(retry?.signature!==signature){retry={signature,id:newID()};roleRetries.set(user.id,retry)}
  select.disabled=true;const old=user.role;
  try{await connection().setUserRole(user,role,retry.id);roleRetries.delete(user.id);user.role=role;toast(`Rolle für „${user.name}“ geändert.`);await load()}
  catch(e){select.value=String(old);toast(e.message,true);await load()}
  finally{select.disabled=false}
 }
 function openPassword(id){selected=users.find(u=>u.id===id);passwordRetry=null;$('user-new-password').value='';$('user-new-password').setCustomValidity('');$('user-password-description').textContent=`Neues Passwort für „${selected.name}“.`;$('user-password-note').textContent=selected.name===currentUsername()?'Nach der Änderung meldest du dich mit dem neuen Passwort erneut an.':'Bestehende Verbindungen dieses Benutzers werden ungültig.';$('user-password-error').textContent='';$('user-password-dialog').showModal();$('user-new-password').focus()}
 $('user-password-form').addEventListener('submit',async e=>{
  e.preventDefault();if(!passwordValid($('user-new-password')))return;
  const user=selected,password=$('user-new-password').value,signature=JSON.stringify([user.id,password]);
  if(passwordRetry?.signature!==signature)passwordRetry={signature,id:newID()};e.submitter.disabled=true;$('user-password-error').textContent='';
  try{await connection().setUserPassword(user.id,password,passwordRetry.id);$('user-password-dialog').close();if(user.name===currentUsername())requireLogin('Passwort geändert. Bitte mit dem neuen Passwort anmelden.');else toast(`Passwort für „${user.name}“ wurde geändert.`)}
  catch(e){if(user.name===currentUsername()&&!e.status){$('user-password-dialog').close();requireLogin('Verbindung beendet. Bitte mit dem neuen Passwort erneut anmelden; bei unklarem Ausgang das bisherige Passwort versuchen.')}else $('user-password-error').textContent=e.message}
  finally{e.submitter.disabled=false}
 });
 function openDelete(id){selected=users.find(u=>u.id===id);if(!selected||selected.name===currentUsername())return;deleteID=newID();$('user-delete-description').textContent=`Soll „${selected.name}“ wirklich gelöscht werden?`;$('user-delete-error').textContent='';$('user-delete-dialog').showModal()}
 $('user-delete-form').addEventListener('submit',async e=>{e.preventDefault();e.submitter.disabled=true;$('user-delete-error').textContent='';try{await connection().deleteUser(selected.id,deleteID);$('user-delete-dialog').close();toast(`User „${selected.name}“ wurde gelöscht.`);await load()}catch(e){$('user-delete-error').textContent=e.message}finally{e.submitter.disabled=false}});
 return {load,clear(){keys.clear();loading++;roleRetries.clear();users=[];render()}};
}

// SPDX-License-Identifier: GPL-3.0-only
import {formatNumber} from './numbers.js';
import {entityRate,throughputHTML} from './throughput.js';
import {EndlessList} from './endless.js';
import {newID} from './protocol.js';
import {setupS3Keys} from './s3-keys.js';
const $=id=>document.getElementById(id);
export function setupUserViews({connection,currentUsername,escape,toast,showView,requireLogin,openBucketCreate,onUserDeleted=()=>{},statsEntries=()=>[]}){
 let users=[],createRetry,passwordRetry,selected,deleteRetry,loading=0,pageMeta={},searchTimer;
 const keys=setupS3Keys({connection});
 const traffic=new Map();let trafficPoll;
 const roleRetries=new Map(),deleteJobs=new Map();
 const utf8=new TextEncoder();
 function passwordValid(input){input.setCustomValidity(utf8.encode(input.value).length>72?'Das Passwort darf maximal 72 UTF-8-Bytes lang sein.':'');return input.reportValidity()}
 function render({append=false,added=users,loading=false}={}){
 if(loading){if(!users.length)$('user-rows').innerHTML='<tr><td colspan="5" class="empty">Benutzer werden geladen …</td></tr>';return}
  $('user-count').textContent=formatNumber(pageMeta.total??users.length);$('nav-users-count').textContent=formatNumber(pageMeta.all_total??users.length);
  const html=(append?added:users).map(u=>{
   const self=u.name===currentUsername(),job=deleteJobs.get(u.id),pending=job?.state==='pending';
   return `<tr data-user="${u.id}"><td><div class="user-cell"><span class="user-avatar">${escape(u.name.slice(0,1).toUpperCase())}</span><div><div class="volume-name">${escape(u.name)}${self?'<span class="system-label">DU</span>':''}</div><div class="volume-path">${escape(u.id)}</div></div></div></td><td><select class="user-role-select" data-user-role="${u.id}" ${self?'disabled title="Die eigene Rolle kann nicht geändert werden"':pending?'disabled':''} aria-label="Rolle für ${escape(u.name)}"><option value="0" ${u.role===0?'selected':''}>User</option><option value="1" ${u.role===1?'selected':''}>Administrator</option></select></td><td data-user-throughput="${u.id}">${userThroughput(u.id)}</td><td><span class="state-badge ${pending?'draining':job?.state==='failed'?'failed':u.disabled?'detached':''}"><i></i>${pending?'Löschung ausstehend':job?.state==='failed'?'Löschung fehlgeschlagen':u.disabled?'Deaktiviert':'Aktiv'}</span>${job?.state==='failed'?'<div class="error deletion-error">'+escape(job.error)+'</div>':''}</td><td><div class="row-actions"><button class="row-action" data-user-s3="${u.id}" ${pending?'disabled':''}>S3-Schlüssel</button><button class="row-action" data-user-password="${u.id}" ${pending?'disabled':''}>Passwort ändern</button><button class="row-action danger-action" data-user-delete="${u.id}" ${self?'disabled title="Das eigene Konto kann nicht gelöscht werden"':pending?'disabled':''}>${job?.state==='failed'?'Erneut versuchen':'Löschen'}</button></div></td></tr>`;
  }).join('');if(append)$('user-rows').insertAdjacentHTML('beforeend',html);else $('user-rows').innerHTML=html||'<tr><td colspan="5">Keine passenden Benutzer.</td></tr>';
  for(const select of document.querySelectorAll('[data-user-role]'))select.onchange=()=>changeRole(select);
  for(const b of document.querySelectorAll('[data-user-s3]'))b.onclick=()=>keys.open(users.find(u=>u.id===b.dataset.userS3));
  for(const b of document.querySelectorAll('[data-user-password]'))b.onclick=()=>openPassword(b.dataset.userPassword);
  for(const b of document.querySelectorAll('[data-user-delete]'))b.onclick=()=>openDelete(b.dataset.userDelete);
 }
 $('user-rows').closest('.storage-panel').querySelector('.panel-heading').insertAdjacentHTML('beforeend','<label class="search"><span>⌕</span><input id="user-search" placeholder="Name oder ID suchen …" aria-label="Benutzer durchsuchen"></label>');
 const pager=new EndlessList({container:$('user-rows').closest('.storage-panel'),visible:()=>!$('users-view').hidden,fetchPage:(after,limit,query)=>connection().listUsersPage(after,limit,query),entries:page=>page.users,onUpdate:(items,state)=>{if(!state.loading){users=items;pageMeta=state.meta;observeTraffic(state.meta);scheduleTraffic()}render(state)},onError:e=>{$('users-error').textContent=e.message},onLoading:busy=>{$('refresh-users').disabled=busy}});
 async function load(force=false){if(pager.busy&&force!==true)return;loading++;$('users-error').textContent='';return pager.reset($('user-search').value.trim())}

 function observeTraffic(page){
  if(!page.node||!page.boot||!page.captured_ns)return;
  const snapshot={boot:page.boot,captured:BigInt(page.captured_ns),users:page.users.map(u=>({id:u.id,io:{logicalRead:BigInt(u.read_bytes??0),logicalWrite:BigInt(u.write_bytes??0)}}))};
  const old=traffic.get(page.node);
  if(old&&snapshot.boot===old.snapshot.boot&&snapshot.captured<=old.snapshot.captured)return;
  traffic.set(page.node,{snapshot,previous:old?.snapshot,received:Date.now()});
 }
 function userThroughput(id){
  const nodes=new Set(statsEntries().map(e=>e.snapshot.node));
  if(!nodes.size)for(const node of traffic.keys())nodes.add(node);
  const rates=['logicalRead','logicalWrite'].map(key=>{let total=0n;for(const node of nodes){const value=entityRate(traffic.get(node),'users',id,key);if(value==null)return null;total+=value}return nodes.size?total:null});
  return throughputHTML(...rates,true);
 }
 function refreshThroughput(){for(const cell of $('user-rows').querySelectorAll('[data-user-throughput]'))cell.innerHTML=userThroughput(cell.dataset.userThroughput)}
 function scheduleTraffic(){clearTimeout(trafficPoll);if(!$('users-view').hidden)trafficPoll=setTimeout(refreshTraffic,3000)}
 async function refreshTraffic(){
  if($('users-view').hidden)return;
  const seq=loading,ids=users.slice(0,1000).map(u=>u.id);
  if(!ids.length){scheduleTraffic();return}
  try{
   const connectionNow=connection();
   const page=await connectionNow.listUsersPage('',ids.length,'',ids);if(seq!==loading)return;observeTraffic(page);
   const local=page.node;
   await Promise.all(statsEntries().filter(e=>e.snapshot.node!==local).map(async entry=>{
    try{const remote=await connectionNow.forNode(entry.snapshot.node).listUsersPage('',ids.length,'',ids);if(seq===loading)observeTraffic(remote)}catch{}
   }));
   if(seq===loading)refreshThroughput();
  }catch(e){if(seq===loading)$('users-error').textContent=e.message}finally{if(seq===loading)scheduleTraffic()}
 }
 $('user-search').oninput=()=>{clearTimeout(searchTimer);searchTimer=setTimeout(load,250)};
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
 function openDelete(id){selected=users.find(u=>u.id===id);if(!selected||selected.name===currentUsername())return;deleteRetry=deleteJobs.get(id)??null;$('user-delete-buckets').checked=deleteRetry?.deleteBuckets??true;$('user-delete-description').textContent=`Soll „${selected.name}“ wirklich gelöscht werden?`;$('user-delete-error').textContent='';$('user-delete-dialog').showModal()}
 $('user-delete-form').addEventListener('submit',async e=>{
  e.preventDefault();const user=selected,owner=connection();if(!user||deleteJobs.get(user.id)?.state==='pending')return;
  const deleteBuckets=$('user-delete-buckets').checked,signature=JSON.stringify([user.id,deleteBuckets]);
  if(deleteRetry?.signature!==signature)deleteRetry={signature,id:newID()};
  const job={...deleteRetry,deleteBuckets,state:'pending'};deleteJobs.set(user.id,job);
  $('user-delete-dialog').close();render();
  try{await owner.deleteUser(user.id,job.id,deleteBuckets)}catch(err){
   if(connection()!==owner)return;if(err.status!==404){job.state='failed';job.error=err.message;render();toast(`User „${user.name}“ konnte nicht gelöscht werden: ${err.message}`,true);return;}
  }
  if(connection()!==owner)return;deleteJobs.delete(user.id);toast(`User „${user.name}“ wurde gelöscht.`);
  await Promise.allSettled([load(true),onUserDeleted()]);
 });
 return {load,clear(){keys.clear();loading++;clearTimeout(trafficPoll);traffic.clear();clearTimeout(searchTimer);pager.clear();pageMeta={};roleRetries.clear();deleteJobs.clear();users=[];render()}};
}

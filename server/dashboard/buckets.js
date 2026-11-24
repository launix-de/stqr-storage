// SPDX-License-Identifier: GPL-3.0-only
import {bucketThroughput} from './throughput.js';
import {EndlessList} from './endless.js';
import {newID,Writer} from './protocol.js';
import {writeBucketPolicy,parseQuota,defaultBucketPolicy} from './bucket-codec.js';
import {formatSize,sizeLabel,sizeSteps} from './size.js';
const $=id=>document.getElementById(id),SYSTEM='53545152535953010000000000000000';
const quotaText=n=>BigInt(n)<1024n?`${n}B`:formatSize(BigInt(n));
const policyBytes=b=>String(writeBucketPolicy(new Writer(),b).bytes());
function quotaUsageBase(b,escape){
 const l=b.policy.limits,accounting=l.quota_accounting==='logical'?'Logisch':'Physisch';
 if(l.absolute_unlimited)return `<span class="bucket-quota">Unbegrenzt</span><div class="bucket-sub">${accounting}</div>`;
 const quota=BigInt(l.absolute_quota_bytes);
 if(b.used_bytes==null)return `<span class="bucket-quota">${escape(sizeLabel(quota))}</span><div class="bucket-sub" title="${escape(b.usage_error??'')}">Belegung nicht verfügbar · ${accounting}</div>`;
 const used=BigInt(b.used_bytes),percent=quota>0n?Number((used*10000n/quota)>10000n?10000n:used*10000n/quota)/100:used>0n?100:0;
 return `<div class="bucket-usage"><div class="usage-label"><strong title="${used} Bytes belegt">${escape(used<1024n?quotaText(used):sizeLabel(used))}</strong><span title="${quota} Bytes Kontingent">von ${escape(sizeLabel(quota))}</span></div><div class="usage-track" role="progressbar" aria-label="${escape('Kontingent für '+b.name)}" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${percent}" aria-valuetext="${escape(quotaText(used)+' von '+sizeLabel(quota))}"><i class="usage-used" data-percent="${percent}"></i></div><div class="bucket-sub">${accounting}</div></div>`;
}
function bucketUsage(b,escape){
 let html=quotaUsageBase(b,escape),g=BigInt(b.policy.limits.guaranteed_quota_bytes);if(g===0n)return html;
 html+=`<div class="bucket-sub">${escape(sizeLabel(g))} garantiert${b.guaranteed_remaining_physical!=null?' · '+escape(sizeLabel(b.guaranteed_remaining_physical))+' physisch geschützt':''}</div>`;
 if(BigInt(b.guarantee_estimate_shortfall??0)>0n)html+=`<div class="error">Zusicherung unterdeckt · ${escape(sizeLabel(b.guarantee_estimate_shortfall))} Reserve fehlt</div>`;
 return html;
}
const permissions=[[1,'Lesen'],[2,'Schreiben'],[4,'Anhängen'],[8,'Löschen']];
export function setupBucketViews({connection,escape,toast,showView,statsEntries=()=>[]}){
 let groups=[],buckets=[],users=[],epoch=0,poll,editor,createProgress,access,grantRetry,action,actionRetry,editorPending=false;
 let pageMeta={},searchTimer,userChoices=[],userSearchTimer,userSearchEpoch=0;
 let quotaInfo=null,quotaSteps=[],quotaEpoch=0,quotaPoll,grantRows=[],lockRetry;
 async function fetchGroups(){const out=[];let after='';const seen=new Set;for(;;){const page=await connection().listPlacementGroups(after);out.push(...page.groups);if(!page.next)return out;if(seen.has(page.next))throw Error('Ungültige PG-Folgeseite.');seen.add(page.next);after=page.next}}
 function applyGroup(){const g=groups.find(g=>g.id===$('bucket-pg').value);if(!g)return;const p=(g.desired??g.effective).storage;$('bucket-storage').value=p.storage_strategy;$('bucket-compression').value=p.compression;$('bucket-placement').value=p.placement_mode;$('bucket-replicas').value=p.redundancy.replicas||1;updateFields()}
 $('bucket-pg').onchange=()=>{applyGroup();refreshQuota()};
 const pager=new EndlessList({container:$('bucket-rows').closest('.storage-panel'),visible:()=>!$('buckets-view').hidden,fetchPage:(after,limit,query)=>connection().listBucketsPage(after,limit,query),entries:page=>page.buckets,onUpdate:(items,state)=>{buckets=items;pageMeta=state.meta;render(state);schedulePoll()},onError:e=>{$('buckets-error').textContent=e.message},onLoading:busy=>{$('refresh-buckets').disabled=busy}});
 async function load(){const seq=++epoch;clearTimeout(poll);$('buckets-error').textContent='';try{groups=await fetchGroups();if(seq!==epoch)return;await pager.reset($('bucket-search').value.trim())}catch(e){if(seq===epoch)$('buckets-error').textContent=e.message}}
 function schedulePoll(){clearTimeout(poll);if(!$('buckets-view').hidden&&buckets.some(b=>!b.policy.limits.absolute_unlimited||BigInt(b.policy.limits.guaranteed_quota_bytes)>0n||b.desired_policy||b.state==='deleting'))poll=setTimeout(refreshVisible,3000)}
 async function refreshVisible(){
  if($('buckets-view').hidden)return;const seq=epoch;
  const ids=[...$('bucket-rows').querySelectorAll('[data-bucket]')].filter(row=>{const r=row.getBoundingClientRect();return r.bottom>=0&&r.top<=innerHeight}).map(row=>row.dataset.bucket);
  if(!ids.length){schedulePoll();return}
  try{const page=await connection().listBucketsPage('',Math.max(1,Math.min(1000,ids.length)),$('bucket-search').value.trim(),ids);if(seq!==epoch)return;
   const updates=new Map(page.buckets.map(b=>[b.id,b])),removed=new Set(ids.filter(id=>!updates.has(id)));pager.items=pager.items.filter(b=>!removed.has(b.id)).map(b=>updates.get(b.id)??b);buckets=pager.items;pageMeta.all_total=page.all_total;if(removed.size)pageMeta.total=Math.max(0,(pageMeta.total??buckets.length)-removed.size);
   for(const row of $('bucket-rows').querySelectorAll('[data-bucket]')){const b=updates.get(row.dataset.bucket);if(removed.has(row.dataset.bucket)){row.remove();continue}if(b){const wrapper=document.createElement('tbody');wrapper.innerHTML=rowHTML(b);row.replaceChildren(...wrapper.firstElementChild.childNodes)}}if(!buckets.length)$('bucket-rows').innerHTML='<tr><td colspan="8" class="empty">Keine passenden Buckets.</td></tr>';bindRows();pager.status();$('bucket-count').textContent=String(pageMeta.total??buckets.length);$('nav-buckets-count').textContent=String(page.all_total);$('bucket-metric-count').textContent=String(page.all_total);
  }catch(e){if(seq===epoch)$('buckets-error').textContent=e.message}finally{if(seq===epoch)schedulePoll()}
 }
 function render({append=false,added=buckets,loading=false}={}){
  $('bucket-count').textContent=String(pageMeta.total??buckets.length);$('nav-buckets-count').textContent=String(pageMeta.all_total??buckets.length);
  const active=pageMeta.active??0,locked=pageMeta.locked??0;
  $('bucket-summary').textContent=`${active} aktiv${locked?' · '+locked+' gesperrt':''}`;
  $('bucket-metric-count').textContent=String(pageMeta.all_total??buckets.length);$('bucket-metric-state').textContent=$('bucket-summary').textContent;
  if(loading&&!append)return;
  const table=$('bucket-rows');if(append)table.insertAdjacentHTML('beforeend',added.map(rowHTML).join(''));else{
   const previous=new Map([...table.querySelectorAll('[data-bucket]')].map(row=>[row.dataset.bucket,row]));
   const rows=buckets.map(b=>{const wrapper=document.createElement('tbody');wrapper.innerHTML=rowHTML(b);const next=wrapper.firstElementChild,row=previous.get(b.id);if(!row)return next;row.replaceChildren(...next.childNodes);return row});
   if(rows.length)table.replaceChildren(...rows);else table.innerHTML='<tr><td colspan="8" class="empty">Keine passenden Buckets.</td></tr>';
  }bindRows();
 }
 function rowHTML(b){
   const p=b.policy,system=b.id===SYSTEM,busy=b.state==='deleting',locked=b.state==='locked';
   const disabled=system||busy?'disabled':'',changing=!!b.desired_policy;
   return `<tr data-bucket="${b.id}"><td><div class="user-cell"><span class="bucket-avatar">▱</span><div><div class="volume-name"><button type="button" class="bucket-open" data-bucket-edit="${b.id}" ${disabled||changing?'disabled':''}>${escape(b.name)}</button>${system?'<span class="system-label">SYSTEM</span>':''}</div><div class="volume-path">${escape(b.id)}</div></div></div></td><td><span class="class-badge ${p.storage_strategy}">${p.storage_strategy.toUpperCase()}</span><div class="bucket-sub">${escape(groups.find(g=>g.id===p.placement_group)?.name??'PG')} · v${p.placement_mask_version??'—'}</div><div class="bucket-sub">${p.placement_mode==='everywhere'?'Jede Node':p.redundancy.replicas+' Kopie'+(p.redundancy.replicas===1?'':'n')}</div></td><td><span class="class-badge">${escape(p.compression.toUpperCase())}</span></td><td>${bucketUsage(b,escape)}</td><td data-bucket-throughput="${b.id}">${bucketThroughput(statsEntries(),b.id)}</td><td>${system?'Alle Admins':(b.grants??[]).length+' User'}</td><td><span class="state-badge ${busy?'draining':locked?'detached':''}"><i></i>${busy?'Wird gelöscht':changing?'Wird umgestellt · PG v'+(b.desired_policy.placement_mask_version??'—'):locked?'Gesperrt':'Aktiv'}</span></td><td><div class="row-actions bucket-actions"><button class="row-action" data-bucket-action="rename" data-id="${b.id}" ${disabled}>Umbenennen</button><button class="row-action danger-action" data-bucket-action="delete" data-id="${b.id}" ${disabled}>Löschen</button></div></td></tr>`;
 }
 function bindRows(){
  for(const bar of $('bucket-rows').querySelectorAll('[data-percent]'))bar.style.width=bar.dataset.percent+'%';
  for(const b of document.querySelectorAll('[data-bucket-edit]'))b.onclick=()=>openEditor(b.dataset.bucketEdit);
  for(const row of document.querySelectorAll('[data-bucket]'))row.onclick=e=>{if(!e.target.closest('button,a,input,select'))openEditor(row.dataset.bucket)};
  for(const b of document.querySelectorAll('[data-bucket-action]'))b.onclick=()=>openAction(b.dataset.id,b.dataset.bucketAction);
 }
 $('refresh-buckets').onclick=()=>load();$('bucket-search').oninput=()=>{clearTimeout(searchTimer);searchTimer=setTimeout(()=>load(),250)};$('add-bucket').onclick=()=>openCreate();
 function fillEditor(b,create){
  const p=b.policy,l=p.limits;editor={bucket:b,create};createProgress=null;
 $('bucket-pg').innerHTML=groups.filter(g=>g.id!=='53545152535953504701000000000000').map(g=>`<option value="${g.id}">${escape(g.name)}</option>`).join('');$('bucket-pg').value=p.placement_group??groups.find(g=>g.name==='Standard')?.id??groups[0]?.id??'';
  quotaInfo=null;quotaEpoch++;clearTimeout(quotaPoll);$('bucket-create-form').reset();userChoices=[];userSearchEpoch++;$('bucket-policy-fields').disabled=false;$('bucket-create-error').textContent='';$('bucket-editor-note').textContent='';
  $('bucket-editor-title').textContent=create?'Bucket anlegen':`Einstellungen · ${b.name}`;$('bucket-create-submit').textContent=create?'Bucket anlegen':'Änderungen speichern';$('bucket-create-submit').disabled=true;
  $('bucket-name').value=b.name;$('bucket-name').readOnly=!create;$('bucket-storage').value=p.storage_strategy;$('bucket-compression').value=p.compression;$('bucket-placement').value=p.placement_mode;
  $('bucket-replicas').value=String(p.redundancy.replicas||1);$('bucket-unlimited').checked=l.absolute_unlimited;$('bucket-quota').value=l.absolute_unlimited&&BigInt(l.absolute_quota_bytes)===0n?'1G':quotaText(l.absolute_quota_bytes);$('bucket-quota').setCustomValidity('');
  $('bucket-accounting').value=l.quota_accounting;$('bucket-guaranteed').value=quotaText(l.guaranteed_quota_bytes);$('bucket-object-size').innerHTML=(create||l.metadata_reserve_ppm===150000)?'<option value="5">1M (Blockdevices)</option>':'<option value="255">Bestehende Einstellung</option>';$('bucket-owner-section').hidden=!create;
  $('bucket-owner').innerHTML='<option value="">Keine zusätzliche Freigabe</option>';
  $('bucket-access-section').hidden=create;$('bucket-state-section').hidden=create;access=null;grantRetry=null;
  applyGroup();updateFields();
 }
 async function userCandidates(query,ids=[]){
  const page=await connection().listUsersPage('',50,query),known=[];
  for(let i=0;i<ids.length;i+=1000){const selected=await connection().listUsersPage('',1000,'',ids.slice(i,i+1000));known.push(...selected.users)}
  return {choices:page.users,known};
 }
 function rememberUsers(result){const known=new Map(users.map(u=>[u.id,u]));for(const user of [...result.choices,...result.known])known.set(user.id,user);users=[...known.values()];userChoices=result.choices}
 function renderOwnerOptions(selected=''){
  const choices=[...userChoices],current=users.find(u=>u.id===selected);if(current&&!choices.some(u=>u.id===current.id))choices.unshift(current);
  $('bucket-owner').innerHTML='<option value="">Keine zusätzliche Freigabe</option>'+choices.map(u=>`<option value="${u.id}">${escape(u.name)}${u.disabled?' (deaktiviert)':''}</option>`).join('');$('bucket-owner').value=selected;
 }
 for(const id of ['bucket-owner-search','bucket-grant-search'])$(id).oninput=()=>{clearTimeout(userSearchTimer);const seq=++userSearchEpoch,owner=editor;userSearchTimer=setTimeout(async()=>{try{const selected=$('bucket-owner').value,result=await userCandidates($(id).value.trim());if(seq!==userSearchEpoch||editor!==owner)return;rememberUsers(result);if(id==='bucket-owner-search')renderOwnerOptions(selected);else renderAccess()}catch(e){if(seq===userSearchEpoch)$(id==='bucket-owner-search'?'bucket-create-error':'bucket-access-error').textContent=e.message}},250)};
 async function openCreate({name='',userID=''}={}){
  showView('buckets');groups=await fetchGroups();const draft={id:newID(),name,state:'active',policy:defaultBucketPolicy()};fillEditor(draft,true);$('bucket-create-dialog').showModal();$('bucket-name').focus();
  $('bucket-create-submit').disabled=true;let ready=false;const capacityReady=refreshQuota();
  try{const result=await userCandidates('',userID?[userID]:[]);if(editor?.bucket!==draft||!$('bucket-create-dialog').open)return;rememberUsers(result);renderOwnerOptions(userID);
   if(userID){if(!users.some(u=>u.id===userID))throw Error('Der neue Benutzer ist nicht mehr vorhanden.');$('bucket-owner').value=userID}
   await capacityReady;ready=!!quotaInfo;
  }catch(e){$('bucket-create-error').textContent=e.message;return}finally{if(editor?.bucket===draft)$('bucket-create-submit').disabled=!ready}
 }
 function openEditor(id){const b=buckets.find(b=>b.id===id);if(!b||b.id===SYSTEM||b.desired_policy||b.state==='deleting')return;fillEditor(b,false);$('bucket-create-dialog').showModal();refreshQuota();renderState(b);openAccess(id)}
 function renderState(b){$('bucket-lock').checked=b.state==='locked';$('bucket-lock').disabled=false;$('bucket-state-error').textContent='';lockRetry=null}
 $('bucket-lock').onchange=async()=>{
  const control=$('bucket-lock'),owner=editor,b=owner.bucket,locked=control.checked;
  if(lockRetry?.locked!==locked)lockRetry={locked,id:newID()};control.disabled=true;$('bucket-state-error').textContent='';
  try{await connection().lockBucket(b.id,locked,lockRetry.id);if(editor===owner)lockRetry=null;await load()}
  catch(e){if(editor===owner){control.checked=!locked;$('bucket-state-error').textContent=e.message}}
  finally{if(editor===owner)control.disabled=false}
 };
 function updateFields(){
  const ram=$('bucket-storage').value==='ram';$('bucket-storage-note').textContent=ram?'RAM-Inhalte bleiben nur erhalten, solange eine Replik lebt.':'HDD darf bei Bedarf auf SSD liegen. SSD bleibt auf SSD.';
  const unlimited=$('bucket-unlimited').checked;$('bucket-quota').disabled=unlimited;$('bucket-quota').required=!unlimited;if(unlimited)$('bucket-quota').setCustomValidity('');
  $('bucket-object-size').disabled=$('bucket-accounting').value==='physical';
  $('bucket-replicas-field').hidden=$('bucket-placement').value==='everywhere';syncQuota();
 }
 $('bucket-storage').onchange=()=>{updateFields();refreshQuota()};$('bucket-object-size').onchange=()=>refreshQuota();$('bucket-accounting').onchange=()=>{updateFields();refreshQuota()};for(const id of ['bucket-unlimited','bucket-placement'])$(id).onchange=updateFields;
 async function refreshQuota(){
  clearTimeout(quotaPoll);const seq=++quotaEpoch,owner=editor,strategy=$('bucket-storage').value,profile=Number($('bucket-object-size').value);
  quotaInfo=null;syncQuota();$('bucket-create-submit').disabled=true;
  try{const info=await connection().bucketQuotaCapacity(owner.create?'0'.repeat(32):owner.bucket.id,strategy,profile,owner.bucket.policy.limits.metadata_reserve_ppm,$('bucket-pg').value,$('bucket-accounting').value);
   if(seq!==quotaEpoch||editor!==owner||!$('bucket-create-dialog').open)return;
   quotaInfo=info;const match=info.profiles.find(p=>p.overhead===info.overhead),selected=profile===255&&match?match.key:profile;
   $('bucket-object-size').innerHTML=(selected===255?'<option value="255">Bestehende Einstellung</option>':'')+info.profiles.map(p=>`<option value="${p.key}">${escape(p.label)}</option>`).join('');$('bucket-object-size').value=String(selected);
   if(owner.create&&!owner.initialCapacity&&parseQuota($('bucket-quota').value)>info.maximum)$('bucket-quota').value=quotaText(info.maximum);
   owner.initialCapacity=true;syncQuota();$('bucket-create-submit').disabled=editorPending;
  }catch(e){if(seq===quotaEpoch){$('bucket-quota-capacity').textContent=e.message;$('bucket-create-submit').disabled=true}}
  finally{if(seq===quotaEpoch&&$('bucket-create-dialog').open)quotaPoll=setTimeout(()=>{if(!editorPending)refreshQuota()},3000)}
 }
 function syncQuota(){
  const slider=$('bucket-quota-slider'),input=$('bucket-quota'),unlimited=$('bucket-unlimited').checked;
  $('bucket-quota-slider-wrap').hidden=false;$('bucket-quota-max').disabled=!quotaInfo;slider.disabled=!quotaInfo||unlimited;
  if(!quotaInfo){$('bucket-quota-capacity').textContent='Verfügbarer Platz wird ermittelt …';$('bucket-quota-limit').textContent='—';$('bucket-quota-value').textContent='—';input.setCustomValidity('');return}
  const info=quotaInfo;$('bucket-quota-capacity').textContent=`${sizeLabel(info.available)} frei${info.reservedOther?' · '+sizeLabel(info.reservedOther)+' geschützt':''}${info.used?' · '+sizeLabel(info.used)+' bereits genutzt':''}`;
  $('bucket-quota-min').textContent=sizeLabel(info.used);$('bucket-quota-limit').textContent=sizeLabel(info.maximum);$('bucket-quota-limit').title=info.maximum+' Bytes';
  try{const value=parseQuota(input.value);if(!unlimited&&(value<info.used||value>info.maximum))throw Error(`Kontingent muss zwischen ${quotaText(info.used)} und ${quotaText(info.maximum)} liegen.`);
   input.setCustomValidity('');quotaSteps=sizeSteps(info.maximum,value,info.used,1n);slider.min='0';slider.max=String(Math.max(0,quotaSteps.length-1));slider.step='1';slider.value=String(Math.max(0,quotaSteps.indexOf(value)));$('bucket-quota-value').textContent=unlimited?'Unbegrenzt':sizeLabel(value);slider.setAttribute('aria-valuetext',unlimited?'Unbegrenzt':quotaText(value));
  }catch(e){input.setCustomValidity(unlimited?'':e.message)}
 }
 $('bucket-quota').oninput=syncQuota;
 $('bucket-quota').onchange=()=>{syncQuota();if($('bucket-quota').validity.valid)$('bucket-quota').value=quotaText(parseQuota($('bucket-quota').value))};
 $('bucket-quota-slider').oninput=()=>{const value=quotaSteps[Number($('bucket-quota-slider').value)];if(value===undefined)return;$('bucket-quota').value=quotaText(value);$('bucket-quota').setCustomValidity('');$('bucket-quota-value').textContent=sizeLabel(value);$('bucket-quota-slider').setAttribute('aria-valuetext',quotaText(value))};
 $('bucket-quota-max').onclick=async()=>{await refreshQuota();if(!quotaInfo)return;$('bucket-unlimited').checked=false;$('bucket-quota').value=quotaText(quotaInfo.maximum);updateFields()};
 $('bucket-create-dialog').addEventListener('close',()=>{quotaEpoch++;clearTimeout(quotaPoll)});

 function readEditor(){
  if(!quotaInfo)throw Error('Kapazität wird noch ermittelt.');const base=editor.bucket,p=structuredClone(base.policy),unlimited=$('bucket-unlimited').checked;
  const group=groups.find(g=>g.id===$('bucket-pg').value);if(!group)throw Error('Bitte eine Placement Group auswählen.');Object.assign(p,structuredClone((group.desired??group.effective).storage));p.placement_group=group.id;
  p.limits={...p.limits,quota_accounting:$('bucket-accounting').value,absolute_unlimited:unlimited,absolute_quota_bytes:unlimited?base.policy.limits.absolute_quota_bytes:parseQuota($('bucket-quota').value).toString(),metadata_reserve_ppm:quotaInfo.overhead,guaranteed_quota_bytes:parseQuota($('bucket-guaranteed').value).toString()};
  if(!unlimited&&BigInt(p.limits.guaranteed_quota_bytes)>BigInt(p.limits.absolute_quota_bytes))throw Error('Garantierter Platz darf die maximale Größe nicht überschreiten.');
  const name=$('bucket-name').value;if(!name||new TextEncoder().encode(name).length>255||name.includes('\0'))throw Error('Der Name muss 1 bis 255 UTF-8-Bytes lang sein.');
  return {...base,name,policy:p};
 }
 $('bucket-create-dialog').addEventListener('cancel',e=>{if(editorPending)e.preventDefault()});
 $('bucket-create-form').addEventListener('submit',async e=>{
  e.preventDefault();$('bucket-create-error').textContent='';const submit=e.submitter;submit.disabled=true;editorPending=true;for(const b of document.querySelectorAll('[data-close="bucket-create-dialog"]'))b.disabled=true;
  try{
   if(!createProgress?.created){await refreshQuota();if(!$('bucket-unlimited').checked&&!$('bucket-quota').reportValidity())return}
   const b=createProgress?.created?createProgress.bucket:readEditor(),owner=createProgress?.created?createProgress.owner:$('bucket-owner').value;
   const signature=policyBytes(b)+':'+b.policy.placement_group+':'+owner;if(createProgress?.signature!==signature)createProgress={signature,id:newID(),grantID:newID(),bucket:b,owner,created:false};
   let result;
   if(editor.create){
    if(!createProgress.created){await connection().createBucket(b,createProgress.id);createProgress.created=true;$('bucket-policy-fields').disabled=true}
    if(owner)await connection().grantBucket(b.id,owner,15,createProgress.grantID);
   }else{
    const base=editor.bucket;
    if(policyBytes(b)===policyBytes(base)&&b.policy.placement_group===base.policy.placement_group){$('bucket-create-dialog').close();return}
    // A quota-only edit does not need a data/policy migration.
    const withoutQuota={...b,policy:{...b.policy,limits:base.policy.limits}};
    if(b.policy.placement_group!==base.policy.placement_group)result=await connection().setBucketPlacementGroup(b.id,base.policy.version,b.policy.placement_group,createProgress.id,b.policy.limits);
    else if(policyBytes(withoutQuota)===policyBytes(base))result=await connection().setBucketQuota(b,base.policy.version,createProgress.id);
    else result=await connection().setBucketPlacementGroup(b.id,base.policy.version,b.policy.placement_group,createProgress.id,b.policy.limits)
   }
   $('bucket-create-dialog').close();toast(editor.create?'Bucket wurde angelegt.':result?.state==='running'?'Bucket wird umgestellt.':'Bucket-Einstellungen gespeichert.');await load();
  }catch(err){$('bucket-create-error').textContent=createProgress?.created?'Bucket ist angelegt. Die Benutzerfreigabe fehlt noch: '+err.message:err.message;if(createProgress?.created)submit.textContent='Freigabe erneut versuchen';if(err.status===409&&!editor.create){$('bucket-editor-note').textContent='Die Konfiguration wurde zwischenzeitlich geändert. Bitte schließen und erneut öffnen.';await load()}}
  finally{submit.disabled=false;editorPending=false;for(const b of document.querySelectorAll('[data-close="bucket-create-dialog"]'))b.disabled=false}
 });
 async function openAccess(id){
  const b=buckets.find(b=>b.id===id);if(!b||b.id===SYSTEM||b.state==='deleting')return;
  access=b;grantRows=[];grantRetry=new Map();$('bucket-access-error').textContent='';$('bucket-grant-add').disabled=true;$('bucket-grants').innerHTML='<tr><td colspan="6" class="field-note">Benutzer werden geladen …</td></tr>';
  try{const result=await userCandidates('',(b.grants??[]).map(g=>g.user_id));if(access?.id!==id||!$('bucket-create-dialog').open)return;rememberUsers(result);grantRows=(b.grants??[]).map(g=>({user:g.user_id,mask:g.permissions}));renderAccess()}catch(e){$('bucket-access-error').textContent=e.message}
 }
 function availableUsers(row){const choices=userChoices.filter(u=>!grantRows.some(other=>other!==row&&other.user===u.id));if(row?.user&&!choices.some(u=>u.id===row.user)){const current=users.find(u=>u.id===row.user);if(current)choices.unshift(current)}return choices}
 function renderAccess(){
  $('bucket-grants').innerHTML=grantRows.length?grantRows.map((row,i)=>{
   const u=users.find(u=>u.id===row.user),name=u?.name??'Gelöschter User';
   const user=row.draft?`<select data-grant-user="${i}" aria-label="Benutzer auswählen">${availableUsers(row).map(u=>`<option value="${u.id}" ${u.id===row.user?'selected':''}>${escape(u.name)}${u.disabled?' (deaktiviert)':''}</option>`).join('')}</select>`:`<strong>${escape(name)}</strong>`;
   return `<tr data-grant-row="${i}"><td>${user}</td>${permissions.map(([bit,label])=>`<td><input type="checkbox" data-grant-permission="${bit}" aria-label="${escape(label+' für '+name)}" ${row.mask&bit?'checked':''}></td>`).join('')}<td><button type="button" class="icon-button danger-action" data-grant-remove="${i}" aria-label="${escape('Zugriff für '+name+' entfernen')}" title="Benutzer entfernen">×</button></td></tr>`;
  }).join(''):'<tr><td colspan="6" class="field-note">Noch keine Benutzer freigegeben.</td></tr>';
  $('bucket-grant-add').disabled=!availableUsers(null).length;
  for(const select of document.querySelectorAll('[data-grant-user]'))select.onchange=()=>{grantRows[Number(select.dataset.grantUser)].user=select.value;renderAccess()};
  for(const control of document.querySelectorAll('[data-grant-permission]'))control.onchange=()=>{
   const tr=control.closest('[data-grant-row]'),i=Number(tr.dataset.grantRow),mask=[...tr.querySelectorAll('[data-grant-permission]:checked')].reduce((n,c)=>n|Number(c.dataset.grantPermission),0);saveGrant(i,mask,false);
  };
  for(const button of document.querySelectorAll('[data-grant-remove]'))button.onclick=()=>saveGrant(Number(button.dataset.grantRemove),0,true);
 }
 $('bucket-grant-add').onclick=()=>{const user=availableUsers(null)[0];if(!user)return;grantRows.push({user:user.id,mask:0,draft:true});renderAccess();$('bucket-grants').querySelector('[data-grant-row]:last-child select')?.focus()};
 async function saveGrant(index,mask,remove){
  const owner=access,row=grantRows[index];if(!owner||!row)return;
  const signature=`${row.user}:${mask}`;if(!grantRetry.has(signature))grantRetry.set(signature,newID());const operation=grantRetry.get(signature);
  for(const control of $('bucket-grants').querySelectorAll('input,select,button'))control.disabled=true;$('bucket-grant-add').disabled=true;$('bucket-access-error').textContent='';
  try{
   if(mask)await connection().grantBucket(owner.id,row.user,mask,operation);else await connection().revokeBucket(owner.id,row.user,operation);
   if(access!==owner||!$('bucket-create-dialog').open)return;for(const key of grantRetry.keys())if(key.startsWith(row.user+':'))grantRetry.delete(key);row.mask=mask;row.draft=false;if(remove)grantRows.splice(index,1);await load();
  }catch(e){if(access===owner)$('bucket-access-error').textContent=e.message}
  finally{if(access===owner&&$('bucket-create-dialog').open)renderAccess()}
 }
 function openAction(id,kind){
  const b=buckets.find(b=>b.id===id);if(!b||b.id===SYSTEM||b.state==='deleting')return;action={bucket:b,kind};actionRetry=null;
  const locked=b.state==='locked',title=kind==='rename'?'Bucket umbenennen':kind==='delete'?'Bucket löschen':locked?'Bucket entsperren':'Bucket sperren';
  $('bucket-action-title').textContent=title;$('bucket-action-submit').textContent=title;$('bucket-action-submit').classList.toggle('danger-button',kind==='delete');$('bucket-action-description').textContent=`„${b.name}“`;$('bucket-action-error').textContent='';
  $('bucket-rename-field').hidden=kind!=='rename';$('bucket-new-name').required=kind==='rename';$('bucket-new-name').value=b.name;
  $('bucket-action-note').hidden=kind==='rename';$('bucket-action-note').textContent=kind==='delete'?'Der Bucket und alle darin gespeicherten Objekte werden gelöscht.':locked?'Schreibzugriffe werden wieder freigegeben.':'Objekte bleiben lesbar. Schreibzugriffe werden gesperrt.';$('bucket-action-dialog').showModal();
 }
 $('bucket-action-form').addEventListener('submit',async e=>{
  e.preventDefault();e.submitter.disabled=true;$('bucket-action-error').textContent='';
  const {bucket:b,kind}=action,name=$('bucket-new-name').value,signature=`${b.id}:${kind}:${kind==='rename'?name:b.state}`;if(actionRetry?.signature!==signature)actionRetry={signature,id:newID()};
  try{if(kind==='rename')await connection().renameBucket(b.id,name,actionRetry.id);else if(kind==='delete')await connection().deleteBucket(b.id,actionRetry.id);else await connection().lockBucket(b.id,b.state!=='locked',actionRetry.id);
   $('bucket-action-dialog').close();toast(kind==='delete'?'Bucket wird gelöscht.':kind==='rename'?'Bucket wurde umbenannt.':b.state==='locked'?'Bucket wurde entsperrt.':'Bucket wurde gesperrt.');await load();
  }catch(err){$('bucket-action-error').textContent=err.message}finally{e.submitter.disabled=false}
 });
 function refreshThroughput(){for(const cell of $('bucket-rows').querySelectorAll('[data-bucket-throughput]'))cell.innerHTML=bucketThroughput(statsEntries(),cell.dataset.bucketThroughput)}
 return {load,openCreate,refreshThroughput,clear(){epoch++;clearTimeout(poll);clearTimeout(searchTimer);pager.clear();pageMeta={};userSearchEpoch++;clearTimeout(userSearchTimer);userChoices=[];buckets=[];users=[];render()}};
}

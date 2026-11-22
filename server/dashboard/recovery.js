// SPDX-License-Identifier: GPL-3.0-only
export function setupRecovery({connection,config,states,toast,refresh,newID}){
 const $=id=>document.getElementById(id);
 let nodeAction,volumeAction,timer;
 const scoped=target=>connection().forNode(target);
 function stop(){clearTimeout(timer);timer=undefined}
 function busy(value){$('recovery-stage-fields').disabled=value;$('recovery-commit-fields').disabled=value}
 async function refreshVolumes(action){
  const view=await scoped(action.target).getConfig();
  if(volumeAction!==action)return;
  const list=$('recovery-volume-list');list.replaceChildren();
  const volumes=[...(view.config.recovery_volumes??[]),...(view.config.volumes??[]).filter(v=>v.recovery_only)];
  for(const volume of volumes){
   const row=document.createElement('label');row.className='checkbox-option';
   const checkbox=document.createElement('input');checkbox.type='checkbox';checkbox.value=volume.id;checkbox.name='recovery-volume';
   const label=document.createElement('span');label.textContent=`${volume.name} · ${volume.path}${volume.state==='recovery'?' · vorgemerkt':' · bereits eingebunden'}`;
   row.append(checkbox,label);list.append(row);
   if(volume.state==='recovery'){
    const release=document.createElement('button');release.type='button';release.className='text-button';release.textContent='Freigeben';
    release.onclick=async()=>{release.disabled=true;try{await scoped(action.target).releaseRecoveryVolume(volume.id,newID());await refreshVolumes(action)}catch(e){$('recovery-volume-error').textContent=e.message;release.disabled=false}};
    list.append(release);
   }
  }
  if(!volumes.length)list.textContent='Noch keine geretteten Datenträger vorgemerkt.';
  action.epoch=BigInt(view.membership_epoch);
 }
 async function complete(action,isNode){
  stop();$(isNode?'recovery-node-dialog':'recovery-volume-dialog').close();
  toast(isNode?'Ersatz vorbereitet. Die geretteten Volumes können jetzt eingebunden werden.':'Gerettete Datenbereiche wurden übernommen.');
  await refresh();if(!isNode)await refresh(action.target);
 }
 async function observe(action,isNode){
  if((isNode?nodeAction:volumeAction)!==action)return;
  try{
   const result=await(isNode?connection():scoped(action.target)).operationStatus(action.id);
   if(result.state==='fulfilled'){await complete(action,isNode);return}
   if(result.state==='failed'){action.pending=false;action.id=undefined;$(isNode?'recovery-node-error':'recovery-volume-error').textContent=result.detail||`Fehler ${result.status}`;if(isNode)$('recovery-node-submit').disabled=false;else busy(false);return}
  }catch(e){$(isNode?'recovery-node-error':'recovery-volume-error').textContent=e.message}
  timer=setTimeout(()=>observe(action,isNode),3000);
 }
 $('recovery-node-form').addEventListener('submit',async event=>{
  event.preventDefault();const action=nodeAction;if(!action||action.pending||!$('recovery-node-fenced').checked)return;
  action.id??=newID();action.pending=true;$('recovery-node-submit').disabled=true;$('recovery-node-error').textContent='';
  try{const result=await connection().beginNodeRecovery([action.target],action.epoch,action.id);if(result.state==='fulfilled'){await complete(action,true);return}$('recovery-node-error').textContent='Vorbereitung läuft …';timer=setTimeout(()=>observe(action,true),3000)}
  catch(e){$('recovery-node-error').textContent=e.message;action.pending=false;$('recovery-node-submit').disabled=false;if(e.status>=400&&e.status<500)action.id=undefined}
 });
 $('recovery-stage-form').addEventListener('submit',async event=>{
  event.preventDefault();const action=volumeAction;if(!action||action.pending)return;
  const name=$('recovery-volume-name').value.trim(),path=$('recovery-volume-path').value.trim();
  if(!action.stage||action.stage.name!==name||action.stage.path!==path)action.stage={name,path,id:newID()};
  busy(true);$('recovery-volume-error').textContent='';
  try{await scoped(action.target).stageRecoveryVolume(name,path,action.stage.id);action.stage=undefined;$('recovery-volume-name').value='';$('recovery-volume-path').value='';await refreshVolumes(action)}
  catch(e){$('recovery-volume-error').textContent=e.message}finally{busy(false)}
 });
 $('recovery-volume-form').addEventListener('submit',async event=>{
  event.preventDefault();const action=volumeAction;if(!action||action.pending)return;
  if(!action.id){action.volumes=[...$('recovery-volume-list').querySelectorAll('input:checked')].map(e=>e.value).sort();if(!action.volumes.length){$('recovery-volume-error').textContent='Bitte mindestens ein Volume auswählen.';return}action.id=newID();action.commitEpoch=action.epoch}
  action.pending=true;busy(true);$('recovery-volume-error').textContent='';
  try{const result=await scoped(action.target).commitRecoveredVolumes(action.volumes,action.commitEpoch,action.id);if(result.state==='fulfilled'){await complete(action,false);return}$('recovery-volume-error').textContent='Übernahme läuft …';timer=setTimeout(()=>observe(action,false),3000)}
  catch(e){$('recovery-volume-error').textContent=e.message;action.pending=false;if(e.status>=400&&e.status<500)action.id=undefined;busy(false)}
 });
 for(const id of ['recovery-node-dialog','recovery-volume-dialog'])$(id).addEventListener('close',stop);
 return {
  openNode(target){const view=config();if(!view?.member_nodes?.includes(target)||view.member_nodes.length<2)return;stop();nodeAction={target,epoch:BigInt(view.membership_epoch)};$('recovery-node-description').textContent=`„${states.get(target)?.config?.config.node.name??target}“ durch neue Hardware ersetzen.`;$('recovery-node-fenced').checked=false;$('recovery-node-error').textContent='';$('recovery-node-submit').disabled=false;$('recovery-node-dialog').showModal()},
  async openVolumes(target){stop();const action=volumeAction={target};busy(false);$('recovery-volume-error').textContent='';$('recovery-volume-list').replaceChildren();$('recovery-volume-dialog').showModal();try{await refreshVolumes(action)}catch(e){$('recovery-volume-error').textContent=e.message}}
 };
}

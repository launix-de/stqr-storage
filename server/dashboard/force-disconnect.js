// SPDX-License-Identifier: GPL-3.0-only
export function setupForceDisconnect({connection,config,states,toast,refresh,newID}){
 const $=id=>document.getElementById(id);
 let action,timer;
 function stop(){clearTimeout(timer);timer=undefined}
 function enable(){
  $('force-submit').disabled=!action||action.busy||!$('force-fenced').checked||!$('force-loss').checked;
 }
 async function observe(current){
  if(action!==current||!$('force-dialog').open)return;
  try{
   const result=await connection().operationStatus(current.id);
   if(action!==current)return;
   if(result.state==='fulfilled'){await complete(current);return}
   if(result.state==='failed'){
    $('force-error').textContent=result.detail||`Ausschluss fehlgeschlagen (${result.status}).`;
    current.busy=false;enable();return;
   }
  }catch(error){if(action===current)$('force-error').textContent=error.message}
  if(action===current&&$('force-dialog').open)timer=setTimeout(()=>observe(current),3000);
 }
 async function complete(current){
  if(action!==current)return;
  stop();$('force-dialog').close();toast('Node wurde aus dem Cluster ausgeschlossen.');
  await refresh();
 }
 $('force-dialog').addEventListener('close',stop);
 $('force-dialog').addEventListener('cancel',event=>{if(action?.busy)event.preventDefault()});
 for(const id of ['force-fenced','force-loss'])$(id).addEventListener('change',enable);
 $('force-form').addEventListener('submit',async event=>{
  event.preventDefault();
  if(!action||action.busy||!$('force-fenced').checked||!$('force-loss').checked)return;
  const current=action;current.busy=true;enable();stop();$('force-error').textContent='';
  try{
   // Preserve the original ID and observed epoch on ambiguous network outcomes.
   const result=await connection().forceDisconnect([current.target],current.epoch,true,true,current.id);
   if(action!==current)return;
   if(result.state==='fulfilled')await complete(current);
   else{
    $('force-error').textContent='Ausschluss läuft. Der Abschluss wird geprüft.';
    timer=setTimeout(()=>observe(current),3000);
   }
  }catch(error){if(action===current)$('force-error').textContent=error.message}
  finally{if(action===current){current.busy=false;enable()}}
 });
 return {open(target){
  const view=config();
  if(!view?.member_nodes?.includes(target)||view.member_nodes.length<2){toast('Mindestens eine Node muss im Cluster verbleiben.',true);return}
  stop();action={target,epoch:BigInt(view.membership_epoch),id:newID(),busy:false};
  const name=states.get(target)?.config?.config.node.name??target;
  $('force-description').textContent=`„${name}“ wird dauerhaft entfernt. Alle verbleibenden Nodes müssen die Änderung bestätigen.`;
  $('force-error').textContent='';$('force-fenced').checked=false;$('force-loss').checked=false;
  enable();$('force-dialog').showModal();
 }};
}

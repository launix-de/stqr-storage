// SPDX-License-Identifier: GPL-3.0-only
import {capacityKnown,capacityAdmissionKnown,renderCapacitySummary} from './capacity.js';
import {setupVolumeGrowth} from './volume-growth.js';
import {formatNumber,formatBytes} from './numbers.js';
import {setupClusterJoin} from './join.js';
import {NodeConnection,newID} from './protocol.js';
import {memberIDs,nodeMembership,currentSnapshots,seedMembers} from './membership.js';
import {versionWarning,versionLabel,protocolLabel} from './node-versions.js';
import {setupRecovery} from './recovery.js';
import {setupForceDisconnect} from './force-disconnect.js';
import {setupUserViews} from './users.js';
import {setupPlacementGroups} from './placement-groups.js';
import {entityRate,nodeRate,throughputHTML,bucketTotals,recordRateSnapshot} from './throughput.js';
import {setupBucketViews} from './buckets.js';
import {MIN_VOLUME_SIZE,parseSize,sizeSteps,sizeLimit,formatSize,sizeLabel} from './size.js';
const $=id=>document.getElementById(id),escape=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let node,configView,snapshot,source=0,candidates=[],volumeNode,drainID,drainVolume,addRetry,addPending=false;
const nodeStates=new Map,fetchingConfigs=new Set;
const localNode=()=>configView?.config.node.id;
const volumeConnection=()=>volumeNode&&volumeNode!==localNode()?node.forNode(volumeNode):node;
const volumeConfig=()=>nodeStates.get(volumeNode)?.config??configView;
const states=['pending','active','draining','detached','failed'],stateNames=['Ausstehend','Aktiv','Wird geleert','Ausgehängt','Fehler'],classes=['hdd','ssd','ram','ramcache'];
export const bytes=formatBytes;
function toast(message,error=false){const div=document.createElement('div');div.className='toast'+(error?' error-toast':'');div.textContent=message;while($('toasts').children.length>=3)$('toasts').firstElementChild.remove();$('toasts').append(div);setTimeout(()=>div.remove(),error?10000:5000)}
function setConnected(connected){$('connection').classList.toggle('offline',!connected);$('connection').innerHTML=`<span class="dot"></span>${connected?'Live · alle 3 Sekunden':'Verbindung wird wiederhergestellt'}`;$('refresh').disabled=!connected}
function beginSession(session){
 joinView.stop();
 $('login').hidden=true;$('app').hidden=false;$('profile-name').textContent=session.username;volumeGrowth.clear();userViews.clear();bucketViews.clear();pgViews.clear();node?.stop();nodeStates.clear();fetchingConfigs.clear();snapshot=null;configView=null;volumeNode=null;
 const connection=node=new NodeConnection(session.csrf_token,connected=>{if(node===connection)setConnected(connected)},stats=>{if(node===connection)onStats(stats)});
 connection.onOpen=async()=>{try{const [cfg]=await Promise.all([connection.getConfig(),connection.subscribeVersions(),connection.subscribeStats(),userViews.load(),bucketViews.load()]);if(node===connection)applyConfig(cfg)}catch(e){if(node===connection)toast(e.message,true)}};
 connection.onVersions=versions=>{if(node===connection){if(configView)configView.node_versions=versions;renderVolumes()}};
 connection.onProtocolError=e=>{if(node===connection)toast('Protokollfehler: '+e.message,true)};
 connection.onDisconnect=async()=>{
  // A process restart loses the in-memory session. Stop blind WebSocket retries
  // once HTTP can distinguish that from a temporarily unavailable daemon.
  const response=await sessionFetch();
  if(node!==connection||connection.stopped)return;
  if(response.status===401||response.status===403)requireLogin('Die Sitzung ist abgelaufen. Bitte erneut anmelden.');
  else if(response.ok){const session=await response.json();if(node===connection&&!connection.stopped&&session.csrf_token!==connection.csrf)beginSession(session)}
 };
}

async function sessionFetch(method='GET',headers={}){return fetch('/api/session',{method,headers,credentials:'same-origin'})}
$('login-form').addEventListener('submit',async e=>{e.preventDefault();const submit=e.submitter;submit.disabled=true;$('login-error').textContent='';try{const raw=new TextEncoder().encode(`${$('username').value}:${$('password').value}`);const response=await sessionFetch('POST',{Authorization:'Basic '+btoa(Array.from(raw,b=>String.fromCharCode(b)).join(''))});if(!response.ok){const failure=await response.json().catch(()=>({}));throw Error(response.status===503?'Die Node wartet auf die Aufnahme durch den Cluster. Bitte anschließend mit dem Zielkonto anmelden.':response.status===401?'Benutzername oder Passwort ist nicht korrekt.':response.status===403&&failure.error==='administrator required'?'Für das Dashboard ist ein Administratorkonto erforderlich.':response.status===403?'Zugriff auf das Dashboard verweigert.':'Anmeldung nicht möglich.')}const data=await response.json();$('password').value='';beginSession(data)}catch(e){$('login-error').textContent=e.message}finally{submit.disabled=false}});
$('logout').addEventListener('click',async()=>{try{await sessionFetch('DELETE',{'X-CSRF-Token':node.csrf})}catch{}volumeGrowth.clear();node?.stop();$('app').hidden=true;$('login').hidden=false;$('password').focus()});
function showView(view){
 for(const b of document.querySelectorAll('[data-view]'))b.classList.toggle('selected',b.dataset.view===view);
 for(const name of ['volumes','buckets','users','groups','settings'])$(name+'-view').hidden=view!==name;
 $('breadcrumb-current').textContent={volumes:'Volumes',buckets:'Buckets',users:'User',groups:'Placement Groups',settings:'Einstellungen'}[view];
 if(view==='groups')pgViews.load();
 if(view==='settings')node.getConfig().then(applyConfig).catch(e=>toast(e.message,true));if(view==='users')userViews.load();if(view==='buckets')bucketViews.load();
}
for(const button of document.querySelectorAll('[data-view]'))button.addEventListener('click',()=>showView(button.dataset.view));
function requireLogin(message){volumeGrowth.clear();node?.stop();$('app').hidden=true;$('login').hidden=false;$('password').value='';$('login-error').textContent=message;$('password').focus()}
const recoveryView=setupRecovery({connection:()=>node,config:()=>configView,states:nodeStates,toast,newID,refresh:target=>refreshConfig(target)});
const forceView=setupForceDisconnect({connection:()=>node,config:()=>configView,states:nodeStates,toast,newID,refresh:()=>refreshConfig()});
$('add-error').insertAdjacentHTML('beforebegin','<section id="volume-pg-selection" class="bucket-form-section"></section>');
const pgViews=setupPlacementGroups({connection:()=>node,escape,toast,states:nodeStates});
const bucketViews=setupBucketViews({onUsersChanged:()=>userViews.load(true),connection:()=>node,escape,toast,showView,statsEntries:()=>currentSnapshots(configView,nodeStates)});
const userViews=setupUserViews({connection:()=>node,currentUsername:()=>$('profile-name').textContent,escape,toast,showView,requireLogin,openBucketCreate:options=>bucketViews.openCreate(options),onUserDeleted:()=>bucketViews.load(),statsEntries:()=>currentSnapshots(configView,nodeStates,true)});

const joinView=setupClusterJoin({connection:()=>node,onJoined:beginSession});
function applyConfig(view){
 joinView.refresh();
 configView=view;const id=view.config.node.id,entry=nodeStates.get(id)??{};entry.config=view;nodeStates.set(id,entry);seedMembers(view,nodeStates);
 snapshot=entry.snapshot??snapshot;volumeNode??=id;
 $('config-location').textContent=view.config_path;$('restart-notice').hidden=!view.restart_required;
 if(!$('settings-fields').children.length)renderSettings();else syncSettingsValues();renderMetrics();renderVolumes();updatePath();
}
async function refreshConfig(target=localNode()){
 const connection=node;
 try{const view=await(target&&target!==localNode()?connection.forNode(target):connection).getConfig();
  if(node!==connection)return;
  if(!target||target===localNode())applyConfig(view);else{const entry=nodeStates.get(target)??{};entry.config=view;nodeStates.set(target,entry);renderVolumes()}
 }catch(e){if(node===connection)toast(e.message,true)}
}
let capacity=null,steps=[],selectedSizes={0:50n*1024n*1024n,2:50n*1024n*1024n};
function onStats(s){
 joinView.refresh();
 const entry=nodeStates.get(s.node)??{};
 if(entry.snapshot&&s.boot===entry.snapshot.boot&&s.sequence<=entry.snapshot.sequence)return;
 recordRateSnapshot(entry,s);nodeStates.set(s.node,entry);
 if(s.node===localNode()||!snapshot)snapshot=s;
 renderMetrics();renderVolumes();bucketViews.syncStats(s);bucketViews.refreshThroughput();
 const count=memberIDs(configView)?.size??nodeStates.size;
 $('snapshot-info').textContent=`${count} NODE${count===1?'':'S'} · GEN ${nodeStates.get(localNode())?.snapshot?.generation??s.generation} · ${new Date(Number(s.captured/1000000n)).toLocaleTimeString('de-DE')}`;
 if(!configView||fetchingConfigs.has(s.node))return;
 const ids=new Set(entry.config?.config.volumes.map(v=>v.id)??[]);
 if(s.node===localNode()&&configView.membership_epoch!==undefined&&configView.membership_epoch!==String(s.membership)||!entry.config||s.volumes.length!==ids.size||s.volumes.some(v=>!ids.has(v.id))){fetchingConfigs.add(s.node);refreshConfig(s.node).finally(()=>fetchingConfigs.delete(s.node))}
}
function totals(volumes){return volumes.reduce((a,v)=>({capacity:a.capacity+v.capacity,allocated:a.allocated+v.allocated,admission:a.admission+v.admission,reserved:a.reserved+v.operating+v.guaranteed+v.inFlightReserved}),{capacity:0n,allocated:0n,admission:0n,reserved:0n})}
function renderMetrics(){
 const entries=currentSnapshots(configView,nodeStates);const memberCount=memberIDs(configView)?.size??entries.length;
 const volumes=entries.flatMap(e=>e.snapshot.volumes),t=totals(volumes);
 for(const prefix of ['', 'bucket-'])renderCapacitySummary($(prefix+'capacity-summary'),t,capacityKnown(entries,memberCount),capacityAdmissionKnown(entries));
 $('metric-volumes').textContent=formatNumber(volumes.length);
 const active=volumes.filter(v=>v.state===1).length,draining=volumes.filter(v=>v.state===2).length;
 $('metric-active').textContent=`${active} aktiv${draining?` · ${draining} im Drain`:''}`;$('nav-count').textContent=formatNumber(volumes.length);
 for(const key of ['read','write']){
  let sum=0n,count=0;
  for(const {snapshot:s,previous:p,received}of entries){const seconds=p&&p.boot===s.boot?Number(s.captured-p.captured)/1e9:0;
   if(seconds>0&&s.io[key]>=p.io[key]&&Date.now()-received<10000){sum+=BigInt(Math.round(Number(s.io[key]-p.io[key])/seconds));count++}}
  $('metric-'+key).textContent=count?bytes(sum)+'/s':'—';
  const logical=bucketTotals(entries)[key==='read'?0:1];
  $('bucket-metric-'+key).textContent=logical==null?'—':bytes(logical)+'/s';
 }
 const count=memberCount===1?'1 Node':`${memberCount} Nodes`;$('node-count').textContent=count;$('topbar-node-count').textContent=count.toUpperCase();
 const mode=document.querySelector('.mode-card');mode.querySelector('strong').textContent=count;
 mode.querySelector('p').textContent='Speicher und Zustand im Überblick.';
}
function usage(v){const allocated=v.allocated??0n,capacity=v.capacity??0n,reserved=v.reserved??v.operating+v.guaranteed+v.inFlightReserved;const percent=n=>capacity>0n?Math.min(100,Number(n*10000n/capacity)/100):0;return `<div class="usage-label"><strong>${escape(bytes(allocated))}</strong><span>von ${escape(bytes(capacity))}</span></div><div class="usage-track"><i class="usage-used" data-percent="${percent(allocated)}"></i><i class="usage-reserved" data-percent="${percent(reserved)}"></i></div>`}
function renderVolumes(){
 const entries=[...nodeStates.entries()].filter(([,e])=>e.snapshot).sort(([a],[b])=>a===localNode()?-1:b===localNode()?1:a.localeCompare(b));
 if(!entries.length)return;
 const query=$('volume-search').value.trim().toLowerCase();let rows='',visibleCount=0;
 for(const [id,entry]of entries){
  const s=entry.snapshot,view=entry.config,cfg=view?.config,membership=nodeMembership(configView,id,s),member=membership==='member',byID=new Map((cfg?.volumes??[]).map(v=>[v.id,v])),t=totals(s.volumes),collapsed=entry.collapsed??false;
  const visible=s.volumes.filter(v=>{const c=byID.get(v.id);return `${cfg?.node.name??id} ${c?.name??''} ${v.path} ${classes[v.class]}`.toLowerCase().includes(query)});
  visibleCount+=visible.length;
  const version=configView?.node_versions?.[id],warning=versionWarning(version),stale=Date.now()-entry.received>=10000,label=versionLabel(version)||(membership==='excluded'?'Ausgeschlossen':membership==='departed'?'Ausgeschieden':entry.placeholder?'Noch keine Statistiken':stale?'Statistiken veraltet':s.reason&1024n?'Speicher wird vorbereitet':s.state===0&&s.needRebuild?(s.reason&256n?'Bucket-Bereinigung':s.reason&512n?'Clusteränderung':'Rebuild'):(['Online','Wird geleert','Wartung','Wird beendet','Beitritt','Ausgeschlossen'][s.state]??'Unbekannt'));
  rows+=`<tr class="node-row ${warning?'version-warning':''}" title="${escape(version?.reason??'')}" data-node="${id}"><td><div class="node-cell"><button class="node-toggle" data-toggle="${id}" aria-label="Volumes ${collapsed?'aufklappen':'einklappen'}" aria-expanded="${!collapsed}">${collapsed?'›':'⌄'}</button><span class="node-symbol">▤</span><div><div class="node-name">${escape(cfg?.node.name??id.slice(0,8))}${id===localNode()?'<span class="node-badge">DIESE NODE</span>':''}</div><div class="node-sub">${escape(id.slice(0,8))} · ${escape(view?.active_listen??'')} ${version?' · '+escape(protocolLabel(version))+' · '+escape(version.profile.version):''}</div></div></div></td><td><span class="class-badge">${s.volumes.length} VOLUMES</span></td><td>${usage(t)}</td><td class="free-value">${escape(bytes(t.admission))}</td><td>${throughputHTML(nodeRate(entry,'read'),nodeRate(entry,'write'))}</td><td><span class="state-badge ${stale?'pending':''}"><i></i>${label}</span></td><td><div class="row-actions"><button class="secondary" data-node-maintenance="${id}" ${!member?'disabled':''}>Wartung</button></div></td></tr>`;
  if(!collapsed)for(const v of visible){const c=byID.get(v.id),state=states[v.state]??'failed';rows+=`<tr data-volume="${v.id}" data-node="${id}"><td><div class="volume-cell"><span class="volume-symbol">▤</span><div><div class="volume-name">${escape(c?.name??v.path.split('/').pop())}${c?.system?'<span class="system-label">SYSTEM</span>':c?.recovery_only?'<span class="system-label" title="Für die übernommenen Datenbereiche reserviert">GERETTET</span>':''}</div><div class="volume-path" title="${escape(v.path)}">${escape(v.path)}</div></div></div></td><td><span class="class-badge ${classes[v.class]}">${classes[v.class].toUpperCase()}</span></td><td>${usage(v)}</td><td class="free-value">${escape(bytes(v.admission))}</td><td>${throughputHTML(entityRate(entry,'volumes',v.id,'read'),entityRate(entry,'volumes',v.id,'write'))}</td><td><span class="state-badge ${state}"><i></i>${stateNames[v.state]??'Unbekannt'}</span></td><td><div class="row-actions">${c?.placement_groups_pending?.length?`<button class="row-action" data-finish-pg="${v.id}" data-node="${id}">PG-Listen fortsetzen</button>`:''}<button class="row-action" data-detail="${v.id}" data-node="${id}" aria-label="Details für ${escape(c?.name??v.path)}">Details</button><button class="row-action" data-grow="${v.id}" data-node="${id}" ${!member||!cfg||v.state!==1?'disabled':''}>Vergrößern</button><button class="row-action" data-drain="${v.id}" data-node="${id}" ${!member||!cfg||c?.system||v.state!==1?'disabled':''} title="${c?.system?'Das Admin-Volume bleibt für Systemdaten eingebunden':'Kooperativ aushängen und entfernen'}">Aushängen ↗</button></div></td></tr>`}
  rows+=`<tr class="add-row"><td colspan="7"><button class="add-volume-button" data-add-node="${id}" ${!member||!cfg?'disabled':''}>＋ Volume hinzufügen</button><button class="row-action" data-recovery-volumes="${id}" ${!member||!cfg?'disabled':''}>Gerettete Volumes</button><span class="add-row-note">Datei, ungemountetes Blockdevice oder RAM</span></td></tr>`;
 }
 if($('volume-rows').dataset.rendered!==rows){$('volume-rows').innerHTML=rows;$('volume-rows').dataset.rendered=rows;}
 $('table-empty').hidden=visibleCount!==0||!query;
 refreshNodeMaintenance();
 for(const bar of document.querySelectorAll('[data-percent]'))bar.style.width=bar.dataset.percent+'%';
 for(const b of document.querySelectorAll('[data-node-maintenance]'))b.onclick=()=>openNodeMaintenance(b.dataset.nodeMaintenance);
 for(const b of document.querySelectorAll('[data-toggle]'))b.onclick=()=>{const e=nodeStates.get(b.dataset.toggle);e.collapsed=!e.collapsed;renderVolumes()};
 for(const b of document.querySelectorAll('[data-recovery-volumes]'))b.onclick=()=>recoveryView.openVolumes(b.dataset.recoveryVolumes);
 for(const b of document.querySelectorAll('[data-add-node]'))b.onclick=()=>openAdd(b.dataset.addNode);
 for(const b of document.querySelectorAll('[data-finish-pg]'))b.onclick=async()=>{b.disabled=true;const target=b.dataset.node,id=b.dataset.finishPg,groups=nodeStates.get(target)?.config?.config.volumes.find(v=>v.id===id)?.placement_groups_pending??[];try{await pgViews.finishVolume(target,id,groups);await refreshConfig(target);toast('Volume für Placement freigegeben.')}catch(e){toast(e.message,true)}finally{b.disabled=false}};
 for(const [attr,fn]of [['detail',openDetail],['grow',openGrow],['drain',openDrain]])for(const b of $('volume-rows').querySelectorAll(`[data-${attr}]`))b.onclick=()=>fn(b.dataset[attr],b.dataset.node);
}
$('volume-search').addEventListener('input',renderVolumes);$('refresh').onclick=async()=>{try{onStats(await node.getStats());toast('Statistik aktualisiert.')}catch(e){toast(e.message,true)}};
for(const b of document.querySelectorAll('[data-close]'))b.addEventListener('click',()=>$(b.dataset.close).close());
function openDetail(id,target=localNode()){volumeNode=target;const v=nodeStates.get(target)?.snapshot?.volumes.find(v=>v.id===id),c=volumeConfig()?.config.volumes.find(v=>v.id===id);if(!v)return;$('detail-title').textContent=c?.name??v.path;const details=[['Volume-ID',id,true],['Pfad / Device',v.path,true],['Klasse',classes[v.class].toUpperCase()],['Zustand',stateNames[v.state]],['Kapazität',bytes(v.capacity)],['Belegt',bytes(v.allocated)],['Metadaten',bytes(v.metadata)],['Betriebsreserve',bytes(v.operating)],['Für neue Daten nutzbar',bytes(v.admission)]];let html='<dl class="detail-grid">'+details.map(([k,value,mono])=>`<div><dt>${escape(k)}</dt><dd class="${mono?'mono':''}">${escape(value)}</dd></div>`).join('')+'</dl>';if(c?.system)html+='<div class="notice">Admin-Dateivolume: wächst bei Platzbedarf automatisch. Die Startgröße ist keine Obergrenze.</div>';if(v.class<2)html+='<button id="rescan-volume" class="secondary">↻ Größe neu einlesen</button><p class="field-note">Nach einer externen Vergrößerung, z. B. mit LVM, wird zusätzlicher Platz crashsicher ins Layout übernommen.</p>';$('detail-body').innerHTML=html;if($('rescan-volume'))$('rescan-volume').onclick=async e=>{e.currentTarget.disabled=true;try{await volumeConnection().rescanVolume(id);toast('Volume-Größe neu eingelesen.');$('detail-dialog').close();await refreshConfig(volumeNode)}catch(e){toast(e.message,true);$('rescan-volume').disabled=false}};$('detail-dialog').showModal()}
const volumeGrowth=setupVolumeGrowth({connection:target=>target&&target!==localNode()?node.forNode(target):node,describe:(id,target)=>{const view=nodeStates.get(target)?.config?.config??configView?.config;return {volume:view?.volumes.find(v=>v.id===id),node:view?.node.name??target}},toast,refresh:async target=>{await refreshConfig(target);onStats(await node.getStats())}});
function openGrow(id,target=localNode()){return volumeGrowth.open(id,target)}

function openDrain(id,target=localNode()){volumeNode=target;const c=volumeConfig()?.config.volumes.find(v=>v.id===id);drainVolume=id;drainID=newID();$('drain-description').textContent=`„${c?.name??id}“ wird erst freigegeben, wenn alle benötigten Objekte sicher auf geeigneten Zielvolumes liegen.`;$('drain-error').textContent='';$('drain-dialog').showModal()}
$('drain-form').addEventListener('submit',async e=>{e.preventDefault();e.submitter.disabled=true;try{await volumeConnection().removeVolume(drainVolume,drainID);$('drain-dialog').close();toast('Kooperatives Aushängen wurde gestartet.');await refreshConfig(volumeNode)}catch(e){$('drain-error').textContent=e.message}finally{e.submitter.disabled=false}});
async function loadCandidates(){
 $('refresh-devices').disabled=true;
 try{
  const [available,limits]=await Promise.all([volumeConnection().listVolumeCandidates(),volumeConnection().volumeCapacity()]);candidates=available??[];capacity=limits;
  const devices=candidates.filter(v=>v.type!=='file'),files=candidates.filter(v=>v.type==='file');
  const devicePath=$('device-path').value,filePath=$('existing-file').value;
  $('device-path').innerHTML=devices.length?'<option value="">Device auswählen …</option>'+devices.map(v=>`<option value="${escape(v.path)}">${escape(v.path)} · ${escape(bytes(BigInt(v.size_bytes)))} · ${escape(v.type.toUpperCase())}</option>`).join(''):'<option value="">Keine verfügbaren ungemounteten Devices</option>';
  $('existing-file').innerHTML='<option value="">Neue Datei anlegen</option>'+files.map(v=>`<option value="${escape(v.path)}">${escape(v.path.split('/').pop())} · ${escape(bytes(BigInt(v.size_bytes)))}</option>`).join('');
  if(devices.some(v=>v.path===devicePath))$('device-path').value=devicePath;
  if(files.some(v=>v.path===filePath))$('existing-file').value=filePath;
  updateSource();
 }catch(e){$('add-error').textContent=e.message;$('add-submit').disabled=true}finally{$('refresh-devices').disabled=false}
}
function openAdd(target=localNode()){
 if(addPending)return;
 volumeNode=target;$('add-node-name').textContent=volumeConfig()?.config.node.name??target;
 addRetry=null;capacity=null;steps=[];selectedSizes={0:50n*1024n*1024n,2:50n*1024n*1024n};
 $('add-error').textContent='';$('volume-name').value='';$('volume-name').readOnly=false;$('volume-class').value='hdd';$('existing-file').value='';$('device-path').value='';source=0;
 updateSource();$('add-dialog').showModal();loadCandidates();pgViews.volumeSelection(target).catch(e=>{$('add-error').textContent=e.message;$('add-submit').disabled=true});$('volume-name').focus();
}
function updatePath(){$('volume-path-preview').textContent=source===0?`${volumeConfig()?.active_volumes_dir??'volumes'}/${$('volume-name').value||'<name>'}`:source===2?'Prozessgebunden · flüchtiger Speicher':$('device-path').value}
function editableSize(){return source!==1&&!(source===0&&$('existing-file').value)}
function currentLimit(){return capacity?sizeLimit(source===2?capacity.ramLimit:capacity.fileAvailable):0n}
function updateSource(){
 for(const b of document.querySelectorAll('[data-source]'))b.classList.toggle('selected',Number(b.dataset.source)===source);
 $('device-section').hidden=source!==1;$('file-section').hidden=source!==0;
 const existing=source===0&&$('existing-file').value,editable=editableSize();
 $('size-field').hidden=false;$('size-slider-wrap').hidden=!editable;$('volume-size').required=editable;$('volume-size').readOnly=!editable;$('volume-name').readOnly=!!existing;
 const select=$('volume-class');for(const o of select.options)o.disabled=source===2?o.value!=='ram':o.value==='ram';if(source===2)select.value='ram';else if(select.value==='ram')select.value='hdd';
 $('add-note').textContent=source===2?'RAM-Inhalte gehen beim Prozessende verloren. Die Größenwahl richtet sich nach verfügbarem RAM und einem gegebenenfalls konfigurierten RAM-Budget.':source===1?'Die volle vorhandene Device-Größe wird genutzt. Die Größe ist nicht einstellbar; ein bestehendes STQR-Format kann nach Vergrößerung über Rescan erweitert werden.':'Wird direkt im Volume-Verzeichnis angelegt. Bestehende Dateien werden nicht automatisch vergrößert.';
 const input=$('volume-size');input.setCustomValidity('');
 if(!editable){const path=source===1?$('device-path').value:existing,device=candidates.find(v=>v.path===path);input.disabled=false;input.value=device?formatSize(BigInt(device.size_bytes)):'—';$('size-note').textContent='Volle vorhandene Größe · nicht einstellbar';$('add-submit').disabled=!path;}
 else{
  const limit=currentLimit();input.disabled=!capacity||limit<MIN_VOLUME_SIZE;
  $('add-submit').disabled=input.disabled;
  if(!capacity){input.value='';$('size-note').textContent='Freier Speicher wird ermittelt …';setSlider([],undefined);}
  else if(limit<MIN_VOLUME_SIZE){input.value='';$('size-note').textContent=source===2?'Kein ausreichender freier RAM bzw. kein RAM-Budget verfügbar.':'Auf dem Volume-Dateisystem sind weniger als 256K verfügbar.';setSlider([],undefined);}
  else{
   const value=selectedSizes[source]>limit?limit:selectedSizes[source];selectedSizes[source]=value;input.value=formatSize(value);setSlider(sizeSteps(limit,value),value);
   $('size-note').textContent=source===2?`Freier RAM: ${sizeLabel(capacity.ramAvailable)}${capacity.ramLimit<capacity.ramAvailable?' · Budgetgrenze: '+sizeLabel(capacity.ramLimit):''}. K/M/G/T/P = 1024er-Schritte.`:`Dateisystem verfügbar (df): ${sizeLabel(capacity.fileAvailable)}. K/M/G/T/P = 1024er-Schritte.`;
  }
 }
 updatePath();
}
function setSlider(values,value){
 steps=values;const slider=$('volume-slider');slider.min='0';slider.max=String(Math.max(0,steps.length-1));slider.step='1';slider.disabled=!steps.length;
 slider.value=String(value===undefined?0:steps.findIndex(n=>n===value));
 $('slider-min').textContent=steps.length?formatSize(steps[0]):'—';$('slider-value').textContent=value===undefined?'—':formatSize(value);$('slider-max').textContent=capacity?sizeLabel(currentLimit()):'—';
 slider.setAttribute('aria-valuetext',value===undefined?'Keine Kapazität verfügbar':formatSize(value));
 $('slider-max').title=capacity?`${currentLimit()} Bytes`:'';
}
function updateSlider(){
 if(!editableSize()||!capacity)return false;
 const input=$('volume-size');
 try{
  const value=parseSize(input.value);if(value>currentLimit())throw Error(`Maximal ${formatSize(currentLimit())} verfügbar.`);
  input.setCustomValidity('');selectedSizes[source]=value;setSlider(sizeSteps(currentLimit(),value),value);return true;
 }catch(e){input.setCustomValidity(e.message);return false}
}
for(const b of document.querySelectorAll('[data-source]'))b.onclick=()=>{source=Number(b.dataset.source);addRetry=null;updateSource()};
$('refresh-devices').onclick=loadCandidates;$('volume-name').addEventListener('input',updatePath);
$('device-path').addEventListener('change',()=>{const d=candidates.find(d=>d.path===$('device-path').value);if(d?.class_detected)$('volume-class').value=d.class;updateSource()});
$('existing-file').addEventListener('change',()=>{if($('existing-file').value)$('volume-name').value=$('existing-file').value.split('/').pop();updateSource()});
$('volume-size').addEventListener('input',updateSlider);
$('volume-size').addEventListener('change',()=>{if(updateSlider())$('volume-size').value=formatSize(selectedSizes[source])});
$('volume-slider').addEventListener('input',()=>{
 const value=steps[Number($('volume-slider').value)];if(value===undefined)return;
 selectedSizes[source]=value;$('volume-size').value=formatSize(value);$('volume-size').setCustomValidity('');$('slider-value').textContent=formatSize(value);$('volume-slider').setAttribute('aria-valuetext',formatSize(value));
});
$('add-dialog').addEventListener('cancel',e=>{if(addPending)e.preventDefault()});
$('add-form').addEventListener('submit',async e=>{
 e.preventDefault();$('add-error').textContent='';e.submitter.disabled=true;addPending=true;const target=volumeNode,requestConnection=volumeConnection();for(const b of document.querySelectorAll('[data-close="add-dialog"],[data-source]'))b.disabled=true;
 try{
  const existing=source===0&&$('existing-file').value,q={name:$('volume-name').value,source,path:source===1?$('device-path').value:existing||'',class:$('volume-class').value,size:source===1||existing?0n:parseSize($('volume-size').value),placementGroups:[...$('volume-pg-selection').querySelectorAll('[data-volume-pg]:checked')].map(e=>e.dataset.volumePg)};
  if(source===1&&!q.path)throw Error('Bitte ein verfügbares Device auswählen.');
  const signature=JSON.stringify(q,(_,v)=>typeof v==='bigint'?String(v):v);if(!addRetry?.created&&addRetry?.signature!==signature)addRetry={signature,id:newID(),request:q};
  const request=addRetry.request??q;
  if(!addRetry.created){const result=await requestConnection.addVolume(request,addRetry.id);addRetry.created=result.volume_id}
  if(request.placementGroups.length)await pgViews.finishVolume(target,addRetry.created,request.placementGroups);
  $('add-dialog').close();toast(`Volume „${request.name}“ wurde hinzugefügt.`);await refreshConfig(target);
 }catch(err){$('add-error').textContent=addRetry?.created?'Volume ist angelegt und wartet auf die PG-Listen. Erneut speichern setzt den Vorgang fort: '+err.message:err.message;await refreshConfig(target)}
 finally{addPending=false;for(const b of document.querySelectorAll('[data-close="add-dialog"],[data-source]'))b.disabled=false;e.submitter.disabled=editableSize()&&(!capacity||currentLimit()<MIN_VOLUME_SIZE)}
});
const settingSections=[{title:'Node',note:'Name und veröffentlichte Adresse gelten sofort. Bind-Adresse und Verzeichnisse werden beim nächsten Start aktiv.',fields:[['name','Anzeigename','Ein verständlicher Name für diese Node.','text'],['listen','HTTP-Bind-Adresse','HTTP und WebSocket auf derselben Adresse. Neustart erforderlich.','text'],['advertise','Veröffentlichte Adresse','Optionales http://host:port für zukünftige Peer-Kontakte.','text'],['data_dir','Datenverzeichnis','Relative Pfade beziehen sich auf den Config-Ordner. Neustart erforderlich.','text'],['volumes_dir','Volume-Verzeichnis','Neue Dateivolumes liegen direkt in diesem Ordner. Neustart erforderlich.','text']]},{title:'Kapazität & System-Volume',note:'Alle Werte sind Bytes. Das Admin-Dateivolume wächst automatisch; die Startgröße ist kein festes Limit.',fields:[['ram_bytes','RAM-Budget','Optionales RAM-Budget; 0 nutzt den freien System-RAM. Neustart erforderlich.','number'],['ramcache_bytes','Read-Cache-Budget','Für die spätere Cache-Implementierung reserviert. Neustart erforderlich.','number'],['system_volume_bytes','Admin-Volume Startgröße','Nur bei Neuanlage; vorhandene Admin-Volumes wachsen nach Bedarf.','number']]},{title:'Netzwerkzugriff',note:'Die echte IPv4-/IPv6-Adresse der Verbindung zählt. Forwarded-Header werden nicht vertraut.',fields:[['allow_subnets','Erlaubte Subnetze','Ein CIDR pro Zeile. Leer erlaubt alle Adressen. Änderungen gelten sofort, auch für diese Verbindung.','subnets']]},{title:'Identität & Bootstrap',note:'Die Identitäten sind unveränderlich. Das initiale Passwort steht in der geschützten YAML-Datei; Accounts mit Passwort-Hashes liegen im Admin-Volume.',fields:[]}];
function renderSettings(){if(!configView)return;const c=configView.config,nodeConfig=c.node;let html='';for(const section of settingSections){html+=`<section class="settings-section"><h2>${section.title}</h2><p>${section.note}</p>`;for(const [key,label,note,type]of section.fields){const value=key in nodeConfig?nodeConfig[key]:c[key];html+=`<form class="setting-row" data-setting="${key}" data-type="${type}"><div class="setting-label"><label for="setting-${key}">${label}</label><p>${note}</p></div>${type==='subnets'?`<textarea id="setting-${key}" spellcheck="false">${escape(value.join('\n'))}</textarea>`:`<input id="setting-${key}" type="${type}" value="${escape(value??'')}" ${type==='number'?'min="0" step="1" required':''}>`}<button class="secondary" type="submit">Speichern</button></form>`}if(!section.fields.length){for(const [label,value]of [['Node-ID',nodeConfig.id],['Cluster-ID',c.cluster_id],['Bootstrap-Admin',c.auth.bootstrap_admin.username],['System-Bucket-Platzierung','EVERYWHERE · Multinode-Replikation folgt'],['Peers',c.peers?.length?c.peers.map(p=>p.url).join(', '):'Keine · Single-node']])html+=`<div class="setting-row"><div class="setting-label"><label>${escape(label)}</label></div><div class="setting-readonly">${escape(value)}</div></div>`}html+='</section>'}$('settings-fields').innerHTML=html;for(const form of document.querySelectorAll('[data-setting]')){form.addEventListener('input',()=>{form.dataset.dirty='true'});form.addEventListener('submit',async e=>{e.preventDefault();const key=form.dataset.setting,input=form.querySelector('input,textarea'),type=form.dataset.type;let value=input.value;if(type==='number'){value=Number(value);if(!Number.isSafeInteger(value)||value<0){toast('Bitte eine nichtnegative ganze Bytezahl eingeben.',true);return}}else if(type==='subnets')value=value.split(/[\n,]+/).map(v=>v.trim()).filter(Boolean);const b=form.querySelector('button');b.disabled=true;try{const view=await node.setConfig({[key]:value});form.dataset.dirty='false';b.disabled=false;applyConfig(view);toast('Einstellung gespeichert.' )}catch(e){toast(e.message,true);b.disabled=false}})}}
function syncSettingsValues(){const c=configView.config;for(const form of document.querySelectorAll('[data-setting]')){if(form.dataset.dirty==='true')continue;const key=form.dataset.setting,value=key in c.node?c.node[key]:c[key];form.querySelector('input,textarea').value=form.dataset.type==='subnets'?value.join('\n'):value??''}}
(async()=>{try{const response=await sessionFetch();if(response.ok)beginSession(await response.json())}catch{}})();

let maintenanceTarget;
function refreshNodeMaintenance(){
 if(!$('node-maintenance-dialog').open)return;
 const target=maintenanceTarget,entry=nodeStates.get(target),s=entry?.snapshot;
 const member=nodeMembership(configView,target,s)==='member',online=member&&s&&Date.now()-entry.received<10000&&s.state===0;
 const multiple=(memberIDs(configView)?.size??0)>1;
 $('node-maintenance-description').textContent=entry?.config?.config.node.name??target.slice(0,8);
 for(const [kind,allowed]of [['short',online],['drain',online],['replace',member&&multiple],['force',member&&multiple]]){
  const button=$('maintenance-'+kind);button.disabled=!allowed;
  const reason=!member?'Diese Node ist kein aktives Clustermitglied.':kind==='short'||kind==='drain'?online?'':'Die Node muss online und im normalen Betriebszustand sein.':multiple?'':'Mindestens eine weitere Node muss im Cluster verbleiben.';
  const note=$('maintenance-'+kind+'-note');note.textContent=reason;note.hidden=!reason;
  button.dataset['node'+kind[0].toUpperCase()+kind.slice(1)]=target;
 }
}
function openNodeMaintenance(target){
 maintenanceTarget=target;$('node-maintenance-dialog').showModal();refreshNodeMaintenance();
}
for(const [kind,action]of [['short',target=>openNodeLifecycle(target,false)],['drain',target=>openNodeLifecycle(target,true)],['replace',target=>recoveryView.openNode(target)],['force',target=>forceView.open(target)]]){
 $('maintenance-'+kind).onclick=()=>{const target=maintenanceTarget;$('node-maintenance-dialog').close();action(target)};
}

let nodeLifecycle;
function openNodeLifecycle(target,drain,cluster=false){
 nodeLifecycle={target,drain,cluster,id:newID()};
 const name=nodeStates.get(target)?.config?.config.node.name??target.slice(0,8);
 $('node-action-title').textContent=drain?'Node leeren':'Node für Neustart herunterfahren';
 $('node-action-description').textContent=drain?`„${name}“ übergibt seine Daten an andere Nodes und scheidet danach aus dem Cluster aus.`:`„${name}“ behält seine persistenten Daten und Zuständigkeiten. Lokale RAM-Inhalte gehen beim Beenden verloren. Betroffene Anfragen warten, bis die Node wieder gestartet wurde.`;
 $('node-action-submit').textContent=drain?'Leeren & herunterfahren':'Herunterfahren';
 $('node-action-error').textContent='';
 if(cluster){
  $('node-action-title').textContent='Cluster herunterfahren';
  $('node-action-description').textContent='Alle Nodes gemeinsam beenden. Persistente Daten bleiben erhalten; RAM-Inhalte gehen bei einem vollständigen Neustart verloren.';
  $('node-action-submit').textContent='Cluster herunterfahren';
 }
 $('node-action-dialog').showModal();
}
$('node-action-form').addEventListener('submit',async e=>{
 e.preventDefault();e.submitter.disabled=true;
 const action=nodeLifecycle;
 try{
  const connection=action.target===localNode()?node:node.forNode(action.target);
  if(action.cluster)await connection.clusterShutdown(action.id);else if(action.drain)await connection.drainNode(action.target,action.id);else await connection.shortShutdown(action.target,action.id);
  $('node-action-dialog').close();toast(action.cluster?'Cluster-Shutdown wurde angefordert.':action.drain?'Node wird kooperativ geleert.':'Wartungs-Shutdown wurde angefordert.');
 }catch(error){$('node-action-error').textContent=error.message}finally{e.submitter.disabled=false}
});
$('cluster-shutdown').addEventListener('click',()=>{if(localNode())openNodeLifecycle(localNode(),false,true)});

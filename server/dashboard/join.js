// SPDX-License-Identifier: GPL-3.0-only
import {Writer,Reader,envelope} from './protocol.js';
const STATUS=0x203b,PREPARE=0x203d,$=id=>document.getElementById(id);
export function setupClusterJoin({connection,onJoined}){
 let busy=false,checking=false,joining=false,timer;
 async function request(op,body=new Uint8Array()){
  const packet=envelope(op,body),response=await fetch('/api/message',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/octet-stream','X-CSRF-Token':connection().csrf},body:packet.bytes});
  if(!response.ok)throw Error('Beitritt konnte nicht vorbereitet werden ('+response.status+').');
  const r=new Reader(new Uint8Array(await response.arrayBuffer()));
  if(r.u8()!==1||r.u8()!==2||r.u16()!==op||r.id()!==packet.id)throw Error('Ungültige Antwort der Node.');
  const status=Number(r.v()),h=new Reader(r.blob());let detail='';for(let i=0,n=h.count(100);i<n;i++){const tag=Number(h.v()),value=new Reader(h.blob());if(tag===3)detail=value.text()}h.done();
  if(status>=300)throw Error(detail||'Beitritt nicht möglich ('+status+').');
  return r;
 }
 async function refresh(){
  if(checking||busy||joining||!connection()||$('app').hidden)return;
  checking=true;
  try{const r=await request(STATUS);const eligible=r.bool();r.text();r.done();$('join-cluster-card').hidden=!eligible}catch{$('join-cluster-card').hidden=true}finally{checking=false}
 }
 $('join-dialog').addEventListener('cancel',event=>{if(busy||joining)event.preventDefault()});
 $('join-cluster').onclick=()=>{$('join-error').textContent='';$('join-dialog').showModal();$('join-uri').focus()};
 $('join-form').onsubmit=async event=>{
  event.preventDefault();if(busy)return;busy=true;$('join-fields').disabled=true;$('join-error').textContent='';
  const username=$('join-username').value,password=$('join-password').value;
  try{
   const r=await request(PREPARE,new Writer().text($('join-uri').value.trim()).text(username).text(password).bytes());r.text();r.done();
   joining=true;$('join-password').value='';$('join-progress').textContent='Beitritt läuft. Die Node wartet auf die Aufnahme durch den Cluster …';
   connection().stop();$('join-cancel').hidden=true;
   // Keep credentials only in this closure until target accounts are installed.
   // No localStorage, URL, saved config or automatic use of the old admin password.
   const encoded=new TextEncoder().encode(username+':'+password),auth='Basic '+btoa(Array.from(encoded,b=>String.fromCharCode(b)).join(''));
   const poll=async()=>{
    try{const response=await fetch('/api/session',{method:'POST',credentials:'same-origin',headers:{Authorization:auth}});if(response.ok){const session=await response.json();$('join-dialog').close();joining=false;busy=false;$('join-fields').disabled=false;$('join-cancel').hidden=false;$('join-progress').textContent='';$('join-form').reset();onJoined(session);return}}catch{}
    timer=setTimeout(poll,1500);
   };timer=setTimeout(poll,300);
  }catch(error){$('join-error').textContent=error.message;busy=false;$('join-fields').disabled=false;$('join-password').value='';await refresh()}
 };
 return {refresh,stop(){clearTimeout(timer);joining=false;busy=false}};
}

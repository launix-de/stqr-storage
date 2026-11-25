// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE,headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({viewport:{width:1440,height:1100}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="buckets"]').click();
 await page.locator('#bucket-rows tr').filter({hasText:'browser-disks'}).locator('[data-bucket-action="disks"]').click();
 const list=page.locator('#block-devices-dialog'),grow=page.locator('#block-device-grow-dialog');
 const system=list.locator('[data-device]').filter({hasText:'system'});
 await system.waitFor().catch(async e=>{throw Error(`${e.message}\nDashboard: ${await list.locator('[data-error]').textContent()} / ${errors.join('; ')}`)});await list.getByText('data <archive>',{exact:true}).waitFor();
 if(!await list.locator('[data-exporter]').textContent().then(s=>s.includes(config.offline?'Kein Exporter':'Exporter verbunden')))throw Error('Exporter state incorrect');
 if(config.readonly){
  if(!await system.locator('[data-grow]').isDisabled())throw Error('Readonly resize enabled');
  if(!await list.locator('[data-exporter]').textContent().then(s=>s.includes('Nur lesen')))throw Error('Readonly state missing');
 }else{
  await system.locator('[data-grow]').click();
  const size=grow.locator('[data-size]'),submit=grow.locator('[data-submit]');
  await size.fill('1M');if(!await submit.isDisabled())throw Error('Shrinking enabled');
  await grow.locator('[data-validation]').filter({hasText:'größer'}).waitFor();
  await size.fill('1049089');if(!await submit.isDisabled())throw Error('Unaligned size enabled');
  await grow.locator('[data-validation]').filter({hasText:'512'}).waitFor();
  await grow.locator('[data-double]').click();if(await submit.isDisabled())throw Error('Quick growth disabled');
  await grow.locator('[data-slider]').fill('25');await grow.locator('[data-slider]').dispatchEvent('input');
  await size.fill('4MiB');await grow.locator('[data-target]').filter({hasText:'4 MiB'}).waitFor();
  if(process.env.STQR_BLOCK_UI_SCREENSHOT)await page.screenshot({path:process.env.STQR_BLOCK_UI_SCREENSHOT});
  await submit.click();await grow.waitFor({state:'hidden'});await system.getByText('4 MiB',{exact:true}).waitFor();
  await list.locator('[data-search]').fill('nothing-matches');await list.getByText('Keine passenden Disks.',{exact:true}).waitFor();
  await list.locator('[data-search]').fill('system');await system.waitFor();
  await system.locator('[data-grow]').click();await size.fill('9007199254741504');await submit.click();await grow.waitFor({state:'hidden'});
  await system.getByText('Sparse · 9.007.199.254.741.504 Bytes',{exact:true}).waitFor();
  // The modal stays within a narrow viewport; its table can scroll horizontally.
  await page.setViewportSize({width:390,height:844});
  if(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth+1))throw Error('Mobile page overflows');
 }
 await list.locator('[data-block-close]').first().click();
 if(errors.length)throw Error(errors.join('\n'));
 // Deterministic pending-state fixture: the grow response is released by the
 // test, and a mutation observer checks the freshly rendered refresh rows
 // before the submit handler's finally continuation can run.
 const fixture=await browser.newPage();
 const fixtureURL=new URL('/resize-action-fixture',config.url).href;
 await fixture.route(fixtureURL,route=>route.fulfill({contentType:'text/html',body:'<!doctype html><html><body></body></html>'}));
 await fixture.goto(fixtureURL);
 await fixture.evaluate(async()=>{
  const {setupBlockDevices}=await import('/assets/block-devices.js');
  const device={id:'12'.repeat(16),name:'fixture disk',size:1048576n,manufacturer:'STQR',model:'Sparse',serial:'1'};
  let release;
  const wire={listBlockDevices:async()=>({devices:[{...device}],next:'',total:1,live:false,readonly:false}),growBlockDevice:()=>new Promise(resolve=>{release=resolve})};
  const ui=setupBlockDevices({connection:()=>wire,escape:s=>String(s),toast:()=>{}});
  await ui.open({id:'34'.repeat(16),name:'fixture bucket'});
  document.querySelector('[data-grow]').click();
  document.querySelector('#block-device-grow-dialog form').requestSubmit();
  if(!document.querySelector('[data-grow]').disabled)throw Error('Resize action is enabled while pending');
  window.resizeRowChecked=new Promise((resolve,reject)=>{
   const observer=new MutationObserver(()=>{
    const button=document.querySelector('[data-grow]');
    if(!button||!document.querySelector('[data-disks]').textContent.includes('2 MiB'))return;
    observer.disconnect();
    if(!button.disabled)reject(Error('Refreshed action enabled before completion'));else resolve();
   });
   observer.observe(document.querySelector('[data-disks]'),{childList:true});
  });
  device.size=2097152n;release({devices:[{...device}]});
 });
 await fixture.evaluate(()=>window.resizeRowChecked);
 await fixture.locator('[data-grow]').click();
 await fixture.locator('#block-device-grow-dialog').waitFor({state:'visible'});
 await fixture.close();
 console.log(config.readonly?'Readonly live disks list and disabled resize passed.':'Live cross-node UI resize, presets, validation, search, mobile bounds and exact sizes above 2^53 passed.');
}finally{await browser.close()}

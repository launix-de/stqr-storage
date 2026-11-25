// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE,headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({viewport:{width:1440,height:1050}}),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();
 const dialog=page.locator('#grow-dialog'),size=page.locator('#grow-size'),submit=page.locator('#grow-submit');
 const row=id=>page.locator(`[data-volume="${id}"][data-node="${cfg.node}"]`);
 await row(cfg.file).locator('[data-grow]').click();await size.waitFor();await page.waitForFunction(()=>!document.querySelector('#grow-size').disabled);
 await page.locator('#grow-source').filter({hasText:'Dateivolume'}).waitFor();
 await size.fill('2M');if(!await submit.isDisabled())throw Error('Shrinking enabled');
 await size.fill('4194305');if(!await submit.isDisabled())throw Error('Unaligned growth enabled');
 await page.locator('#grow-validation').filter({hasText:'4096'}).waitFor();
 await page.locator('#grow-double').click();await page.locator('#grow-target').filter({hasText:'8 MiB'}).waitFor();
 await page.locator('#grow-slider').fill('1');await page.locator('#grow-slider').dispatchEvent('input');
 await size.fill('8MiB');await page.locator('#grow-added').filter({hasText:'4 MiB'}).waitFor();
 if(process.env.STQR_GROW_UI_SCREENSHOT)await page.screenshot({path:process.env.STQR_GROW_UI_SCREENSHOT});
 await submit.click();await dialog.waitFor({state:'hidden'});await row(cfg.file).getByText('8 MiB',{exact:false}).first().waitFor();
 await row(cfg.ram).locator('[data-grow]').click();await page.waitForFunction(()=>!document.querySelector('#grow-size').disabled);await page.locator('#grow-source').filter({hasText:'RAM-Volume'}).waitFor();
 await size.fill('4M');await submit.click();await dialog.waitFor({state:'hidden'});
 await row(cfg.file).locator('[data-grow]').click();await page.waitForFunction(()=>!document.querySelector('#grow-size').disabled);
 await page.locator('#grow-refresh').click();await page.waitForFunction(()=>!document.querySelector('#grow-size').disabled);
 if(await size.inputValue()!=='8M')throw Error('Reloaded capacity stale');
 await page.setViewportSize({width:390,height:844});if(await dialog.evaluate(el=>el.getBoundingClientRect().width>innerWidth))throw Error('Growth dialog overflows mobile viewport');
 await dialog.locator('[data-grow-close]').first().click();
 await page.locator('[data-view="buckets"]').click();await page.locator('#bucket-rows tr').filter({hasText:'storage-growth-data'}).locator('[data-bucket-action="clone"]').waitFor();
 // Exercise the physical-device UI without requiring a privileged loop device.
 await page.evaluate(async()=>{
  const {setupVolumeGrowth}=await import('/assets/volume-growth.js');window.rawCalls=[];
  window.rawGrowth=setupVolumeGrowth({connection:target=>({volumeGrowInfo:async()=>({kind:1,current:8388608n,available:8388608n,maximum:16777216n}),growVolume:async(id,size)=>{window.rawCalls.push({target,id,size:String(size)});return new Promise(resolve=>window.finishRaw=()=>resolve({state:'fulfilled'}))}}),describe:()=>({node:'Storage node',volume:{name:'Physical storage',path:'/dev/storage-test'}}),toast:()=>{},refresh:async target=>{window.refreshedRaw=target}});
  await window.rawGrowth.open('physical-volume','owner-node');
 });
 if(!await size.evaluate(el=>el.readOnly)||await page.locator('#grow-slider-wrap').isVisible())throw Error('Physical device permits arbitrary resizing');
 await submit.click();await page.keyboard.press('Escape');if(!await dialog.isVisible())throw Error('Pending growth can be dismissed');
 await page.evaluate(()=>{window.rawGrowth.open('wrong-volume','wrong-node');window.finishRaw()});await dialog.waitFor({state:'hidden'});
 const raw=await page.evaluate(()=>({calls:window.rawCalls,refreshed:window.refreshedRaw}));
 if(raw.calls.length!==1||raw.calls[0].id!=='physical-volume'||raw.calls[0].target!=='owner-node'||raw.calls[0].size!=='0'||raw.refreshed!=='owner-node')throw Error('Pending physical resize switched node/volume or sent a partial device size');
 if(errors.length)throw Error(errors.join('\n'));
 console.log('Real remote storage-file and RAM growth, bounds, slider, presets, capacity refresh, mobile dialog and bucket administration passed.');
}finally{await browser.close()}

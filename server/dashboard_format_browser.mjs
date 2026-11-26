// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="buckets"]').click();
 if(await page.locator('[data-bucket-action="disks"]').count())throw Error('Disk management is still exposed');
 const removed=await page.request.get(new URL('/assets/block-devices.js',cfg.url).href);if(removed.status()!==404)throw Error('Removed disk asset is still served');
 for(const [name,id]of Object.entries(cfg.ids)){
  const cell=page.locator(`[data-bucket="${id}"] .bucket-format`);await cell.waitFor();
  if(await cell.textContent()!==(name==='format-fresh'?'-':name.slice(7)))throw Error('Wrong bucket format: '+name);
 }
 const format='<img src=x onerror="window.formatInjected=true">',id=cfg.ids['format-fresh'];
 // Exercise explicit format binding on an administrative object socket.
 await page.evaluate(async({id,format})=>{
  const {Writer,envelope}=await import('/assets/protocol.js');
  const session=await(await fetch('/api/session')).json();
  await new Promise((resolve,reject)=>{
   const ws=new WebSocket(`ws://${location.host}/api/ws`,['stqr.v2',`csrf.${session.csrf_token}`]);ws.binaryType='arraybuffer';
   ws.onopen=()=>ws.send(envelope(0x120,new Writer().id(id).text(format).bytes()).bytes);
   ws.onmessage=e=>{const b=new Uint8Array(e.data);if(b[2]===0x20&&b[3]===1){ws.close();resolve()}};ws.onerror=reject;
  });
 },{id,format});
 await page.locator('#refresh-buckets').click();
 await page.waitForFunction(({id,format})=>document.querySelector(`[data-bucket="${id}"] .bucket-format`)?.textContent===format,{id,format});
 if(await page.locator(`[data-bucket="${id}"] .bucket-format img`).count()||await page.evaluate(()=>!!window.formatInjected))throw Error('Unescaped format');
 if(errors.length)throw Error(errors.join('\n'));console.log('All declared formats, fresh dash, live first-open refresh and HTML escaping passed.');
}finally{await browser.close()}

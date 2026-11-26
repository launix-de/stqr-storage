// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const context=await browser.newContext();await context.grantPermissions(['clipboard-read','clipboard-write'],{origin:cfg.url});
 const page=await context.newPage();await page.goto(cfg.url);
 await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();await page.locator('[data-view="buckets"]').click();
 const id=cfg.ids['copy-my-id'],button=page.locator(`[data-copy-bucket="${id}"]`);await button.waitFor();
 await button.click();if(await page.evaluate(()=>navigator.clipboard.readText())!==id)throw Error('Clipboard API copied wrong ID');
 if(await page.locator('#bucket-create-dialog').evaluate(e=>e.open))throw Error('Copy opened settings');
 // Ordinary intranet HTTP has no Clipboard API. Exercise the actual fallback.
 await page.evaluate(async()=>{await navigator.clipboard.writeText('fallback-control');window.readClipboard=navigator.clipboard.readText.bind(navigator.clipboard);if(await window.readClipboard()!=='fallback-control')throw Error('Clipboard control failed');Object.defineProperty(navigator,'clipboard',{configurable:true,value:undefined})});
 await button.click();if(await page.evaluate(()=>window.readClipboard())!==id)throw Error('HTTP fallback copied wrong ID');
 const code=page.locator(`[data-bucket="${id}"] .bucket-id code`);if(await code.textContent()!==id || await code.evaluate(e=>getComputedStyle(e).userSelect)!=='text')throw Error('ID is not selectable');
 await code.click();if(await page.locator('#bucket-create-dialog').evaluate(e=>e.open))throw Error('Selecting ID opened settings');
 await page.evaluate(()=>{document.execCommand=()=>false});await button.click();await page.getByText('Kopieren nicht möglich.',{exact:false}).waitFor();
 console.log('Bucket ID clipboard API, plain-HTTP fallback, selection and failure feedback passed');
}finally{await browser.close()}

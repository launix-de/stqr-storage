// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const cfg=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage({bypassCSP:true,viewport:{width:1280,height:800}}),ops=[],errors=[];
 page.on('pageerror',e=>errors.push(e.message));page.on('websocket',ws=>ws.on('framesent',({payload})=>{const b=Buffer.from(payload);if(b.length>=4)ops.push(b.readUInt16LE(2))}));
 await page.goto(cfg.url);await page.locator('#username').fill(cfg.username);await page.locator('#password').fill(cfg.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="users"]').click();await page.waitForFunction(()=>document.querySelectorAll('#user-rows [data-user]').length===50);
 await page.evaluate(()=>window.firstUserRow=document.querySelector('#user-rows [data-user]'));
 await page.locator('#users-view .endless-status').scrollIntoViewIfNeeded();await page.waitForFunction(()=>document.querySelectorAll('#user-rows [data-user]').length>=100);
 if(!await page.evaluate(()=>window.firstUserRow===document.querySelector('#user-rows [data-user]')))throw Error('Appending users rebuilt existing rows');
 if(await page.locator('#user-rows [data-user]').count()>=221)throw Error('User list eagerly drained every API page');
 await page.locator('#user-search').fill('tenant-user-219');await page.waitForFunction(()=>document.querySelectorAll('#user-rows [data-user]').length===1&&document.querySelector('#user-rows').textContent.includes('tenant-user-219'));
 await page.locator('[data-view="buckets"]').click();await page.waitForFunction(()=>document.querySelectorAll('#bucket-rows [data-bucket]').length===50);
 await page.evaluate(()=>window.firstBucketRow=document.querySelector('#bucket-rows [data-bucket]'));
 await page.locator('#buckets-view .endless-status').scrollIntoViewIfNeeded();await page.waitForFunction(()=>document.querySelectorAll('#bucket-rows [data-bucket]').length>=100);
 if(!await page.evaluate(()=>window.firstBucketRow===document.querySelector('#bucket-rows [data-bucket]')))throw Error('Appending buckets rebuilt existing rows');
 if(await page.locator('#bucket-rows [data-bucket]').count()>=221)throw Error('Bucket list eagerly drained every API page');
 await page.locator('#bucket-search').fill('tenant-bucket-219');await page.waitForFunction(()=>document.querySelectorAll('#bucket-rows [data-bucket]').length===1&&document.querySelector('#bucket-rows').textContent.includes('tenant-bucket-219'));
 if(await page.locator('#nav-users-count').textContent()!=='221'||await page.locator('#nav-buckets-count').textContent()!=='221')throw Error('Sidebar counts show only the loaded page');
 if(ops.includes(0x2021)||ops.includes(0x2022))throw Error('Dashboard still requests an unbounded legacy list');
 if(errors.length)throw Error(errors.join('\n'));console.log('Cursor pages, append-only endless scroll, server search and total counts passed for users and buckets.');
}finally{await browser.close()}

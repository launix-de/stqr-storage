// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);
 await page.locator('#login-form button[type=submit]').click();
 const row=page.locator(`tr.node-row[data-node="${config.target}"]`);
 await row.waitFor();
 if(!await row.textContent().then(s=>s.includes('Noch keine Statistiken')))throw Error('Offline committed member must remain visible.');
 await row.locator('[data-node-force]').click();
 if(await page.locator('#force-submit').isEnabled())throw Error('Missing fencing and loss confirmations.');
 await page.locator('#force-fenced').check();
 if(await page.locator('#force-submit').isEnabled())throw Error('Missing loss confirmation.');
 await page.locator('#force-loss').check();await page.locator('#force-submit').click();
 await page.waitForFunction(()=>!document.querySelector('#force-dialog').open);
 await page.waitForFunction(()=>document.querySelector('#node-count').textContent==='1 Node');
 await row.getByText('Ausgeschlossen',{exact:true}).waitFor();
 if(await row.locator('[data-node-force]').count())throw Error('Excluded member is still offered for exclusion.');
 if(errors.length)throw Error(errors.join('; '));
 console.log('Offline committed row, native exclusion, both confirmations and refreshed membership verified in browser.');
}finally{await browser.close()}

// SPDX-License-Identifier: GPL-3.0-only
import {createRequire} from 'node:module';
const require=createRequire(import.meta.url),{chromium}=require(process.env.STQR_TEST_BROWSER_MODULE);
let input='';for await(const chunk of process.stdin)input+=chunk;
const config=JSON.parse(input),browser=await chromium.launch({executablePath:process.env.STQR_TEST_BROWSER_EXECUTABLE??'/usr/bin/google-chrome',headless:true,args:['--no-sandbox']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));
 await page.goto(config.url);await page.locator('#username').fill(config.username);await page.locator('#password').fill(config.password);await page.locator('#login-form button[type=submit]').click();
 await page.locator('[data-view="buckets"]').click();await page.locator('#add-bucket').click();await page.locator('#bucket-name').fill('Physical quota browser');
 await page.locator('#bucket-accounting').selectOption('physical');await page.locator('#bucket-unlimited').uncheck();await page.locator('#bucket-quota').fill('4M');await page.locator('#bucket-guaranteed').fill('2M');
 if(await page.locator('#bucket-guaranteed').isDisabled())throw Error('Guaranteed quota is disabled');
 await page.locator('#bucket-create-submit').click();await page.locator('#bucket-create-dialog').waitFor({state:'hidden'});
 const row=page.locator('#bucket-rows tr').filter({hasText:'Physical quota browser'});await row.waitFor();await row.locator('[data-bucket-edit]').click();
 await page.waitForFunction(()=>document.querySelector('#bucket-guaranteed').value==='2M');if(await page.locator('#bucket-accounting').inputValue()!=='physical')throw Error('Physical accounting not retained');
 await page.locator('#bucket-guaranteed').fill('1M');await page.locator('#bucket-quota').fill('3M');await page.locator('#bucket-create-submit').click();await page.locator('#bucket-create-dialog').waitFor({state:'hidden'});
 await row.locator('[data-bucket-edit]').click();await page.waitForFunction(()=>document.querySelector('#bucket-guaranteed').value==='1M');
 if(await page.locator('#bucket-accounting').inputValue()!=='physical')throw Error('Edit reset accounting');
 if(errors.length)throw Error(errors.join('\n'));console.log('Native dashboard create/edit preserves physical absolute quotas and protected guarantees.');
}finally{await browser.close()}
